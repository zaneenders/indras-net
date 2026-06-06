import Foundation
import Logging
import NIOCore

// MARK: Raft
// This might be able to be a protocl for someone to implment that Shell can run/drive
extension Shell {

  private func startElectionTimer() async {
    resetElectionTimeout()
    while !Task.isCancelled {
      if instance.role == .leader {
        let heartbeat = AppendEntries.Args(term: instance.currentTerm, leaderId: instance.id)
        for peer in instance.peers {
          sendAppendEntries(to: peer, args: heartbeat)
        }
        try? await Task.sleep(for: .milliseconds(500))
        continue
      }

      guard let nextElectionTimeout else { return }

      let timeout = ContinuousClock.now.duration(to: nextElectionTimeout)
      if timeout > .zero {
        try? await Task.sleep(for: timeout)
      }

      guard instance.role == .follower else {
        resetElectionTimeout()
        continue
      }

      for action in instance.onElectionTimeOut() {
        switch action {
        case .requstVote(let peer, let args):
          sendRequestVote(to: peer, args: args)
        }
      }
      resetElectionTimeout()
    }
  }

  private func resetElectionTimeout() {
    self.nextElectionTimeout = ContinuousClock.now.advanced(by: randomElectionTimeout())
  }

  private func randomElectionTimeout() -> Duration {
    Duration(.milliseconds(Int64.random(in: 1500..<3000)))
  }

  private func sendRequestVote(to peer: PeerId, args: RequestVote.Args) {
    self.enqueue { await self.deliverRequestVote(to: peer, args: args) }
  }

  private func sendRequestVoteReply(to peer: PeerId, term: Term, voteGranted: Bool) {
    self.enqueue { await self.deliverRequestVoteReply(to: peer, term: term, voteGranted: voteGranted) }
  }

  private func sendAppendEntries(to peer: PeerId, args: AppendEntries.Args) {
    self.enqueue { await self.deliverAppendEntries(to: peer, args: args) }
  }

  private func deliverRequestVote(to peer: PeerId, args: RequestVote.Args) async {
    await deliver(to: peer, message: .requestVote(args), kind: "requestVote")
  }

  private func deliverRequestVoteReply(to peer: PeerId, term: Term, voteGranted: Bool) async {
    await deliver(
      to: peer,
      message: .requestVoteReply(.init(granted: voteGranted, term: term)),
      kind: "requestVoteResponse"
    )
  }

  private func deliverAppendEntries(to peer: PeerId, args: AppendEntries.Args) async {
    await deliver(to: peer, message: .appendEntries(args), kind: "appendEntries")
  }

  func receiveMessage(message: AppMessage, from peer: PeerId) {
    switch message {
    case .requestVote(let args):
      logEvent(kind: "requestVote", direction: "in", peer: peer)
      onRequestVote(from: peer, request: args)
    case .requestVoteReply(let reply):
      logEvent(kind: "requestVoteResponse", direction: "in", peer: peer)
      onRequestVoteResponse(from: peer, reply: reply)
    case .appendEntries(let args):
      logEvent(kind: "appendEntries", direction: "in", peer: peer)
      onAppendEntries(from: peer, args: args)
    }
  }

  private func onRequestVote(from peer: PeerId, request: RequestVote.Args) {
    for action in instance.onRequestVote(peer, request) {
      switch action {
      case .sendRequestVoteReply(let to, let term, let voteGranted):
        sendRequestVoteReply(to: to, term: term, voteGranted: voteGranted)
      case .roleChanged, .persist:
        ()
      case .resetElectionTimeout:
        resetElectionTimeout()
      }
    }
  }

  private func onRequestVoteResponse(from peer: PeerId, reply: RequestVote.Reply) {
    for action in instance.onRequestVoteReply(peer, reply) {
      switch action {
      case .sendAppendEntry(let peer, let args):
        sendAppendEntries(to: peer, args: args)
      }
    }
  }

  private func onAppendEntries(from leader: PeerId, args: AppendEntries.Args) {
    for action in instance.onAppendEntries(leader, args) {
      switch action {
      case .resetElectionTimeout:
        resetElectionTimeout()
      }
    }
  }
}

public actor Shell {
  // Raft
  private var nextElectionTimeout: ContinuousClock.Instant?

  // Node
  var instance: Instance
  let peerId: PeerId
  let transport: TCPTransport
  private let logger: Logger
  private var endpoints: [PeerId: NodeAddress] = [:]
  private typealias Job = @Sendable () async -> Void
  private var supervisor: Task<Void, Never>?
  private var cancelableJobs: AsyncStream<Job>.Continuation?

  public init(_ node: NodeAddress, transport: TCPTransport, logger: Logger? = nil) {
    self.peerId = node.addressKey
    self.transport = transport
    self.instance = Instance(id: node.addressKey)
    self.logger = logger ?? Logger(label: "indras-net.shell")
  }

  public init(_ node: NodeAddress, logger: Logger? = nil) {
    self.init(node, transport: TCPTransport(configuration: node.tcpConfiguration()), logger: logger)
  }

  public func start(with peers: [NodeAddress]) async throws -> Int {
    if self.supervisor != nil {
      guard let port = await transport.listenPort() else { throw ShellError.noListenPort }
      return port
    }

    self.endpoints = Dictionary(uniqueKeysWithValues: peers.map { ($0.addressKey, $0) })
    self.instance = Instance(id: peerId, peers: Set(self.endpoints.keys))

    let (stream, continuation) = AsyncStream.makeStream(of: Job.self)
    self.cancelableJobs = continuation
    self.supervisor = Task {
      await withDiscardingTaskGroup { group in
        for await job in stream {
          group.addTask { await job() }
        }
      }
    }

    try await transport.start { message, from in
      await self.receiveMessage(message: message, from: from)
    }

    self.enqueue { await self.startElectionTimer() }

    guard let port = await transport.listenPort() else {
      await stop()
      try await transport.shutdown()
      throw ShellError.noListenPort
    }
    return port
  }

  private func deliver(to peer: PeerId, message: AppMessage, kind: String) async {
    do {
      guard await ensureConnected(to: peer) else { return }
      try await transport.send(message, to: peer)
      logEvent(kind: kind, direction: "out", peer: peer)
    } catch is CancellationError {
      return
    } catch IndrasNetTransportError.peerNotConnected {
      return
    } catch {
      self.logger.notice("[\(self.peerId)] \(kind) -> \(peer) failed: \(error)")
    }
  }

  public func shutdown() async throws {
    await stop()
    try await transport.shutdown()
  }

  func connectedPeers() async -> Set<PeerId> {
    await transport.connectedPeers()
  }

  public func stop() async {
    self.cancelableJobs?.finish()
    self.cancelableJobs = nil
    self.supervisor?.cancel()
    _ = await self.supervisor?.value
    self.supervisor = nil
  }

  private func enqueue(_ job: @escaping Job) {
    self.cancelableJobs?.yield(job)
  }

  private func ensureConnected(to peer: PeerId) async -> Bool {
    if await transport.isConnected(to: peer) {
      return true
    }
    guard let endpoint = endpoints[peer] else {
      return false
    }
    await transport.connect(to: endpoint)

    let deadline = ContinuousClock.now.advanced(by: .seconds(5))
    while ContinuousClock.now < deadline {
      if await transport.isConnected(to: peer) {
        return true
      }
      if Task.isCancelled {
        return false
      }
      try? await Task.sleep(for: .milliseconds(25))
    }
    return false
  }

  private func logEvent(kind: String, direction: String, peer: PeerId) {
    let arrow = direction == "out" ? "->" : "<-"
    self.logger.info(
      "[\(self.peerId)] \(kind) \(arrow) \(peer)",
      metadata: [
        ShellLogKey.kind: .string(kind),
        ShellLogKey.direction: .string(direction),
        ShellLogKey.peer: .string(peer),
      ]
    )
  }
}

enum ShellLogKey {
  static let kind = "shell.kind"
  static let direction = "shell.direction"
  static let peer = "shell.peer"
}

public enum ShellError: Error, LocalizedError {
  case noListenPort

  public var errorDescription: String? {
    switch self {
    case .noListenPort: "no port bound"
    }
  }
}
