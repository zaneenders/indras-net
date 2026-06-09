import Foundation
import Logging
import NIOCore

// MARK: Raft
// This might be able to be a protocl for someone to implment that Shell can run/drive
extension Shell {

  private func startElectionTimer() async {
    var sleep = instance.getNextTimeout()
    while !Task.isCancelled {
      try? await Task.sleep(for: sleep)
      let previousRole = instance.role
      let tick = instance.onElectionTimeout()
      logRoleChangeIfNeeded(from: previousRole)
      await performTimerActions(tick.actions)
      sleep = tick.sleep
    }
  }

  private func performTimerActions(_ actions: [TimerAction]) async {
    for action in actions {
      switch action {
      case .requestVote(let peer, let args):
        await deliverRequestVote(to: peer, args: args)
      case .sendAppendEntry(let peer, let args):
        await deliverAppendEntries(to: peer, args: args)
      }
    }
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

  func receiveMessage(message: AppMessage, from peer: PeerId) async {
    switch message {
    case .requestVote(let args):
      logEvent(kind: "requestVote", direction: "in", peer: peer)
      await receiveRequestVote(from: peer, request: args)
    case .requestVoteReply(let reply):
      logEvent(kind: "requestVoteResponse", direction: "in", peer: peer)
      await receiveRequestVoteReply(from: peer, reply: reply)
    case .appendEntries(let args):
      logEvent(kind: "appendEntries", direction: "in", peer: peer)
      receiveAppendEntries(from: peer, args: args)
    }
  }

  private func receiveRequestVote(from peer: PeerId, request: RequestVote.Args) async {
    let previousRole = instance.role
    for action in instance.receiveRequestVote(peer, request) {
      switch action {
      case .sendRequestVoteReply(let to, let term, let voteGranted):
        await deliverRequestVoteReply(to: to, term: term, voteGranted: voteGranted)
      case .persist:
        ()
      case .resetElectionTimeout:
        instance.resetElectionTimeout()
      }
    }
    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveRequestVoteReply(from peer: PeerId, reply: RequestVote.Reply) async {
    let previousRole = instance.role
    for action in instance.receiveRequestVoteReply(peer, reply) {
      switch action {
      case .sendAppendEntry(let peer, let args):
        await deliverAppendEntries(to: peer, args: args)
      }
    }
    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntries(from leader: PeerId, args: AppendEntries.Args) {
    let previousRole = instance.role
    for action in instance.receiveAppendEntries(leader, args) {
      switch action {
      case .resetElectionTimeout:
        instance.resetElectionTimeout()
      }
    }
    logRoleChangeIfNeeded(from: previousRole)
  }

  private func logRoleChangeIfNeeded(from previousRole: Role) {
    guard instance.role != previousRole else { return }
    if instance.role == .leader {
      logger.info("[\(peerId)] became leader in term \(instance.currentTerm)")
    }
  }
}

public actor Shell {
  // Node
  var instance: Instance
  let peerId: PeerId
  let transport: TCPTransport
  private let logger: Logger
  private var endpoints: [PeerId: NodeAddress] = [:]
  private var electionTimer: Task<Void, Never>?
  private let timing: NodeTiming

  public init(
    _ node: NodeAddress,
    timing: NodeTiming = .default,
    transport: TCPTransport,
    logger: Logger? = nil
  ) {
    self.peerId = node.addressKey
    self.timing = timing
    self.transport = transport
    self.instance = Instance(id: node.addressKey, timing: timing)
    self.logger = logger ?? Logger(label: "indras-net.shell")
  }

  public init(_ node: NodeAddress, timing: NodeTiming = .default, logger: Logger? = nil) {
    self.init(node, timing: timing, transport: TCPTransport(configuration: node.tcpConfiguration()), logger: logger)
  }

  public func start(with peers: [NodeAddress]) async throws -> Int {
    self.endpoints = Dictionary(uniqueKeysWithValues: peers.map { ($0.addressKey, $0) })
    self.instance = Instance(id: peerId, peers: Set(self.endpoints.keys), timing: timing)

    try await transport.start { message, from in
      await self.receiveMessage(message: message, from: from)
    }

    self.electionTimer = Task {
      await self.startElectionTimer()
    }

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
    self.electionTimer?.cancel()
    _ = await self.electionTimer?.value
    self.electionTimer = nil
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
