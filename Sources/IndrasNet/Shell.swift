import Foundation
import Logging
import NIOCore

// MARK: Raft
// This might be able to be a protocl for someone to implment that Shell can run/drive
extension Shell {

  private func scheduleNext(delay: Duration = .zero) {
    timerTask?.cancel()
    timerTask = Task {
      var nextDelay: Duration = delay
      repeat {
        try? await Task.sleep(for: nextDelay)
        if Task.isCancelled { break }
        nextDelay = await self.handleTimerTick()
      } while !Task.isCancelled
    }
  }

  private func handleTimerTick() async -> Duration {
    let previousRole = instance.role
    var nextDelay = timing.heartbeatInterval

    for directive in instance.onTimerTick() {
      switch directive {
      case .scheduleNext(let delay):
        nextDelay = delay
      case .requestVote(let peer, let args):
        await deliverRequestVote(to: peer, args: args)
      case .sendAppendEntry(let peer, let args):
        await deliverAppendEntries(to: peer, args: args)
      }
    }
    logRoleChangeIfNeeded(from: previousRole)
    return nextDelay
  }

  private func deliverRequestVote(to peer: PeerId, args: RequestVote.Args) async {
    await deliver(to: peer, message: .requestVote(args), context: .requestVote(direction: "out", peer: peer, term: args.term))
  }

  private func deliverRequestVoteReply(to peer: PeerId, term: Term, voteGranted: Bool) async {
    await deliver(
      to: peer,
      message: .requestVoteReply(.init(granted: voteGranted, term: term)),
      context: .requestVoteResponse(direction: "out", peer: peer, term: term, granted: voteGranted)
    )
  }

  private func deliverAppendEntries(to peer: PeerId, args: AppendEntries.Args) async {
    await deliver(to: peer, message: .appendEntries(args), context: .appendEntries(direction: "out", peer: peer, term: args.term))
  }

  private func deliverAppendEntriesReply(to peer: PeerId, term: Term, success: Bool) async {
    await deliver(
      to: peer,
      message: .appendEntriesReply(.init(term: term, success: success)),
      context: .appendEntriesResponse(direction: "out", peer: peer, term: term, success: success)
    )
  }

  func receiveMessage(message: AppMessage, from peer: PeerId) async {
    switch message {
    case .requestVote(let args):
      logRaftEvent(.requestVote(direction: "in", peer: peer, term: args.term))
      await receiveRequestVote(from: peer, request: args)
    case .requestVoteReply(let reply):
      logRaftEvent(.requestVoteResponse(direction: "in", peer: peer, term: reply.term, granted: reply.granted))
      await receiveRequestVoteReply(from: peer, reply: reply)
    case .appendEntries(let args):
      logRaftEvent(.appendEntries(direction: "in", peer: peer, term: args.term))
      await receiveAppendEntries(from: peer, args: args)
    case .appendEntriesReply(let reply):
      logRaftEvent(.appendEntriesResponse(direction: "in", peer: peer, term: reply.term, success: reply.success))
      await receiveAppendEntriesReply(from: peer, reply: reply)
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
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
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
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntries(from leader: PeerId, args: AppendEntries.Args) async {
    let previousRole = instance.role

    for action in instance.receiveAppendEntries(leader, args) {
      switch action {
      case .sendAppendEntriesReply(let to, let term, let success):
        await deliverAppendEntriesReply(to: to, term: term, success: success)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntriesReply(from peer: PeerId, reply: AppendEntries.Reply) async {
    let previousRole = instance.role

    for action in instance.receiveAppendEntriesReply(peer, reply) {
      switch action {
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func logRoleChangeIfNeeded(from previousRole: Role) {
    guard instance.role != previousRole else { return }
    let term = instance.currentTerm
    switch instance.role {
    case .leader:
      logger.info("[\(peerId)] became leader in term \(term)")
    case .candidate:
      logger.info("[\(peerId)] became candidate in term \(term)")
    case .follower:
      logger.info("[\(peerId)] became follower in term \(term)")
    }
  }

  private func logRaftEvent(_ context: RaftLogContext) {
    logger.log(level: context.level, "\(context.message(selfNode: peerId))", metadata: context.metadata)
  }
}

public actor Shell {
  // Node
  var instance: Instance
  let peerId: PeerId
  let transport: TCPTransport
  private let logger: Logger
  private var endpoints: [PeerId: NodeAddress] = [:]
  private var timerTask: Task<Void, Never>?
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

    scheduleNext()

    guard let port = await transport.listenPort() else {
      await stop()
      try await transport.shutdown()
      throw ShellError.noListenPort
    }
    return port
  }

  private func deliver(to peer: PeerId, message: AppMessage, context: RaftLogContext) async {
    do {
      guard await ensureConnected(to: peer) else { return }
      try await transport.send(message, to: peer)
      logRaftEvent(context)
    } catch is CancellationError {
      return
    } catch IndrasNetTransportError.peerNotConnected {
      return
    } catch {
      self.logger.notice("[\(self.peerId)] \(context.kind) -> \(peer) failed: \(error)")
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
    timerTask?.cancel()
    _ = await timerTask?.value
    timerTask = nil
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
}

enum ShellLogKey {
  static let kind = "shell.kind"
  static let direction = "shell.direction"
  static let peer = "shell.peer"
}

private struct RaftLogContext {
  let kind: String
  let direction: String
  let peer: PeerId
  let term: Term
  var granted: Bool?
  var success: Bool?

  var level: Logger.Level {
    switch kind {
    case "appendEntries", "appendEntriesResponse":
      if success == false { return .info }
      return .trace
    default:
      return .info
    }
  }

  var metadata: Logger.Metadata {
    var metadata: Logger.Metadata = [
      ShellLogKey.kind: .string(kind),
      ShellLogKey.direction: .string(direction),
      ShellLogKey.peer: .string(peer),
    ]
    metadata["shell.term"] = .stringConvertible(term)
    if let granted {
      metadata["shell.granted"] = .stringConvertible(granted)
    }
    if let success {
      metadata["shell.success"] = .stringConvertible(success)
    }
    return metadata
  }

  func message(selfNode: PeerId) -> String {
    let arrow = direction == "out" ? "->" : "<-"
    var text = "[\(selfNode)] \(kind) \(arrow) \(peer) term=\(term)"
    if let granted {
      text += granted ? " granted" : " denied"
    }
    if success == false {
      text += " rejected"
    }
    return text
  }

  static func requestVote(direction: String, peer: PeerId, term: Term) -> RaftLogContext {
    RaftLogContext(kind: "requestVote", direction: direction, peer: peer, term: term)
  }

  static func requestVoteResponse(
    direction: String,
    peer: PeerId,
    term: Term,
    granted: Bool
  ) -> RaftLogContext {
    RaftLogContext(kind: "requestVoteResponse", direction: direction, peer: peer, term: term, granted: granted)
  }

  static func appendEntries(direction: String, peer: PeerId, term: Term) -> RaftLogContext {
    RaftLogContext(kind: "appendEntries", direction: direction, peer: peer, term: term)
  }

  static func appendEntriesResponse(
    direction: String,
    peer: PeerId,
    term: Term,
    success: Bool
  ) -> RaftLogContext {
    RaftLogContext(kind: "appendEntriesResponse", direction: direction, peer: peer, term: term, success: success)
  }
}

public enum ShellError: Error, LocalizedError {
  case noListenPort

  public var errorDescription: String? {
    switch self {
    case .noListenPort: "no port bound"
    }
  }
}
