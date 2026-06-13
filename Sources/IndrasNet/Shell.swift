import Foundation
import Logging
import NIOCore

// MARK: Raft
// This might be able to be a protocl for someone to implment that Shell can run/drive
extension Shell {

  private func scheduleNext(delay: Duration) {
    timerTask?.cancel()
    timerTask = Task {
      var nextDelay: Duration = delay
      repeat {
        try? await Task.sleep(for: nextDelay)
        if Task.isCancelled { break }
        nextDelay = self.handleTimerTick()
      } while !Task.isCancelled
    }
  }

  private func handleTimerTick() -> Duration {
    let previousRole = instance.role
    var nextDelay = timing.heartbeatInterval

    for directive in instance.onTimerTick() {
      switch directive {
      case .scheduleNext(let delay):
        nextDelay = delay
      case .requestVote(let peer, let args):
        deliverRequestVote(to: peer, args: args)
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(to: peer, args: args)
      }
    }
    logRoleChangeIfNeeded(from: previousRole)
    return nextDelay
  }

  private func deliverRequestVote(to peer: PeerId, args: RequestVote.Args) {
    deliver(to: peer, message: .requestVote(args), context: .requestVote(direction: "out", peer: peer, term: args.term))
  }

  private func deliverRequestVoteReply(to peer: PeerId, term: Term, voteGranted: Bool) {
    deliver(
      to: peer,
      message: .requestVoteReply(.init(granted: voteGranted, term: term)),
      context: .requestVoteResponse(direction: "out", peer: peer, term: term, granted: voteGranted)
    )
  }

  private func deliverAppendEntries(to peer: PeerId, args: AppendEntries.Args) {
    deliver(
      to: peer, message: .appendEntries(args), context: .appendEntries(direction: "out", peer: peer, term: args.term))
  }

  private func deliverAppendEntriesReply(to peer: PeerId, term: Term, success: Bool) {
    deliver(
      to: peer,
      message: .appendEntriesReply(.init(term: term, success: success)),
      context: .appendEntriesResponse(direction: "out", peer: peer, term: term, success: success)
    )
  }

  func receiveMessage(message: RaftMessage, from peer: PeerId) {
    switch message {
    case .clientSubmit(let args):
      receiveClientSubmit(from: peer, args: args)
    case .clientSubmitReply(let reply):
      receiveClientSubmitReply(reply)
    case .requestVote(let args):
      logRaftEvent(.requestVote(direction: "in", peer: peer, term: args.term))
      receiveRequestVote(from: peer, request: args)
    case .requestVoteReply(let reply):
      logRaftEvent(.requestVoteResponse(direction: "in", peer: peer, term: reply.term, granted: reply.granted))
      receiveRequestVoteReply(from: peer, reply: reply)
    case .appendEntries(let args):
      logRaftEvent(.appendEntries(direction: "in", peer: peer, term: args.term))
      receiveAppendEntries(from: peer, args: args)
    case .appendEntriesReply(let reply):
      logRaftEvent(.appendEntriesResponse(direction: "in", peer: peer, term: reply.term, success: reply.success))
      receiveAppendEntriesReply(from: peer, reply: reply)
    }
  }

  private func receiveRequestVote(from peer: PeerId, request: RequestVote.Args) {
    let previousRole = instance.role

    for action in instance.receiveRequestVote(peer, request) {
      switch action {
      case .sendRequestVoteReply(let to, let term, let voteGranted):
        deliverRequestVoteReply(to: to, term: term, voteGranted: voteGranted)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      case .persist:
        ()  // TODO: persist state
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveRequestVoteReply(from peer: PeerId, reply: RequestVote.Reply) {
    let previousRole = instance.role

    for action in instance.receiveRequestVoteReply(peer, reply) {
      switch action {
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(to: peer, args: args)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntries(from leader: PeerId, args: AppendEntries.Args) {
    let previousRole = instance.role

    for action in instance.receiveAppendEntries(leader, args) {
      switch action {
      case .sendAppendEntriesReply(let to, let term, let success):
        deliverAppendEntriesReply(to: to, term: term, success: success)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      case .apply(let entry):
        applyLogEntry(entry)
      case .persist:
        ()  // TODO: persist state
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntriesReply(from peer: PeerId, reply: AppendEntries.Reply) {
    let previousRole = instance.role

    for action in instance.receiveAppendEntriesReply(peer, reply) {
      switch action {
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(to: peer, args: args)
      case .apply(let entry):
        applyLogEntry(entry)
      case .notifyClient(let requestId, let logIndex, let client):
        completeClientSubmit(
          reply: ClientSubmit.Reply(requestId: requestId, status: .ok, logIndex: logIndex),
          to: client)
      case .persist:
        ()  // TODO: persist state
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveClientSubmit(from clientPeer: PeerId, args: ClientSubmit.Args) {
    handleClientSubmitActions(instance.receiveClientSubmit(clientPeer, args))
  }

  private func handleClientSubmitActions(_ actions: [ClientSubmit.Args.Action]) {
    for action in actions {
      switch action {
      case .sendClientSubmitReply(let to, let reply):
        completeClientSubmit(reply: reply, to: to)
      case .sendAppendEntry(let peer, let appendArgs):
        deliverAppendEntries(to: peer, args: appendArgs)
      case .persist:
        ()  // TODO: persist state
      }
    }
  }

  private func completeClientSubmit(reply: ClientSubmit.Reply, to clientPeer: PeerId) {
    if let continuation = clientContinuations.removeValue(forKey: reply.requestId) {
      continuation.resume(returning: reply)
      return
    }
    deliverClientSubmitReply(to: clientPeer, reply: reply)
  }

  private func receiveClientSubmitReply(_ reply: ClientSubmit.Reply) {
    if let continuation = clientContinuations.removeValue(forKey: reply.requestId) {
      continuation.resume(returning: reply)
    }
  }

  private func deliverClientSubmitReply(to client: PeerId, reply: ClientSubmit.Reply) {
    deliver(to: client, message: .clientSubmitReply(reply), context: .clientSubmitResponse(peer: client))
  }

  private func applyLogEntry(_ entry: LogEntry) {
    logger.info("[\(peerId)] applied log entry term=\(entry.term) bytes=\(entry.command.count)")
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
  private var isStopped = false
  private var inflightDeliveries: [UUID: Task<Void, Never>] = [:]
  // TODO: Switch to `Continuation` + `withContinuation` and `UniqueDictionary` once Swiftly
  // main snapshots resolve stored `Continuation` generic metadata in test bundles (weak-symbol
  // lookup currently crashes IndrasNetTests with signal 6).
  private var clientContinuations: [UInt128: CheckedContinuation<ClientSubmit.Reply, Never>] = [:]
  private var client = RaftClient()
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
    isStopped = false
    self.endpoints = Dictionary(uniqueKeysWithValues: peers.map { ($0.addressKey, $0) })
    self.instance = Instance(id: peerId, peers: Set(self.endpoints.keys), timing: timing)

    try await transport.start { message, from in
      await self.receiveMessage(message: message, from: from)
    }

    scheduleNext(delay: instance.getNextDelay())

    guard let port = await transport.listenPort() else {
      await stop()
      try await transport.shutdown()
      throw ShellError.noListenPort
    }
    return port
  }

  private func deliver(to peer: PeerId, message: RaftMessage, context: RaftLogContext) {
    guard !isStopped else { return }

    let id = UUID()

    inflightDeliveries[id] = Task {
      await self.performDelivery(to: peer, message: message, context: context)
      await self.deliveryFinished(id: id)
    }
  }

  private func performDelivery(to peer: PeerId, message: RaftMessage, context: RaftLogContext) async {
    guard !Task.isCancelled else { return }

    do {
      guard await ensureConnected(to: peer) else {
        logger.notice("[\(peerId)] \(context.kind) -> \(peer) dropped: could not connect")
        return
      }
      try await transport.send(message, to: peer)
      logRaftEvent(context)
    } catch is CancellationError {
      return
    } catch IndrasNetTransportError.peerNotConnected {
      return
    } catch {
      logger.notice("[\(peerId)] \(context.kind) -> \(peer) failed: \(error)")
    }
  }

  private func deliveryFinished(id: UUID) async {
    inflightDeliveries.removeValue(forKey: id)
  }

  func submit(command: Data) async -> ClientSubmit.Reply {
    let request = client.makeRequest(command: command)
    return await withCheckedContinuation { continuation in
      clientContinuations[request.requestId] = continuation
      handleClientSubmitActions(instance.receiveClientSubmit(client.id, request))
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
    isStopped = true

    timerTask?.cancel()
    _ = await timerTask?.value
    timerTask = nil

    let deliveries = Array(inflightDeliveries.values)
    inflightDeliveries.removeAll()
    for task in deliveries {
      task.cancel()
    }
    for task in deliveries {
      _ = await task.value
    }
  }

  private func ensureConnected(to peer: PeerId) async -> Bool {
    if await transport.isConnected(to: peer) {
      return true
    }
    guard let endpoint = endpoints[peer] else {
      return false
    }
    await transport.connect(to: endpoint)
    return await transport.waitForConnection(to: peer, timeout: .seconds(5))
  }
}
