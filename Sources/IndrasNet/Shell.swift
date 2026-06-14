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
        await self.timerSleep(nextDelay)
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
    inflightRequestVotes[peer, default: []].append(args)
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
    inflightAppendEntries[peer, default: []].append(args)
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
      receiveRequestVote(from: peer, args: args)
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

  private func receiveRequestVote(from peer: PeerId, args: RequestVote.Args) {
    let previousRole = instance.role

    for action in instance.receiveRequestVote(peer, args) {
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
    guard let sent = inflightRequestVotes[peer]?.removeFirst() else {
      logger.notice("[\(peerId)] requestVote reply from \(peer) with no inflight request")
      return
    }

    let previousRole = instance.role

    for action in instance.receiveRequestVoteReply(peer, sent, reply) {
      switch action {
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(to: peer, args: args)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntries(from peer: PeerId, args: AppendEntries.Args) {
    let previousRole = instance.role

    for action in instance.receiveAppendEntries(peer, args) {
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
    guard let sent = inflightAppendEntries[peer]?.removeFirst() else {
      logger.notice("[\(peerId)] appendEntries reply from \(peer) with no inflight request")
      return
    }

    let previousRole = instance.role

    for action in instance.receiveAppendEntriesReply(peer, sent, reply) {
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

package actor Shell<Transport: NodeTransport> {
  // Node
  var instance: Instance
  let peerId: PeerId
  let transport: Transport
  private let logger: Logger
  private var endpoints: [PeerId: NodeAddress] = [:]
  private var timerTask: Task<Void, Never>?
  private var isStopped = false
  private var inflightDeliveries: [UUID: Task<Void, Never>] = [:]
  private var inflightAppendEntries: [PeerId: [AppendEntries.Args]] = [:]
  private var inflightRequestVotes: [PeerId: [RequestVote.Args]] = [:]
  // TODO: Switch to `Continuation` + `withContinuation` and `UniqueDictionary` once Swiftly
  // main snapshots resolve stored `Continuation` generic metadata in test bundles (weak-symbol
  // lookup currently crashes IndrasNetTests with signal 6).
  private var clientContinuations: [UInt128: CheckedContinuation<ClientSubmit.Reply, Never>] = [:]
  private var client = RaftClient()
  private let timing: NodeTiming
  private let rng: any RandomNumberGenerator & Sendable
  private let timerSleep: @Sendable (Duration) async -> Void

  package init(
    _ node: NodeAddress,
    timing: NodeTiming = .default,
    transport: Transport,
    rng: any RandomNumberGenerator & Sendable = SystemRandomNumberGenerator(),
    timerSleep: @escaping @Sendable (Duration) async -> Void = { try? await Task.sleep(for: $0) },
    logger: Logger? = nil
  ) {
    self.peerId = node.addressKey
    self.timing = timing
    self.transport = transport
    self.rng = rng
    self.timerSleep = timerSleep
    self.instance = Instance(id: node.addressKey, timing: timing, rng: rng)
    self.logger = logger ?? Logger(label: "indras-net.shell")
  }

  package func start(with peers: [NodeAddress]) async throws -> Int {
    isStopped = false
    self.endpoints = Dictionary(uniqueKeysWithValues: peers.map { ($0.addressKey, $0) })
    self.instance = Instance(id: peerId, peers: Set(self.endpoints.keys), timing: timing, rng: rng)

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
        removeInflightOutbound(message, to: peer)
        logger.notice("[\(peerId)] \(context.kind) -> \(peer) dropped: could not connect")
        return
      }
      try await transport.send(message, to: peer)
      logRaftEvent(context)
    } catch is CancellationError {
      removeInflightOutbound(message, to: peer)
      return
    } catch IndrasNetTransportError.peerNotConnected {
      removeInflightOutbound(message, to: peer)
      return
    } catch {
      removeInflightOutbound(message, to: peer)
      logger.notice("[\(peerId)] \(context.kind) -> \(peer) failed: \(error)")
    }
  }

  private func removeInflightOutbound(_ message: RaftMessage, to peer: PeerId) {
    switch message {
    case .appendEntries(let args):
      inflightAppendEntries[peer]?.removeAll { $0 == args }
      if inflightAppendEntries[peer]?.isEmpty == true {
        inflightAppendEntries.removeValue(forKey: peer)
      }
    case .requestVote(let args):
      inflightRequestVotes[peer]?.removeAll { $0 == args }
      if inflightRequestVotes[peer]?.isEmpty == true {
        inflightRequestVotes.removeValue(forKey: peer)
      }
    default:
      break
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

  package func shutdown() async throws {
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
    inflightAppendEntries.removeAll()
    inflightRequestVotes.removeAll()
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

typealias TCPShell = Shell<TCPTransport>

extension Shell where Transport == TCPTransport {
  init(_ node: NodeAddress, timing: NodeTiming = .default, logger: Logger? = nil) {
    self.init(
      node,
      timing: timing,
      transport: TCPTransport(configuration: node.tcpConfiguration()),
      logger: logger
    )
  }
}
