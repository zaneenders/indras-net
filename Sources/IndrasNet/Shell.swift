import Foundation
import Logging
import NIOCore
import OrderedCollections

// MARK: Raft
// This might be able to be a protocol for someone to implement that Shell can run/drive
extension Shell {

  private func scheduleNext(delay: Duration) {
    timerTask?.cancel()
    timerTask = Task {
      var nextDelay: Duration = delay
      repeat {
        await self.timerSleep(nextDelay)
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
      case .persist:
        guard await persistOrHalt() else { return nextDelay }
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
    let id = UUID()
    trackInflight(peer: peer, id: id, outbound: .requestVote(args))
    deliver(
      to: peer,
      message: .requestVote(args),
      context: .requestVote(direction: .outbound, peer: peer, term: args.term),
      outboundID: id
    )
  }

  private func deliverRequestVoteReply(to peer: PeerId, term: Term, voteGranted: Bool) {
    deliver(
      to: peer,
      message: .requestVoteReply(.init(granted: voteGranted, term: term)),
      context: .requestVoteResponse(direction: .outbound, peer: peer, term: term, granted: voteGranted)
    )
  }

  private func deliverAppendEntries(to peer: PeerId, args: AppendEntries.Args) {
    let id = UUID()
    trackInflight(peer: peer, id: id, outbound: .appendEntries(args))
    deliver(
      to: peer,
      message: .appendEntries(args),
      context: .appendEntries(direction: .outbound, peer: peer, term: args.term),
      outboundID: id
    )
  }

  private func deliverAppendEntriesReply(to peer: PeerId, term: Term, success: Bool) {
    deliver(
      to: peer,
      message: .appendEntriesReply(.init(term: term, success: success)),
      context: .appendEntriesResponse(direction: .outbound, peer: peer, term: term, success: success)
    )
  }

  func receiveMessage(message: RaftMessage, from peer: PeerId) async {
    switch message {
    case .clientSubmit(let args):
      await receiveClientSubmit(from: peer, args: args)
    case .clientSubmitReply(let reply):
      receiveClientSubmitReply(reply)
    case .requestVote(let args):
      logRaftEvent(.requestVote(direction: .inbound, peer: peer, term: args.term))
      await receiveRequestVote(from: peer, args: args)
    case .requestVoteReply(let reply):
      logRaftEvent(.requestVoteResponse(direction: .inbound, peer: peer, term: reply.term, granted: reply.granted))
      await receiveRequestVoteReply(from: peer, reply: reply)
    case .appendEntries(let args):
      logRaftEvent(.appendEntries(direction: .inbound, peer: peer, term: args.term))
      await receiveAppendEntries(from: peer, args: args)
    case .appendEntriesReply(let reply):
      logRaftEvent(.appendEntriesResponse(direction: .inbound, peer: peer, term: reply.term, success: reply.success))
      await receiveAppendEntriesReply(from: peer, reply: reply)
    }
  }

  private func receiveRequestVote(from peer: PeerId, args: RequestVote.Args) async {
    let previousRole = instance.role

    for action in instance.receiveRequestVote(peer, args) {
      switch action {
      case .sendRequestVoteReply(let to, let term, let voteGranted):
        deliverRequestVoteReply(to: to, term: term, voteGranted: voteGranted)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      case .persist:
        guard await persistOrHalt() else { return }
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveRequestVoteReply(from peer: PeerId, reply: RequestVote.Reply) async {
    guard let sent = dequeueSentRequestVote(from: peer) else {
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
      case .persist:
        guard await persistOrHalt() else { return }
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntries(from peer: PeerId, args: AppendEntries.Args) async {
    let previousRole = instance.role

    for action in instance.receiveAppendEntries(peer, args) {
      switch action {
      case .sendAppendEntriesReply(let to, let term, let success):
        deliverAppendEntriesReply(to: to, term: term, success: success)
      case .scheduleNext(let delay):
        scheduleNext(delay: delay)
      case .apply(let entry, let index):
        applyLogEntry(entry, atIndex: index)
      case .persist:
        guard await persistOrHalt() else { return }
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveAppendEntriesReply(from peer: PeerId, reply: AppendEntries.Reply) async {
    guard let sent = dequeueSentAppendEntries(from: peer) else {
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
      case .apply(let entry, let index):
        applyLogEntry(entry, atIndex: index)
      case .persist:
        guard await persistOrHalt() else { return }
      }
    }

    logRoleChangeIfNeeded(from: previousRole)
  }

  private func receiveClientSubmit(from clientPeer: PeerId, args: ClientSubmit.Args) async {
    if let completedIndex = clientRequests.completedIndex(client: clientPeer, requestId: args.requestId) {
      completeClientSubmit(
        reply: ClientSubmit.Reply(
          requestId: args.requestId, status: .ok, logIndex: completedIndex),
        to: clientPeer)
      return
    }

    if let inFlightIndex = clientRequests.inFlightIndex(client: clientPeer, requestId: args.requestId) {
      if let pending = clientRequests.pending(at: inFlightIndex),
        pending.requestId == args.requestId,
        pending.client != clientPeer
      {
        clientRequests.addWaiter(clientPeer, forRequestId: args.requestId)
      }
      return
    }

    await handleClientSubmitActions(instance.receiveClientSubmit(clientPeer, args))
  }

  private func handleClientSubmitActions(_ actions: [ClientSubmit.Args.Action]) async {
    for action in actions {
      switch action {
      case .sendClientSubmitReply(let to, let reply):
        completeClientSubmit(reply: reply, to: to)
      case .clientWriteAppended(let logIndex, let requestId, let client):
        clientRequests.append(requestId: requestId, client: client, atIndex: logIndex)
      case .sendAppendEntry(let peer, let appendArgs):
        deliverAppendEntries(to: peer, args: appendArgs)
      case .persist:
        guard await persistOrHalt() else { return }
      }
    }
  }

  private func persistOrHalt() async -> Bool {
    do {
      try await store.save(instance.persistentState)
      return true
    } catch {
      logger.error("[\(peerId)] failed to persist raft state: \(error)")
      halt()
      return false
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

  private func applyLogEntry(_ entry: LogEntry, atIndex index: LogIndex) {
    logger.info("[\(peerId)] applied log entry index=\(index) term=\(entry.term) bytes=\(entry.command.count)")
    guard let result = clientRequests.complete(atIndex: index) else { return }
    let reply = ClientSubmit.Reply(requestId: result.pending.requestId, status: .ok, logIndex: index)
    completeClientSubmit(reply: reply, to: result.pending.client)
    for client in result.waiters {
      deliverClientSubmitReply(to: client, reply: reply)
    }
  }

  private func failPendingClientWrites() {
    let pending = clientRequests.drainForAbort()
    for (index, request) in pending {
      completeClientSubmit(
        reply: ClientSubmit.Reply(
          requestId: request.requestId, status: .aborted, logIndex: index),
        to: request.client)
    }
    for (requestId, continuation) in clientContinuations {
      continuation.resume(
        returning: ClientSubmit.Reply(requestId: requestId, status: .aborted))
    }
    clientContinuations.removeAll()
  }

  private func logRoleChangeIfNeeded(from previousRole: Role) {
    if previousRole == .leader, instance.role != .leader {
      failPendingClientWrites()
    } else if previousRole != .leader, instance.role == .leader {
      clientRequests.resetSessions()
    }
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
  private enum InflightOutbound {
    case appendEntries(AppendEntries.Args)
    case requestVote(RequestVote.Args)

    var kind: InflightMessageKind {
      switch self {
      case .appendEntries: .appendEntries
      case .requestVote: .requestVote
      }
    }
  }

  private enum InflightMessageKind {
    case appendEntries
    case requestVote
  }

  private struct InflightRPC {
    let id: UUID
    let outbound: InflightOutbound
    var sent: Bool = false
  }

  // Node
  var instance: Instance
  let peerId: PeerId
  let transport: Transport
  private let logger: Logger
  private var endpoints: [PeerId: NodeAddress] = [:]
  private var timerTask: Task<Void, Never>?
  private var isStopped = false
  private var inflightDeliveries: [UUID: Task<Void, Never>] = [:]
  private var inflightMessages: [PeerId: [InflightRPC]] = [:]
  private var clientRequests = ClientRequestLog()
  // TODO: Switch to `Continuation` + `withContinuation` and `UniqueDictionary` once Swiftly
  // main snapshots resolve stored `Continuation` generic metadata in test bundles (weak-symbol
  // lookup currently crashes IndrasNetTests with signal 6).
  private var clientContinuations: [UInt128: CheckedContinuation<ClientSubmit.Reply, Never>] = [:]
  private var client = RaftClient()
  private let store: any RaftStore
  private let timing: NodeTiming
  private let rng: any RandomNumberGenerator & Sendable
  private let timerSleep: @Sendable (Duration) async -> Void

  package init(
    _ node: NodeAddress,
    timing: NodeTiming = .default,
    transport: Transport,
    store: any RaftStore = InMemoryRaftStore(),
    rng: any RandomNumberGenerator & Sendable = SystemRandomNumberGenerator(),
    timerSleep: @escaping @Sendable (Duration) async -> Void = { try? await Task.sleep(for: $0) },
    logger: Logger? = nil
  ) {
    self.peerId = node.addressKey
    self.timing = timing
    self.transport = transport
    self.store = store
    self.rng = rng
    self.timerSleep = timerSleep
    self.instance = Instance(id: node.addressKey, timing: timing, rng: rng)
    self.logger = logger ?? Logger(label: "indras-net.shell")
  }

  package func start(with peers: [NodeAddress]) async throws -> Int {
    isStopped = false
    self.endpoints = Dictionary(uniqueKeysWithValues: peers.map { ($0.addressKey, $0) })

    var instance = Instance(
      id: peerId, peers: OrderedSet(peers.map(\.addressKey)), timing: timing, rng: rng)
    if let saved = try await store.load() {
      instance.restore(from: saved)
    }
    self.instance = instance

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

  private func deliver(
    to peer: PeerId,
    message: RaftMessage,
    context: RaftLogContext,
    outboundID: UUID? = nil
  ) {
    guard !isStopped else { return }

    let deliveryID = outboundID ?? UUID()

    inflightDeliveries[deliveryID] = Task {
      await self.performDelivery(
        to: peer,
        message: message,
        context: context,
        outboundID: outboundID
      )
      await self.deliveryFinished(id: deliveryID)
    }
  }

  private func performDelivery(
    to peer: PeerId,
    message: RaftMessage,
    context: RaftLogContext,
    outboundID: UUID?
  ) async {
    guard !Task.isCancelled else { return }

    do {
      guard await ensureConnected(to: peer) else {
        removeInflightOutbound(id: outboundID, to: peer)
        logger.notice("[\(peerId)] \(context.kind.rawValue) -> \(peer) dropped: could not connect")
        return
      }
      try await transport.send(message, to: peer)
      markOutboundSent(id: outboundID, to: peer)
      logRaftEvent(context)
    } catch is CancellationError {
      removeInflightOutbound(id: outboundID, to: peer)
      return
    } catch IndrasNetTransportError.peerNotConnected {
      removeInflightOutbound(id: outboundID, to: peer)
      return
    } catch {
      removeInflightOutbound(id: outboundID, to: peer)
      logger.notice("[\(peerId)] \(context.kind.rawValue) -> \(peer) failed: \(error)")
    }
  }

  private func trackInflight(peer: PeerId, id: UUID, outbound: InflightOutbound) {
    inflightMessages[peer, default: []].append(InflightRPC(id: id, outbound: outbound))
  }

  private func markOutboundSent(id: UUID?, to peer: PeerId) {
    guard let id,
      var inflight = inflightMessages[peer],
      let index = inflight.firstIndex(where: { $0.id == id })
    else { return }
    inflight[index].sent = true
    inflightMessages[peer] = inflight
  }

  private func dequeueSentAppendEntries(from peer: PeerId) -> AppendEntries.Args? {
    guard case .appendEntries(let args) = dequeueSent(from: peer, kind: .appendEntries) else {
      return nil
    }
    return args
  }

  private func dequeueSentRequestVote(from peer: PeerId) -> RequestVote.Args? {
    guard case .requestVote(let args) = dequeueSent(from: peer, kind: .requestVote) else {
      return nil
    }
    return args
  }

  private func dequeueSent(from peer: PeerId, kind: InflightMessageKind) -> InflightOutbound? {
    guard var inflight = inflightMessages[peer],
      let index = inflight.firstIndex(where: { $0.sent && $0.outbound.kind == kind })
    else { return nil }
    let outbound = inflight.remove(at: index).outbound
    if inflight.isEmpty {
      inflightMessages.removeValue(forKey: peer)
    } else {
      inflightMessages[peer] = inflight
    }
    return outbound
  }

  private func removeInflightOutbound(id: UUID?, to peer: PeerId) {
    guard let id else { return }
    inflightMessages[peer]?.removeAll { $0.id == id }
    if inflightMessages[peer]?.isEmpty == true {
      inflightMessages.removeValue(forKey: peer)
    }
  }

  private func deliveryFinished(id: UUID) async {
    inflightDeliveries.removeValue(forKey: id)
  }

  func submit(command: Data) async -> ClientSubmit.Reply {
    let request = client.makeRequest(command: command)
    return await withCheckedContinuation { continuation in
      clientContinuations[request.requestId] = continuation
      Task {
        await self.receiveClientSubmit(from: client.id, args: request)
      }
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
    failPendingClientWrites()

    timerTask?.cancel()
    _ = await timerTask?.value
    timerTask = nil

    let deliveries = Array(inflightDeliveries.values)
    inflightDeliveries.removeAll()
    inflightMessages.removeAll()
    for task in deliveries {
      task.cancel()
    }
    for task in deliveries {
      _ = await task.value
    }
  }

  private func halt() {
    // Invalid state, crashing
    isStopped = true
    failPendingClientWrites()
    timerTask?.cancel()
    let deliveries = Array(inflightDeliveries.values)
    inflightDeliveries.removeAll()
    inflightMessages.removeAll()
    for task in deliveries {
      task.cancel()
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
  init(
    _ node: NodeAddress,
    timing: NodeTiming = .default,
    store: any RaftStore = InMemoryRaftStore(),
    logger: Logger? = nil
  ) {
    self.init(
      node,
      timing: timing,
      transport: TCPTransport(configuration: node.tcpConfiguration()),
      store: store,
      logger: logger
    )
  }
}
