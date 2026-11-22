import Crypto
import Foundation
import OrderedCollections
import RaftCore
import _NIOFileSystem


enum RaftRequest: Codable, Sendable {
  case get(key: String)
  case put(key: String, value: RaftValue, requestID: UUID)
  case delete(key: String, requestID: UUID)
  case status
}

package struct RaftStatus: Codable, Sendable, Equatable {
  package static let serviceName = "indra.raft.v1"
  package let node: String
  package let term: UInt64
  package let commitIndex: UInt64
  package let appliedIndex: UInt64
  package let voters: Int
  package let readers: Int
  package let group: String
  package let role: String
  package let leader: String?
  package let snapshotIndex: UInt64
  package let configuration: RaftConfigurationStatus?
}

package struct RaftConfigurationStatus: Codable, Sendable, Equatable {
  package let voters: [String]
  package let previousVoters: [String]?
  package let readers: [String]
}

struct RaftWriteResult: Codable, Sendable, Equatable {
  let requestID: UUID
  let index: UInt64
  let removed: Bool?
}

enum RaftReply: Codable, Sendable, Equatable {
  case value(RaftValue?)
  case applied(RaftWriteResult)
  case status(RaftStatus)
  case client(String)
  case notLeader(String?)
}

enum RaftStartMode: Sendable {
  case bootstrap
  case discover
  case join(PeerId)
}

actor RaftHost {
  private enum Lifecycle {
    case running, failed, closed
  }

  private struct SnapshotTransfer: Sendable {
    let index: LogIndex
    let term: Term
    let bytes: Data
    let checksum: Data
  }

  private struct IncomingSnapshot {
    let leader: PeerId
    let term: Term
    let index: LogIndex
    let indexTerm: Term
    let size: UInt64
    let checksum: Data
    var bytes = Data()

    init(_ request: InstallSnapshot.Args) {
      leader = request.leaderId
      term = request.term
      index = request.lastIncludedIndex
      indexTerm = request.lastIncludedTerm
      size = request.totalLength
      checksum = request.checksum
    }

    func matches(_ request: InstallSnapshot.Args) -> Bool {
      leader == request.leaderId && term == request.term && index == request.lastIncludedIndex
        && indexTerm == request.lastIncludedTerm && size == request.totalLength && checksum == request.checksum
    }
  }

  static let snapshotInterval: LogIndex = 256
  static let inspectionPageSize = 8
  private static let timing = NodeTiming(
    heartbeatIntervalMs: 100, electionTimeoutMinMs: 750, electionTimeoutMaxMs: 1_500)

  let group: String
  private let storage: RaftStorage
  private var instance: Instance
  private var persisted: PersistentState
  private var snapshot: SnapshotTransfer?
  private var incoming: IncomingSnapshot?
  private var nextSnapshotAt: LogIndex
  private var lifecycle = Lifecycle.running
  private var inTransition = false
  private var waiters: [CheckedContinuation<Void, Never>] = []
  private var transport: RaftTransport?
  private var timer: Task<Void, Never>?
  private var timerID: UInt64 = 0
  private var nextRPC: UInt64 = 0
  private var lastEvent = ContinuousClock.now
  private var replication: [PeerId: Task<Void, Never>] = [:]
  private var joining: Task<Void, Never>?

  private init(storage: RaftStorage, instance: Instance, persisted: PersistentState, group: String) throws {
    self.storage = storage
    self.instance = instance
    self.persisted = persisted
    self.group = group
    self.nextSnapshotAt = Self.snapshotDeadline(after: instance.logBaseIndex)
    if let saved = persisted.snapshot {
      let bytes = try RaftStorage.encodeSnapshot(saved)
      snapshot = SnapshotTransfer(
        index: saved.lastIncludedIndex, term: saved.lastIncludedTerm, bytes: bytes,
        checksum: Data(SHA256.hash(data: bytes)))
    }
  }

  static func open(in directory: FilePath, node: String, cluster: String, mode: RaftStartMode) async throws -> Self {
    let storage = try await RaftStorage.open(in: directory, node: node, cluster: cluster)
    do {
      let (state, applied) = try await storage.load()
      guard state.currentTerm < Term.max else { throw RaftServiceError.invalidState }
      let initial: ClusterMembership
      if let saved = state.initialMembership {
        guard saved.voters.count == 1 else { throw RaftServiceError.wrongReplica }
        if case .join(let group) = mode, group != saved.voters.first { throw RaftServiceError.wrongReplica }
        initial = saved
      } else {
        let empty = state.currentTerm == 0 && state.commitIndex == 0 && state.log.count == 1
        let origin: PeerId
        switch mode {
        case .bootstrap:
          origin = node
        case .discover:
          guard !empty else { throw RaftServiceError.discoveringGroup }
          origin = node
        case .join(let group):
          guard empty, group != node else { throw RaftServiceError.wrongReplica }
          origin = group
        }
        initial = ClusterMembership(voters: OrderedSet([origin]), oldVoters: nil, readers: [])
      }
      for index in (applied + 1)..<(state.commitIndex + 1) {
        _ = try await storage.apply(state.log[Int(index - state.logBaseIndex)], at: index, beforeCommit: nil)
      }
      let instance = Instance(
        id: node, peers: [], role: .follower, currentTerm: state.currentTerm, votedFor: state.votedFor,
        votes: [:], commitIndex: state.commitIndex, lastApplied: state.commitIndex, log: state.log,
        logBaseIndex: state.logBaseIndex, snapshot: state.snapshot, initialMembership: initial,
        timing: timing, rng: SystemRandomNumberGenerator())
      let host = try Self(storage: storage, instance: instance, persisted: state, group: initial.voters[0])
      try await host.initialize()
      return host
    } catch {
      try? await storage.close()
      throw error
    }
  }

  private func initialize() async throws {
    if instance.membership.hasQuorum([instance.id]) {
      let effects = instance.onTimerTick()
      try await process(effects)
    } else if persisted.initialMembership == nil {
      try await process([.persist])
    }
  }

  func start(transport: RaftTransport) {
    guard lifecycle == .running, self.transport == nil else { return }
    self.transport = transport
    reschedule(after: instance.getNextDelay())
    tryJoin()
  }

  func execute(_ request: RaftRequest) async throws -> RaftReply {
    let plan = try await prepare(request)
    switch plan {
    case .complete(let reply): return reply
    case .write(let id, let command, let term): return try await waitForWrite(id: id, command: command, term: term)
    case .read(let key, let index, let term): return try await waitForRead(key: key, index: index, term: term)
    }
  }

  private enum Plan: Sendable {
    case complete(RaftReply)
    case write(UUID, Data, Term)
    case read(String, LogIndex, Term)
  }

  private func prepare(_ request: RaftRequest) async throws -> Plan {
    await acquireTransition()
    defer { releaseTransition() }
    try Task.checkCancellation()
    try requireRunning()
    if case .status = request {
      let committed = instance.committedMembership
      return .complete(.status(RaftStatus(
        node: instance.id, term: instance.currentTerm, commitIndex: instance.commitIndex,
        appliedIndex: instance.lastApplied, voters: committed.voters.count, readers: committed.readers.count,
        group: group, role: String(describing: instance.role), leader: instance.leaderId,
        snapshotIndex: instance.logBaseIndex,
        configuration: RaftConfigurationStatus(membership: committed))))
    }
    guard instance.role == .leader, instance.membership.allVoters.contains(instance.id) else {
      return .complete(.notLeader(instance.leaderId))
    }
    switch request {
    case .get(let key):
      if instance.membership.hasQuorum([instance.id]) {
        return .complete(.value(try await storage.value(for: key)))
      }
      let effects = instance.proposeReadBarrier()
      guard !effects.isEmpty else { return .complete(.notLeader(nil)) }
      try await process(effects)
      return .read(key, instance.lastLogIndex, instance.currentTerm)
    case .put(let key, let value, let id):
      return try await submit(.put(key: key, value: value), id: id)
    case .delete(let key, let id):
      return try await submit(.delete(key: key), id: id)
    case .status:
      throw RaftServiceError.invalidState
    }
  }

  func inspectLog(before: LogIndex?) async throws -> RaftLogPage {
    await acquireTransition()
    defer { releaseTransition() }
    try Task.checkCancellation()
    try requireRunning()
    let base = instance.logBaseIndex
    let end = before.map { boundary in min(boundary, instance.lastLogIndex + 1) } ?? (instance.lastLogIndex + 1)
    let available = Int(end > base ? end - base - 1 : 0)
    let start = available - min(available, Self.inspectionPageSize)
    let entries = try instance.log.dropFirst(1)[(start + 1)..<(available + 1)].enumerated().map { offset, entry in
      let operation: RaftLogRecord.Operation
      if let membership = entry.membership {
        operation = .membership(.init(membership: membership))
      } else if entry.isNoOp {
        operation = .noOp
      } else {
        switch try JSONDecoder().decode(RaftMutation.self, from: entry.command) {
        case .put(let key, let value): operation = .put(key: key, value: value)
        case .delete(let key): operation = .delete(key: key)
        }
      }
      return RaftLogRecord(index: base + UInt64(start + offset + 1), term: entry.term, operation: operation)
    }
    return RaftLogPage(
      compactedThrough: base, lastIndex: instance.lastLogIndex, commitIndex: instance.commitIndex,
      appliedIndex: instance.lastApplied, entries: Array(entries.reversed()),
      olderBefore: start == 0 ? nil : entries.first?.index)
  }

  func inspectionSnapshot() async throws -> RaftSnapshot? {
    await acquireTransition()
    defer { releaseTransition() }
    try Task.checkCancellation()
    try requireRunning()
    return persisted.snapshot
  }

  private func submit(_ mutation: RaftMutation, id: UUID) async throws -> Plan {
    let command = try mutation.encode()
    if let receipt = try await storage.receipt(for: id) {
      return .complete(try Self.reply(id: id, command: command, receipt: receipt))
    }
    let identity = RequestIdentity(client: id.uuidString, requestId: 0)
    if let pending = instance.log.first(where: { $0.request == identity }), pending.command != command {
      throw RaftServiceError.requestConflict
    }
    guard instance.lastLogIndex < LogIndex.max - 1 else { throw RaftServiceError.invalidState }
    let entry = LogEntry(term: instance.currentTerm, command: command, request: identity, isNoOp: false)
    try RaftStorage.validate(mutation, entry: entry, at: instance.lastLogIndex + 1)
    let actions = instance.receiveClientSubmit(
      identity.client, .init(requestId: identity.requestId, command: command, traceHeaders: [:]))
    let effects = try actions.compactMap { action -> TimerDirective? in
      switch action {
      case .persist: return .persist
      case .apply(let entry, let index): return .apply(entry: entry, atIndex: index)
      case .sendAppendEntry(let peer, let args): return .sendAppendEntry(to: peer, args: args)
      case .clientWriteAppended: return nil
      case .sendClientSubmitReply(_, let reply):
        if reply.status == .commandTooLarge { throw RaftServiceError.commandTooLarge }
        throw RaftServiceError.invalidState
      }
    }
    try await process(effects)
    return .write(id, command, instance.currentTerm)
  }

  private static func reply(id: UUID, command: Data, receipt: RaftReceipt) throws -> RaftReply {
    guard receipt.matches(command) else { throw RaftServiceError.requestConflict }
    return .applied(RaftWriteResult(requestID: id, index: receipt.index, removed: receipt.removed))
  }

  private func waitForWrite(id: UUID, command: Data, term: Term) async throws -> RaftReply {
    let deadline = ContinuousClock.now.advanced(by: .seconds(2))
    repeat {
      if let reply = try await writeResult(id: id, command: command, term: term) { return reply }
      try await Task.sleep(for: .milliseconds(20))
    } while ContinuousClock.now < deadline
    return .notLeader(nil)
  }

  private func writeResult(id: UUID, command: Data, term: Term) async throws -> RaftReply? {
    await acquireTransition()
    defer { releaseTransition() }
    try requireRunning()
    if let receipt = try await storage.receipt(for: id) { return try Self.reply(id: id, command: command, receipt: receipt) }
    if instance.role != .leader || term != instance.currentTerm { return .notLeader(instance.leaderId) }
    return nil
  }

  private func waitForRead(key: String, index: LogIndex, term: Term) async throws -> RaftReply {
    let deadline = ContinuousClock.now.advanced(by: .seconds(2))
    repeat {
      if let reply = try await readResult(key: key, index: index, term: term) { return reply }
      try await Task.sleep(for: .milliseconds(20))
    } while ContinuousClock.now < deadline
    return .notLeader(nil)
  }

  private func readResult(key: String, index: LogIndex, term: Term) async throws -> RaftReply? {
    await acquireTransition()
    defer { releaseTransition() }
    try requireRunning()
    guard instance.role == .leader, term == instance.currentTerm else { return .notLeader(instance.leaderId) }
    guard instance.lastApplied >= index else { return nil }
    return .value(try await storage.value(for: key))
  }

  func admit(_ peer: PeerId) async throws -> Bool {
    await acquireTransition()
    defer { releaseTransition() }
    try requireRunning()
    guard instance.role == .leader else { return false }
    if instance.membership.members.contains(peer) { return true }
    var proposed = instance
    let effects: [TimerDirective]
    do {
      effects = try proposed.addReader(peer)
    } catch MembershipChangeError.changeInProgress {
      return false
    }
    guard let entry = proposed.log.last else { throw RaftServiceError.invalidState }
    try RaftStorage.validateLog(entry, index: proposed.lastLogIndex)
    instance = proposed
    try await process(effects)
    return true
  }

  func vote(_ request: RequestVote.Args) async throws -> RequestVote.Reply {
    await acquireTransition()
    defer { releaseTransition() }
    try requireRunning()
    guard request.term < Term.max else { throw RaftServiceError.invalidState }
    advanceClock()
    var reply: RequestVote.Reply?
    let effects = instance.receiveRequestVote(request.candidateId, request).compactMap { action -> TimerDirective? in
      switch action {
      case .persist: return .persist
      case .scheduleNext(let delay): return .scheduleNext(delay: delay)
      case .sendRequestVoteReply(_, let term, let granted):
        reply = RequestVote.Reply(granted: granted, term: term)
        return nil
      }
    }
    try await process(effects)
    guard let reply else { throw RaftServiceError.invalidState }
    return reply
  }

  func append(_ request: AppendEntries.Args) async throws -> AppendEntries.Reply {
    await acquireTransition()
    defer { releaseTransition() }
    try requireRunning()
    guard request.term < Term.max, request.prevLogIndex < LogIndex.max - LogIndex(request.entries.count) else {
      throw RaftServiceError.invalidState
    }
    advanceClock()
    var reply: AppendEntries.Reply?
    let effects = instance.receiveAppendEntries(request.leaderId, request).compactMap { action -> TimerDirective? in
      switch action {
      case .persist: return .persist
      case .scheduleNext(let delay): return .scheduleNext(delay: delay)
      case .apply(let entry, let index): return .apply(entry: entry, atIndex: index)
      case .sendAppendEntriesReply(_, let term, let success, let prefix):
        reply = AppendEntries.Reply(term: term, success: success, rpcId: request.rpcId, compactedPrefix: prefix)
        return nil
      }
    }
    try await process(effects)
    guard let reply else { throw RaftServiceError.invalidState }
    return reply
  }

  func install(_ args: InstallSnapshot.Args) async throws -> InstallSnapshot.Reply {
    await acquireTransition()
    defer { releaseTransition() }
    try requireRunning()
    guard args.term < Term.max, args.lastIncludedIndex < LogIndex.max - 1,
      args.totalLength > 0, args.totalLength <= UInt64(RaftStorage.snapshotLimit),
      !args.data.isEmpty, args.data.count <= RaftTransport.snapshotChunkBytes,
      args.offset <= args.totalLength, UInt64(args.data.count) <= args.totalLength - args.offset,
      args.checksum.count == InstallSnapshot.Args.checksumLength
    else { throw RaftServiceError.invalidState }
    advanceClock()
    guard instance.receiveInstallSnapshot(args) else { return snapshotReply(args, next: 0, installed: false) }
    let delay = instance.getNextDelay()
    try await process(persisted.currentTerm == instance.currentTerm
      ? [.scheduleNext(delay: delay)] : [.persist, .scheduleNext(delay: delay)])
    if args.lastIncludedIndex <= instance.commitIndex {
      let covered = args.lastIncludedIndex < instance.logBaseIndex
        || instance.term(at: args.lastIncludedIndex) == args.lastIncludedTerm
      return snapshotReply(args, next: covered ? args.totalLength : 0, installed: covered)
    }

    if incoming?.matches(args) != true {
      guard args.offset == 0 else { return snapshotReply(args, next: 0, installed: false) }
      incoming = IncomingSnapshot(args)
    }
    guard var staged = incoming else { throw RaftServiceError.invalidState }
    incoming = nil
    if args.offset == UInt64(staged.bytes.count) { staged.bytes.append(args.data) }
    let count = UInt64(staged.bytes.count)
    guard count == args.totalLength else {
      incoming = staged
      return snapshotReply(args, next: count, installed: false)
    }
    guard Data(SHA256.hash(data: staged.bytes)) == args.checksum else { throw RaftServiceError.invalidState }
    let received = try JSONDecoder().decode(RaftSnapshot.self, from: staged.bytes)
    guard received.lastIncludedIndex == args.lastIncludedIndex, received.lastIncludedTerm == args.lastIncludedTerm else {
      throw RaftServiceError.invalidState
    }
    let contents = try RaftStorage.contents(of: received)
    var proposed = instance
    guard proposed.installSnapshot(received) else { return snapshotReply(args, next: 0, installed: false) }
    do {
      try await withTaskCancellationShield {
        let state = proposed.persistentState
        try await storage.persist(state, replacing: persisted, install: contents)
        instance = proposed
        persisted = state
        snapshot = SnapshotTransfer(
          index: args.lastIncludedIndex, term: args.lastIncludedTerm, bytes: staged.bytes, checksum: args.checksum)
        nextSnapshotAt = Self.snapshotDeadline(after: args.lastIncludedIndex)
      }
    } catch {
      fail()
      throw error
    }
    return snapshotReply(args, next: count, installed: true)
  }

  private func snapshotReply(_ args: InstallSnapshot.Args, next: UInt64, installed: Bool) -> InstallSnapshot.Reply {
    .init(
      term: instance.currentTerm, rpcId: args.rpcId, lastIncludedIndex: args.lastIncludedIndex,
      nextOffset: next, installed: installed)
  }

  func stop() async {
    await acquireTransition()
    defer { releaseTransition() }
    guard lifecycle != .closed else { return }
    lifecycle = .closed
    timer?.cancel()
    joining?.cancel()
    replication.values.forEach { $0.cancel() }
    transport = nil
    try? await storage.close()
  }

  private func requireRunning() throws {
    guard lifecycle == .running else { throw RaftServiceError.stopped }
  }

  private func advanceClock() {
    let now = ContinuousClock.now
    instance.elapse(lastEvent.duration(to: now))
    lastEvent = now
  }

  private func process(_ effects: [TimerDirective]) async throws {
    do {
      try await withTaskCancellationShield {
        for effect in effects {
          switch effect {
          case .persist:
            let state = instance.persistentState
            try await storage.persist(state, replacing: persisted, install: nil)
            persisted = state
          case .apply(let entry, let index):
            _ = try await storage.apply(entry, at: index, beforeCommit: nil)
          case .scheduleNext(let delay): reschedule(after: delay)
          case .requestVote(let peer, let request): requestVote(from: peer, request: request)
          case .sendAppendEntry(let peer, let request): replicate(to: peer, request: request)
          }
        }
        if instance.lastApplied >= nextSnapshotAt {
          try await compact()
        }
      }
    } catch {
      fail()
      throw error
    }
  }

  private func fail() {
    lifecycle = .failed
    timer?.cancel()
    joining?.cancel()
    replication.values.forEach { $0.cancel() }
  }

  private static func snapshotDeadline(after index: LogIndex) -> LogIndex {
    index + min(snapshotInterval, LogIndex.max - index)
  }

  private func compact() async throws {
    let index = instance.lastApplied
    nextSnapshotAt = Self.snapshotDeadline(after: index)
    guard let term = instance.term(at: index) else { throw RaftServiceError.invalidState }
    do {
      let saved = try await storage.makeSnapshot(index: index, term: term, membership: instance.membership(at: index))
      let bytes = try RaftStorage.encodeSnapshot(saved)
      guard bytes.count <= RaftStorage.snapshotLimit else { return }
      var proposed = instance
      proposed.recordSnapshot(saved)
      proposed.compactLog()
      let state = proposed.persistentState
      try await storage.persist(state, replacing: persisted, install: nil)
      instance = proposed
      persisted = state
      snapshot = SnapshotTransfer(index: index, term: term, bytes: bytes, checksum: Data(SHA256.hash(data: bytes)))
    } catch RaftServiceError.snapshotTooLarge {
      return
    }
  }

  private func reschedule(after delay: Duration) {
    guard transport != nil else { return }
    timer?.cancel()
    timerID &+= 1
    let id = timerID
    let deadline = ContinuousClock.now.advanced(by: delay)
    timer = Task { [weak self] in
      do { try await ContinuousClock().sleep(until: deadline) } catch { return }
      await self?.tick(id: id)
    }
  }

  private func tick(id: UInt64) async {
    await acquireTransition()
    defer { releaseTransition() }
    guard lifecycle == .running, timerID == id, !Task.isCancelled else { return }
    guard instance.currentTerm < Term.max else {
      fail()
      return
    }
    advanceClock()
    tryJoin()
    let effects = instance.onTimerTick()
    do {
      try await process(effects)
      try await promote()
    } catch {}
  }

  private func tryJoin() {
    guard joining == nil, !instance.membership.members.contains(instance.id), let transport else { return }
    let id = instance.id
    let group = self.group
    joining = Task { [weak self] in
      await transport.join(peer: id, group: group)
      await self?.joinFinished()
    }
  }

  private func joinFinished() {
    joining = nil
  }

  private func requestVote(from peer: PeerId, request: RequestVote.Args) {
    guard let transport else { return }
    let group = self.group
    Task { [weak self] in
      guard let reply = try? await transport.vote(peer: peer, group: group, request: request) else { return }
      await self?.voteFinished(peer: peer, request: request, reply: reply)
    }
  }

  private func voteFinished(peer: PeerId, request: RequestVote.Args, reply: RequestVote.Reply) async {
    await acquireTransition()
    defer { releaseTransition() }
    guard lifecycle == .running, reply.term < Term.max else { return }
    advanceClock()
    let effects = instance.receiveRequestVoteReply(peer, request, reply).map { action -> TimerDirective in
      switch action {
      case .persist: .persist
      case .sendAppendEntry(let peer, let request): .sendAppendEntry(to: peer, args: request)
      case .scheduleNext(let delay): .scheduleNext(delay: delay)
      }
    }
    try? await process(effects)
  }

  private func replicate(to peer: PeerId, request: AppendEntries.Args) {
    guard instance.role == .leader, replication[peer] == nil, let transport else { return }
    nextRPC &+= 1
    let group = self.group
    if instance.needsSnapshot(for: peer), let snapshot {
      let args = InstallSnapshot.Args(
        term: request.term, rpcId: nextRPC, leaderId: request.leaderId, lastIncludedIndex: snapshot.index,
        lastIncludedTerm: snapshot.term, offset: 0, totalLength: UInt64(snapshot.bytes.count),
        checksum: snapshot.checksum, data: Data())
      replication[peer] = Task { [weak self] in
        let reply = try? await transport.install(peer: peer, group: group, image: snapshot.bytes, request: args)
        await self?.snapshotFinished(peer: peer, request: args, reply: reply)
      }
    } else {
      let args = request.withRPCId(nextRPC, traceHeaders: [:])
      replication[peer] = Task { [weak self] in
        let reply = try? await transport.append(peer: peer, group: group, request: args)
        await self?.appendFinished(peer: peer, request: args, reply: reply)
      }
    }
  }

  private func appendFinished(peer: PeerId, request: AppendEntries.Args, reply: AppendEntries.Reply?) async {
    await acquireTransition()
    defer { releaseTransition() }
    replication.removeValue(forKey: peer)
    guard lifecycle == .running, let reply, reply.term < Term.max, reply.rpcId == request.rpcId else { return }
    advanceClock()
    await replicationFinished(instance.receiveAppendEntriesReply(peer, request, reply))
  }

  private func snapshotFinished(peer: PeerId, request: InstallSnapshot.Args, reply: InstallSnapshot.Reply?) async {
    await acquireTransition()
    defer { releaseTransition() }
    replication.removeValue(forKey: peer)
    guard lifecycle == .running, let reply, reply.term < Term.max else { return }
    advanceClock()
    await replicationFinished(instance.receiveInstallSnapshotReply(from: peer, sent: request, reply: reply))
  }

  private func replicationFinished(_ actions: [AppendEntries.Reply.Action]) async {
    let effects = actions.map { action -> TimerDirective in
      switch action {
      case .persist: .persist
      case .apply(let entry, let index): .apply(entry: entry, atIndex: index)
      case .sendAppendEntry(let peer, let request): .sendAppendEntry(to: peer, args: request)
      case .scheduleNext(let delay): .scheduleNext(delay: delay)
      }
    }
    do {
      try await process(effects)
      try await promote()
    } catch {}
  }

  private func promote() async throws {
    var proposed = instance
    let effects = proposed.promoteCaughtUpReaders()
    guard !effects.isEmpty else { return }
    guard let entry = proposed.log.last else { throw RaftServiceError.invalidState }
    try RaftStorage.validateLog(entry, index: proposed.lastLogIndex)
    instance = proposed
    try await process(effects)
  }

  // Persist and apply a transition before the next event can observe it.
  private func acquireTransition() async {
    if !inTransition {
      inTransition = true
    } else {
      await withCheckedContinuation { waiters.append($0) }
    }
  }

  private func releaseTransition() {
    if waiters.isEmpty {
      inTransition = false
    } else {
      waiters.removeFirst().resume()
    }
  }
}
