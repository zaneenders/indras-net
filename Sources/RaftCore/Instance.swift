import Foundation
public import OrderedCollections

/// Synchronous consensus transitions. The caller serializes access and executes effects
/// in order, completing durable persistence before subsequent sends or application.
/// Commands are opaque bytes; transport and application execution belong to the caller.
public struct Instance: Sendable {

  public let id: PeerId
  public private(set) var role: Role
  public private(set) var currentTerm: Term
  public private(set) var votedFor: PeerId?
  public let initialMembership: ClusterMembership
  private var leaderContactRemaining: Duration = .zero
  public private(set) var votes: OrderedDictionary<PeerId, Bool>
  public private(set) var log: [LogEntry]
  public private(set) var logBaseIndex: LogIndex
  public private(set) var snapshot: RaftSnapshot?
  public private(set) var commitIndex: LogIndex
  public private(set) var lastApplied: LogIndex
  public private(set) var leaderId: PeerId?
  private var nextIndex: OrderedDictionary<PeerId, LogIndex>
  private var matchIndex: OrderedDictionary<PeerId, LogIndex>
  public let timing: NodeTiming
  private var rng: any RandomNumberGenerator & Sendable

  public var logBaseTerm: Term { log[0].term }
  public var lastLogIndex: LogIndex { logBaseIndex + log.lastLogIndex }
  public var lastLogTerm: Term { log.lastLogTerm }
  public var persistentState: PersistentState {
    PersistentState(
      currentTerm: currentTerm, votedFor: votedFor, log: log, commitIndex: commitIndex,
      logBaseIndex: logBaseIndex, snapshot: snapshot, initialMembership: initialMembership)
  }

  public init(
    id: PeerId,
    peers: OrderedSet<PeerId>,
    role: Role,
    currentTerm: Term,
    votedFor: PeerId?,
    votes: OrderedDictionary<PeerId, Bool>,
    commitIndex: LogIndex,
    lastApplied: LogIndex,
    log: [LogEntry],
    logBaseIndex: LogIndex,
    snapshot: RaftSnapshot?,
    initialMembership: ClusterMembership?,
    timing: NodeTiming,
    rng: any RandomNumberGenerator & Sendable
  ) {
    self.id = id
    self.initialMembership =
      initialMembership ?? ClusterMembership(voters: OrderedSet([id]).union(peers), oldVoters: nil, readers: [])
    self.role = role
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.votes = votes
    self.log = log
    self.logBaseIndex = logBaseIndex
    self.snapshot = snapshot
    self.commitIndex = commitIndex
    self.lastApplied = lastApplied
    self.nextIndex = [:]
    self.matchIndex = [:]
    self.timing = timing
    self.rng = rng
  }

  /// The newest configuration takes effect as soon as it is in the local log,
  /// including while uncommitted (Raft §6). Deriving it also handles truncation.
  public var membership: ClusterMembership { membership(at: lastLogIndex) }
  public var committedMembership: ClusterMembership { membership(at: commitIndex) }

  public func membership(at index: LogIndex) -> ClusterMembership {
    precondition(index >= logBaseIndex && index <= lastLogIndex)
    var result =
      log[0].membership
      ?? (snapshot?.lastIncludedIndex == logBaseIndex ? snapshot?.membership : nil)
      ?? initialMembership
    for entry in log.dropFirst().prefix(Int(index - logBaseIndex)) {
      if let membership = entry.membership { result = membership }
    }
    return result
  }

  private var membershipIndex: LogIndex {
    logBaseIndex + LogIndex(log.lastIndex(where: { $0.membership != nil }) ?? 0)
  }

  /// Replication targets include readers and the previous configuration until
  /// a final configuration commits. Voting uses only `membership.allVoters`.
  public var peers: OrderedSet<PeerId> {
    var members = membership.members
    if membershipIndex > commitIndex, !membership.isJoint {
      members.formUnion(membership(at: membershipIndex - 1).members)
    }
    members.remove(id)
    return members
  }

  /// Adds a non-voting reader. Admit it with the same bootstrap configuration on
  /// every node; a new node must not bootstrap itself into the voter set.
  public mutating func addReader(_ peer: PeerId) throws -> [TimerDirective] {
    try requireStableMembership()
    guard !membership.members.contains(peer) else { throw MembershipChangeError.invalidConfiguration }
    return try appendMembership(
      ClusterMembership(voters: membership.voters, oldVoters: nil, readers: membership.readers.union([peer])))
  }

  /// Removes a reader without changing the voting quorum.
  public mutating func removeReader(_ peer: PeerId) throws -> [TimerDirective] {
    try requireStableMembership()
    guard membership.readers.contains(peer) else { throw MembershipChangeError.invalidConfiguration }
    return try appendMembership(
      ClusterMembership(voters: membership.voters, oldVoters: nil, readers: membership.readers.subtracting([peer])))
  }

  /// Promotes/removes voters through joint consensus. Every new voter
  /// must first be a reader acknowledged through the pre-change log tail.
  public mutating func changeMembership(to voters: OrderedSet<PeerId>) throws -> [TimerDirective] {
    try requireStableMembership()
    guard Set(voters) != Set(membership.voters) else { throw MembershipChangeError.invalidConfiguration }
    let next = ClusterMembership(
      voters: voters, oldVoters: membership.voters,
      readers: membership.readers.subtracting(voters))
    for peer in voters.subtracting(membership.voters) {
      guard membership.readers.contains(peer), let matched = matchIndex[peer], matched >= lastLogIndex else {
        throw MembershipChangeError.readerNotCaughtUp(peer)
      }
    }
    return try appendMembership(next)
  }

  /// Start with three voters. Once formed, the group can promote each learner
  /// as it catches up, including when only one learner is ready.
  public mutating func promoteCaughtUpReaders() -> [TimerDirective] {
    guard role == .leader, !membership.isJoint, membershipIndex <= commitIndex else { return [] }
    let count = membership.voters.count == 1 ? 2 : 1
    let ready = membership.readers.filter { matchIndex[$0].map { $0 >= lastLogIndex } ?? false }
    guard ready.count >= count else { return [] }
    return (try? changeMembership(to: membership.voters.union(ready.prefix(count)))) ?? []
  }

  private func requireStableMembership() throws {
    guard role == .leader else { throw MembershipChangeError.notLeader }
    guard !membership.isJoint, membershipIndex <= commitIndex else { throw MembershipChangeError.changeInProgress }
  }

  private mutating func appendMembership(_ next: ClusterMembership) throws -> [TimerDirective] {
    do { try next.validate() } catch { throw MembershipChangeError.invalidConfiguration }
    let entry = LogEntry(term: currentTerm, membership: next)
    let longest = next.members.union([id]).max { $0.utf8.count < $1.utf8.count } ?? id
    let headerLength = AppendEntries.Args.basePayloadLength(leaderId: longest)
    guard headerLength + entry.encodedLength <= Int(RaftLimits.maximumPayloadLength),
      log.dropFirst().allSatisfy({ headerLength + $0.encodedLength <= Int(RaftLimits.maximumPayloadLength) })
    else { throw MembershipChangeError.invalidConfiguration }
    let previousPeers = peers
    log.append(entry)
    reconcileReplication(previousPeers: previousPeers)
    let applied = advanceCommitIndex()
    var actions: [TimerDirective] = [.persist]
    actions += applied.map { .apply(entry: $0.1, atIndex: $0.0) }
    actions += peers.map { .sendAppendEntry(to: $0, args: makeAppendEntries(for: $0)) }
    actions.append(.scheduleNext(delay: getNextDelay()))
    return actions
  }

  private mutating func reconcileReplication(previousPeers: OrderedSet<PeerId>) {
    let currentPeers = peers
    for peer in currentPeers.subtracting(previousPeers) {
      // An added reader starts with no trusted progress in this leadership term.
      nextIndex[peer] = lastLogIndex + 1
      matchIndex.removeValue(forKey: peer)
    }
    for peer in previousPeers.subtracting(currentPeers) {
      nextIndex.removeValue(forKey: peer)
      matchIndex.removeValue(forKey: peer)
    }
  }

  // Array offset zero is the retained boundary, not necessarily Raft index zero.
  public func entry(at index: LogIndex) -> LogEntry? {
    guard index >= logBaseIndex, index <= lastLogIndex else { return nil }
    return log[Int(index - logBaseIndex)]
  }

  public func term(at index: LogIndex) -> Term? { entry(at: index)?.term }

  public func needsSnapshot(for peer: PeerId) -> Bool {
    nextIndex[peer, default: lastLogIndex + 1] <= logBaseIndex
  }

  public mutating func recordSnapshot(_ snapshot: RaftSnapshot) {
    precondition(snapshot.lastIncludedIndex <= commitIndex)
    precondition(term(at: snapshot.lastIncludedIndex) == snapshot.lastIncludedTerm)
    let boundaryConfiguration = membership(at: logBaseIndex)
    let configuration = membership(at: snapshot.lastIncludedIndex)
    precondition(snapshot.membership == nil || snapshot.membership == configuration)
    if logBaseIndex > 0 { log[0] = LogEntry(term: logBaseTerm, membership: boundaryConfiguration) }
    self.snapshot = RaftSnapshot(
      lastIncludedIndex: snapshot.lastIncludedIndex, lastIncludedTerm: snapshot.lastIncludedTerm,
      applicationState: snapshot.applicationState, requests: snapshot.requests, membership: configuration)
  }

  public mutating func compactLog() {
    guard let snapshot, let configuration = snapshot.membership, snapshot.lastIncludedIndex > logBaseIndex else {
      return
    }
    let offset = Int(snapshot.lastIncludedIndex - logBaseIndex)
    log = [LogEntry(term: snapshot.lastIncludedTerm, membership: configuration)] + log.dropFirst(offset + 1)
    logBaseIndex = snapshot.lastIncludedIndex
  }

  @discardableResult
  public mutating func installSnapshot(_ snapshot: RaftSnapshot) -> Bool {
    guard let configuration = snapshot.membership, snapshot.lastIncludedIndex > commitIndex,
      snapshot.lastIncludedTerm <= currentTerm
    else { return false }
    do { try snapshot.validate() } catch { return false }
    let suffix: [LogEntry]
    if term(at: snapshot.lastIncludedIndex) == snapshot.lastIncludedTerm {
      suffix = Array(log.dropFirst(Int(snapshot.lastIncludedIndex - logBaseIndex) + 1))
    } else {
      suffix = []
    }
    log = [LogEntry(term: snapshot.lastIncludedTerm, membership: configuration)] + suffix
    logBaseIndex = snapshot.lastIncludedIndex
    self.snapshot = snapshot
    commitIndex = snapshot.lastIncludedIndex
    lastApplied = commitIndex
    return true
  }

  public mutating func receiveInstallSnapshot(_ args: InstallSnapshot.Args) -> Bool {
    guard args.term >= currentTerm else { return false }
    if args.term > currentTerm {
      currentTerm = args.term
      votedFor = nil
      votes = [:]
    }
    role = .follower
    leaderId = args.leaderId
    leaderContactRemaining = .milliseconds(timing.electionTimeoutRange.lowerBound)
    return true
  }

  public mutating func receiveInstallSnapshotReply(
    from peer: PeerId, sent: InstallSnapshot.Args, reply: InstallSnapshot.Reply
  ) -> [AppendEntries.Reply.Action] {
    guard reply.rpcId == sent.rpcId, reply.lastIncludedIndex == sent.lastIncludedIndex,
      reply.installed || reply.term > currentTerm
    else { return [] }
    let boundary = AppendEntries.Args(
      term: sent.term, leaderId: sent.leaderId, prevLogIndex: sent.lastIncludedIndex,
      prevLogTerm: sent.lastIncludedTerm, entries: [], leaderCommit: commitIndex, rpcId: sent.rpcId, traceHeaders: [:])
    return receiveAppendEntriesReply(
      peer, boundary,
      .init(term: reply.term, success: reply.installed, rpcId: reply.rpcId, compactedPrefix: nil))
  }

  public mutating func proposeReadBarrier() -> [TimerDirective] {
    guard role == .leader, membership.allVoters.contains(id), lastLogIndex < LogIndex.max - 1 else { return [] }
    log.append(LogEntry(term: currentTerm, command: Data(), request: nil, isNoOp: true))
    let applied = advanceCommitIndex()
    var actions: [TimerDirective] = [.persist]
    actions += applied.map { .apply(entry: $0.1, atIndex: $0.0) }
    actions += peers.map { .sendAppendEntry(to: $0, args: makeAppendEntries(for: $0)) }
    return actions
  }

  /// Advances the minimum-election-timeout leader-contact window without firing
  /// a timer. A host with a monotonic clock supplies elapsed time before inputs;
  /// deterministic drivers can advance time without changing terms or votes.
  public mutating func elapse(_ duration: Duration) {
    precondition(duration >= .zero)
    leaderContactRemaining = max(.zero, leaderContactRemaining - duration)
  }

  public mutating func onTimerTick() -> [TimerDirective] {
    var directives: [TimerDirective] = []
    switch role {
    case .leader:
      let previousCommit = commitIndex
      let previousLast = lastLogIndex
      let applied = advanceCommitIndex()
      if commitIndex != previousCommit || lastLogIndex != previousLast { directives.append(.persist) }
      directives += applied.map { .apply(entry: $0.1, atIndex: $0.0) }
      for peer in peers {
        let args = makeAppendEntries(for: peer)
        directives.append(.sendAppendEntry(to: peer, args: args))
      }
    case .follower, .candidate:
      leaderContactRemaining = .zero
      if membership.allVoters.contains(id) { directives = convertToCandidate() }
    }
    directives.append(.scheduleNext(delay: getNextDelay()))
    return directives
  }

  public mutating func getNextDelay() -> Duration {
    switch role {
    case .leader:
      return timing.heartbeatInterval
    case .follower, .candidate:
      return .milliseconds(Int64.random(in: timing.electionTimeoutRange, using: &rng))
    }
  }

  public mutating func receiveClientSubmit(
    _ client: PeerId,
    _ args: ClientSubmit.Args
  ) -> [ClientSubmit.Args.Action] {
    let command = args.command
    let requestId = args.requestId
    guard role == .leader else {
      return [
        .sendClientSubmitReply(
          to: client,
          reply: ClientSubmit.Reply(
            requestId: requestId,
            status: .notLeader,
            leaderId: leaderId, logIndex: 0, result: nil
          ))
      ]
    }

    let request = RequestIdentity(client: client, requestId: requestId)
    if let existing = log.firstIndex(where: { $0.request == request }) {
      let index = logBaseIndex + LogIndex(existing)
      if index <= lastApplied {
        return [
          .sendClientSubmitReply(
            to: client,
            reply: .init(requestId: requestId, status: .ok, leaderId: nil, logIndex: index, result: nil))
        ]
      }
      var actions: [ClientSubmit.Args.Action] = [
        .clientWriteAppended(logIndex: index, requestId: requestId, client: client)
      ]
      let previousCommit = commitIndex
      let previousLast = lastLogIndex
      let applied = advanceCommitIndex()
      if commitIndex != previousCommit || lastLogIndex != previousLast { actions.insert(.persist, at: 0) }
      actions += applied.map { .apply(entry: $0.1, atIndex: $0.0) }
      for peer in peers {
        actions.append(.sendAppendEntry(to: peer, args: makeAppendEntries(for: peer)))
      }
      return actions
    }
    let entry = LogEntry(term: currentTerm, command: command, request: request, isNoOp: false)
    let largestLeaderId = ([id] + Array(peers)).max { $0.utf8.count < $1.utf8.count } ?? id
    guard
      AppendEntries.Args.basePayloadLength(leaderId: largestLeaderId) + entry.encodedLength
        <= Int(RaftLimits.maximumPayloadLength)
    else {
      return [
        .sendClientSubmitReply(
          to: client,
          reply: .init(requestId: requestId, status: .commandTooLarge, leaderId: nil, logIndex: 0, result: nil))
      ]
    }
    log.append(entry)

    var actions: [ClientSubmit.Args.Action] = [
      .persist,
      .clientWriteAppended(logIndex: lastLogIndex, requestId: requestId, client: client)
    ]
    for peer in peers {
      let args = makeAppendEntries(for: peer)
      actions.append(.sendAppendEntry(to: peer, args: args))
    }
    for (index, entry) in advanceCommitIndex() {
      actions.append(.apply(entry: entry, atIndex: index))
    }
    return actions
  }

  public mutating func receiveRequestVote(
    _ peer: PeerId,
    _ args: RequestVote.Args
  ) -> [RequestVote.Args.Action] {
    var actions: [RequestVote.Args.Action] = []
    var needsPersistence = false

    // A recently contacted follower ignores disruptive elections from removed
    // servers before inspecting their term (§6). The host advances monotonic time.
    if leaderContactRemaining > .zero || role == .leader {
      return [.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: false)]
    }
    if args.term > currentTerm {
      currentTerm = args.term
      votedFor = nil
      votes = [:]
      role = .follower
      leaderId = nil
      needsPersistence = true
    }

    // A promoted reader may not have received the configuration entry yet.
    // Grant by term/log freshness; the candidate counts only its configuration's voters.
    let grantVote =
      args.term == currentTerm
      && role != .leader && (role != .candidate || args.candidateId == id)
      && (votedFor == nil || votedFor == args.candidateId)
      && (args.lastLogTerm, args.lastLogIndex) >= (lastLogTerm, lastLogIndex)
    if grantVote && votedFor != args.candidateId {
      votedFor = args.candidateId
      needsPersistence = true
    }
    if needsPersistence {
      actions.append(.persist)
    }
    actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
    if grantVote {
      actions.append(.scheduleNext(delay: getNextDelay()))
    }
    return actions
  }

  public mutating func receiveRequestVoteReply(
    _ peer: PeerId,
    _ sent: RequestVote.Args,
    _ reply: RequestVote.Reply
  ) -> [RequestVote.Reply.Action] {
    var actions: [RequestVote.Reply.Action] = []
    guard peers.contains(peer) else { return actions }

    if reply.term > currentTerm {
      role = .follower
      currentTerm = reply.term
      votedFor = nil
      votes = [:]
      leaderId = nil
      actions.append(.persist)
      actions.append(.scheduleNext(delay: getNextDelay()))
      return actions
    }

    guard role == .candidate, reply.term == currentTerm, sent.term == currentTerm else { return actions }

    guard membership.allVoters.contains(peer), membership.allVoters.contains(id) else { return actions }
    if votes[peer] != true { votes[peer] = reply.granted }
    if membership.hasQuorum(Set(votes.compactMap { $0.value ? $0.key : nil })) {
      becomeLeader()
      let previousLast = lastLogIndex
      appendFinalMembershipIfCommitted()
      _ = appendLeadershipBarrier()
      if lastLogIndex != previousLast { actions.append(.persist) }
      for follower in peers {
        let args = makeAppendEntries(for: follower)
        actions.append(.sendAppendEntry(to: follower, args: args))
      }
      actions.append(.scheduleNext(delay: timing.heartbeatInterval))
    }

    return actions
  }

  public mutating func receiveAppendEntries(
    _ peer: PeerId,
    _ args: AppendEntries.Args
  ) -> [AppendEntries.Args.Action] {
    var actions: [AppendEntries.Args.Action] = []
    var needsPersistence = false

    if args.term < currentTerm {
      actions.append(.sendAppendEntriesReply(to: peer, term: currentTerm, success: false, compactedPrefix: nil))
      return actions
    }

    if args.term > currentTerm {
      currentTerm = args.term
      votedFor = nil
      votes = [:]
      needsPersistence = true
    }

    role = .follower
    leaderId = args.leaderId
    leaderContactRemaining = .milliseconds(timing.electionTimeoutRange.lowerBound)

    guard term(at: args.prevLogIndex) == args.prevLogTerm else {
      if needsPersistence {
        actions.append(.persist)
      }
      let compactedPrefix: AppendEntries.CompactedPrefix? =
        args.prevLogIndex < logBaseIndex ? .init(index: logBaseIndex, term: logBaseTerm) : nil
      actions.append(
        .sendAppendEntriesReply(to: peer, term: currentTerm, success: false, compactedPrefix: compactedPrefix))
      actions.append(.scheduleNext(delay: getNextDelay()))
      return actions
    }

    if !args.entries.isEmpty {
      log.appendReplicationEntries(prevLogIndex: args.prevLogIndex - logBaseIndex, entries: args.entries)
      needsPersistence = true
    }
    let newCommit = max(commitIndex, min(args.leaderCommit, args.replicatedThrough))
    if newCommit != commitIndex {
      commitIndex = newCommit
      needsPersistence = true
    }
    if needsPersistence { actions.append(.persist) }
    for (index, entry) in drainAppliedEntries() {
      actions.append(.apply(entry: entry, atIndex: index))
    }

    actions.append(.sendAppendEntriesReply(to: peer, term: currentTerm, success: true, compactedPrefix: nil))
    actions.append(.scheduleNext(delay: getNextDelay()))
    return actions
  }

  public mutating func receiveAppendEntriesReply(
    _ peer: PeerId,
    _ sent: AppendEntries.Args,
    _ reply: AppendEntries.Reply
  ) -> [AppendEntries.Reply.Action] {
    var actions: [AppendEntries.Reply.Action] = []
    guard peers.contains(peer) else { return actions }

    if reply.term > currentTerm {
      role = .follower
      currentTerm = reply.term
      votedFor = nil
      votes = [:]
      leaderId = nil
      actions.append(.persist)
      actions.append(.scheduleNext(delay: getNextDelay()))
      return actions
    }

    guard role == .leader, reply.term == currentTerm, sent.term == currentTerm else { return actions }

    if reply.success {
      let acknowledged = sent.replicatedThrough
      guard acknowledged <= lastLogIndex else { return actions }
      if matchIndex[peer] == nil || acknowledged > matchIndex[peer, default: 0] {
        matchIndex[peer] = acknowledged
        nextIndex[peer] = acknowledged + 1
      } else {
        // A duplicate snapshot acknowledgement must still repair a backed-off probe.
        nextIndex[peer] = max(nextIndex[peer, default: lastLogIndex + 1], matchIndex[peer, default: 0] + 1)
      }
      let previousCommit = commitIndex
      let previousLast = lastLogIndex
      let applied = advanceCommitIndex()
      if commitIndex != previousCommit || lastLogIndex != previousLast { actions.append(.persist) }
      for (index, entry) in applied {
        actions.append(.apply(entry: entry, atIndex: index))
      }
      if commitIndex != previousCommit || lastLogIndex != previousLast {
        for follower in peers {
          actions.append(.sendAppendEntry(to: follower, args: makeAppendEntries(for: follower)))
        }
      } else if nextIndex[peer, default: lastLogIndex + 1] <= lastLogIndex {
        actions.append(.sendAppendEntry(to: peer, args: makeAppendEntries(for: peer)))
      }
    } else {
      let matched = matchIndex[peer, default: 0]
      guard matched == 0 || sent.replicatedThrough > matched else { return actions }
      guard sent.prevLogIndex + 1 == nextIndex[peer, default: lastLogIndex + 1],
        sent.prevLogIndex >= matchIndex[peer, default: 0]
      else { return actions }
      let currentNext = nextIndex[peer, default: lastLogIndex + 1]
      if let prefix = reply.compactedPrefix, prefix.index > sent.prevLogIndex,
        prefix.index < LogIndex.max, term(at: prefix.index) == prefix.term
      {
        // The hint chooses a probe, not a match or commit. Verify the boundary
        // with the next AppendEntries before crediting any replicated entries.
        nextIndex[peer] = max(matched + 1, prefix.index + 1)
      } else {
        nextIndex[peer] = max(matched + 1, currentNext - 1)
      }
      let args = makeAppendEntries(for: peer)
      actions.append(.sendAppendEntry(to: peer, args: args))
    }

    return actions
  }

  private mutating func becomeLeader() {
    role = .leader
    leaderId = id
    nextIndex = OrderedDictionary(uniqueKeysWithValues: peers.map { ($0, lastLogIndex + 1) })
    matchIndex = [:]
  }

  private mutating func appendLeadershipBarrier() -> Bool {
    guard lastLogIndex > commitIndex, lastLogTerm < currentTerm else { return false }
    log.append(LogEntry(term: currentTerm, command: Data(), request: nil, isNoOp: true))
    return true
  }

  private mutating func convertToCandidate() -> [TimerDirective] {
    currentTerm += 1
    role = .candidate
    leaderId = nil
    votedFor = id
    votes = [id: true]

    if membership.hasQuorum([id]) {
      becomeLeader()
      appendFinalMembershipIfCommitted()
      _ = appendLeadershipBarrier()
      return [.persist] + advanceCommitIndex().map { .apply(entry: $0.1, atIndex: $0.0) }
    }

    return [.persist]
      + membership.allVoters.filter { $0 != id }.map { peer in
        .requestVote(
          to: peer,
          args: RequestVote.Args(
            term: currentTerm,
            candidateId: id,
            lastLogIndex: lastLogIndex,
            lastLogTerm: lastLogTerm
          ))
      }
  }

  private func makeAppendEntries(for peer: PeerId) -> AppendEntries.Args {
    let next = max(logBaseIndex + 1, nextIndex[peer, default: lastLogIndex + 1])
    let prevIndex = next - 1
    let prevTerm = term(at: prevIndex)!
    let entries: [LogEntry]
    if next <= lastLogIndex {
      var batch: [LogEntry] = []
      var length = AppendEntries.Args.basePayloadLength(leaderId: id)
      for index in next...lastLogIndex {
        let entry = entry(at: index)!
        guard length + entry.encodedLength <= Int(RaftLimits.maximumPayloadLength) else { break }
        batch.append(entry)
        length += entry.encodedLength
      }
      entries = batch
    } else {
      entries = []
    }
    return AppendEntries.Args(
      term: currentTerm,
      leaderId: id,
      prevLogIndex: prevIndex,
      prevLogTerm: prevTerm,
      entries: entries,
      leaderCommit: commitIndex, rpcId: 0, traceHeaders: [:]
    )
  }

  private mutating func appendFinalMembershipIfCommitted() {
    guard membership.isJoint, membershipIndex <= commitIndex else { return }
    log.append(
      LogEntry(
        term: currentTerm,
        membership: ClusterMembership(
          voters: membership.voters, oldVoters: nil, readers: membership.readers)))
  }

  private mutating func advanceCommitIndex() -> [(LogIndex, LogEntry)] {
    let previousPeers = peers
    while role == .leader {
      let configuration = membership
      if lastLogIndex > commitIndex {
        for index in (commitIndex + 1)...lastLogIndex {
          guard term(at: index) == currentTerm else { continue }
          var replicated: Set<PeerId> = []
          if configuration.allVoters.contains(id) { replicated.insert(id) }
          for peer in configuration.allVoters where matchIndex[peer, default: 0] >= index {
            replicated.insert(peer)
          }
          if configuration.hasQuorum(replicated) { commitIndex = index }
        }
      }
      let previousLast = lastLogIndex
      appendFinalMembershipIfCommitted()
      if previousLast == lastLogIndex { break }
    }
    // A removed incumbent drives the final entry but cannot count itself and
    // relinquishes leadership only once that final configuration is committed.
    if !membership.isJoint, membershipIndex <= commitIndex, !membership.voters.contains(id) {
      role = .follower
      leaderId = nil
      votes = [:]
    }
    reconcileReplication(previousPeers: previousPeers)
    return drainAppliedEntries()
  }

  private mutating func drainAppliedEntries() -> [(LogIndex, LogEntry)] {
    var entries: [(LogIndex, LogEntry)] = []
    while lastApplied < commitIndex {
      lastApplied += 1
      entries.append((lastApplied, entry(at: lastApplied)!))
    }
    return entries
  }
}
