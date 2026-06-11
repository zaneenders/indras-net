import Foundation

struct Instance {

  let id: PeerId
  private(set) var role: Role
  private(set) var currentTerm: Term
  private(set) var votedFor: PeerId?
  private(set) var peers: Set<PeerId>
  private(set) var votes: [PeerId: Bool]
  private(set) var log: [LogEntry]
  private(set) var commitIndex: LogIndex
  private(set) var lastApplied: LogIndex
  private(set) var leaderId: PeerId?
  private var nextIndex: [PeerId: LogIndex]
  private var matchIndex: [PeerId: LogIndex]
  private var lastSentEndIndex: [PeerId: LogIndex]
  private var pendingClientRequests: [LogIndex: (requestId: UInt128, client: PeerId)]
  let timing: NodeTiming

  var lastLogIndex: LogIndex { log.lastLogIndex }
  var lastLogTerm: Term { log.lastLogTerm }

  init(
    id: PeerId,
    peers: Set<PeerId> = [],
    role: Role = .follower,
    currentTerm: Term = 0,
    votedFor: PeerId? = nil,
    votes: [PeerId: Bool] = [:],
    commitIndex: LogIndex = 0,
    lastApplied: LogIndex = 0,
    log: [LogEntry] = .sentinel,
    timing: NodeTiming = .default
  ) {
    self.id = id
    self.peers = peers
    self.role = role
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.votes = votes
    self.log = log
    self.commitIndex = commitIndex
    self.lastApplied = lastApplied
    self.nextIndex = [:]
    self.matchIndex = [:]
    self.lastSentEndIndex = [:]
    self.pendingClientRequests = [:]
    self.timing = timing
  }

  mutating func onTimerTick(at now: ContinuousClock.Instant = .now) -> [TimerDirective] {
    var directives: [TimerDirective] = []
    switch role {
    case .leader:
      for peer in peers {
        let args = makeAppendEntries(for: peer)
        lastSentEndIndex[peer] = args.prevLogIndex + LogIndex(args.entries.count)
        directives.append(.sendAppendEntry(to: peer, args: args))
      }
    case .follower, .candidate:
      directives = convertToCandidate()
    }
    directives.append(.scheduleNext(delay: getNextDelay(at: now)))
    return directives
  }

  func getNextDelay(at now: ContinuousClock.Instant) -> Duration {
    switch role {
    case .leader:
      return timing.heartbeatInterval
    case .follower, .candidate:
      return Duration(.milliseconds(Int64.random(in: timing.electionTimeoutRange)))
    }
  }

  private mutating func submit(
    _ command: Data,
    requestId: UInt128,
    from client: PeerId,
    at now: ContinuousClock.Instant = .now
  ) -> [ClientSubmit.Args.Action] {
    guard role == .leader else {
      return [
        .sendClientSubmitReply(
          to: client,
          reply: ClientSubmit.Reply(
            requestId: requestId,
            status: .notLeader,
            leaderId: leaderId
          ))
      ]
    }

    log.append(LogEntry(term: currentTerm, command: command))
    pendingClientRequests[lastLogIndex] = (requestId: requestId, client: client)

    var actions: [ClientSubmit.Args.Action] = []
    for peer in peers {
      let args = makeAppendEntries(for: peer)
      lastSentEndIndex[peer] = args.prevLogIndex + LogIndex(args.entries.count)
      actions.append(.sendAppendEntry(to: peer, args: args))
    }
    return actions
  }

  mutating func receiveClientSubmit(
    _ client: PeerId,
    _ args: ClientSubmit.Args,
    at now: ContinuousClock.Instant = .now
  ) -> [ClientSubmit.Args.Action] {
    submit(args.command, requestId: args.requestId, from: client, at: now)
  }

  mutating func receiveRequestVote(
    _ peer: PeerId,
    _ request: RequestVote.Args,
    at now: ContinuousClock.Instant = .now
  ) -> [RequestVote.Args.Action] {
    var actions: [RequestVote.Args.Action] = []
    var grantVote = false
    var shouldResetElectionTimer = false

    if request.term > currentTerm {
      currentTerm = request.term
      votedFor = nil
      votes = [:]
      role = .follower
      leaderId = nil
      shouldResetElectionTimer = true
    }

    if request.term < currentTerm {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if role == .leader || (role == .candidate && request.candidateId != id) {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if votedFor == nil || votedFor == request.candidateId {
      let upToDate = isLogUpToDate(
        candidateLastIndex: request.lastLogIndex,
        candidateLastTerm: request.lastLogTerm,
        receiverLastIndex: lastLogIndex,
        receiverLastTerm: lastLogTerm
      )
      if upToDate {
        grantVote = true
        votedFor = request.candidateId
        shouldResetElectionTimer = true
        actions.append(.persist)
      }
    }
    actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
    if shouldResetElectionTimer {
      actions.append(.scheduleNext(delay: getNextDelay(at: now)))
    }
    return actions
  }

  mutating func receiveRequestVoteReply(
    _ peer: PeerId,
    _ reply: RequestVote.Reply,
    at now: ContinuousClock.Instant = .now
  ) -> [RequestVote.Reply.Action] {
    var actions: [RequestVote.Reply.Action] = []

    if reply.term > currentTerm {
      role = .follower
      currentTerm = reply.term
      votedFor = nil
      votes = [:]
      leaderId = nil
      actions.append(.scheduleNext(delay: getNextDelay(at: now)))
      return actions
    }

    guard role == .candidate, reply.term == currentTerm else { return actions }

    votes[peer] = reply.granted
    if votes.isLeader(peers.count) {
      becomeLeader(&actions)
    }

    return actions
  }

  mutating func receiveAppendEntries(
    _ leader: PeerId,
    _ args: AppendEntries.Args,
    at now: ContinuousClock.Instant = .now
  ) -> [AppendEntries.Args.Action] {
    var actions: [AppendEntries.Args.Action] = []

    if args.term < currentTerm {
      actions.append(.sendAppendEntriesReply(to: leader, term: currentTerm, success: false))
      return actions
    }

    if args.term > currentTerm {
      currentTerm = args.term
      votedFor = nil
      votes = [:]
      actions.append(.persist)
    }

    role = .follower
    leaderId = args.leaderId

    guard log.matches(prevLogIndex: args.prevLogIndex, prevLogTerm: args.prevLogTerm) else {
      actions.append(.sendAppendEntriesReply(to: leader, term: currentTerm, success: false))
      return actions
    }

    if !args.entries.isEmpty {
      log.appendReplicationEntries(prevLogIndex: args.prevLogIndex, entries: args.entries)
      actions.append(.persist)
    }

    if args.leaderCommit > commitIndex {
      commitIndex = min(args.leaderCommit, lastLogIndex)
      for entry in drainAppliedEntries() {
        actions.append(.apply(entry: entry))
      }
    }

    actions.append(.sendAppendEntriesReply(to: leader, term: currentTerm, success: true))
    actions.append(.scheduleNext(delay: getNextDelay(at: now)))
    return actions
  }

  mutating func receiveAppendEntriesReply(
    _ peer: PeerId,
    _ reply: AppendEntries.Reply,
    at now: ContinuousClock.Instant = .now
  ) -> [AppendEntries.Reply.Action] {
    var actions: [AppendEntries.Reply.Action] = []

    if reply.term > currentTerm {
      role = .follower
      currentTerm = reply.term
      votedFor = nil
      votes = [:]
      leaderId = nil
      actions.append(.scheduleNext(delay: getNextDelay(at: now)))
      return actions
    }

    guard role == .leader, reply.term == currentTerm else { return actions }

    if reply.success {
      let endIndex = lastSentEndIndex[peer, default: 0]
      matchIndex[peer] = endIndex
      nextIndex[peer] = endIndex + 1
      actions.append(contentsOf: advanceCommitIndex())
    } else {
      let currentNext = nextIndex[peer, default: lastLogIndex + 1]
      nextIndex[peer] = max(1, currentNext - 1)
      let args = makeAppendEntries(for: peer)
      lastSentEndIndex[peer] = args.prevLogIndex + LogIndex(args.entries.count)
      actions.append(.sendAppendEntry(to: peer, args: args))
    }

    return actions
  }

  private mutating func becomeLeader(_ actions: inout [RequestVote.Reply.Action]) {
    role = .leader
    leaderId = id
    nextIndex = Dictionary(uniqueKeysWithValues: peers.map { ($0, lastLogIndex + 1) })
    matchIndex = Dictionary(uniqueKeysWithValues: peers.map { ($0, LogIndex(0)) })
    lastSentEndIndex = [:]

    for peer in peers {
      let args = makeAppendEntries(for: peer)
      lastSentEndIndex[peer] = args.prevLogIndex + LogIndex(args.entries.count)
      actions.append(.sendAppendEntry(to: peer, args: args))
    }
    actions.append(.scheduleNext(delay: timing.heartbeatInterval))
  }

  private mutating func convertToCandidate() -> [TimerDirective] {
    currentTerm += 1
    role = .candidate
    leaderId = nil
    votedFor = id
    votes = [id: true]

    return peers.map { peer in
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
    let next = nextIndex[peer, default: lastLogIndex + 1]
    let prevIndex = next - 1
    let prevTerm = log[Int(prevIndex)].term
    let entries: [LogEntry]
    if next <= lastLogIndex {
      entries = Array(log[Int(next)...Int(lastLogIndex)])
    } else {
      entries = []
    }
    return AppendEntries.Args(
      term: currentTerm,
      leaderId: id,
      prevLogIndex: prevIndex,
      prevLogTerm: prevTerm,
      entries: entries,
      leaderCommit: commitIndex
    )
  }

  private mutating func advanceCommitIndex() -> [AppendEntries.Reply.Action] {
    var actions: [AppendEntries.Reply.Action] = []
    let clusterSize = peers.count + 1

    if lastLogIndex > commitIndex {
      for index in (commitIndex + 1)...lastLogIndex {
        guard log[Int(index)].term == currentTerm else { continue }
        var replicated = 1
        for peer in peers where matchIndex[peer, default: 0] >= index {
          replicated += 1
        }
        if replicated * 2 > clusterSize {
          commitIndex = index
        }
      }
    }

    drainCommittedEntries(into: &actions)
    return actions
  }

  private mutating func drainAppliedEntries() -> [LogEntry] {
    var entries: [LogEntry] = []
    while lastApplied < commitIndex {
      lastApplied += 1
      entries.append(log[Int(lastApplied)])
    }
    return entries
  }

  private mutating func drainCommittedEntries(into actions: inout [AppendEntries.Reply.Action]) {
    while lastApplied < commitIndex {
      lastApplied += 1
      let index = lastApplied
      actions.append(.apply(entry: log[Int(index)]))
      if let pending = pendingClientRequests.removeValue(forKey: index) {
        actions.append(
          .notifyClient(requestId: pending.requestId, logIndex: index, to: pending.client))
      }
    }
  }
}
