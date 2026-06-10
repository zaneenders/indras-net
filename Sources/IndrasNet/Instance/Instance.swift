struct Instance {

  let id: PeerId
  private(set) var role: Role
  private(set) var currentTerm: Term
  private(set) var votedFor: PeerId?
  private(set) var peers: Set<PeerId>
  private(set) var votes: [PeerId: Bool]
  let timing: NodeTiming

  init(
    id: PeerId,
    peers: Set<PeerId> = [],
    role: Role = .follower,
    currentTerm: Term = 0,
    votedFor: PeerId? = nil,
    votes: [PeerId: Bool] = [:],
    timing: NodeTiming = .default
  ) {
    self.id = id
    self.peers = peers
    self.role = role
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.votes = votes
    self.timing = timing
  }

  mutating func onTimerTick(at now: ContinuousClock.Instant = .now) -> [TimerDirective] {
    var directives: [TimerDirective] = []
    switch role {
    case .leader:
      let heartbeat = AppendEntries.Args(term: currentTerm, leaderId: id)
      for peer in peers {
        directives.append(.sendAppendEntry(to: peer, args: heartbeat))
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
      // TODO: At least as upto date
      grantVote = true
      votedFor = request.candidateId
      shouldResetElectionTimer = true
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
      actions.append(.scheduleNext(delay: getNextDelay(at: now)))
      return actions
    }

    guard role == .candidate, reply.term == currentTerm else { return actions }

    votes[peer] = reply.granted
    if votes.isLeader(peers.count) {
      role = .leader
      let heartbeat = AppendEntries.Args(term: currentTerm, leaderId: id)
      for peer in peers {
        actions.append(.sendAppendEntry(to: peer, args: heartbeat))
      }
      actions.append(.scheduleNext(delay: timing.heartbeatInterval))
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
    }

    role = .follower
    actions.append(.sendAppendEntriesReply(to: leader, term: currentTerm, success: true))
    actions.append(.scheduleNext(delay: getNextDelay(at: now)))
    return actions
  }

  mutating func receiveAppendEntriesReply(
    _ peer: PeerId,
    _ reply: AppendEntries.Reply,
    at now: ContinuousClock.Instant = .now
  ) -> [AppendEntries.Reply.Action] {
    if reply.term > currentTerm {
      role = .follower
      currentTerm = reply.term
      votedFor = nil
      votes = [:]
      return [.scheduleNext(delay: getNextDelay(at: now))]
    }

    return []
  }

  private mutating func convertToCandidate() -> [TimerDirective] {
    currentTerm += 1
    role = .candidate
    votedFor = id
    votes = [id: true]

    return peers.map { peer in
      .requestVote(
        to: peer,
        args: RequestVote.Args(
          term: currentTerm, candidateId: id, lostLogIndex: 0, lastLogTerm: 0))
    }
  }
}
