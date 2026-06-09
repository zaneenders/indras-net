struct Instance {
  static let heartbeatInterval = Duration.milliseconds(50)
  static let electionTimeoutRange: Range<Int64> = 150..<300

  let id: PeerId
  private(set) var role: Role
  private(set) var currentTerm: Term
  private(set) var votedFor: PeerId?
  private(set) var peers: Set<PeerId>
  private(set) var votes: [PeerId: Bool]
  private(set) var electionTimeout: Duration

  init(
    id: PeerId,
    peers: Set<PeerId> = [],
    role: Role = .follower,
    currentTerm: Term = 0,
    votedFor: PeerId? = nil,
    votes: [PeerId: Bool] = [:],
    electionTimeout: Duration = Instance.randomElectionTimeout()
  ) {
    self.id = id
    self.peers = peers
    self.role = role
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.votes = votes
    self.electionTimeout = electionTimeout
  }

  mutating func getNextTimeout() -> Duration {
    resetElectionTimeout()
    return timerSleepDuration()
  }

  mutating func onElectionTimeout() -> TimerTick {
    switch role {
    case .leader:
      var actions: [TimerAction] = []
      let heartbeat = AppendEntries.Args(term: currentTerm, leaderId: id)
      for peer in peers {
        actions.append(.sendAppendEntry(to: peer, args: heartbeat))
      }
      return TimerTick(sleep: Self.heartbeatInterval, actions: actions)
    case .follower:
      let actions = convertToCandidate()
      resetElectionTimeout()
      return TimerTick(sleep: electionTimeout, actions: actions)
    case .candidate:
      let actions = convertToCandidate()
      resetElectionTimeout()
      return TimerTick(sleep: electionTimeout, actions: actions)
    }
  }

  mutating func resetElectionTimeout() {
    electionTimeout = Self.randomElectionTimeout()
  }

  private static func randomElectionTimeout() -> Duration {
    Duration(.milliseconds(Int64.random(in: electionTimeoutRange)))
  }

  private func timerSleepDuration() -> Duration {
    switch role {
    case .leader: Self.heartbeatInterval
    case .follower, .candidate: electionTimeout
    }
  }

  mutating func onRequestVote(_ peer: PeerId, _ requst: RequestVote.Args) -> [RequestVote.Args.Action] {
    var actions: [RequestVote.Args.Action] = []
    var grantVote = false

    if requst.term > currentTerm {
      currentTerm = requst.term
      votedFor = nil
      votes = [:]
      role = .follower
    }

    if requst.term < currentTerm {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if role == .leader || (role == .candidate && requst.candidateId != id) {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if votedFor == nil || votedFor == requst.candidateId {
      // TODO: At least as upto date
      grantVote = true
      votedFor = requst.candidateId
    }
    actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
    return actions
  }

  private mutating func convertToCandidate() -> [TimerAction] {
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

  mutating func onRequestVoteReply(_ peer: PeerId, _ reply: RequestVote.Reply) -> [RequestVote.Reply.Action] {
    var actions: [RequestVote.Reply.Action] = []

    if reply.term > currentTerm {
      self.role = .follower
      self.currentTerm = reply.term
      self.votedFor = nil
      self.votes = [:]
      return actions
    }

    guard role == .candidate, reply.term == currentTerm else { return actions }

    self.votes[peer] = reply.granted
    if self.votes.isLeader(peers.count) {
      self.role = .leader
      let heartbeat = AppendEntries.Args(term: currentTerm, leaderId: id)
      for peer in peers {
        actions.append(.sendAppendEntry(to: peer, args: heartbeat))
      }
    }

    return actions
  }

  mutating func onAppendEntries(_ leader: PeerId, _ args: AppendEntries.Args) -> [AppendEntries.Args.Action] {
    var actions: [AppendEntries.Args.Action] = []

    if args.term < currentTerm {
      return actions
    }

    if args.term > currentTerm {
      currentTerm = args.term
      votedFor = nil
      votes = [:]
    }

    role = .follower
    actions.append(.resetElectionTimeout)
    return actions
  }
}
