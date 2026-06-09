import Testing

@testable import IndrasNet

@Suite struct InstanceTests {
  @Test func startsAsFollowerInTermZero() {
    let instance = Instance(id: "a")

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 0)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(instance.peers.isEmpty)
  }

  @Test func followerTimerFiredStartsCandidacyAndRequestsVotesFromPeers() {
    var instance = Instance(id: "a", peers: ["b", "c"])

    let tick = instance.onElectionTimeout()

    #expect(instance.role == .candidate)
    #expect(instance.currentTerm == 1)
    #expect(instance.votedFor == "a")
    #expect(instance.votes == ["a": true])
    #expect(tick.actions.count == 2)
    #expect(
      tick.actions.contains(
        .requestVote(
          to: "b",
          args: RequestVote.Args(term: 1, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
    #expect(
      tick.actions.contains(
        .requestVote(
          to: "c",
          args: RequestVote.Args(term: 1, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
  }

  @Test func candidateTimerFiredRestartsElection() {
    var instance = Instance(id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 1, votes: ["a": true])

    let tick = instance.onElectionTimeout()

    #expect(instance.role == .candidate)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == "a")
    #expect(instance.votes == ["a": true])
    #expect(tick.actions.count == 2)
    #expect(
      tick.actions.contains(
        .requestVote(
          to: "b",
          args: RequestVote.Args(term: 2, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
    #expect(
      tick.actions.contains(
        .requestVote(
          to: "c",
          args: RequestVote.Args(term: 2, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
  }

  @Test func leaderRejectsRequestVote() {
    var instance = Instance(id: "leader", peers: ["b"], role: .leader, currentTerm: 2)
    let request = RequestVote.Args(term: 2, candidateId: "b", lostLogIndex: 0, lastLogTerm: 0)

    let actions = instance.receiveRequestVote("b", request)

    #expect(instance.role == .leader)
    #expect(actions == [.sendRequestVoteReply(to: "b", term: 2, voteGranted: false)])
  }

  @Test func leaderTimerFiredSendsHeartbeats() {
    var instance = Instance(id: "a", peers: ["b", "c"], role: .leader, currentTerm: 2)

    let tick = instance.onElectionTimeout()

    #expect(instance.role == .leader)
    #expect(instance.currentTerm == 2)
    #expect(tick.sleep == NodeTiming.default.heartbeatInterval)
    #expect(tick.actions.count == 2)
    #expect(
      tick.actions.contains(.sendAppendEntry(to: "b", args: AppendEntries.Args(term: 2, leaderId: "a"))))
    #expect(
      tick.actions.contains(.sendAppendEntry(to: "c", args: AppendEntries.Args(term: 2, leaderId: "a"))))
  }

  @Test func prepareTimerReturnsHeartbeatIntervalForLeader() {
    var instance = Instance(id: "a", peers: ["b"], role: .leader, currentTerm: 1)

    let sleep = instance.getNextTimeout()

    #expect(sleep == NodeTiming.default.heartbeatInterval)
  }

  @Test func prepareTimerReturnsElectionTimeoutForFollower() {
    var instance = Instance(id: "a", peers: ["b"])

    let sleep = instance.getNextTimeout()

    let timing = NodeTiming.default
    #expect(sleep == instance.nextTimeout)
    #expect(sleep >= .milliseconds(timing.electionTimeoutRange.lowerBound))
    #expect(sleep < .milliseconds(timing.electionTimeoutRange.upperBound))
  }

  @Test func grantsVoteToFirstCandidateInTerm() {
    var instance = Instance(id: "follower", currentTerm: 1)

    let request = RequestVote.Args(
      term: 1, candidateId: "candidate", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request)

    #expect(instance.votedFor == "candidate")
    #expect(actions == [.sendRequestVoteReply(to: "candidate", term: 1, voteGranted: true)])
  }

  @Test func rejectsVoteForSecondCandidateInSameTerm() {
    var instance = Instance(id: "follower", currentTerm: 1, votedFor: "first")

    let request = RequestVote.Args(term: 1, candidateId: "second", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("second", request)

    #expect(instance.votedFor == "first")
    #expect(actions == [.sendRequestVoteReply(to: "second", term: 1, voteGranted: false)])
  }

  @Test func rejectsVoteFromStaleTerm() {
    var instance = Instance(id: "follower", currentTerm: 3)

    let request = RequestVote.Args(term: 2, candidateId: "candidate", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request)

    #expect(instance.votedFor == nil)
    #expect(actions == [.sendRequestVoteReply(to: "candidate", term: 3, voteGranted: false)])
  }

  @Test func stepsDownWhenSeeingHigherTermOnVoteRequest() {
    var instance = Instance(
      id: "follower", role: .candidate, currentTerm: 1, votes: ["follower": true])

    let request = RequestVote.Args(term: 3, candidateId: "candidate", lostLogIndex: 0, lastLogTerm: 0)
    _ = instance.receiveRequestVote("candidate", request)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 3)
    #expect(instance.votedFor == "candidate")
    #expect(instance.votes.isEmpty)
  }

  @Test func candidateBecomesLeaderWithMajorityVotes() {
    var instance = Instance(
      id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 1, votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1))

    #expect(instance.role == .leader)
    #expect(instance.votes == ["a": true, "b": true])
    #expect(actions.count == 2)
    #expect(
      actions.contains(.sendAppendEntry(to: "b", args: AppendEntries.Args(term: 1, leaderId: "a"))))
    #expect(
      actions.contains(.sendAppendEntry(to: "c", args: AppendEntries.Args(term: 1, leaderId: "a"))))
  }

  @Test func ignoresVoteReplyWhenNotCandidate() {
    var instance = Instance(id: "a", peers: ["b", "c"], currentTerm: 1)

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1))

    #expect(actions.isEmpty)
    #expect(instance.role == .follower)
    #expect(instance.votes.isEmpty)
  }

  @Test func ignoresVoteReplyFromStaleTerm() {
    var instance = Instance(
      id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 2, votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1))

    #expect(actions.isEmpty)
    #expect(instance.role == .candidate)
    #expect(instance.votes == ["a": true])
  }

  @Test func stepsDownWhenVoteReplyHasHigherTerm() {
    var instance = Instance(
      id: "a", role: .candidate, currentTerm: 1, votedFor: "a", votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: false, term: 2))

    #expect(actions.isEmpty)
    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
  }

  @Test func appendEntriesFromLeaderResetsElectionTimeout() {
    var instance = Instance(id: "follower", role: .candidate, currentTerm: 2)

    let actions = instance.receiveAppendEntries("leader", .init(term: 2, leaderId: "leader"))

    #expect(instance.role == .follower)
    #expect(actions == [.resetElectionTimeout])
  }

  @Test func ignoresStaleAppendEntries() {
    var instance = Instance(id: "follower", role: .candidate, currentTerm: 5)

    let actions = instance.receiveAppendEntries("leader", .init(term: 3, leaderId: "leader"))

    #expect(actions.isEmpty)
    #expect(instance.role == .candidate)
    #expect(instance.currentTerm == 5)
  }

  @Test func appendEntriesWithHigherTermUpdatesFollower() {
    var instance = Instance(
      id: "follower", role: .candidate, currentTerm: 1, votedFor: "a", votes: ["a": true])

    let actions = instance.receiveAppendEntries("leader", .init(term: 4, leaderId: "leader"))

    #expect(instance.currentTerm == 4)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(instance.role == .follower)
    #expect(actions == [.resetElectionTimeout])
  }
}
