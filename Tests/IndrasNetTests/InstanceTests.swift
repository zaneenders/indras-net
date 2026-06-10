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

    let directives = instance.onTimerTick(at: ContinuousClock.now)

    #expect(instance.role == .candidate)
    #expect(instance.currentTerm == 1)
    #expect(instance.votedFor == "a")
    #expect(instance.votes == ["a": true])
    #expect(directives.filter { if case .scheduleNext = $0 { false } else { true } }.count == 2)
    #expect(
      directives.contains(
        .requestVote(
          to: "b",
          args: RequestVote.Args(term: 1, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
    #expect(
      directives.contains(
        .requestVote(
          to: "c",
          args: RequestVote.Args(term: 1, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
    let scheduleNext = directives.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func candidateTimerFiredRestartsElection() {
    var instance = Instance(id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 1, votes: ["a": true])

    let directives = instance.onTimerTick(at: ContinuousClock.now)

    #expect(instance.role == .candidate)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == "a")
    #expect(instance.votes == ["a": true])
    #expect(directives.filter { if case .scheduleNext = $0 { false } else { true } }.count == 2)
    #expect(
      directives.contains(
        .requestVote(
          to: "b",
          args: RequestVote.Args(term: 2, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
    #expect(
      directives.contains(
        .requestVote(
          to: "c",
          args: RequestVote.Args(term: 2, candidateId: "a", lostLogIndex: 0, lastLogTerm: 0))))
    let scheduleNext = directives.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func leaderRejectsRequestVote() {
    var instance = Instance(id: "leader", peers: ["b"], role: .leader, currentTerm: 2)
    let request = RequestVote.Args(term: 2, candidateId: "b", lostLogIndex: 0, lastLogTerm: 0)

    let actions = instance.receiveRequestVote("b", request, at: ContinuousClock.now)

    #expect(instance.role == .leader)
    #expect(actions == [.sendRequestVoteReply(to: "b", term: 2, voteGranted: false)])
  }

  @Test func leaderTimerFiredSendsHeartbeats() {
    var instance = Instance(id: "a", peers: ["b", "c"], role: .leader, currentTerm: 2)

    let directives = instance.onTimerTick(at: ContinuousClock.now)

    #expect(instance.role == .leader)
    #expect(instance.currentTerm == 2)
    #expect(
      directives.compactMap {
        if case .scheduleNext(let delay) = $0 { delay } else { nil }
      }.last == NodeTiming.default.heartbeatInterval)
    #expect(directives.filter { if case .scheduleNext = $0 { false } else { true } }.count == 2)
    #expect(
      directives.contains(.sendAppendEntry(to: "b", args: AppendEntries.Args(term: 2, leaderId: "a"))))
    #expect(
      directives.contains(.sendAppendEntry(to: "c", args: AppendEntries.Args(term: 2, leaderId: "a"))))
  }

  @Test func initialTimerDelayReturnsElectionTimeoutForFollower() {
    let instance = Instance(id: "a", peers: ["b"])
    let delay = instance.initialTimerDelay(at: ContinuousClock.now)
    let timing = NodeTiming.default

    #expect(
      delay >= .milliseconds(timing.electionTimeoutRange.lowerBound)
        && delay < .milliseconds(timing.electionTimeoutRange.upperBound))
  }

  @Test func timeUntilNextTimerReturnsHeartbeatIntervalForLeader() {
    var instance = Instance(id: "a", peers: ["b"], role: .leader, currentTerm: 1)

    let delay = instance.onTimerTick(at: ContinuousClock.now).compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last

    #expect(delay == NodeTiming.default.heartbeatInterval)
  }

  @Test func grantsVoteToFirstCandidateInTerm() {
    var instance = Instance(id: "follower", currentTerm: 1)

    let request = RequestVote.Args(
      term: 1, candidateId: "candidate", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.votedFor == "candidate")
    #expect(actions.contains(.sendRequestVoteReply(to: "candidate", term: 1, voteGranted: true)))
    let scheduleNext = actions.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func rejectsVoteForSecondCandidateInSameTerm() {
    var instance = Instance(id: "follower", currentTerm: 1, votedFor: "first")

    let request = RequestVote.Args(term: 1, candidateId: "second", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("second", request, at: ContinuousClock.now)

    #expect(instance.votedFor == "first")
    #expect(actions == [.sendRequestVoteReply(to: "second", term: 1, voteGranted: false)])
  }

  @Test func rejectsVoteFromStaleTerm() {
    var instance = Instance(id: "follower", currentTerm: 3)

    let request = RequestVote.Args(term: 2, candidateId: "candidate", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.votedFor == nil)
    #expect(actions == [.sendRequestVoteReply(to: "candidate", term: 3, voteGranted: false)])
  }

  @Test func stepsDownWhenSeeingHigherTermOnVoteRequest() {
    var instance = Instance(
      id: "follower", role: .candidate, currentTerm: 1, votes: ["follower": true])

    let request = RequestVote.Args(term: 3, candidateId: "candidate", lostLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 3)
    #expect(instance.votedFor == "candidate")
    #expect(instance.votes.isEmpty)
    let scheduleNext = actions.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func candidateBecomesLeaderWithMajorityVotes() {
    var instance = Instance(
      id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 1, votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1), at: ContinuousClock.now)

    #expect(instance.role == .leader)
    #expect(instance.votes == ["a": true, "b": true])
    #expect(actions.contains(.sendAppendEntry(to: "b", args: AppendEntries.Args(term: 1, leaderId: "a"))))
    #expect(actions.contains(.sendAppendEntry(to: "c", args: AppendEntries.Args(term: 1, leaderId: "a"))))
    #expect(
      actions.compactMap {
        if case .scheduleNext(let delay) = $0 { delay } else { nil }
      }.last == NodeTiming.default.heartbeatInterval)
  }

  @Test func ignoresVoteReplyWhenNotCandidate() {
    var instance = Instance(id: "a", peers: ["b", "c"], currentTerm: 1)

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1), at: ContinuousClock.now)

    #expect(actions.isEmpty)
    #expect(instance.role == .follower)
    #expect(instance.votes.isEmpty)
  }

  @Test func ignoresVoteReplyFromStaleTerm() {
    var instance = Instance(
      id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 2, votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1), at: ContinuousClock.now)

    #expect(actions.isEmpty)
    #expect(instance.role == .candidate)
    #expect(instance.votes == ["a": true])
  }

  @Test func stepsDownWhenVoteReplyHasHigherTerm() {
    var instance = Instance(
      id: "a", role: .candidate, currentTerm: 1, votedFor: "a", votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: false, term: 2), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    let scheduleNext = actions.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func appendEntriesFromLeaderResetsElectionTimeout() {
    var instance = Instance(id: "follower", role: .candidate, currentTerm: 2)

    let actions = instance.receiveAppendEntries(
      "leader", .init(term: 2, leaderId: "leader"), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(actions.contains(.sendAppendEntriesReply(to: "leader", term: 2, success: true)))
    let scheduleNext = actions.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func rejectsStaleAppendEntries() {
    var instance = Instance(id: "follower", role: .candidate, currentTerm: 5)

    let actions = instance.receiveAppendEntries(
      "leader", .init(term: 3, leaderId: "leader"), at: ContinuousClock.now)

    #expect(actions == [.sendAppendEntriesReply(to: "leader", term: 5, success: false)])
    #expect(instance.role == .candidate)
    #expect(instance.currentTerm == 5)
  }

  @Test func appendEntriesWithHigherTermUpdatesFollower() {
    var instance = Instance(
      id: "follower", role: .candidate, currentTerm: 1, votedFor: "a", votes: ["a": true])

    let actions = instance.receiveAppendEntries(
      "leader", .init(term: 4, leaderId: "leader"), at: ContinuousClock.now)

    #expect(instance.currentTerm == 4)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(instance.role == .follower)
    #expect(actions.contains(.sendAppendEntriesReply(to: "leader", term: 4, success: true)))
    let scheduleNext = actions.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }

  @Test func stepsDownWhenAppendEntriesReplyHasHigherTerm() {
    var instance = Instance(id: "a", peers: ["b"], role: .leader, currentTerm: 1)

    let actions = instance.receiveAppendEntriesReply(
      "b", .init(term: 2, success: false), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    let scheduleNext = actions.compactMap {
      if case .scheduleNext(let delay) = $0 { delay } else { nil }
    }.last
    let timing = NodeTiming.default
    #expect(
      scheduleNext.map {
        $0 >= .milliseconds(timing.electionTimeoutRange.lowerBound)
          && $0 < .milliseconds(timing.electionTimeoutRange.upperBound)
      } == true)
  }
}
