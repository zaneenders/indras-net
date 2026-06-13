import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct InstanceTests {
  private let electionTimeout = InstanceTestSupport.electionTimeout()

  @Test func startsAsFollowerInTermZero() {
    let instance = Instance(id: "a")

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 0)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(instance.peers.isEmpty)
  }

  @Test func followerTimerFiredStartsCandidacyAndRequestsVotesFromPeers() {
    var instance = Instance.forTests(id: "a", peers: ["b", "c"])

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
          args: RequestVote.Args(term: 1, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0))))
    #expect(
      directives.contains(
        .requestVote(
          to: "c",
          args: RequestVote.Args(term: 1, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0))))
    #expect(directives.scheduledDelay == electionTimeout)
  }

  @Test func candidateTimerFiredRestartsElection() {
    var instance = Instance.forTests(
      id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 1, votes: ["a": true])

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
          args: RequestVote.Args(term: 2, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0))))
    #expect(
      directives.contains(
        .requestVote(
          to: "c",
          args: RequestVote.Args(term: 2, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0))))
    #expect(directives.scheduledDelay == electionTimeout)
  }

  @Test func leaderRejectsRequestVote() {
    var instance = Instance(id: "leader", peers: ["b"], role: .leader, currentTerm: 2)
    let request = RequestVote.Args(term: 2, candidateId: "b", lastLogIndex: 0, lastLogTerm: 0)

    let actions = instance.receiveRequestVote("b", request, at: ContinuousClock.now)

    #expect(instance.role == .leader)
    #expect(actions == [.sendRequestVoteReply(to: "b", term: 2, voteGranted: false)])
  }

  @Test func leaderTimerFiredSendsHeartbeats() {
    var instance = Instance(id: "a", peers: ["b", "c"], role: .leader, currentTerm: 2)

    let directives = instance.onTimerTick(at: ContinuousClock.now)

    #expect(instance.role == .leader)
    #expect(instance.currentTerm == 2)
    #expect(directives.scheduledDelay == NodeTiming.default.heartbeatInterval)
    #expect(directives.filter { if case .scheduleNext = $0 { false } else { true } }.count == 2)
    #expect(
      directives.contains(.sendAppendEntry(to: "b", args: AppendEntries.Args(term: 2, leaderId: "a"))))
    #expect(
      directives.contains(.sendAppendEntry(to: "c", args: AppendEntries.Args(term: 2, leaderId: "a"))))
  }

  @Test func initialTimerDelayReturnsElectionTimeoutForFollower() {
    var instance = Instance.forTests(id: "a", peers: ["b"])

    let delay = instance.getNextDelay()

    #expect(delay == electionTimeout)
  }

  @Test func timeUntilNextTimerReturnsHeartbeatIntervalForLeader() {
    var instance = Instance.forTests(id: "a", peers: ["b"], role: .leader, currentTerm: 1)

    let delay = instance.onTimerTick(at: ContinuousClock.now).scheduledDelay

    #expect(delay == NodeTiming.default.heartbeatInterval)
  }

  @Test func grantsVoteToFirstCandidateInTerm() {
    var instance = Instance.forTests(id: "follower", currentTerm: 1)

    let request = RequestVote.Args(
      term: 1, candidateId: "candidate", lastLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.votedFor == "candidate")
    #expect(actions.contains(.sendRequestVoteReply(to: "candidate", term: 1, voteGranted: true)))
    #expect(actions.scheduledDelay == electionTimeout)
  }

  @Test func rejectsVoteForSecondCandidateInSameTerm() {
    var instance = Instance(id: "follower", currentTerm: 1, votedFor: "first")

    let request = RequestVote.Args(term: 1, candidateId: "second", lastLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("second", request, at: ContinuousClock.now)

    #expect(instance.votedFor == "first")
    #expect(actions == [.sendRequestVoteReply(to: "second", term: 1, voteGranted: false)])
  }

  @Test func rejectsVoteFromStaleTerm() {
    var instance = Instance(id: "follower", currentTerm: 3)

    let request = RequestVote.Args(term: 2, candidateId: "candidate", lastLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.votedFor == nil)
    #expect(actions == [.sendRequestVoteReply(to: "candidate", term: 3, voteGranted: false)])
  }

  @Test func stepsDownWhenSeeingHigherTermOnVoteRequest() {
    var instance = Instance.forTests(
      id: "follower", role: .candidate, currentTerm: 1, votes: ["follower": true])

    let request = RequestVote.Args(term: 3, candidateId: "candidate", lastLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 3)
    #expect(instance.votedFor == "candidate")
    #expect(instance.votes.isEmpty)
    #expect(actions.scheduledDelay == electionTimeout)
  }

  @Test func candidateBecomesLeaderWithMajorityVotes() {
    var instance = Instance(
      id: "a", peers: ["b", "c"], role: .candidate, currentTerm: 1, votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: true, term: 1), at: ContinuousClock.now)

    #expect(instance.role == .leader)
    #expect(instance.votes == ["a": true, "b": true])
    #expect(actions.contains(.sendAppendEntry(to: "b", args: AppendEntries.Args(term: 1, leaderId: "a"))))
    #expect(actions.contains(.sendAppendEntry(to: "c", args: AppendEntries.Args(term: 1, leaderId: "a"))))
    #expect(actions.scheduledDelay == NodeTiming.default.heartbeatInterval)
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
    var instance = Instance.forTests(
      id: "a", role: .candidate, currentTerm: 1, votedFor: "a", votes: ["a": true])

    let actions = instance.receiveRequestVoteReply("b", .init(granted: false, term: 2), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(actions.scheduledDelay == electionTimeout)
  }

  @Test func appendEntriesFromLeaderResetsElectionTimeout() {
    var instance = Instance.forTests(id: "follower", role: .candidate, currentTerm: 2)

    let actions = instance.receiveAppendEntries(
      "leader", .init(term: 2, leaderId: "leader"), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(actions.contains(.sendAppendEntriesReply(to: "leader", term: 2, success: true)))
    #expect(actions.scheduledDelay == electionTimeout)
  }

  @Test func sameTermAppendEntriesFromOtherLeaderStepsDownToFollower() {
    var instance = Instance(id: "a", peers: ["b", "c"], role: .leader, currentTerm: 2)

    let actions = instance.receiveAppendEntries(
      "b", .init(term: 2, leaderId: "b"), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 2)
    #expect(actions.contains(.sendAppendEntriesReply(to: "b", term: 2, success: true)))
  }

  @Test func majorityRequiresMoreThanHalfOfCluster() {
    #expect([PeerId: Bool]().isLeader(2) == false)
    #expect(["a": true].isLeader(2) == false)
    #expect(["a": true, "b": true].isLeader(2) == true)
    #expect(["a": true, "b": true].isLeader(3) == false)
    #expect(["a": true, "b": true, "c": true].isLeader(3) == true)
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
    var instance = Instance.forTests(
      id: "follower", role: .candidate, currentTerm: 1, votedFor: "a", votes: ["a": true])

    let actions = instance.receiveAppendEntries(
      "leader", .init(term: 4, leaderId: "leader"), at: ContinuousClock.now)

    #expect(instance.currentTerm == 4)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(instance.role == .follower)
    #expect(actions.contains(.sendAppendEntriesReply(to: "leader", term: 4, success: true)))
    #expect(actions.scheduledDelay == electionTimeout)
  }

  @Test func rejectsVoteWhenCandidateLogIsStale() {
    let entry = LogEntry(term: 1, command: Data("x".utf8))
    var instance = Instance(id: "follower", currentTerm: 1, log: .sentinel + [entry])

    let request = RequestVote.Args(term: 1, candidateId: "candidate", lastLogIndex: 0, lastLogTerm: 0)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.votedFor == nil)
    #expect(actions.contains(.sendRequestVoteReply(to: "candidate", term: 1, voteGranted: false)))
  }

  @Test func grantsVoteWhenCandidateLogIsMoreUpToDate() {
    let entry = LogEntry(term: 2, command: Data("x".utf8))
    var instance = Instance(id: "follower", currentTerm: 1, log: .sentinel + [entry])

    let request = RequestVote.Args(term: 1, candidateId: "candidate", lastLogIndex: 1, lastLogTerm: 2)
    let actions = instance.receiveRequestVote("candidate", request, at: ContinuousClock.now)

    #expect(instance.votedFor == "candidate")
    #expect(actions.contains(.sendRequestVoteReply(to: "candidate", term: 1, voteGranted: true)))
  }

  @Test func appendEntriesReplicatesLogEntries() {
    var instance = Instance(id: "follower", currentTerm: 1)
    let entries = [LogEntry(term: 1, command: Data("cmd".utf8))]

    let actions = instance.receiveAppendEntries(
      "leader",
      .init(
        term: 1,
        leaderId: "leader",
        prevLogIndex: 0,
        prevLogTerm: 0,
        entries: entries,
        leaderCommit: 1
      ),
      at: ContinuousClock.now
    )

    #expect(instance.log.count == 2)
    #expect(instance.log[1] == entries[0])
    #expect(instance.commitIndex == 1)
    #expect(actions.contains(.apply(entry: entries[0])))
    #expect(actions.contains(.sendAppendEntriesReply(to: "leader", term: 1, success: true)))
  }

  @Test func appendEntriesRejectsMismatchedPreviousEntry() {
    let stale = LogEntry(term: 1, command: Data("old".utf8))
    var instance = Instance(id: "follower", currentTerm: 2, log: .sentinel + [stale])

    let actions = instance.receiveAppendEntries(
      "leader",
      .init(
        term: 2,
        leaderId: "leader",
        prevLogIndex: 1,
        prevLogTerm: 9,
        entries: [LogEntry(term: 2, command: Data("new".utf8))],
        leaderCommit: 1
      ),
      at: ContinuousClock.now
    )

    #expect(instance.log.count == 2)
    #expect(actions == [.sendAppendEntriesReply(to: "leader", term: 2, success: false)])
  }

  @Test func appendEntriesTruncatesConflictingSuffix() {
    let first = LogEntry(term: 1, command: Data("a".utf8))
    let conflict = LogEntry(term: 1, command: Data("b".utf8))
    var instance = Instance(id: "follower", currentTerm: 2, log: .sentinel + [first, conflict])

    let replacement = LogEntry(term: 2, command: Data("c".utf8))
    _ = instance.receiveAppendEntries(
      "leader",
      .init(
        term: 2,
        leaderId: "leader",
        prevLogIndex: 1,
        prevLogTerm: 1,
        entries: [replacement, LogEntry(term: 2, command: Data("d".utf8))],
        leaderCommit: 2
      ),
      at: ContinuousClock.now
    )

    #expect(instance.log.map(\.command) == [Data(), first.command, replacement.command, Data("d".utf8)])
    #expect(instance.lastLogIndex == 3)
  }

  @Test func clientSubmitRejectsNonLeader() {
    var follower = Instance(id: "b", peers: ["a"], currentTerm: 2)
    _ = follower.receiveAppendEntries(
      "a", AppendEntries.Args(term: 2, leaderId: "a"), at: ContinuousClock.now)

    let actions = follower.receiveClientSubmit(
      RaftClient.defaultClientID,
      ClientSubmit.Args(requestId: 1, command: Data("set z=3".utf8)))

    #expect(
      actions == [
        .sendClientSubmitReply(
          to: RaftClient.defaultClientID,
          reply: ClientSubmit.Reply(requestId: 1, status: .notLeader, leaderId: "a"))
      ])
  }

  @Test func leaderAdvancesCommitIndexWithMajorityMatch() {
    let entry = LogEntry(term: 2, command: Data("x".utf8))
    var instance = Instance(
      id: "a",
      peers: ["b", "c"],
      role: .leader,
      currentTerm: 2,
      log: .sentinel + [entry]
    )

    _ = instance.onTimerTick(at: ContinuousClock.now)
    let sent = AppendEntries.Args(
      term: 2, leaderId: "a", prevLogIndex: 1, prevLogTerm: 2, entries: [], leaderCommit: 0)
    let actions = instance.receiveAppendEntriesReply(
      "b", sent, .init(term: 2, success: true), at: ContinuousClock.now)

    #expect(instance.commitIndex == 1)
    #expect(actions.contains(.apply(entry: entry)))
  }

  @Test func leaderStepsBackNextIndexOnReplicationFailure() {
    let first = LogEntry(term: 1, command: Data("a".utf8))
    let second = LogEntry(term: 2, command: Data("b".utf8))
    var instance = Instance(
      id: "a",
      peers: ["b"],
      role: .leader,
      currentTerm: 2,
      log: .sentinel + [first, second]
    )

    _ = instance.onTimerTick(at: ContinuousClock.now)
    let sent = AppendEntries.Args(
      term: 2, leaderId: "a", prevLogIndex: 2, prevLogTerm: 2, entries: [], leaderCommit: 0)
    let actions = instance.receiveAppendEntriesReply(
      "b", sent, .init(term: 2, success: false), at: ContinuousClock.now)

    #expect(
      actions.contains { action in
        if case .sendAppendEntry(let peer, let args) = action {
          return peer == "b" && args.prevLogIndex == 1 && args.entries == [second]
        }
        return false
      })
  }

  @Test func stepsDownWhenAppendEntriesReplyHasHigherTerm() {
    var instance = Instance.forTests(id: "a", peers: ["b"], role: .leader, currentTerm: 1)

    let sent = AppendEntries.Args(term: 2, leaderId: "a", prevLogIndex: 0, prevLogTerm: 0)
    let actions = instance.receiveAppendEntriesReply(
      "b", sent, .init(term: 2, success: false), at: ContinuousClock.now)

    #expect(instance.role == .follower)
    #expect(instance.currentTerm == 2)
    #expect(instance.votedFor == nil)
    #expect(instance.votes.isEmpty)
    #expect(actions.scheduledDelay == electionTimeout)
  }

  @Test func staleAppendEntriesReplyDoesNotCommitUnreplicatedEntries() {
    let entry1 = LogEntry(term: 2, command: Data("1".utf8))
    let entry2 = LogEntry(term: 2, command: Data("2".utf8))
    var leader = Instance(
      id: "a",
      peers: ["b"],
      role: .leader,
      currentTerm: 2,
      log: .sentinel + [entry1, entry2]
    )

    // Delayed reply that only acknowledged index 1 must not commit index 2.
    let sent = AppendEntries.Args(
      term: 2, leaderId: "a", prevLogIndex: 0, prevLogTerm: 0, entries: [entry1], leaderCommit: 0)
    let actions = leader.receiveAppendEntriesReply(
      "b", sent, .init(term: 2, success: true), at: ContinuousClock.now)

    #expect(leader.commitIndex == 1)
    #expect(!actions.contains(.apply(entry: entry2)))
  }

  @Test func outOfOrderAppendEntriesRepliesUseMonotonicMatchIndex() {
    let entry1 = LogEntry(term: 2, command: Data("1".utf8))
    let entry2 = LogEntry(term: 2, command: Data("2".utf8))
    var leader = Instance(
      id: "a",
      peers: ["b"],
      role: .leader,
      currentTerm: 2,
      log: .sentinel + [entry1, entry2]
    )

    let sentThrough2 = AppendEntries.Args(
      term: 2, leaderId: "a", prevLogIndex: 0, prevLogTerm: 0, entries: [entry1, entry2], leaderCommit: 0)
    let sentThrough1 = AppendEntries.Args(
      term: 2, leaderId: "a", prevLogIndex: 0, prevLogTerm: 0, entries: [entry1], leaderCommit: 0)
    _ = leader.receiveAppendEntriesReply(
      "b", sentThrough2, .init(term: 2, success: true), at: ContinuousClock.now)
    let actions = leader.receiveAppendEntriesReply(
      "b", sentThrough1, .init(term: 2, success: true), at: ContinuousClock.now)

    #expect(leader.commitIndex == 2)
    #expect(!actions.contains(.apply(entry: entry2)))
  }
}
