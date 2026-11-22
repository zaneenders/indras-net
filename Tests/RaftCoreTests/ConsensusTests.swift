import Foundation
import OrderedCollections
import RaftCore
import Testing

struct ConsensusTests {
  @Test func aReplyToAnEarlierCampaignCannotElectTheCurrentCandidate() {
    var candidate = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 0, entries: [], commitIndex: 0)
    _ = candidate.onTimerTick()
    let old = RequestVote.Args(term: 1, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0)
    _ = candidate.onTimerTick()
    let current = RequestVote.Args(term: 2, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0)
    let granted = RequestVote.Reply(granted: true, term: 2)
    let delayed = candidate.receiveRequestVoteReply("b", old, granted)
    #expect(delayed.isEmpty)
    #expect(candidate.role == .candidate)
    #expect(candidate.votes == ["a": true])

    _ = candidate.receiveRequestVoteReply("b", current, granted)
    #expect(candidate.role == .leader)
    #expect(candidate.currentTerm == 2)
  }

  @Test func electsWithDistinctCurrentTermVotesFromVoters() {
    let membership = ClusterMembership(voters: ["a", "b", "c", "d", "e"], oldVoters: nil, readers: ["reader"])
    var candidate = RaftFixture.follower(membership: membership, term: 0, entries: [], commitIndex: 0)
    let vote = RequestVote.Args(term: 1, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0)
    #expect(
      candidate.onTimerTick() == [
        .persist,
        .requestVote(to: "b", args: vote), .requestVote(to: "c", args: vote),
        .requestVote(to: "d", args: vote), .requestVote(to: "e", args: vote),
        .scheduleNext(delay: .milliseconds(150))
      ])
    #expect(candidate.votedFor == "a")

    let granted = RequestVote.Reply(granted: true, term: 1)
    #expect(candidate.receiveRequestVoteReply("c", vote, .init(granted: true, term: 0)).isEmpty)
    #expect(candidate.receiveRequestVoteReply("reader", vote, granted).isEmpty)
    #expect(candidate.receiveRequestVoteReply("b", vote, granted).isEmpty)
    #expect(candidate.receiveRequestVoteReply("b", vote, granted).isEmpty)
    #expect(candidate.role == .candidate)

    let elected = candidate.receiveRequestVoteReply("c", vote, granted)
    #expect(candidate.role == .leader)
    #expect(candidate.leaderId == "a")
    #expect(elected.last == .scheduleNext(delay: .milliseconds(50)))

    #expect(
      candidate.receiveRequestVoteReply("d", vote, .init(granted: false, term: 2)) == [
        .persist, .scheduleNext(delay: .milliseconds(150))
      ])
    #expect(candidate.role == .follower)
    #expect(candidate.currentTerm == 2)
    #expect(candidate.votedFor == nil)
    #expect(candidate.leaderId == nil)
  }

  @Test func protectsRecentLeaderThenChecksLogFreshnessAndPriorVote() {
    let entry = LogEntry(term: 2, command: Data([1]), request: nil, isNoOp: false)
    var follower = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 2, entries: [entry], commitIndex: 0)
    let heartbeat = AppendEntries.Args(
      term: 2, leaderId: "b", prevLogIndex: 1, prevLogTerm: 2, entries: [],
      leaderCommit: 0, rpcId: 0, traceHeaders: [:])
    #expect(
      follower.receiveAppendEntries("b", heartbeat) == [
        .sendAppendEntriesReply(to: "b", term: 2, success: true, compactedPrefix: nil),
        .scheduleNext(delay: .milliseconds(150))
      ])

    let olderLog = RequestVote.Args(term: 3, candidateId: "c", lastLogIndex: 2, lastLogTerm: 1)
    follower.elapse(.milliseconds(149))
    #expect(follower.receiveRequestVote("c", olderLog) == [.sendRequestVoteReply(to: "c", term: 2, voteGranted: false)])
    #expect(follower.leaderId == "b")

    follower.elapse(.milliseconds(1))
    #expect(
      follower.receiveRequestVote("c", olderLog) == [
        .persist, .sendRequestVoteReply(to: "c", term: 3, voteGranted: false)
      ])
    #expect(follower.votedFor == nil)
    let upToDate = RequestVote.Args(term: 3, candidateId: "c", lastLogIndex: 1, lastLogTerm: 2)
    #expect(
      follower.receiveRequestVote("c", upToDate) == [
        .persist, .sendRequestVoteReply(to: "c", term: 3, voteGranted: true), .scheduleNext(delay: .milliseconds(150))
      ])
    let competing = RequestVote.Args(term: 3, candidateId: "b", lastLogIndex: 1, lastLogTerm: 2)
    #expect(
      follower.receiveRequestVote("b", competing) == [.sendRequestVoteReply(to: "b", term: 3, voteGranted: false)])
    #expect(follower.votedFor == "c")
  }

  @Test func commitsInheritedEntriesAfterReplicatingAnEntryFromTheCurrentTerm() throws {
    let empty = LogEntry(term: 1, command: Data(), request: nil, isNoOp: false)
    let overhead = AppendEntries.Args.basePayloadLength(leaderId: "a") + empty.encodedLength
    let command = Data(repeating: 1, count: Int(RaftLimits.maximumPayloadLength) - overhead)
    let inherited = LogEntry(term: 1, command: command, request: nil, isNoOp: false)
    var leader = RaftFixture.follower(
      membership: RaftFixture.threeVoters, term: 1, entries: [inherited], commitIndex: 0)
    let elected = try RaftFixture.elect(&leader)
    #expect(elected.first == .persist)
    let barrier = try #require(leader.entry(at: 2))
    #expect(barrier.term == 2)
    #expect(barrier.isNoOp)
    let sent = try RaftFixture.replicationProbe(to: "b", from: &leader)
    #expect(sent.prevLogIndex == 1)
    #expect(sent.entries == [barrier])

    let rejected = AppendEntries.Reply(term: 2, success: false, rpcId: sent.rpcId, compactedPrefix: nil)
    _ = leader.receiveAppendEntriesReply("b", sent, rejected)
    let prefixOnly = try RaftFixture.replicationProbe(to: "b", from: &leader)
    #expect(prefixOnly.prevLogIndex == 0)
    #expect(prefixOnly.entries == [inherited])
    let retry = RaftFixture.acknowledge(prefixOnly, from: "b", on: &leader)
    #expect(retry == [.sendAppendEntry(to: "b", args: sent)])
    #expect(leader.commitIndex == 0)
    #expect(leader.lastApplied == 0)

    let committed = RaftFixture.acknowledge(sent, from: "b", on: &leader)
    #expect(
      Array(committed.prefix(3)) == [
        .persist, .apply(entry: inherited, atIndex: 1), .apply(entry: barrier, atIndex: 2)
      ])
    #expect(leader.commitIndex == 2)
    #expect(leader.lastApplied == 2)
    #expect(leader.receiveAppendEntriesReply("b", prefixOnly, rejected).isEmpty)
  }

  @Test func boundsFollowerCommitAndReplacesAConflictingSuffix() {
    let committed = LogEntry(term: 1, command: Data([1]), request: nil, isNoOp: false)
    let divergent = LogEntry(term: 1, command: Data([2]), request: nil, isNoOp: false)
    var follower = RaftFixture.follower(
      membership: RaftFixture.threeVoters, term: 2, entries: [committed, divergent, divergent], commitIndex: 1)
    let heartbeat = AppendEntries.Args(
      term: 2, leaderId: "b", prevLogIndex: 1, prevLogTerm: 1, entries: [],
      leaderCommit: 3, rpcId: 0, traceHeaders: [:])
    #expect(
      follower.receiveAppendEntries("b", heartbeat) == [
        .sendAppendEntriesReply(to: "b", term: 2, success: true, compactedPrefix: nil),
        .scheduleNext(delay: .milliseconds(150))
      ])
    #expect(follower.commitIndex == 1)
    #expect(follower.lastApplied == 1)

    let replacement = LogEntry(term: 3, command: Data([3]), request: nil, isNoOp: false)
    let wrongPrefix = AppendEntries.Args(
      term: 3, leaderId: "b", prevLogIndex: 1, prevLogTerm: 2, entries: [replacement],
      leaderCommit: 2, rpcId: 0, traceHeaders: [:])
    #expect(
      follower.receiveAppendEntries("b", wrongPrefix) == [
        .persist, .sendAppendEntriesReply(to: "b", term: 3, success: false, compactedPrefix: nil),
        .scheduleNext(delay: .milliseconds(150))
      ])
    #expect(follower.log == .sentinel + [committed, divergent, divergent])

    let repair = AppendEntries.Args(
      term: 3, leaderId: "b", prevLogIndex: 1, prevLogTerm: 1, entries: [replacement],
      leaderCommit: 2, rpcId: 0, traceHeaders: [:])
    #expect(
      follower.receiveAppendEntries("b", repair) == [
        .persist, .apply(entry: replacement, atIndex: 2),
        .sendAppendEntriesReply(to: "b", term: 3, success: true, compactedPrefix: nil),
        .scheduleNext(delay: .milliseconds(150))
      ])
    #expect(follower.log == .sentinel + [committed, replacement])
    #expect(follower.commitIndex == 2)
    #expect(follower.lastApplied == 2)
    #expect(
      follower.receiveAppendEntries("b", heartbeat) == [
        .sendAppendEntriesReply(to: "b", term: 3, success: false, compactedPrefix: nil)
      ])
  }

  @Test(arguments: [false, true])
  func aCompactedPrefixHintSkipsBacktrackingOnlyWhenItsTermMatches(matchesTerm: Bool) throws {
    let entry = LogEntry(term: 1, command: Data([1]), request: nil, isNoOp: false)
    var leader = RaftFixture.follower(
      membership: RaftFixture.threeVoters, term: 1, entries: [entry, entry, entry], commitIndex: 3)
    _ = try RaftFixture.elect(&leader)
    let rejected = AppendEntries.Reply(term: 2, success: false, rpcId: 0, compactedPrefix: nil)
    let initialProbeIndices: [LogIndex] = [3, 2]
    for index in initialProbeIndices {
      let sent = try RaftFixture.replicationProbe(to: "b", from: &leader)
      try #require(sent.prevLogIndex == index)
      _ = leader.receiveAppendEntriesReply("b", sent, rejected)
    }
    let belowBoundary = try RaftFixture.replicationProbe(to: "b", from: &leader)
    try #require(belowBoundary.prevLogIndex == 1)
    let hint = AppendEntries.Reply(
      term: 2, success: false, rpcId: 0, compactedPrefix: .init(index: 2, term: matchesTerm ? 1 : 2))
    let retry = leader.receiveAppendEntriesReply("b", belowBoundary, hint)
    let newProbe = try RaftFixture.replicationProbe(to: "b", from: &leader)
    #expect(retry == [.sendAppendEntry(to: "b", args: newProbe)])
    #expect(newProbe.prevLogIndex == (matchesTerm ? 2 : 0))
    #expect(leader.commitIndex == 3)
  }
}
