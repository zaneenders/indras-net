import Foundation
import RaftCore
import Testing

struct SnapshotTests {
  @Test func installationAcknowledgementMustMatchTheRPCAndBoundaryBeforeReplicationResumes() throws {
    var leader = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 0, entries: [], commitIndex: 0)
    _ = try RaftFixture.elect(&leader)
    _ = leader.proposeReadBarrier()
    let replicated = try RaftFixture.replicationProbe(to: "b", from: &leader)
    _ = RaftFixture.acknowledge(replicated, from: "b", on: &leader)
    try #require(leader.commitIndex == 1)
    leader.recordSnapshot(
      .init(
        lastIncludedIndex: 1, lastIncludedTerm: 1, applicationState: Data([1]), requests: [],
        membership: RaftFixture.threeVoters))
    leader.compactLog()
    _ = leader.proposeReadBarrier()
    try #require(leader.needsSnapshot(for: "c"))
    let request = InstallSnapshot.Args(
      term: 1, rpcId: 42, leaderId: "a", lastIncludedIndex: 1, lastIncludedTerm: 1, offset: 0, totalLength: 1,
      checksum: Data(repeating: 0, count: InstallSnapshot.Args.checksumLength), data: Data([1]))
    let irrelevant: [InstallSnapshot.Reply] = [
      .init(term: 9, rpcId: 41, lastIncludedIndex: 1, nextOffset: 1, installed: true),
      .init(term: 1, rpcId: 42, lastIncludedIndex: 2, nextOffset: 1, installed: true),
      .init(term: 1, rpcId: 42, lastIncludedIndex: 1, nextOffset: 0, installed: false)
    ]
    for reply in irrelevant {
      let actions = leader.receiveInstallSnapshotReply(from: "c", sent: request, reply: reply)
      #expect(actions.isEmpty)
      #expect(leader.needsSnapshot(for: "c"))
      #expect(leader.currentTerm == 1)
      #expect(leader.commitIndex == 1)
    }

    let installed = InstallSnapshot.Reply(term: 1, rpcId: 42, lastIncludedIndex: 1, nextOffset: 1, installed: true)
    let actions = leader.receiveInstallSnapshotReply(from: "c", sent: request, reply: installed)
    let suffix = try RaftFixture.replicationProbe(to: "c", from: &leader)
    #expect(actions == [.sendAppendEntry(to: "c", args: suffix)])
    #expect(!leader.needsSnapshot(for: "c"))
    #expect(suffix.prevLogIndex == 1)
    #expect(suffix.replicatedThrough == 2)
    #expect(leader.commitIndex == 1)
    _ = RaftFixture.acknowledge(suffix, from: "c", on: &leader)
    #expect(leader.commitIndex == 2)

    let higherTerm = InstallSnapshot.Reply(term: 2, rpcId: 42, lastIncludedIndex: 1, nextOffset: 0, installed: false)
    let steppedDown = leader.receiveInstallSnapshotReply(from: "c", sent: request, reply: higherTerm)
    #expect(steppedDown == [.persist, .scheduleNext(delay: .milliseconds(150))])
    #expect(leader.role == .follower)
    #expect(leader.currentTerm == 2)
  }

  @Test func snapshotContactRejectsStaleTermsAndStepsDownForANewerLeader() throws {
    var candidate = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 1, entries: [], commitIndex: 0)
    _ = candidate.onTimerTick()
    try #require(candidate.role == .candidate)
    let initial = candidate.persistentState
    let stale = InstallSnapshot.Args(
      term: 1, rpcId: 1, leaderId: "b", lastIncludedIndex: 1, lastIncludedTerm: 1, offset: 0, totalLength: 1,
      checksum: Data(repeating: 0, count: InstallSnapshot.Args.checksumLength), data: Data([1]))
    let acceptedStale = candidate.receiveInstallSnapshot(stale)
    #expect(!acceptedStale)
    #expect(candidate.persistentState == initial)
    #expect(candidate.role == .candidate)

    let newer = InstallSnapshot.Args(
      term: 3, rpcId: 2, leaderId: "b", lastIncludedIndex: 1, lastIncludedTerm: 1, offset: 0, totalLength: 1,
      checksum: stale.checksum, data: stale.data)
    let acceptedNewer = candidate.receiveInstallSnapshot(newer)
    #expect(acceptedNewer)
    #expect(candidate.role == .follower)
    #expect(candidate.currentTerm == 3)
    #expect(candidate.leaderId == "b")
    #expect(candidate.votedFor == nil)
    #expect(candidate.commitIndex == 0)
    let immediateElection = RequestVote.Args(term: 4, candidateId: "c", lastLogIndex: 0, lastLogTerm: 0)
    let vote = candidate.receiveRequestVote("c", immediateElection)
    #expect(vote == [.sendRequestVoteReply(to: "c", term: 3, voteGranted: false)])
  }

  @Test func compactsThroughTheSnapshotAndReportsItsBoundaryToTheLeader() throws {
    let entry = LogEntry(term: 1, command: Data([1]), request: nil, isNoOp: false)
    let tail = LogEntry(term: 2, command: Data([2]), request: nil, isNoOp: false)
    var follower = RaftFixture.follower(
      membership: RaftFixture.threeVoters, term: 2, entries: [entry, entry, tail], commitIndex: 2)
    follower.recordSnapshot(
      RaftSnapshot(
        lastIncludedIndex: 2, lastIncludedTerm: 1, applicationState: Data([1]), requests: [], membership: nil))
    follower.compactLog()
    #expect(follower.snapshot?.membership == RaftFixture.threeVoters)
    #expect(follower.logBaseIndex == 2)
    #expect(follower.entry(at: 1) == nil)
    #expect(follower.term(at: 2) == 1)
    #expect(follower.entry(at: 3) == tail)
    try follower.persistentState.validate(node: "a", previouslyValidatedSnapshot: nil)

    let probe = AppendEntries.Args(
      term: 2, leaderId: "b", prevLogIndex: 1, prevLogTerm: 1, entries: [],
      leaderCommit: 2, rpcId: 0, traceHeaders: [:])
    #expect(
      follower.receiveAppendEntries("b", probe) == [
        .sendAppendEntriesReply(to: "b", term: 2, success: false, compactedPrefix: .init(index: 2, term: 1)),
        .scheduleNext(delay: .milliseconds(150))
      ])
  }

  @Test(arguments: [false, true])
  func installingSnapshotPreservesSuffixAtMatchingBoundary(matchesBoundary: Bool) throws {
    let first = LogEntry(term: 1, command: Data([1]), request: nil, isNoOp: false)
    let boundary = LogEntry(term: matchesBoundary ? 2 : 1, command: Data([2]), request: nil, isNoOp: false)
    let tail = LogEntry(term: 3, command: Data([3]), request: nil, isNoOp: false)
    var follower = RaftFixture.follower(
      membership: RaftFixture.threeVoters, term: 3, entries: [first, boundary, tail], commitIndex: 1)
    let snapshot = RaftSnapshot(
      lastIncludedIndex: 2, lastIncludedTerm: 2, applicationState: Data([1, 2]), requests: [],
      membership: RaftFixture.threeVoters)

    let didInstall = follower.installSnapshot(snapshot)
    try #require(didInstall)
    #expect(follower.snapshot == snapshot)
    #expect(follower.logBaseIndex == 2)
    #expect(follower.term(at: 2) == 2)
    #expect(follower.entry(at: 3) == (matchesBoundary ? tail : nil))
    #expect(follower.commitIndex == 2)
    #expect(follower.lastApplied == 2)
    let installed = follower.persistentState
    try installed.validate(node: "a", previouslyValidatedSnapshot: nil)

    let stale = RaftSnapshot(
      lastIncludedIndex: 1, lastIncludedTerm: 1, applicationState: Data([1]), requests: [],
      membership: RaftFixture.threeVoters)
    let didInstallStale = follower.installSnapshot(stale)
    #expect(!didInstallStale)
    #expect(follower.persistentState == installed)
  }
}
