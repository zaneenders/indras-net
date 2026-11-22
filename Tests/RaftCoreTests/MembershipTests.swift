import Foundation
import OrderedCollections
import RaftCore
import Testing

struct MembershipTests {
  @Test(arguments: ["b", "d"])
  func jointElectionRequiresVotesFromBothConfigurations(firstVote: PeerId) throws {
    let initial = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: ["d", "e"])
    let joint = ClusterMembership(voters: ["c", "d", "e"], oldVoters: ["a", "b", "c"], readers: [])
    let state = PersistentState(
      currentTerm: 1, votedFor: nil, log: .sentinel + [LogEntry(term: 1, membership: joint)], commitIndex: 0,
      logBaseIndex: 0, snapshot: nil, initialMembership: initial)
    var candidate = RaftFixture.restore(id: "c", state: state, lastApplied: 0)
    let directives = candidate.onTimerTick()
    try #require(directives.first == .persist)
    let request = RequestVote.Args(term: 2, candidateId: "c", lastLogIndex: 1, lastLogTerm: 1)
    for peer in ["a", "b", "d", "e"] {
      #expect(directives.contains(.requestVote(to: peer, args: request)))
    }
    #expect(candidate.role == .candidate)
    let granted = RequestVote.Reply(granted: true, term: 2)
    let oneMajority = candidate.receiveRequestVoteReply(firstVote, request, granted)
    #expect(oneMajority.isEmpty)
    #expect(candidate.role == .candidate)
    let secondVote = firstVote == "b" ? "d" : "b"
    _ = candidate.receiveRequestVoteReply(secondVote, request, granted)
    #expect(candidate.role == .leader)
    #expect(candidate.commitIndex == 0)
  }

  @Test func overwritingAnUncommittedConfigurationRestoresThePriorVoters() throws {
    let stable = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: ["d"])
    let joint = ClusterMembership(voters: ["a", "b", "d"], oldVoters: ["a", "b", "c"], readers: [])
    var follower = RaftFixture.follower(
      membership: stable, term: 1, entries: [LogEntry(term: 1, membership: joint)], commitIndex: 0)
    try #require(follower.membership == joint)
    let replacement = LogEntry(term: 2, command: Data([1]), request: nil, isNoOp: false)
    let append = AppendEntries.Args(
      term: 2, leaderId: "b", prevLogIndex: 0, prevLogTerm: 0, entries: [replacement], leaderCommit: 1, rpcId: 0,
      traceHeaders: [:])
    let effects = follower.receiveAppendEntries("b", append)
    #expect(effects.first == .persist)
    #expect(follower.membership == stable)
    #expect(follower.committedMembership == stable)
    #expect(follower.entry(at: 1) == replacement)
    let campaign = follower.onTimerTick()
    let vote = RequestVote.Args(term: 3, candidateId: "a", lastLogIndex: 1, lastLogTerm: 2)
    #expect(
      campaign == [
        .persist, .requestVote(to: "b", args: vote), .requestVote(to: "c", args: vote),
        .scheduleNext(delay: .milliseconds(150))
      ])
  }

  @Test func readerDoesNotCampaignWhenItsElectionTimerExpires() {
    let membership = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: ["reader"])
    let initial = PersistentState(
      currentTerm: 1, votedFor: nil, log: .sentinel, commitIndex: 0, logBaseIndex: 0, snapshot: nil,
      initialMembership: membership)
    var reader = RaftFixture.restore(id: "reader", state: initial, lastApplied: 0)
    let directives = reader.onTimerTick()
    #expect(directives == [.scheduleNext(delay: .milliseconds(150))])
    #expect(reader.role == .follower)
    #expect(reader.persistentState == initial)
  }

  @Test func singletonWaitsForTwoCaughtUpReadersBeforePromotingThem() throws {
    let initial = ClusterMembership(voters: ["a"], oldVoters: nil, readers: ["b", "c", "d"])
    var leader = RaftFixture.follower(membership: initial, term: 0, entries: [], commitIndex: 0)
    _ = leader.onTimerTick()
    try #require(leader.role == .leader)
    _ = leader.proposeReadBarrier()
    try #require(leader.commitIndex == 1)
    let first = try RaftFixture.replicationProbe(to: "b", from: &leader)
    _ = RaftFixture.acknowledge(first, from: "b", on: &leader)
    let tooFewReaders = leader.promoteCaughtUpReaders()
    #expect(tooFewReaders.isEmpty)
    #expect(leader.membership == initial)

    let second = try RaftFixture.replicationProbe(to: "c", from: &leader)
    _ = RaftFixture.acknowledge(second, from: "c", on: &leader)
    let promotion = leader.promoteCaughtUpReaders()
    #expect(promotion.first == .persist)
    #expect(leader.membership == ClusterMembership(voters: ["a", "b", "c"], oldVoters: ["a"], readers: ["d"]))
    #expect(leader.committedMembership == initial)
  }

  @Test(arguments: [false, true])
  func jointChangeRequiresBothMajoritiesAndRemovedLeaderStepsDownAfterFinalCommit(oldMajorityFirst: Bool) throws {
    let initial = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: ["d", "e"])
    var leader = RaftFixture.follower(membership: initial, term: 0, entries: [], commitIndex: 0)
    _ = try RaftFixture.elect(&leader)
    let barrier = leader.proposeReadBarrier()
    #expect(barrier.first == .persist)
    #expect(throws: MembershipChangeError.readerNotCaughtUp("d")) {
      try leader.changeMembership(to: ["c", "d", "e"])
    }

    for reader in ["d", "e"] {
      let probe = try RaftFixture.replicationProbe(to: reader, from: &leader)
      _ = RaftFixture.acknowledge(probe, from: reader, on: &leader)
    }
    #expect(leader.commitIndex == 0)
    let voterProbe = try RaftFixture.replicationProbe(to: "b", from: &leader)
    _ = RaftFixture.acknowledge(voterProbe, from: "b", on: &leader)
    #expect(leader.commitIndex == 1)

    let change = try leader.changeMembership(to: ["c", "d", "e"])
    #expect(change.first == .persist)
    #expect(leader.membership == ClusterMembership(voters: ["c", "d", "e"], oldVoters: ["a", "b", "c"], readers: []))
    #expect(leader.committedMembership == initial)
    #expect(throws: MembershipChangeError.changeInProgress) { try leader.addReader("f") }
    let oldMajorityAcknowledgements = ["b"]
    let newMajorityAcknowledgements = ["d", "e"]
    for voter in oldMajorityFirst ? oldMajorityAcknowledgements : newMajorityAcknowledgements {
      let probe = try RaftFixture.replicationProbe(to: voter, from: &leader)
      _ = RaftFixture.acknowledge(probe, from: voter, on: &leader)
    }
    #expect(leader.commitIndex == 1)
    #expect(leader.lastLogIndex == 2)

    for voter in oldMajorityFirst ? newMajorityAcknowledgements : oldMajorityAcknowledgements {
      let probe = try RaftFixture.replicationProbe(to: voter, from: &leader)
      _ = RaftFixture.acknowledge(probe, from: voter, on: &leader)
    }
    #expect(leader.commitIndex == 2)
    #expect(leader.committedMembership.isJoint)
    #expect(leader.lastLogIndex == 3)
    let final = ClusterMembership(voters: ["c", "d", "e"], oldVoters: nil, readers: [])
    #expect(leader.membership == final)
    #expect(leader.role == .leader)

    let firstFinalProbe = try RaftFixture.replicationProbe(to: "d", from: &leader)
    _ = RaftFixture.acknowledge(firstFinalProbe, from: "d", on: &leader)
    #expect(leader.commitIndex == 2)
    #expect(leader.role == .leader)

    let secondFinalProbe = try RaftFixture.replicationProbe(to: "e", from: &leader)
    _ = RaftFixture.acknowledge(secondFinalProbe, from: "e", on: &leader)
    #expect(leader.commitIndex == 3)
    #expect(leader.committedMembership == final)
    #expect(leader.role == .follower)
    #expect(leader.leaderId == nil)
  }
}
