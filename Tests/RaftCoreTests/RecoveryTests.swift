import Foundation
import OrderedCollections
import RaftCore
import Testing

struct RecoveryTests {
  @Test func restoredVoterKeepsItsVoteUntilItObservesANewerTerm() {
    var voter = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 0, entries: [], commitIndex: 0)
    let first = RequestVote.Args(term: 1, candidateId: "b", lastLogIndex: 0, lastLogTerm: 0)
    let grant = voter.receiveRequestVote("b", first)
    #expect(grant.first == .persist)
    let durable = voter.persistentState
    #expect(durable.votedFor == "b")
    var restored = RaftFixture.restore(id: "a", state: durable, lastApplied: 0)

    let competing = RequestVote.Args(term: 1, candidateId: "c", lastLogIndex: 0, lastLogTerm: 0)
    let rejected = restored.receiveRequestVote("c", competing)
    #expect(rejected == [.sendRequestVoteReply(to: "c", term: 1, voteGranted: false)])
    let retry = restored.receiveRequestVote("b", first)
    #expect(
      retry == [.sendRequestVoteReply(to: "b", term: 1, voteGranted: true), .scheduleNext(delay: .milliseconds(150))])
    #expect(restored.persistentState == durable)

    let newer = RequestVote.Args(term: 2, candidateId: "c", lastLogIndex: 0, lastLogTerm: 0)
    let nextVote = restored.receiveRequestVote("c", newer)
    #expect(
      nextVote == [
        .persist, .sendRequestVoteReply(to: "c", term: 2, voteGranted: true), .scheduleNext(delay: .milliseconds(150))
      ])
    #expect(restored.votedFor == "c")
  }

  @Test func restoredFollowerAppliesOnlyTheCommittedEntriesItHadNotYetApplied() {
    let first = LogEntry(term: 1, command: Data([1]), request: nil, isNoOp: false)
    let second = LogEntry(term: 1, command: Data([2]), request: nil, isNoOp: false)
    let uncommitted = LogEntry(term: 1, command: Data([3]), request: nil, isNoOp: false)
    let state = PersistentState(
      currentTerm: 1, votedFor: "b", log: .sentinel + [first, second, uncommitted], commitIndex: 2, logBaseIndex: 0,
      snapshot: nil, initialMembership: RaftFixture.threeVoters)
    var restored = RaftFixture.restore(id: "a", state: state, lastApplied: 1)
    let heartbeat = AppendEntries.Args(
      term: 1, leaderId: "b", prevLogIndex: 2, prevLogTerm: 1, entries: [], leaderCommit: 2, rpcId: 0,
      traceHeaders: [:])
    let acknowledgement: [AppendEntries.Args.Action] = [
      .sendAppendEntriesReply(to: "b", term: 1, success: true, compactedPrefix: nil),
      .scheduleNext(delay: .milliseconds(150))
    ]
    let replay = restored.receiveAppendEntries("b", heartbeat)
    #expect(replay == [.apply(entry: second, atIndex: 2)] + acknowledgement)
    #expect(restored.lastApplied == 2)
    #expect(restored.persistentState == state)
    let duplicate = restored.receiveAppendEntries("b", heartbeat)
    #expect(duplicate == acknowledgement)
  }

  @Test func durableMembershipRequiresACommittedJointEntryBeforeFinalization() throws {
    let initial = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: ["d"])
    let joint = ClusterMembership(voters: ["a", "b", "d"], oldVoters: ["a", "b", "c"], readers: [])
    let final = ClusterMembership(voters: ["a", "b", "d"], oldVoters: nil, readers: [])
    let finalEntry = LogEntry(term: 1, membership: final)
    let log = Log.sentinel + [LogEntry(term: 1, membership: joint), finalEntry]
    let persisted = PersistentState(
      currentTerm: 1, votedFor: "a", log: log, commitIndex: 1, logBaseIndex: 0, snapshot: nil,
      initialMembership: initial)
    try persisted.validate(node: "a", previouslyValidatedSnapshot: nil)

    let prematureFinal = PersistentState(
      currentTerm: 1, votedFor: "a", log: log, commitIndex: 0, logBaseIndex: 0, snapshot: nil,
      initialMembership: initial)
    #expect(throws: StateValidationError.invalidState) {
      try prematureFinal.validate(node: "a", previouslyValidatedSnapshot: nil)
    }
    let skippedJoint = PersistentState(
      currentTerm: 1, votedFor: "a", log: .sentinel + [finalEntry], commitIndex: 1, logBaseIndex: 0, snapshot: nil,
      initialMembership: initial)
    #expect(throws: StateValidationError.invalidState) {
      try skippedJoint.validate(node: "a", previouslyValidatedSnapshot: nil)
    }
  }

  @Test func durableSnapshotMustMatchTheLogTermAndMembershipAtItsBoundary() throws {
    let membership = RaftFixture.threeVoters
    let differentMembership = ClusterMembership(voters: ["a", "b", "d"], oldVoters: nil, readers: [])
    let log = [LogEntry(term: 1, membership: membership)]
    for snapshot in [
      RaftSnapshot(
        lastIncludedIndex: 1, lastIncludedTerm: 2, applicationState: Data(), requests: [],
        membership: membership),
      RaftSnapshot(
        lastIncludedIndex: 1, lastIncludedTerm: 1, applicationState: Data(), requests: [],
        membership: differentMembership)
    ] {
      let state = PersistentState(
        currentTerm: 2, votedFor: nil, log: log, commitIndex: 1, logBaseIndex: 1, snapshot: snapshot,
        initialMembership: membership)
      try snapshot.validate()
      #expect(throws: StateValidationError.invalidState) {
        try state.validate(node: "a", previouslyValidatedSnapshot: nil)
      }
    }
  }

  @Test func snapshotReceiptsAreUniqueAndReferToIncludedEntries() throws {
    let identity = RequestIdentity(client: "client", requestId: 1)
    let receipt = AppliedRequest(identity: identity, index: 1, result: Data([1]))
    let valid = RaftSnapshot(
      lastIncludedIndex: 2, lastIncludedTerm: 1, applicationState: Data(), requests: [receipt],
      membership: RaftFixture.threeVoters)
    try valid.validate()

    let duplicate = AppliedRequest(identity: identity, index: 2, result: Data([2]))
    let future = AppliedRequest(identity: identity, index: 3, result: Data([3]))
    for receipts in [[receipt, duplicate], [future]] {
      let snapshot = RaftSnapshot(
        lastIncludedIndex: 2, lastIncludedTerm: 1, applicationState: Data(), requests: receipts,
        membership: RaftFixture.threeVoters)
      #expect(throws: StateValidationError.invalidState) { try snapshot.validate() }
    }
  }
}
