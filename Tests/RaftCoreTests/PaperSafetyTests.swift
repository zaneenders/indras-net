import Foundation
import OrderedCollections
import RaftCore
import Testing

struct PaperSafetyTests {
  @Test(arguments: [UInt64(7), 17, 27])
  func figure7LeaderRepairsAllSixDivergentLogs(seed: UInt64) throws {
    let histories: [PeerId: [Term]] = [
      "leader": [1, 1, 1, 4, 4, 5, 5, 6, 6, 6],
      "a": [1, 1, 1, 4, 4, 5, 5, 6, 6],
      "b": [1, 1, 1, 4],
      "c": [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],
      "d": [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7],
      "e": [1, 1, 1, 4, 4, 4, 4],
      "f": [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
    ]
    let membership = ClusterMembership(voters: OrderedSet(histories.keys), oldVoters: nil, readers: [])
    let states = histories.mapValues { terms in
      let entries = terms.enumerated().map { index, term in
        LogEntry(term: term, command: Data([UInt8(term), UInt8(index + 1)]), request: nil, isNoOp: false)
      }
      return PersistentState(
        currentTerm: 7, votedFor: nil, log: .sentinel + entries, commitIndex: 3, logBaseIndex: 0, snapshot: nil,
        initialMembership: membership)
    }
    let leaderLog = try #require(states["leader"]?.log)
    var cluster = try RaftSimulation(states: states, seed: seed)
    try cluster.tick("leader")
    for voter in ["c", "d"] {
      try cluster.deliverNext(from: "leader", to: voter)
      try cluster.deliverNext(from: voter, to: "leader")
      #expect(cluster.nodes[voter]?.votedFor == nil)
      #expect(cluster.nodes["leader"]?.votes[voter] == false)
    }
    try cluster.deliverPending()
    let leader = try #require(cluster.nodes["leader"])
    try #require(leader.role == .leader)
    #expect(leader.currentTerm == 8)
    let barrier = try #require(leader.entry(at: 11))
    #expect(barrier.term == 8)
    #expect(barrier.isNoOp)
    for peer in cluster.peers {
      #expect(cluster.nodes[peer]?.log == leaderLog + [barrier])
      #expect(cluster.nodes[peer]?.commitIndex == 11)
      #expect(cluster.applied[peer] == Array(leaderLog.dropFirst()) + [barrier])
    }
  }

  @Test func splitVoteResolvesInTheNextTermAndTheLosingCandidateFollowsThatTermsLeader() throws {
    var cluster = try RaftSimulation.empty(voters: ["a", "b", "c", "d"], seed: 2)
    try cluster.tick("a")
    try cluster.tick("b")
    try cluster.deliverNext(from: "a", to: "c")
    try cluster.deliverNext(from: "b", to: "d")
    try cluster.deliverPending()
    for peer in ["a", "b"] {
      try #require(cluster.nodes[peer]?.role == .candidate)
      #expect(cluster.nodes[peer]?.currentTerm == 1)
    }
    #expect(cluster.nodes["c"]?.votedFor == "a")
    #expect(cluster.nodes["d"]?.votedFor == "b")

    try cluster.tick("a")
    try cluster.tick("b")
    for voter in ["c", "d"] {
      try cluster.deliverNext(from: "b", to: voter)
      try cluster.deliverNext(from: voter, to: "b")
    }
    try #require(cluster.nodes["b"]?.role == .leader)
    try #require(cluster.nodes["a"]?.role == .candidate)
    try cluster.deliverPending()
    #expect(cluster.nodes["a"]?.role == .follower)
    #expect(cluster.nodes["a"]?.votedFor == "a")
    for peer in cluster.peers {
      #expect(cluster.nodes[peer]?.currentTerm == 2)
      #expect(cluster.nodes[peer]?.leaderId == "b")
    }
  }

  @Test(arguments: [false, true])
  func figure11SuccessorCompletesJointConsensusAfterTheReconfiguringLeaderCrashes(knowsJointWasCommitted: Bool) throws {
    let old = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: ["d", "e"])
    let joint = ClusterMembership(voters: ["b", "d", "e"], oldVoters: old.voters, readers: [])
    let final = ClusterMembership(voters: joint.voters, oldVoters: nil, readers: [])
    let entry = LogEntry(term: 1, command: Data([1]), request: nil, isNoOp: false)
    let jointEntry = LogEntry(term: 2, membership: joint)
    let base = Log.sentinel + [entry]
    let jointLog = base + [jointEntry]
    let states = [
      "a": PersistentState(
        currentTerm: 2, votedFor: "a", log: jointLog + [LogEntry(term: 2, membership: final)], commitIndex: 2,
        logBaseIndex: 0, snapshot: nil, initialMembership: old),
      "b": PersistentState(
        currentTerm: 2, votedFor: "a", log: jointLog, commitIndex: knowsJointWasCommitted ? 2 : 1, logBaseIndex: 0,
        snapshot: nil, initialMembership: old),
      "c": PersistentState(
        currentTerm: 2, votedFor: "a", log: base, commitIndex: 1, logBaseIndex: 0, snapshot: nil, initialMembership: old
      ),
      "d": PersistentState(
        currentTerm: 2, votedFor: nil, log: jointLog, commitIndex: 1, logBaseIndex: 0, snapshot: nil,
        initialMembership: old),
      "e": PersistentState(
        currentTerm: 2, votedFor: nil, log: base, commitIndex: 1, logBaseIndex: 0, snapshot: nil, initialMembership: old
      )
    ]
    var cluster = try RaftSimulation(states: states, seed: 11)
    cluster.crash("a")
    try cluster.tick("b")
    try cluster.deliverPending()
    let leader = try #require(cluster.nodes["b"])
    try #require(leader.role == .leader)
    #expect(leader.currentTerm == 3)
    let finalIndex: LogIndex = knowsJointWasCommitted ? 3 : 4
    if !knowsJointWasCommitted {
      #expect(leader.entry(at: 3)?.term == 3)
      #expect(leader.entry(at: 3)?.isNoOp == true)
    }
    for peer in ["b", "d", "e"] {
      #expect(cluster.nodes[peer]?.log == leader.log)
      #expect(cluster.nodes[peer]?.entry(at: 2) == jointEntry)
      #expect(cluster.nodes[peer]?.entry(at: finalIndex) == LogEntry(term: 3, membership: final))
      #expect(cluster.nodes[peer]?.commitIndex == finalIndex)
      #expect(cluster.nodes[peer]?.committedMembership == final)
    }
  }
}
