import Foundation
import RaftCore
import Testing

struct ClusterTests {
  @Test(arguments: [UInt64(1), 2, 3, 5, 8, 13, 21, 34])
  func majorityReplacesAnIsolatedLeadersUncommittedWrite(seed: UInt64) throws {
    var cluster = try RaftSimulation.empty(voters: ["a", "b", "c", "d", "e"], seed: seed)
    let first = ClientSubmit.Args(requestId: 1, command: Data([1]), traceHeaders: [:])
    let abandoned = ClientSubmit.Args(requestId: 2, command: Data([2]), traceHeaders: [:])
    let replacement = ClientSubmit.Args(requestId: 3, command: Data([3]), traceHeaders: [:])

    try cluster.tick("a")
    try cluster.deliverPending()
    try #require(cluster.nodes["a"]?.role == .leader)
    try cluster.submit(first, to: "a")
    try cluster.deliverPending()
    try expectConvergence(cluster, leader: "a", commands: [first.command])

    cluster.partition = ["a", "b"]
    try cluster.submit(abandoned, to: "a")
    try cluster.deliverPending()
    try #require(cluster.nodes["b"]?.entry(at: 2)?.command == abandoned.command)
    #expect(cluster.applied["a"]?.count == 1)
    #expect(cluster.applied["b"]?.count == 1)

    cluster.elapse(.milliseconds(150))
    try cluster.tick("c")
    try cluster.deliverPending()
    try #require(cluster.nodes["c"]?.role == .leader)
    #expect(cluster.nodes["a"]?.role == .leader)
    #expect(cluster.nodes["a"]?.currentTerm == 1)
    #expect(cluster.nodes["c"]?.currentTerm == 2)
    try cluster.submit(replacement, to: "c")
    try cluster.deliverPending()
    #expect(cluster.applied["c"]?.map(\.command) == [first.command, replacement.command])
    #expect(cluster.applied["a"]?.map(\.command) == [first.command])

    cluster.partition = []
    try cluster.tick("a")
    try cluster.tick("c")
    try cluster.deliverPending()
    try expectConvergence(cluster, leader: "c", commands: [first.command, replacement.command])
  }

  @Test(arguments: [UInt64(1), 2, 3, 5, 8, 13, 21, 34])
  func staleCandidateLosesAfterLeaderCrashAndRestartedReplicasCatchUp(seed: UInt64) throws {
    var cluster = try RaftSimulation.empty(voters: ["a", "b", "c", "d", "e"], seed: seed)
    let first = ClientSubmit.Args(requestId: 1, command: Data([1]), traceHeaders: [:])
    let next = ClientSubmit.Args(requestId: 2, command: Data([2]), traceHeaders: [:])
    try cluster.tick("a")
    try cluster.deliverPending()
    try #require(cluster.nodes["a"]?.role == .leader)
    cluster.partition = ["a", "b", "c"]
    try cluster.submit(first, to: "a")
    try cluster.deliverPending()
    for peer in ["a", "b", "c"] {
      try #require(cluster.applied[peer]?.map(\.command) == [first.command])
    }
    #expect(cluster.nodes["d"]?.lastLogIndex == 0)
    #expect(cluster.nodes["e"]?.lastLogIndex == 0)

    cluster.crash("a")
    cluster.crash("b")
    cluster.partition = []
    cluster.elapse(.milliseconds(150))
    try cluster.tick("d")
    try cluster.deliverPending()
    #expect(cluster.nodes["d"]?.role == .candidate)
    #expect(cluster.nodes["c"]?.currentTerm == 2)
    #expect(cluster.nodes["c"]?.votedFor == nil)

    try cluster.tick("c")
    try cluster.deliverPending()
    try #require(cluster.nodes["c"]?.role == .leader)
    #expect(cluster.nodes["c"]?.currentTerm == 3)
    try cluster.submit(next, to: "c")
    try cluster.deliverPending()

    try cluster.restart("a")
    try cluster.restart("b")
    #expect(cluster.nodes["a"]?.role == .follower)
    #expect(cluster.nodes["a"]?.commitIndex == 1)
    #expect(cluster.nodes["b"]?.commitIndex == 1)
    try cluster.tick("c")
    try cluster.deliverPending()
    try expectConvergence(cluster, leader: "c", commands: [first.command, next.command])
  }

  private func expectConvergence(_ cluster: RaftSimulation, leader: PeerId, commands: [Data]) throws {
    let elected = try #require(cluster.nodes[leader])
    #expect(elected.role == .leader)
    for peer in cluster.peers {
      let node = try #require(cluster.nodes[peer])
      let applied = try #require(cluster.applied[peer])
      #expect(node.role == (peer == leader ? .leader : .follower))
      #expect(node.leaderId == leader)
      #expect(node.currentTerm == elected.currentTerm)
      #expect(node.log == elected.log)
      #expect(node.commitIndex == LogIndex(commands.count))
      #expect(node.lastApplied == LogIndex(commands.count))
      #expect(applied.map(\.command) == commands)
    }
  }
}
