import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct InstanceClusterTests {
  @Test func clientSubmitReplicatesAndCommitsOnLeader() {
    var cluster = TestCluster(
      nodes: [
        "a": Instance(id: "a", peers: ["b"], role: .leader, currentTerm: 2),
        "b": Instance(id: "b", currentTerm: 2),
      ])

    let reply = cluster.submit(command: Data("set z=3".utf8), to: "a")

    #expect(reply == ClientSubmit.Reply(requestId: 1, status: .ok, logIndex: 1))
    #expect(cluster.nodes["a"]!.commitIndex == 1)
    #expect(cluster.nodes["b"]!.log[1].command == Data("set z=3".utf8))
  }

  @Test func clientSubmitReplicatesToMajorityInThreeNodeCluster() {
    var cluster = TestCluster(
      nodes: [
        "a": Instance(id: "a", peers: ["b", "c"], role: .leader, currentTerm: 2),
        "b": Instance(id: "b", currentTerm: 2),
        "c": Instance(id: "c", currentTerm: 2),
      ])

    let reply = cluster.submit(command: Data("set z=3".utf8), to: "a")
    let command = Data("set z=3".utf8)

    #expect(reply == ClientSubmit.Reply(requestId: 1, status: .ok, logIndex: 1))
    #expect(cluster.nodes["a"]!.commitIndex == 1)
    #expect(cluster.nodes["b"]!.log[1].command == command)
    #expect(cluster.nodes["c"]!.log[1].command == command)
  }
}
