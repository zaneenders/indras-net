import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct InstanceClusterTests {

  @Test func disconnectTwoNodesNoLeaderReconnectOneNode() {
    var cluster = TestCluster(peers: ["a", "b", "c"])
    cluster.disconnect("a")
    cluster.disconnect("b")
    cluster.fireTimer("a")
    cluster.fireTimer("b")
    cluster.fireTimer("c")
    #expect(cluster.leader == nil)

    cluster.reconnect("a")
    cluster.fireTimer("a")
    cluster.fireTimer("b")
    cluster.fireTimer("c")
    let leader = cluster.leader
    #expect(leader != nil)

    cluster.reconnect("b")
    #expect(cluster.leader == leader, "Leader should not have chagned")
  }

  @Test func disconnectNodeSubmitEntryThenReconnectNode() {
    var cluster = TestCluster(peers: ["a", "b", "c"])
    cluster.disconnect("a")
    cluster.fireTimer("a")
    cluster.fireTimer("b")
    cluster.fireTimer("c")
    guard let leader = cluster.leader else {
      Issue.record("No leader")
      return
    }

    _ = cluster.submit(command: Data("set z=3".utf8), to: leader)
    #expect(cluster.nodes["a"]!.log.count == 1)
    #expect(cluster.nodes["b"]!.log.count == 2)
    #expect(cluster.nodes["c"]!.log.count == 2)
    cluster.reconnect("a")
    cluster.fireTimer("a")
    cluster.fireTimer("b")
    cluster.fireTimer("c")
    #expect(cluster.nodes["a"]!.log.count == 2)
    #expect(cluster.nodes["b"]!.log.count == 2)
    #expect(cluster.nodes["c"]!.log.count == 2)
  }

  @Test func disconnectTwoNodesSubmitEntryThenReconnectNode() {
    var cluster = TestCluster(peers: ["a", "b", "c", "d", "e"])
    cluster.disconnect("a")
    cluster.disconnect("b")
    cluster.fireTimer("c")
    cluster.fireTimer("d")
    cluster.fireTimer("e")
    guard let leader = cluster.leader else {
      Issue.record("No leader")
      return
    }

    _ = cluster.submit(command: Data("set z=3".utf8), to: leader)
    #expect(cluster.nodes["a"]!.log.count == 1)
    #expect(cluster.nodes["b"]!.log.count == 1)
    #expect(cluster.nodes["c"]!.log.count == 2)
    #expect(cluster.nodes["d"]!.log.count == 2)
    #expect(cluster.nodes["e"]!.log.count == 2)
    cluster.reconnect("a")
    cluster.reconnect("b")
    cluster.fireTimer("a")
    cluster.fireTimer("b")
    cluster.fireTimer("c")
    cluster.fireTimer("d")
    cluster.fireTimer("e")
    #expect(cluster.nodes["a"]!.log.count == 2)
    #expect(cluster.nodes["b"]!.log.count == 2)
    #expect(cluster.nodes["c"]!.log.count == 2)
    #expect(cluster.nodes["d"]!.log.count == 2)
    #expect(cluster.nodes["e"]!.log.count == 2)
  }
}
