import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct InstanceClusterTests {

  @Test func disconnectNodeSubmitEntryThenReconnectNode() {
    var cluster = TestCluster(peers: ["a", "b", "c"])
    cluster.disconnect("a")
    cluster.tick("a")
    cluster.tick("b")
    cluster.tick("c")
    guard let leader = cluster.leader else {
      Issue.record("No leader")
      return
    }

    _ = cluster.submit(command: Data("set z=3".utf8), to: leader)
    #expect(cluster.nodes["a"]!.log.count == 1)
    #expect(cluster.nodes["b"]!.log.count == 2)
    #expect(cluster.nodes["c"]!.log.count == 2)
    cluster.reconnect("a")
    cluster.tick("a")
    cluster.tick("b")
    cluster.tick("c")
    #expect(cluster.nodes["a"]!.log.count == 2)
    #expect(cluster.nodes["b"]!.log.count == 2)
    #expect(cluster.nodes["c"]!.log.count == 2)
  }

  @Test func disconnectTwoNodesSubmitEntryThenReconnectNode() {
    var cluster = TestCluster(peers: ["a", "b", "c", "d", "e"])
    cluster.disconnect("a")
    cluster.disconnect("b")
    cluster.tick("c")
    cluster.tick("d")
    cluster.tick("e")
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
    cluster.tick("a")
    cluster.tick("b")
    cluster.tick("c")
    cluster.tick("d")
    cluster.tick("e")
    #expect(cluster.nodes["a"]!.log.count == 2)
    #expect(cluster.nodes["b"]!.log.count == 2)
    #expect(cluster.nodes["c"]!.log.count == 2)
    #expect(cluster.nodes["d"]!.log.count == 2)
    #expect(cluster.nodes["e"]!.log.count == 2)
  }
}
