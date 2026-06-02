import NIOPosix
import Testing

@testable import IndrasNet

@Suite struct ThreeNodeMeshInProcessTests {
  @Test func threeNodesFormMeshAndPingPong() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let endpointC = ClusterEndpoint(host: host, port: 29_200)
      let endpointB = ClusterEndpoint(host: host, port: 29_201)
      let endpointA = ClusterEndpoint(host: host, port: 29_202)

      func makeNode(
        local: ClusterEndpoint,
        peers: [ClusterEndpoint]
      ) async throws -> (node: IndrasNetTCPTransport, collector: MessageCollector) {
        let collector = MessageCollector()
        let node = IndrasNetTCPTransport(
          configuration: IndrasNetTCPConfiguration(
            localPeerID: local.addressKey,
            host: local.host,
            port: local.port,
            peers: peers
          ),
          eventLoopGroup: group
        )
        try await node.start { message, from in
          await collector.record(message, from: from)
          if message.type == .ping {
            // TODO: Send pong
          }
        }
        return (node, collector)
      }

      let a = try await makeNode(local: endpointA, peers: [endpointB, endpointC])
      let b = try await makeNode(local: endpointB, peers: [endpointC, endpointA])
      let c = try await makeNode(local: endpointC, peers: [endpointB, endpointA])

      let nodes: [(endpoint: ClusterEndpoint, node: IndrasNetTCPTransport, collector: MessageCollector)] =
        [(endpointA, a.node, a.collector), (endpointB, b.node, b.collector), (endpointC, c.node, c.collector)]

      await TestHelpers.waitUntil(timeout: .seconds(10)) {
        // TODO: Check that all nodes are conencted
        return true
      }

      try await withThrowingTaskGroup(of: Void.self) { group in
        for sender in nodes {
          for receiver in nodes where receiver.endpoint.addressKey != sender.endpoint.addressKey {
            group.addTask {
              let pingCount = Int.random(in: 1...5)
              for _ in 0..<pingCount {
                // TODO: Send ping
              }
              try await receiver.collector.waitForCount(
                type: .ping, from: sender.endpoint.addressKey, atLeast: pingCount, timeout: .seconds(5))
              try await sender.collector.waitForCount(
                type: .pong, from: receiver.endpoint.addressKey, atLeast: pingCount, timeout: .seconds(5))

              let pingsReceived = await receiver.collector.count(
                type: .ping, from: sender.endpoint.addressKey)
              let pongsReceived = await sender.collector.count(
                type: .pong, from: receiver.endpoint.addressKey)
              #expect(pingsReceived == pingCount)
              #expect(pongsReceived == pingCount)
            }
          }
        }
        try await group.waitForAll()
      }

      for entry in nodes {
        try await entry.node.shutdown()
      }
    }
  }
}
