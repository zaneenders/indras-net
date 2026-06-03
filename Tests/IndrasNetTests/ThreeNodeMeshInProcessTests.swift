import NIOPosix
import Testing

@testable import IndrasNet

@Suite struct ThreeNodeMeshInProcessTests {
  @Test func threeNodesFormMeshAndPingPong() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let endpointC = NodeAddress(host: host, port: 29_200)
      let endpointB = NodeAddress(host: host, port: 29_201)
      let endpointA = NodeAddress(host: host, port: 29_202)

      func makeNode(
        local: NodeAddress
      ) async throws -> (node: TCPTransport, collector: MessageCollector) {
        let collector = MessageCollector()
        let node = TCPTransport(
          configuration: TransportConfiguration(
            localPeerID: local.addressKey,
            host: local.host,
            port: local.port
          ),
          eventLoopGroup: group,
          logger: TestHelpers.quietLogger
        )
        try await node.start { message, from in
          await collector.record(message, from: from)
          if message == .ping {
            try? await node.send(.pong, to: from)
          }
        }
        return (node, collector)
      }

      let a = try await makeNode(local: endpointA)
      let b = try await makeNode(local: endpointB)
      let c = try await makeNode(local: endpointC)

      let nodes: [(endpoint: NodeAddress, node: TCPTransport, collector: MessageCollector)] =
        [(endpointA, a.node, a.collector), (endpointB, b.node, b.collector), (endpointC, c.node, c.collector)]

      for entry in nodes {
        for other in nodes where other.endpoint.addressKey != entry.endpoint.addressKey {
          await entry.node.connect(to: other.endpoint)
        }
      }

      await TestHelpers.waitUntil(timeout: .seconds(10)) {
        for node in nodes {
          for other in nodes where other.endpoint.addressKey != node.endpoint.addressKey {
            if await !node.node.isConnected(to: other.endpoint.addressKey) {
              return false
            }
          }
        }
        return true
      }

      try await withThrowingTaskGroup(of: Void.self) { group in
        for sender in nodes {
          for receiver in nodes where receiver.endpoint.addressKey != sender.endpoint.addressKey {
            group.addTask {
              let pingCount = Int.random(in: 1...5)
              for _ in 0..<pingCount {
                try await sender.node.send(.ping, to: receiver.endpoint.addressKey)
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
