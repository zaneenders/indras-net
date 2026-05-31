import NIOPosix
import Testing

@testable import IndrasNet

@Suite struct IndrasNetTCPNodeTests {
  @Test func twoNodesExchangePingPong() async throws {
    try await TestHelpers.withEventLoopGroup { sharedGroup in
      let host = "127.0.0.1"
      let peerA = ClusterEndpoint(host: host, port: 29_100)
      let peerB = ClusterEndpoint(host: host, port: 29_101)

      let collectorB = MessageCollector()
      let nodeB = IndrasNetTCPNode(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerB.peerID,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: sharedGroup
      )

      try await nodeB.start { message, peerID in
        await collectorB.record(message, from: peerID)
        if message.type == .ping {
          try? await nodeB.send(Message(type: .pong, payload: .init()), to: peerID)
        }
      }

      let collectorA = MessageCollector()
      let nodeA = IndrasNetTCPNode(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerA.peerID,
          host: host,
          port: peerA.port,
          peers: [peerB]
        ),
        eventLoopGroup: sharedGroup
      )

      try await nodeA.start { message, peerID in
        await collectorA.record(message, from: peerID)
      }

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        await nodeA.isConnected(to: peerB.peerID)
      }

      try await nodeA.send(Message(type: .ping, payload: .init()), to: peerB.peerID)

      let pongFromB = try await collectorA.waitForMessage(
        type: .pong, from: peerB.peerID, timeout: .seconds(5))
      #expect(pongFromB.type == .pong)

      let pingAtB = try await collectorB.waitForMessage(
        type: .ping, from: peerA.peerID, timeout: .seconds(5))
      #expect(pingAtB.type == .ping)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }

  @Test
  func inboundAcceptRegistersPeer() async throws {
    try await TestHelpers.withEventLoopGroup { sharedGroup in
      let host = "127.0.0.1"
      let peerA = ClusterEndpoint(host: host, port: 29_102)
      let peerB = ClusterEndpoint(host: host, port: 29_103)

      let collectorB = MessageCollector()
      let nodeB = IndrasNetTCPNode(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerB.peerID,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: sharedGroup
      )
      try await nodeB.start { message, peerID in
        await collectorB.record(message, from: peerID)
      }

      let nodeA = IndrasNetTCPNode(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerA.peerID,
          host: host,
          port: peerA.port,
          peers: [peerB]
        ),
        eventLoopGroup: sharedGroup
      )
      try await nodeA.start { _, _ in }

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        await nodeB.isConnected(to: peerA.peerID)
      }

      try await nodeA.send(Message(type: .ping, payload: .init()), to: peerB.peerID)
      let ping = try await collectorB.waitForMessage(
        type: .ping, from: peerA.peerID, timeout: .seconds(5))
      #expect(ping.type == .ping)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }
}
