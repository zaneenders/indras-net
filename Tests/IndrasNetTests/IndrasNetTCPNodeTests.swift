import NIOPosix
import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct IndrasNetTCPNodeTests {
  @Test func twoNodesExchangePingPong() async throws {
    try await TestHelpers.withEventLoopGroup { sharedGroup in
      let host = "127.0.0.1"
      let peerA = ClusterEndpoint(host: host, port: 29_100)
      let peerB = ClusterEndpoint(host: host, port: 29_101)

      let collectorB = MessageCollector()
      let nodeB = IndrasNetTCPTransport(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerB.addressKey,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: sharedGroup
      )

      try await nodeB.start { message, peerID in
        await collectorB.record(message, from: peerID)
        if message.type == .ping {
          // TODO: send message
        }
      }

      let collectorA = MessageCollector()
      let nodeA = IndrasNetTCPTransport(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerA.addressKey,
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
        // TODO: Is Node connected
        return true
      }

      // TODO: Send ping A -> B

      let pongFromB = try await collectorA.waitForMessage(
        type: .pong, from: peerB.addressKey, timeout: .seconds(5))
      #expect(pongFromB.type == .pong)

      let pingAtB = try await collectorB.waitForMessage(
        type: .ping, from: peerA.addressKey, timeout: .seconds(5))
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
      let nodeB = IndrasNetTCPTransport(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerB.addressKey,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: sharedGroup
      )
      try await nodeB.start { message, peerID in
        await collectorB.record(message, from: peerID)
      }

      let nodeA = IndrasNetTCPTransport(
        configuration: IndrasNetTCPConfiguration(
          localPeerID: peerA.addressKey,
          host: host,
          port: peerA.port,
          peers: [peerB]
        ),
        eventLoopGroup: sharedGroup
      )
      try await nodeA.start { _, _ in }

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        // TODO: Is Node connected
        return true
      }

      // TODO: send ping
      let ping = try await collectorB.waitForMessage(
        type: .ping, from: peerA.addressKey, timeout: .seconds(5))
      #expect(ping.type == .ping)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }
}
