import NIOPosix
import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct IndrasNetTCPNodeTests {
  @Test func twoNodesExchangePingPong() async throws {
    try await TestHelpers.withEventLoopGroup { sharedGroup in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_100)
      let peerB = NodeAddress(host: host, port: 29_101)

      let collectorB = MessageCollector()
      let nodeB = TCPTransport(
        configuration: TransportConfiguration(
          localPeerID: peerB.addressKey,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: sharedGroup
      )

      try await nodeB.start { message, peerID in
        await collectorB.record(message, from: peerID)
        if message == .ping {
          try? await nodeB.send(.pong, to: peerID)
        }
      }

      let collectorA = MessageCollector()
      let nodeA = TCPTransport(
        configuration: TransportConfiguration(
          localPeerID: peerA.addressKey,
          host: host,
          port: peerA.port
        ),
        eventLoopGroup: sharedGroup
      )

      try await nodeA.start { message, peerID in
        await collectorA.record(message, from: peerID)
      }
      await nodeA.connect(to: peerB)

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        let aReady = await nodeA.isConnected(to: peerB.addressKey)
        let bReady = await nodeB.isConnected(to: peerA.addressKey)
        return aReady && bReady
      }

      try await nodeA.send(.ping, to: peerB.addressKey)

      let pongFromB = try await collectorA.waitForMessage(
        type: .pong, from: peerB.addressKey, timeout: .seconds(5))
      #expect(pongFromB == .pong)

      let pingAtB = try await collectorB.waitForMessage(
        type: .ping, from: peerA.addressKey, timeout: .seconds(5))
      #expect(pingAtB == .ping)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }

  @Test
  func inboundAcceptRegistersPeer() async throws {
    try await TestHelpers.withEventLoopGroup { sharedGroup in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_102)
      let peerB = NodeAddress(host: host, port: 29_103)

      let collectorB = MessageCollector()
      let nodeB = TCPTransport(
        configuration: TransportConfiguration(
          localPeerID: peerB.addressKey,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: sharedGroup
      )
      try await nodeB.start { message, peerID in
        await collectorB.record(message, from: peerID)
      }

      let nodeA = TCPTransport(
        configuration: TransportConfiguration(
          localPeerID: peerA.addressKey,
          host: host,
          port: peerA.port
        ),
        eventLoopGroup: sharedGroup
      )
      try await nodeA.start { _, _ in }
      await nodeA.connect(to: peerB)

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        let aReady = await nodeA.isConnected(to: peerB.addressKey)
        let bReady = await nodeB.isConnected(to: peerA.addressKey)
        return aReady && bReady
      }

      try await nodeA.send(.ping, to: peerB.addressKey)
      let ping = try await collectorB.waitForMessage(
        type: .ping, from: peerA.addressKey, timeout: .seconds(5))
      #expect(ping == .ping)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }
}
