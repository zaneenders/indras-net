import NIOPosix
import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct ConnectionTieBreakTests {

  @Test func mutualDialConvergesToSingleConnection() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerLow = NodeAddress(host: host, port: 29_110)
      let peerHigh = NodeAddress(host: host, port: 29_111)
      // Sanity: the lexicographic ordering the tie-break relies on.
      #expect(peerLow.addressKey < peerHigh.addressKey)

      func makeNode(_ local: NodeAddress) async throws -> (TCPTransport, MessageCollector) {
        let collector = MessageCollector()
        let node = TCPTransport(
          configuration: TransportConfiguration(
            localPeerID: local.addressKey,
            host: local.host,
            port: local.port
          ),
          eventLoopGroup: group
        )
        try await node.start { message, from in
          await collector.record(message, from: from)
          if message == .ping {
            try? await node.send(.pong, to: from)
          }
        }
        return (node, collector)
      }

      let (low, lowCollector) = try await makeNode(peerLow)
      let (high, highCollector) = try await makeNode(peerHigh)

      await low.connect(to: peerHigh)
      await high.connect(to: peerLow)

      await TestHelpers.waitUntil(timeout: .seconds(10)) {
        let lowReady = await low.isConnected(to: peerHigh.addressKey)
        let highReady = await high.isConnected(to: peerLow.addressKey)
        return lowReady && highReady
      }

      #expect(await low.connectedPeers() == [peerHigh.addressKey])
      #expect(await high.connectedPeers() == [peerLow.addressKey])

      try await low.send(.ping, to: peerHigh.addressKey)
      try await high.send(.ping, to: peerLow.addressKey)
      _ = try await highCollector.waitForMessage(
        type: .ping, from: peerLow.addressKey, timeout: .seconds(5))
      _ = try await lowCollector.waitForMessage(
        type: .pong, from: peerHigh.addressKey, timeout: .seconds(5))
      _ = try await lowCollector.waitForMessage(
        type: .ping, from: peerHigh.addressKey, timeout: .seconds(5))
      _ = try await highCollector.waitForMessage(
        type: .pong, from: peerLow.addressKey, timeout: .seconds(5))

      try await low.shutdown()
      try await high.shutdown()
    }
  }
}
