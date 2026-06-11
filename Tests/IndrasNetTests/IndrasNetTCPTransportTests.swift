import NIOPosix
import TestUtils
import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct IndrasNetTCPTransportTests {
  @Test func connectedPeersExchangeMessages() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_100)
      let peerB = NodeAddress(host: host, port: 29_101)
      let probe = TestHelpers.transportProbe

      let collectorB = MessageCollector()
      let nodeB = TCPTransport(
        configuration: TransportConfiguration(
          localPeerID: peerB.addressKey,
          host: host,
          port: peerB.port
        ),
        eventLoopGroup: group,
        logger: TestHelpers.quietLogger
      )
      try await nodeB.start { message, from in
        await collectorB.record(message, from: from)
        try? await nodeB.send(message, to: from)
      }

      let collectorA = MessageCollector()
      let nodeA = try await makeTransport(local: peerA, group: group) { message, from in
        await collectorA.record(message, from: from)
      }

      await nodeA.connect(to: peerB)
      await waitForMutualConnection(nodeA, nodeB, peerA: peerA, peerB: peerB)

      try await nodeA.send(probe, to: peerB.addressKey)

      let echoed = try await collectorA.waitForMessage(
        type: probe, from: peerB.addressKey, timeout: .seconds(5))
      #expect(echoed == probe)

      let delivered = try await collectorB.waitForMessage(
        type: probe, from: peerA.addressKey, timeout: .seconds(5))
      #expect(delivered == probe)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }

  @Test func outboundDialDeliversMessageToPeerHandler() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_102)
      let peerB = NodeAddress(host: host, port: 29_103)
      let probe = TestHelpers.transportProbe

      let collectorB = MessageCollector()
      let nodeB = try await makeTransport(local: peerB, group: group) { message, from in
        await collectorB.record(message, from: from)
      }

      let nodeA = try await makeTransport(local: peerA, group: group) { _, _ in }
      await nodeA.connect(to: peerB)
      await waitForMutualConnection(nodeA, nodeB, peerA: peerA, peerB: peerB)

      try await nodeA.send(probe, to: peerB.addressKey)

      let received = try await collectorB.waitForMessage(
        type: probe, from: peerA.addressKey, timeout: .seconds(5))
      #expect(received == probe)

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }

  @Test func mutualDialConvergesToSingleConnection() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerLow = NodeAddress(host: host, port: 29_110)
      let peerHigh = NodeAddress(host: host, port: 29_111)
      let probe = TestHelpers.transportProbe

      #expect(peerLow.addressKey < peerHigh.addressKey)

      let lowCollector = MessageCollector()
      let low = try await makeTransport(local: peerLow, group: group) { message, from in
        await lowCollector.record(message, from: from)
      }

      let highCollector = MessageCollector()
      let high = try await makeTransport(local: peerHigh, group: group) { message, from in
        await highCollector.record(message, from: from)
      }

      await low.connect(to: peerHigh)
      await high.connect(to: peerLow)
      await waitForMutualConnection(low, high, peerA: peerLow, peerB: peerHigh)

      #expect(await low.connectedPeers() == [peerHigh.addressKey])
      #expect(await high.connectedPeers() == [peerLow.addressKey])

      try await low.send(probe, to: peerHigh.addressKey)
      try await high.send(probe, to: peerLow.addressKey)

      let atHigh = try await highCollector.waitForMessage(
        type: probe, from: peerLow.addressKey, timeout: .seconds(5))
      let atLow = try await lowCollector.waitForMessage(
        type: probe, from: peerHigh.addressKey, timeout: .seconds(5))
      #expect(atHigh == probe)
      #expect(atLow == probe)

      try await low.shutdown()
      try await high.shutdown()
    }
  }
}

extension IndrasNetTCPTransportTests {
  private func makeTransport(
    local: NodeAddress,
    group: MultiThreadedEventLoopGroup,
    onMessage: @escaping @Sendable (RaftMessage, PeerId) async -> Void
  ) async throws -> TCPTransport {
    let node = TCPTransport(
      configuration: TransportConfiguration(
        localPeerID: local.addressKey,
        host: local.host,
        port: local.port
      ),
      eventLoopGroup: group,
      logger: TestHelpers.quietLogger
    )
    try await node.start(onMessage: onMessage)
    return node
  }

  private func waitForMutualConnection(
    _ nodeA: TCPTransport,
    _ nodeB: TCPTransport,
    peerA: NodeAddress,
    peerB: NodeAddress
  ) async {
    await TestHelpers.waitUntil(timeout: .seconds(10)) {
      let aReady = await nodeA.isConnected(to: peerB.addressKey)
      let bReady = await nodeB.isConnected(to: peerA.addressKey)
      return aReady && bReady
    }
  }
}
