import NIOCore
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

  @Test func waitForConnectionReturnsWhenPeerConnects() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_120)
      let peerB = NodeAddress(host: host, port: 29_121)

      let nodeB = try await makeTransport(local: peerB, group: group) { _, _ in }
      let nodeA = try await makeTransport(local: peerA, group: group) { _, _ in }

      async let connected = nodeA.waitForConnection(to: peerB.addressKey, timeout: .seconds(100))
      await nodeA.connect(to: peerB)

      #expect(await connected)
      #expect(await nodeA.isConnected(to: peerB.addressKey))

      try await nodeA.shutdown()
      try await nodeB.shutdown()
    }
  }

  @Test func waitForConnectionTimesOutWhenPeerIsUnreachable() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_122)
      let unreachable = NodeAddress(host: host, port: 29_199)

      let nodeA = try await makeTransport(local: peerA, group: group) { _, _ in }
      await nodeA.connect(to: unreachable)

      let connected = await nodeA.waitForConnection(to: unreachable.addressKey, timeout: .milliseconds(100))
      #expect(!connected)
      #expect(await !nodeA.isConnected(to: unreachable.addressKey))

      try await nodeA.shutdown()
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

  @Test func rejectsDialedPeerAnnouncingWrongIdentity() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_130)
      let rogueAddress = NodeAddress(host: host, port: 29_131)

      let rogue = try await TCPHandshakeTestPeer.startWrongIdentityAcceptor(
        host: host,
        port: rogueAddress.port,
        helloID: "wrong-peer-id",
        eventLoopGroup: group
      )

      let nodeA = try await makeTransport(local: peerA, group: group) { _, _ in }
      await nodeA.connect(to: rogueAddress)

      let connected = await nodeA.waitForConnection(to: rogueAddress.addressKey, timeout: .milliseconds(500))
      #expect(!connected)
      #expect(await nodeA.connectedPeers().isEmpty)

      await rogue.shutdown()
      try await nodeA.shutdown()
    }
  }

  @Test func rejectsAcceptedPeerGreetingAsLocalIdentity() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_132)

      let nodeA = try await makeTransport(local: peerA, group: group) { _, _ in }

      try await TCPHandshakeTestPeer.dialAndGreet(
        target: peerA,
        greetAs: peerA.addressKey,
        eventLoopGroup: group
      )

      try await Task.sleep(for: .milliseconds(200))
      #expect(await nodeA.connectedPeers().isEmpty)

      try await nodeA.shutdown()
    }
  }

  @Test func reapsStalledHandshakeAfterTimeout() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_134)
      let silentAddress = NodeAddress(host: host, port: 29_135)

      let silent = try await TCPHandshakeTestPeer.startSilentAcceptor(
        host: host,
        port: silentAddress.port,
        eventLoopGroup: group
      )

      let nodeA = TCPTransport(
        configuration: TransportConfiguration(
          localPeerID: peerA.addressKey,
          host: host,
          port: peerA.port,
          handshakeTimeout: .milliseconds(250)
        ),
        eventLoopGroup: group,
        logger: TestHelpers.quietLogger
      )
      try await nodeA.start { _, _ in }

      await nodeA.connect(to: silentAddress)

      // The peer never sends a handshake frame, so the only thing that can close
      // the half-open connection is the dialer's handshake-timeout reaper.
      await TestHelpers.waitUntil(timeout: .seconds(2)) {
        await silent.closedConnectionCount > 0
      }

      #expect(await silent.closedConnectionCount > 0)
      #expect(await !nodeA.isConnected(to: silentAddress.addressKey))
      #expect(await nodeA.connectedPeers().isEmpty)

      // The dialing slot is freed, so a fresh dial is admitted (not silently
      // dropped as a duplicate of the still-pending one). Re-dial in the poll
      // loop since `finishDialing` clears the slot only after the close
      // propagates back to the dialer.
      await TestHelpers.waitUntil(timeout: .seconds(3)) {
        await nodeA.connect(to: silentAddress)
        return await silent.closedConnectionCount > 1
      }
      #expect(await silent.closedConnectionCount > 1)

      await silent.shutdown()
      try await nodeA.shutdown()
    }
  }

  @Test func rejectsSelfDial() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let peerA = NodeAddress(host: host, port: 29_133)

      let nodeA = try await makeTransport(local: peerA, group: group) { _, _ in }
      await nodeA.connect(to: peerA)

      let connected = await nodeA.waitForConnection(to: peerA.addressKey, timeout: .milliseconds(500))
      #expect(!connected)
      #expect(await nodeA.connectedPeers().isEmpty)

      try await nodeA.shutdown()
    }
  }
}

private enum TCPHandshakeTestPeer {
  struct Acceptor: Sendable {
    let server: NIOAsyncChannel<NIOAsyncChannel<Message, Message>, Never>
    let supervisor: Task<Void, Never>

    func shutdown() async {
      supervisor.cancel()
      server.channel.close(promise: nil)
      _ = await supervisor.value
    }
  }

  actor SilentAcceptor {
    private var server: NIOAsyncChannel<NIOAsyncChannel<Message, Message>, Never>?
    private var supervisor: Task<Void, Never>?
    private var closedConnections = 0

    var closedConnectionCount: Int { closedConnections }

    func start(host: String, port: Int, eventLoopGroup: MultiThreadedEventLoopGroup) async throws {
      let server = try await ServerBootstrap(group: eventLoopGroup)
        .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
        .bind(host: host, port: port, childChannelInitializer: messageChannelInitializer())
      self.server = server
      self.supervisor = Task { [weak self] in
        await withDiscardingTaskGroup { group in
          do {
            try await server.executeThenClose { inbound in
              for try await child in inbound {
                group.addTask {
                  do {
                    try await child.executeThenClose { inbound, _ in
                      for try await _ in inbound {}
                    }
                  } catch {}
                  await self?.noteConnectionClosed()
                }
              }
            }
          } catch {}
        }
      }
    }

    private func noteConnectionClosed() {
      closedConnections += 1
    }

    func shutdown() async {
      supervisor?.cancel()
      server?.channel.close(promise: nil)
      _ = await supervisor?.value
      supervisor = nil
      server = nil
    }
  }

  static func startWrongIdentityAcceptor(
    host: String,
    port: Int,
    helloID: PeerId,
    eventLoopGroup: MultiThreadedEventLoopGroup
  ) async throws -> Acceptor {
    let server = try await ServerBootstrap(group: eventLoopGroup)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .bind(host: host, port: port, childChannelInitializer: messageChannelInitializer())

    let supervisor = Task {
      await withDiscardingTaskGroup { group in
        do {
          try await server.executeThenClose { inbound in
            for try await child in inbound {
              group.addTask {
                await respondToHandshake(asAcceptedPeer: child, helloID: helloID)
              }
            }
          }
        } catch {}
      }
    }

    return Acceptor(server: server, supervisor: supervisor)
  }

  static func startSilentAcceptor(
    host: String,
    port: Int,
    eventLoopGroup: MultiThreadedEventLoopGroup
  ) async throws -> SilentAcceptor {
    let acceptor = SilentAcceptor()
    try await acceptor.start(host: host, port: port, eventLoopGroup: eventLoopGroup)
    return acceptor
  }

  static func dialAndGreet(
    target: NodeAddress,
    greetAs: PeerId,
    eventLoopGroup: MultiThreadedEventLoopGroup
  ) async throws {
    let asyncChannel = try await ClientBootstrap(group: eventLoopGroup)
      .channelOption(.socketOption(.so_reuseaddr), value: 1)
      .connect(
        host: target.host,
        port: target.port,
        channelInitializer: messageChannelInitializer()
      )

    try await asyncChannel.executeThenClose { inbound, outbound in
      try await outbound.write(
        HandshakeFrame.signal(magic: HandshakeFrame.magic, version: HandshakeFrame.version).message
      )
      try await outbound.write(HandshakeFrame.greet(greetAs).message)
      try await Task.sleep(for: .milliseconds(200))
      for try await _ in inbound {}
    }
  }

  private static func respondToHandshake(
    asAcceptedPeer asyncChannel: NIOAsyncChannel<Message, Message>,
    helloID: PeerId
  ) async {
    do {
      try await asyncChannel.executeThenClose { inbound, outbound in
        try await outbound.write(
          HandshakeFrame.signal(magic: HandshakeFrame.magic, version: HandshakeFrame.version).message
        )

        var handshakeVerified = false
        for try await wire in inbound {
          guard let frame = HandshakeFrame(wire) else { return }

          if !handshakeVerified {
            guard case .signal(let magic, let version) = frame,
              magic == HandshakeFrame.magic,
              version == HandshakeFrame.version
            else { return }
            handshakeVerified = true
            continue
          }

          guard case .greet = frame else { return }
          try await outbound.write(HandshakeFrame.hello(helloID).message)
          try await Task.sleep(for: .milliseconds(100))
          return
        }
      }
    } catch {}
  }

  @Sendable
  static func messageChannelInitializer() -> @Sendable (Channel) -> EventLoopFuture<
    NIOAsyncChannel<Message, Message>
  > {
    { channel in
      channel.eventLoop.makeCompletedFuture {
        try channel.pipeline.syncOperations.addHandler(
          ByteToMessageHandler(MessageDecoder(maxPayloadLength: Message.defaultMaxPayloadLength))
        )
        try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(MessageEncoder()))
        return try NIOAsyncChannel(
          wrappingChannelSynchronously: channel,
          configuration: .init(inboundType: Message.self, outboundType: Message.self)
        )
      }
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
      let aConnected = await nodeA.isConnected(to: peerB.addressKey)
      let bConnected = await nodeB.isConnected(to: peerA.addressKey)
      return aConnected && bConnected
    }
  }
}
