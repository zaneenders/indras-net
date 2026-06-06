import Logging
import NIO
import NIOCore
import NIOPosix

typealias PeerId = String
private typealias MessageChannel = NIOAsyncChannel<Message, Message>

typealias IndrasNetInboundHandler = @Sendable (AppMessage, PeerId) async -> Void

public actor TCPTransport {
  private typealias ConnectionJob = @Sendable () async -> Void

  private enum ConnectionOrigin {
    case accepted
    case created
  }

  private struct Connection {
    let id: UInt64
    let channel: any Channel
    let writer: NIOAsyncChannelOutboundWriter<Message>
  }

  private let configuration: TransportConfiguration
  private let eventLoopGroup: MultiThreadedEventLoopGroup
  private let logger: Logger
  private var serverChannel: NIOAsyncChannel<MessageChannel, Never>?
  private var onMessage: IndrasNetInboundHandler?

  private var supervisorTask: Task<Void, Never>?
  private var jobContinuation: AsyncStream<ConnectionJob>.Continuation?

  private var connections: [PeerId: Connection] = [:]
  private var dialing: Set<PeerId> = []
  private var nextConnectionID: UInt64 = 0

  public init(
    configuration: TransportConfiguration,
    eventLoopGroup: MultiThreadedEventLoopGroup = .singleton,
    logger: Logger? = nil
  ) {
    self.configuration = configuration
    self.eventLoopGroup = eventLoopGroup
    self.logger = logger ?? Logger(label: "indras-net.transport")
  }

  public func listenPort() async -> Int? {
    guard let address = self.serverChannel?.channel.localAddress else {
      return nil
    }
    return address.port
  }

  func connectedPeers() -> Set<PeerId> {
    Set(self.connections.keys)
  }

  func isConnected(to peer: PeerId) -> Bool {
    self.connections[peer] != nil
  }

  func send(_ message: AppMessage, to peer: PeerId) async throws {
    guard let connection = self.connections[peer] else {
      throw IndrasNetTransportError.peerNotConnected(peer)
    }
    try await connection.writer.write(message.message)
  }

  func start(onMessage: @escaping IndrasNetInboundHandler) async throws {
    guard self.onMessage == nil else {
      return
    }
    self.onMessage = onMessage

    let server = try await ServerBootstrap(group: self.eventLoopGroup)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .bind(
        host: self.configuration.host,
        port: self.configuration.port,
        childChannelInitializer: asyncChannelInitializer()
      )

    self.serverChannel = server

    let (jobs, continuation) = AsyncStream.makeStream(of: ConnectionJob.self)
    self.jobContinuation = continuation
    self.supervisorTask = Task {
      await withDiscardingTaskGroup { group in
        for await job in jobs {
          group.addTask { await job() }
        }
      }
    }

    self.enqueue { await self.runAcceptLoop(server: server) }
  }

  func connect(to peer: NodeAddress) {
    let key = peer.addressKey
    guard self.connections[key] == nil, !self.dialing.contains(key) else { return }
    self.dialing.insert(key)
    let scheduled = self.enqueue {
      await self.dial(peer)
      await self.finishDialing(key)
    }
    if !scheduled {
      self.dialing.remove(key)
    }
  }

  private func finishDialing(_ key: PeerId) {
    self.dialing.remove(key)
  }

  private func mintConnectionID() -> UInt64 {
    defer { self.nextConnectionID += 1 }
    return self.nextConnectionID
  }

  public func shutdown() async throws {
    self.onMessage = nil

    self.jobContinuation?.finish()
    self.jobContinuation = nil

    if let server = self.serverChannel {
      server.channel.close(promise: nil)
      self.serverChannel = nil
    }

    self.supervisorTask?.cancel()
    _ = await self.supervisorTask?.value
    self.supervisorTask = nil
  }

  @discardableResult
  private func enqueue(_ job: @escaping ConnectionJob) -> Bool {
    guard let continuation = self.jobContinuation else { return false }
    continuation.yield(job)
    return true
  }

  private func runAcceptLoop(server: NIOAsyncChannel<MessageChannel, Never>) async {
    do {
      try await server.executeThenClose { inbound in
        for try await childChannel in inbound {
          let scheduled = self.enqueue {
            await self.handleConnection(asyncChannel: childChannel, origin: .accepted)
          }
          if !scheduled {
            childChannel.channel.close(promise: nil)
          }
        }
      }
    } catch {
      self.logger.notice("Accept loop ended: \(error)")
    }
  }

  private func dial(_ peer: NodeAddress) async {
    do {
      let asyncChannel = try await ClientBootstrap(group: self.eventLoopGroup)
        .channelOption(.socketOption(.so_reuseaddr), value: 1)
        .connect(
          host: peer.host,
          port: peer.port,
          channelInitializer: asyncChannelInitializer()
        )

      await self.handleConnection(asyncChannel: asyncChannel, origin: .created)
    } catch {
      self.logger.debug("Outbound dial failed: \(peer)")
    }
  }

  private func handleConnection(
    asyncChannel: MessageChannel,
    origin: ConnectionOrigin
  ) async {
    let channel = asyncChannel.channel
    guard let onMessage = self.onMessage else {
      channel.close(promise: nil)
      return
    }

    let connectionID = self.mintConnectionID()
    var peerID: PeerId?
    defer {
      if let peerID, self.connections[peerID]?.id == connectionID {
        self.connections.removeValue(forKey: peerID)
      }
    }

    var handshakeVerified = false
    do {
      try await asyncChannel.executeThenClose { inbound, outbound in
        try await outbound.write(
          HandshakeFrame.signal(
            magic: self.configuration.magic,
            version: self.configuration.version
          ).message
        )
        if origin == .created {
          try await outbound.write(HandshakeFrame.greet(self.configuration.localPeerID).message)
        }

        for try await wire in inbound {
          if let peerID {
            guard let app = AppMessage(wire) else {
              self.logger.warning("Have: \(peerID) unable to decode AppMessage from \(wire)")
              continue
            }
            await onMessage(app, peerID)
            continue
          }

          guard let frame = HandshakeFrame(wire) else {
            self.logger.warning("unable to decode HandshakeFrame from \(wire)")
            return
          }

          if !handshakeVerified {
            guard case .signal(let magic, let version) = frame,
              self.configuration.magic == magic,
              self.configuration.version == version
            else { return }
            handshakeVerified = true
            continue
          }

          switch (frame, origin) {
          case (.hello(let id), .created):
            peerID = id  // I dialed and expect their hello.
          case (.greet(let id), .accepted):
            peerID = id  // They dialed and greeted; answer with my hello.
            try await outbound.write(HandshakeFrame.hello(self.configuration.localPeerID).message)
          default:
            return  // Wrong handshake frame for this side. Bad state.
          }

          guard let peerID else { return }
          let connection = Connection(id: connectionID, channel: channel, writer: outbound)
          guard self.adopt(connection, peerID: peerID, origin: origin) else {
            return
          }
          self.logger.info("Connection: \(peerID)")
        }
      }
    } catch {
      // Connection ended; fall through to unregister.
    }
  }

  // Weather to adopt the given connection over an existing one
  private func adopt(_ connection: Connection, peerID: PeerId, origin: ConnectionOrigin) -> Bool {
    if let existing = self.connections[peerID] {
      // Both ends keep the connection initiated by the lower peer ID, so they
      // deterministically converge on the same surviving socket.
      let initiatorIsLocal = origin == .created
      let localIsLower = self.configuration.localPeerID < peerID
      guard initiatorIsLocal == localIsLower else {
        return false
      }
      existing.channel.close(promise: nil)
      self.logger.info("Resolved duplicate to \(peerID): kept #\(connection.id), dropped #\(existing.id)")
    }
    self.connections[peerID] = connection
    return true
  }
}

@Sendable
private func asyncChannelInitializer(
  maxPayloadLength: UInt32 = Message.defaultMaxPayloadLength
) -> @Sendable (Channel) -> EventLoopFuture<MessageChannel> {
  { channel in
    channel.eventLoop.makeCompletedFuture {
      try channel.pipeline.syncOperations.addHandler(
        ByteToMessageHandler(MessageDecoder(maxPayloadLength: maxPayloadLength))
      )
      try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(MessageEncoder()))
      return try NIOAsyncChannel(
        wrappingChannelSynchronously: channel,
        configuration: .init(
          inboundType: Message.self,
          outboundType: Message.self
        )
      )
    }
  }
}

enum IndrasNetTransportError: Error, Equatable, Sendable {
  case peerNotConnected(PeerId)
}
