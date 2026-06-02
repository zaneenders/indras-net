import Logging
import NIO
import NIOCore
import NIOPosix

typealias PeerID = String
private typealias MessageChannel = NIOAsyncChannel<Message, Message>

private let log = Logger(label: "indras-net.transport")

typealias IndrasNetInboundHandler = @Sendable (Message, PeerID) async -> Void

public actor IndrasNetTCPTransport {
  private typealias ConnectionJob = @Sendable () async -> Void

  private enum ConnectionOrigin {
    case accepted
    case created
  }

  private let configuration: IndrasNetTCPConfiguration
  private let eventLoopGroup: MultiThreadedEventLoopGroup
  private var serverChannel: NIOAsyncChannel<MessageChannel, Never>?
  private var onMessage: IndrasNetInboundHandler?

  private var supervisorTask: Task<Void, Never>?
  private var jobContinuation: AsyncStream<ConnectionJob>.Continuation?

  private var peerWriterChannels: [PeerID: NIOAsyncChannelOutboundWriter<Message>] = [:]
  private var dialing: Set<PeerID> = []

  public init(
    configuration: IndrasNetTCPConfiguration,
    eventLoopGroup: MultiThreadedEventLoopGroup = .singleton
  ) {
    self.configuration = configuration
    self.eventLoopGroup = eventLoopGroup
  }

  public func listenPort() async -> Int? {
    guard let address = self.serverChannel?.channel.localAddress else {
      return nil
    }
    return address.port
  }

  func connectedPeers() -> Set<PeerID> {
    Set(self.peerWriterChannels.keys)
  }

  func isConnected(to peer: PeerID) -> Bool {
    self.peerWriterChannels[peer] != nil
  }

  func send(_ message: Message, to peer: PeerID) async throws {
    guard let writer = self.peerWriterChannels[peer] else {
      throw IndrasNetTransportError.peerNotConnected(peer)
    }
    try await writer.write(message)
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
        childChannelInitializer: messageAsyncChannelInitializer()
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

  func connect(to peer: ClusterEndpoint) {
    let key = peer.addressKey
    guard self.configuration.localPeerID < key else { return }
    guard self.peerWriterChannels[key] == nil, !self.dialing.contains(key) else { return }
    self.dialing.insert(key)
    let scheduled = self.enqueue {
      await self.dial(peer)
      await self.finishDialing(key)
    }
    if !scheduled {
      self.dialing.remove(key)
    }
  }

  private func finishDialing(_ key: PeerID) {
    self.dialing.remove(key)
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
      log.notice("Accept loop ended: \(error)")
    }
  }

  private func dial(_ peer: ClusterEndpoint) async {
    do {
      let asyncChannel = try await ClientBootstrap(group: self.eventLoopGroup)
        .channelOption(.socketOption(.so_reuseaddr), value: 1)
        .connect(
          host: peer.host,
          port: peer.port,
          channelInitializer: messageAsyncChannelInitializer()
        )

      await self.handleConnection(asyncChannel: asyncChannel, origin: .created)
    } catch {
      log.debug("Outbound dial failed: \(peer)")
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

    var peerID: PeerID?
    defer {
      if let peerID { self.peerWriterChannels[peerID] = nil }
    }

    var version: UInt8?
    var magic: UInt8?
    do {
      try await asyncChannel.executeThenClose { inbound, outbound in
        try await outbound.write(Message.signal())
        if origin == .created {
          try await outbound.write(Message.greet(id: self.configuration.localPeerID))
        }

        for try await message in inbound {
          if let peerID {
            await onMessage(message, peerID)
          } else {
            guard magic != nil, version != nil else {
              guard let (m, v) = message.signalRead() else {
                return
              }
              guard m == self.configuration.magic && v == self.configuration.version else {
                return
              }
              magic = m
              version = v
              continue
            }
            switch (message.type, origin) {
            case (.hello, .created):
              peerID = message.helloPeerID()
            case (.greet, .accepted):
              peerID = message.greetPeerID()
              try await outbound.write(Message.hello(id: self.configuration.localPeerID))
            case (.greet, .created):
              return  // Both sides think they're the initiator. The < rule forbids this for any pair.
            case (.hello, .accepted):
              return  // Bad state
            default:
              return  // First frame wasn't a handshake message; reject.
            }
            guard let peerID else { return }
            self.peerWriterChannels[peerID] = outbound
            log.info("Connection: \(peerID)")
          }
        }
      }
    } catch {
      // Connection ended; fall through to unregister.
    }
  }
}

@Sendable
private func messageAsyncChannelInitializer(
  maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength
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
  case peerNotConnected(PeerID)
}
