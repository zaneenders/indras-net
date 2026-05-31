import NIOCore
import NIOPosix

public typealias IndrasNetInboundHandler = @Sendable (Message, PeerID) async -> Void

public actor IndrasNetTCPNode {
  /// A unit of connection work (the accept loop, an outbound dial, or a single
  /// peer-connection handler) run as a child of the supervisor's task group.
  private typealias ConnectionJob = @Sendable () async -> Void

  private let configuration: IndrasNetTCPConfiguration
  private let eventLoopGroup: MultiThreadedEventLoopGroup
  private let peerManager = PeerConnectionManager()

  private var serverChannel: NIOAsyncChannel<MessageAsyncChannel.AsyncChannel, Never>?
  private var onMessage: IndrasNetInboundHandler?

  /// Owns the discarding task group that runs every connection job. Awaiting it
  /// in `shutdown()` awaits all in-flight connection work before the caller can
  /// tear down the `EventLoopGroup`.
  private var supervisorTask: Task<Void, Never>?
  /// Feeds connection jobs to the supervisor. `nil` once shutting down, after
  /// which no new jobs are accepted.
  private var jobContinuation: AsyncStream<ConnectionJob>.Continuation?

  /// Open peer-connection channels, so `shutdown()` can close them and thereby
  /// end each handler's inbound sequence.
  private var connectionChannels: [ObjectIdentifier: any Channel] = [:]

  public init(
    configuration: IndrasNetTCPConfiguration,
    eventLoopGroup: MultiThreadedEventLoopGroup = .singleton
  ) {
    self.configuration = configuration
    self.eventLoopGroup = eventLoopGroup
  }

  private var dialablePeers: [ClusterEndpoint] {
    self.configuration.peers.filter { self.configuration.localPeerID < $0.peerID }
  }

  public func listenPort() async -> Int? {
    guard let address = self.serverChannel?.channel.localAddress else {
      return nil
    }
    return address.port
  }

  public func start(onMessage: @escaping IndrasNetInboundHandler) async throws {
    guard self.onMessage == nil else {
      return
    }
    self.onMessage = onMessage

    let server = try await ServerBootstrap(group: self.eventLoopGroup)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .bind(
        host: self.configuration.host,
        port: self.configuration.port,
        childChannelInitializer: MessageAsyncChannel.messageAsyncChannelInitializer()
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

    // The accept loop and the initial dials run as connection jobs, so they are
    // owned and awaited by the supervisor's task group.
    self.enqueue { await self.runAcceptLoop(server: server) }
    for peer in self.dialablePeers {
      self.enqueue { await self.connect(to: peer) }
    }
  }

  public func send(_ message: Message, to peerID: PeerID) async throws {
    try await self.peerManager.send(message, to: peerID)
  }

  public func isConnected(to peerID: PeerID) async -> Bool {
    await self.peerManager.contains(peerID: peerID)
  }

  public func connectMissingPeers() async {
    for peer in self.dialablePeers {
      if await !self.peerManager.contains(peerID: peer.peerID) {
        self.enqueue { await self.connect(to: peer) }
      }
    }
  }

  public func shutdown() async throws {
    self.onMessage = nil

    // Stop accepting new connection jobs; the supervisor's task group keeps
    // running the jobs already in flight until they complete.
    self.jobContinuation?.finish()
    self.jobContinuation = nil

    if let server = self.serverChannel {
      server.channel.close(promise: nil)
      self.serverChannel = nil
    }

    // Close every established peer connection so each handler's inbound sequence
    // ends and its `executeThenClose` returns.
    for channel in self.connectionChannels.values {
      channel.close(promise: nil)
    }

    // Awaiting the supervisor awaits the whole discarding task group: the accept
    // loop, every dial, and every connection handler. Once it returns, no
    // channel is still being torn down, so the caller can safely shut down the
    // EventLoopGroup.
    _ = await self.supervisorTask?.value
    self.supervisorTask = nil
  }

  /// Submits a connection job to the supervisor. Returns `false` if the node is
  /// shutting down and the job was not scheduled, so the caller can clean up any
  /// resource it was about to hand off.
  @discardableResult
  private func enqueue(_ job: @escaping ConnectionJob) -> Bool {
    guard let continuation = self.jobContinuation else { return false }
    continuation.yield(job)
    return true
  }

  private func registerChannel(_ channel: any Channel) {
    self.connectionChannels[ObjectIdentifier(channel)] = channel
  }

  private func unregisterChannel(_ channel: any Channel) {
    self.connectionChannels[ObjectIdentifier(channel)] = nil
  }

  private func runAcceptLoop(server: NIOAsyncChannel<MessageAsyncChannel.AsyncChannel, Never>) async {
    do {
      try await server.executeThenClose { inbound in
        for try await childChannel in inbound {
          let scheduled = self.enqueue {
            await self.handleConnection(asyncChannel: childChannel)
          }
          if !scheduled {
            // Shutting down: nothing will handle this channel, so close it here
            // rather than leak it past the EventLoopGroup's lifetime.
            childChannel.channel.close(promise: nil)
          }
        }
      }
    } catch {
      // Accept loop ended (shutdown or error).
    }
  }

  private func connect(to peer: ClusterEndpoint) async {
    do {
      let asyncChannel = try await ClientBootstrap(group: self.eventLoopGroup)
        .channelOption(.socketOption(.so_reuseaddr), value: 1)
        .connect(
          host: peer.host,
          port: peer.port,
          channelInitializer: MessageAsyncChannel.messageAsyncChannelInitializer()
        )

      await self.handleConnection(asyncChannel: asyncChannel)
    } catch {
      // Outbound dial failed; caller may retry via connectMissingPeers().
    }
  }

  private func handleConnection(
    asyncChannel: MessageAsyncChannel.AsyncChannel,
  ) async {
    let channel = asyncChannel.channel
    guard let onMessage = self.onMessage else {
      // Node is shutting down; don't leave the freshly opened channel open.
      channel.close(promise: nil)
      return
    }

    self.registerChannel(channel)
    defer { self.unregisterChannel(channel) }

    let localPeerID = self.configuration.localPeerID

    var peerID: PeerID?
    do {
      try await asyncChannel.executeThenClose { inbound, outbound in
        try await outbound.write(Message.hello(peerID: localPeerID))

        for try await message in inbound {
          if let peerID {
            await onMessage(message, peerID)
          } else {
            // Protocol invariant: the first frame is the peer's hello.
            guard let remotePeerID = try? message.helloPeerID() else { return }
            peerID = remotePeerID
            await self.peerManager.register(peerID: remotePeerID, outbound: outbound)
          }
        }
      }
    } catch {
      // Connection ended; fall through to unregister.
    }

    if let peerID {
      await self.peerManager.unregister(peerID: peerID)
    }
  }
}
