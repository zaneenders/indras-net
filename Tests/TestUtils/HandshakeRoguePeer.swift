import Foundation
import NIOCore
import NIOPosix

@testable import IndrasNet

/// Minimal TCP peer for exercising handshake identity checks outside `TCPTransport`.
public enum HandshakeRoguePeer {
  public struct Acceptor: Sendable {
    private let server: NIOAsyncChannel<NIOAsyncChannel<Message, Message>, Never>
    private let supervisor: Task<Void, Never>

    fileprivate init(
      server: NIOAsyncChannel<NIOAsyncChannel<Message, Message>, Never>,
      supervisor: Task<Void, Never>
    ) {
      self.server = server
      self.supervisor = supervisor
    }

    public var listenPort: Int? {
      server.channel.localAddress?.port
    }

    public func shutdown() async {
      supervisor.cancel()
      server.channel.close(promise: nil)
      _ = await supervisor.value
    }
  }

  /// Listens like a Raft peer but responds to `greet` with the given `helloID`.
  public static func startAcceptor(
    host: String = "127.0.0.1",
    port: Int,
    helloID: PeerId,
    eventLoopGroup: MultiThreadedEventLoopGroup
  ) async throws -> Acceptor {
    let server = try await ServerBootstrap(group: eventLoopGroup)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .bind(
        host: host,
        port: port,
        childChannelInitializer: messageChannelInitializer()
      )

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
        } catch {
          // Acceptor shut down.
        }
      }
    }

    return Acceptor(server: server, supervisor: supervisor)
  }

  /// Accepts TCP connections but never sends a handshake frame, so a dialer's
  /// handshake can never complete. Tracks how many connections the remote side
  /// closed, which is the observable signal that the dialer reaped a stalled
  /// handshake.
  public actor SilentAcceptor {
    private var server: NIOAsyncChannel<NIOAsyncChannel<Message, Message>, Never>?
    private var supervisor: Task<Void, Never>?
    private var closedConnections = 0

    public init() {}

    public var listenPort: Int? {
      server?.channel.localAddress?.port
    }

    public var closedConnectionCount: Int {
      closedConnections
    }

    fileprivate func start(
      host: String,
      port: Int,
      eventLoopGroup: MultiThreadedEventLoopGroup
    ) async throws {
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
                  await Self.drainSilently(child)
                  await self?.noteConnectionClosed()
                }
              }
            }
          } catch {
            // Acceptor shut down.
          }
        }
      }
    }

    private func noteConnectionClosed() {
      closedConnections += 1
    }

    public func shutdown() async {
      supervisor?.cancel()
      server?.channel.close(promise: nil)
      _ = await supervisor?.value
      supervisor = nil
      server = nil
    }

    private static func drainSilently(_ asyncChannel: NIOAsyncChannel<Message, Message>) async {
      do {
        try await asyncChannel.executeThenClose { inbound, _ in
          for try await _ in inbound {}
        }
      } catch {
        // Connection closed.
      }
    }
  }

  /// Starts a `SilentAcceptor` listening on the given port.
  public static func startSilentAcceptor(
    host: String = "127.0.0.1",
    port: Int,
    eventLoopGroup: MultiThreadedEventLoopGroup
  ) async throws -> SilentAcceptor {
    let acceptor = SilentAcceptor()
    try await acceptor.start(host: host, port: port, eventLoopGroup: eventLoopGroup)
    return acceptor
  }

  /// Dials a listening peer and greets with the given identity.
  public static func dialAndGreet(
    target: NodeAddress,
    greetAs: PeerId,
    eventLoopGroup: MultiThreadedEventLoopGroup,
    settle: Duration = .milliseconds(200)
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
      try await Task.sleep(for: settle)
      for try await _ in inbound {}
    }
  }
}

extension HandshakeRoguePeer {
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
    } catch {
      // Peer closed the connection after rejecting the handshake.
    }
  }

  @Sendable
  static func messageChannelInitializer(
    maxPayloadLength: UInt32 = Message.defaultMaxPayloadLength
  ) -> @Sendable (Channel) -> EventLoopFuture<NIOAsyncChannel<Message, Message>> {
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
}
