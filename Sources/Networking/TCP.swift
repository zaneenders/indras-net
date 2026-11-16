public import NIOCore
import NIOPosix

public typealias Listener = NIOAsyncChannel<Connection, Never>

/// TCP adapters for the frame codec, using NIO's scoped async I/O and backpressure.
/// Each connection has one inbound consumer. There is no automatic replay or reconnect.
public enum TCP: Sendable {
  public static func connect(unixDomainSocketPath: String, connectTimeout: TimeAmount) async throws -> Connection {
    try Task.checkCancellation()
    let connection = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
      .connectTimeout(connectTimeout)
      .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
      .connect(unixDomainSocketPath: unixDomainSocketPath) { channel in configure(channel) }
    if Task.isCancelled {
      try? await connection.channel.close()
      throw CancellationError()
    }
    return connection
  }

  public static func listen(unixDomainSocketPath: String) async throws -> Listener {
    try Task.checkCancellation()
    let listener = try await ServerBootstrap(group: .singletonMultiThreadedEventLoopGroup)
      .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
      .bind(unixDomainSocketPath: unixDomainSocketPath) { channel in configure(channel) }
    if Task.isCancelled {
      try? await listener.channel.close()
      throw CancellationError()
    }
    return listener
  }

  /// Opens one connection. Use `executeThenClose` to consume it and release its socket.
  public static func connect(
    host: String, port: Int,
    connectTimeout: TimeAmount
  ) async throws -> Connection {
    try Task.checkCancellation()
    let connection = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
      .connectTimeout(connectTimeout)
      .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
      .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
      .connect(host: host, port: port) { channel in
        configure(channel)
      }
    if Task.isCancelled {
      try? await connection.channel.close()
      throw CancellationError()
    }
    return connection
  }

  /// Binds a listener. Port zero requests an ephemeral port, available via
  /// `listener.channel.localAddress?.port` after this function returns.
  ///
  /// The caller owns accepted connections: run each in `executeThenClose` and
  /// manage their tasks with structured concurrency. Closing the listener stops
  /// accepting; it does not close connections already handed to the caller.
  public static func listen(
    host: String, port: Int
  ) async throws -> Listener {
    try Task.checkCancellation()
    let listener = try await ServerBootstrap(group: .singletonMultiThreadedEventLoopGroup)
      .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
      .childChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
      .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
      .bind(host: host, port: port) { channel in
        configure(channel)
      }
    if Task.isCancelled {
      try? await listener.channel.close()
      throw CancellationError()
    }
    return listener
  }

  private static func configure(_ channel: any Channel) -> EventLoopFuture<Connection> {
    channel.eventLoop.makeSucceededVoidFuture().flatMapThrowing {
      try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(FrameDecoder()))
      try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(FrameEncoder()))
      let asyncChannel = try NIOAsyncChannel<Frame, Frame>(
        wrappingChannelSynchronously: channel,
        configuration: .init(isOutboundHalfClosureEnabled: true))
      // The async bridge sees the error first; malformed input also closes the socket
      // when the application has not started consuming its inbound stream yet.
      try channel.pipeline.syncOperations.addHandler(CloseOnError())
      return Connection(asyncChannel: asyncChannel)
    }
  }
}

private final class CloseOnError: ChannelInboundHandler {
  typealias InboundIn = Frame

  func errorCaught(context: ChannelHandlerContext, error: any Error) {
    context.close(promise: nil)
  }
}
