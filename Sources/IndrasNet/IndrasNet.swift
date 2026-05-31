import NIO
import NIOCore

public enum MessageAsyncChannel {
  public typealias AsyncChannel = NIOAsyncChannel<Message, Message>

  public static func installMessageCodec(
    on channel: Channel,
    maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength
  ) throws {
    try channel.pipeline.syncOperations.addHandler(
      ByteToMessageHandler(MessageDecoder(maxPayloadLength: maxPayloadLength))
    )
    try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(MessageEncoder()))
  }

  public static func wrapChannel(_ channel: Channel) throws -> AsyncChannel {
    try NIOAsyncChannel(
      wrappingChannelSynchronously: channel,
      configuration: .init(
        inboundType: Message.self,
        outboundType: Message.self
      )
    )
  }

  @Sendable
  public static func messageCodecChannelInitializer(
    maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength
  ) -> @Sendable (Channel) -> EventLoopFuture<Void> {
    { channel in
      channel.eventLoop.makeCompletedFuture {
        try installMessageCodec(on: channel, maxPayloadLength: maxPayloadLength)
      }
    }
  }

  @Sendable
  public static func messageAsyncChannelInitializer(
    maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength
  ) -> @Sendable (Channel) -> EventLoopFuture<AsyncChannel> {
    { channel in
      channel.eventLoop.makeCompletedFuture {
        try installMessageCodec(on: channel, maxPayloadLength: maxPayloadLength)
        return try wrapChannel(channel)
      }
    }
  }
}

public struct IndrasNet {
  let group: any EventLoopGroup
  let config: Config

  public init(config: Config, group: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton) {
    self.config = config
    self.group = group
  }

  /// Run this node according to `config.mode`: bind and serve, or dial a peer and
  /// run its client script. A node will eventually do both concurrently.
  public func runNode() async throws {
    switch config.mode {
    case .serve:
      try await run()
    case .connect:
      try await runClient()
    }
  }
}
