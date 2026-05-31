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
