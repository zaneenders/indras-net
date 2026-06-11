import Foundation
import NIOCore
import NIOPosix

public enum IndrasNetClient {
  public static let defaultClientID = "raft"

  public static func submit(
    command: Data,
    to address: NodeAddress,
    clientID: String = defaultClientID,
    timeout: Duration = .seconds(10),
    eventLoopGroup: MultiThreadedEventLoopGroup = .singleton
  ) async throws -> ClientSubmitResult {
    let asyncChannel = try await ClientBootstrap(group: eventLoopGroup)
      .channelOption(.socketOption(.so_reuseaddr), value: 1)
      .connect(
        host: address.host,
        port: address.port,
        channelInitializer: clientChannelInitializer()
      )

    return try await asyncChannel.executeThenClose { inbound, outbound in
      try await outbound.write(
        HandshakeFrame.signal(magic: HandshakeFrame.magic, version: HandshakeFrame.version).message
      )
      try await outbound.write(HandshakeFrame.greet(clientID).message)

      var handshakeVerified = false
      var requestID: UInt128?
      var client = RaftClient(id: clientID)

      let deadline = ContinuousClock.now.advanced(by: timeout)

      for try await wire in inbound {
        if ContinuousClock.now >= deadline {
          throw RaftClientConnectionError.timedOut
        }

        if requestID == nil {
          guard let frame = HandshakeFrame(wire) else {
            throw RaftClientConnectionError.handshakeFailed
          }

          if !handshakeVerified {
            guard case .signal(let magic, let version) = frame,
              magic == HandshakeFrame.magic,
              version == HandshakeFrame.version
            else {
              throw RaftClientConnectionError.handshakeFailed
            }
            handshakeVerified = true
            continue
          }

          guard case .hello = frame else {
            throw RaftClientConnectionError.handshakeFailed
          }

          let request = client.makeRequest(command: command)
          requestID = request.requestId
          try await outbound.write(RaftMessage.clientSubmit(request).message)
          continue
        }

        guard let requestID,
          let message = RaftMessage(wire),
          case .clientSubmitReply(let reply) = message,
          reply.requestId == requestID
        else {
          continue
        }
        return ClientSubmitResult(reply)
      }

      throw RaftClientConnectionError.connectionClosed
    }
  }
}

@Sendable
private func clientChannelInitializer(
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
