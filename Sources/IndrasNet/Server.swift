import NIO
import NIOCore

extension IndrasNet {
  public func run() async throws {
    let jsonEventLog = config.jsonEventLog
    let events = EventLogger(enabled: jsonEventLog)
    let bootstrap = ServerBootstrap(group: group)
      .serverChannelOption(.backlog, value: 256)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .childChannelInitializer { channel in
        Self.acceptConnection(channel: channel, jsonEventLog: jsonEventLog)
      }

    let serverChannel = try await bootstrap.bind(host: config.host, port: config.port).get()
    // Report the actually-bound port so callers can pass 0 and let the OS pick.
    let boundPort = serverChannel.localAddress?.port ?? config.port
    events.emit(.listening(host: config.host, port: boundPort))
    ProcessLog.human("indras-net listening on \(config.host):\(boundPort)")
    try await serverChannel.closeFuture.get()
  }

  private static func acceptConnection(channel: Channel, jsonEventLog: Bool) -> EventLoopFuture<Void> {
    MessageAsyncChannel.messageAsyncChannelInitializer()(channel).flatMap { asyncChannel in
      channel.eventLoop.makeCompletedFuture {
        Task {
          await serveConnection(asyncChannel, jsonEventLog: jsonEventLog)
        }
      }
    }
  }

  private static func serveConnection(
    _ channel: MessageAsyncChannel.AsyncChannel,
    jsonEventLog: Bool
  ) async {
    let events = EventLogger(enabled: jsonEventLog)
    do {
      try await channel.executeThenClose { inbound, outbound in
        for try await message in inbound {
          let payload = message.payloadString
          events.emit(.message(direction: .received, type: message.type.name, payload: payload))
          ProcessLog.human("recv \(message.type.name) \(payload)")

          guard let reply = reply(to: message) else { continue }
          let replyPayload = reply.payloadString
          events.emit(.message(direction: .sent, type: reply.type.name, payload: replyPayload))
          try await outbound.write(reply)
        }
      }
    } catch {
      return
    }
  }

  private static func reply(to message: Message) -> Message? {
    switch message.type {
    case .ping:
      return Message(type: .pong, payload: message.payload)
    case .hello:
      var payload = ByteBuffer()
      payload.writeString("ok")
      return Message(type: .hello, payload: payload)
    default:
      return nil
    }
  }
}
