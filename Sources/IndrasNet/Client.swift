import NIO
import NIOCore

extension IndrasNet {
  public func runClient() async throws {
    let events = EventLogger(enabled: config.jsonEventLog)
    let asyncChannel = try await ClientBootstrap(group: group)
      .channelInitializer(MessageAsyncChannel.messageCodecChannelInitializer())
      .connect(host: config.host, port: config.port)
      .flatMap { channel in
        channel.eventLoop.makeCompletedFuture {
          try MessageAsyncChannel.wrapChannel(channel)
        }
      }
      .get()

    do {
      try await asyncChannel.executeThenClose { inbound, outbound in
        var inbound = inbound.makeAsyncIterator()
        for step in config.clientScript {
          switch step.action {
          case .send:
            let type = try step.messageType()
            let message = Message(type: type, payload: step.payloadBuffer())
            let payload = message.payloadString
            events.emit(.message(direction: .sent, type: type.name, payload: payload))
            ProcessLog.human("send \(type.name)")
            try await outbound.write(message)

          case .expect:
            guard let message = try await inbound.next() else {
              throw ClientScriptStep.ScriptError.inboundEndedBeforeExpectation(expected: step.type)
            }
            try step.verify(message)
            let payload = message.payloadString
            events.emit(.message(direction: .received, type: message.type.name, payload: payload))
            ProcessLog.human("recv \(message.type.name): \(payload)")
          }
        }
        events.emit(.sessionComplete)
        ProcessLog.human("session complete")
      }
    } catch let error as ClientScriptStep.ScriptError {
      events.emit(.failed(error: error.description))
      ProcessLog.human("error: \(error)")
      throw error
    } catch {
      events.emit(.failed(error: String(describing: error)))
      throw error
    }
  }
}

extension ClientScriptStep {
  fileprivate func verify(_ message: Message) throws {
    let expectedType = try messageType()
    guard message.type == expectedType else {
      throw ScriptError.unexpectedMessage(expected: type, got: message)
    }
    if let payload {
      let got = message.payloadString
      guard got == payload else {
        throw ScriptError.payloadMismatch(expected: payload, got: got)
      }
    }
  }
}
