public import NIOCore

/// One duplex socket with scoped ownership and a single inbound consumer.
public final class Connection: Sendable {
  private let asyncChannel: NIOAsyncChannel<Frame, Frame>
  private let events: NIOLoopBound<ConnectionEvents>
  private let writes: NIOLoopBound<ConnectionWrites>

  /// The underlying NIO channel, for addresses, channel options, and explicit closure.
  public var channel: any Channel { asyncChannel.channel }

  init(channel: any Channel) throws {
    let writes = ConnectionWrites()
    try channel.pipeline.syncOperations.addHandler(writes, name: nil, position: .last)
    let asyncChannel = try NIOAsyncChannel<Frame, Frame>(
      wrappingChannelSynchronously: channel,
      configuration: .init(isOutboundHalfClosureEnabled: true))
    let events = ConnectionEvents()
    // Let the async bridge receive decoding errors before closing the socket.
    try channel.pipeline.syncOperations.addHandler(events, name: nil, position: .last)
    self.asyncChannel = asyncChannel
    self.events = NIOLoopBound(events, eventLoop: channel.eventLoop)
    self.writes = NIOLoopBound(writes, eventLoop: channel.eventLoop)
  }

  package func setInputCloseHandler(_ handler: (@Sendable () -> Void)?) async throws {
    try await events.eventLoop.submitVoid { [events] in
      events.value.setInputCloseHandler(handler)
    }.get()
  }

  /// Drains queued writer output on success and closes the connection when the scope exits.
  public func executeThenClose<Result: Sendable>(
    _ body: @Sendable (NIOAsyncChannelInboundStream<Frame>, NIOAsyncChannelOutboundWriter<Frame>) async throws -> Result
  ) async throws -> Result {
    defer { withExtendedLifetime(self) {} }
    return try await asyncChannel.executeThenClose { [writes] inbound, outbound in
      let result = try await body(inbound, outbound)
      if !Task.isCancelled {
        // The writer's event-loop writes are queued before this lookup.
        try await writes.eventLoop.flatSubmitVoid {
          writes.value.lastWrite ?? writes.eventLoop.makeSucceededVoidFuture()
        }.getAbandoningOnCancel()
      }
      return result
    }
  }

  deinit {
    // A dropped, unconsumed connection must not retain a live socket. Use the
    // scoped API for awaited teardown; deinit only schedules a fallback close.
    // NIO's async writer must be finished even if a scope was never entered.
    let asyncChannel = self.asyncChannel
    Task {
      try? await asyncChannel.executeThenClose { _, _ in }
    }
  }
}

private final class ConnectionWrites: ChannelOutboundHandler {
  typealias OutboundIn = Frame
  private(set) var lastWrite: EventLoopFuture<Void>?

  func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
    if let promise {
      context.write(data, promise: promise)
    } else {
      let completion = context.eventLoop.makeVoidPromise()
      lastWrite = completion.futureResult
      context.write(data, promise: completion)
    }
  }

  func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
    if mode == .output, let lastWrite {
      lastWrite.assumeIsolated().whenComplete { result in
        switch result {
        case .success: context.close(mode: mode, promise: promise)
        case .failure(let error): promise?.fail(error)
        }
      }
    } else {
      context.close(mode: mode, promise: promise)
    }
  }
}

extension EventLoop {
  fileprivate func submitVoid(_ task: @escaping @Sendable () -> Void) -> EventLoopFuture<Void> {
    submit(task)
  }

  fileprivate func flatSubmitVoid(_ task: @escaping @Sendable () -> EventLoopFuture<Void>) -> EventLoopFuture<Void> {
    flatSubmit(task)
  }

  fileprivate func makeVoidPromise() -> EventLoopPromise<Void> {
    makePromise(of: Void.self)
  }
}

private final class ConnectionEvents: ChannelInboundHandler {
  typealias InboundIn = Frame

  private enum InputState {
    case open(onClose: (@Sendable () -> Void)?)
    case closed
  }

  private var input = InputState.open(onClose: nil)

  func setInputCloseHandler(_ handler: (@Sendable () -> Void)?) {
    switch input {
    case .open: input = .open(onClose: handler)
    case .closed: handler?()
    }
  }

  private func closeInput() {
    guard case .open(let onClose) = input else { return }
    input = .closed
    onClose?()
  }

  func handlerRemoved(context: ChannelHandlerContext) { closeInput() }

  func channelInactive(context: ChannelHandlerContext) {
    closeInput()
    context.fireChannelInactive()
  }

  func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
    if case ChannelEvent.inputClosed = event { closeInput() }
    context.fireUserInboundEventTriggered(event)
  }

  func errorCaught(context: ChannelHandlerContext, error: any Error) {
    context.close(promise: nil)
  }
}
