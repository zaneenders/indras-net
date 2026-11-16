public import NIOCore

/// One duplex socket with scoped ownership and a single inbound consumer.
public final class Connection: Sendable {
  private let asyncChannel: NIOAsyncChannel<Frame, Frame>

  /// The underlying NIO channel, for addresses, channel options, and explicit closure.
  public var channel: any Channel { asyncChannel.channel }

  init(asyncChannel: NIOAsyncChannel<Frame, Frame>) {
    self.asyncChannel = asyncChannel
  }

  /// Provides scoped access to the connection and closes it when the body exits,
  /// including when the body throws or an inbound wait is cancelled.
  public func executeThenClose<Result: Sendable>(
    _ body: @Sendable (NIOAsyncChannelInboundStream<Frame>, NIOAsyncChannelOutboundWriter<Frame>) async throws -> Result
  ) async throws -> Result {
    defer { withExtendedLifetime(self) {} }
    return try await asyncChannel.executeThenClose(body)
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
