import NIOCore
import Networking
import Synchronization

final class RequestActivity: Sendable {
  private let last = Mutex(ContinuousClock.now)

  func record() { last.withLock { $0 = .now } }

  func waitForTimeout(_ timeout: Duration) async throws {
    while true {
      let deadline = last.withLock { $0 + timeout }
      try await Task.sleep(until: deadline, clock: .continuous)
      if last.withLock({ $0 + timeout <= .now }) { return }
    }
  }
}

func withInactivityTimeout<Value: Sendable>(
  timeout: Duration, activity: RequestActivity, operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
  precondition(timeout > .zero)
  return try await withThrowingTaskGroup(of: Value.self) { tasks in
    tasks.addTask { try await operation() }
    tasks.addTask {
      try await activity.waitForTimeout(timeout)
      throw PeerTransportError.timedOut
    }
    defer { tasks.cancelAll() }
    return try await tasks.next()!
  }
}

final class RequestActivityHandler: ChannelDuplexHandler, Sendable {
  typealias InboundIn = ByteBuffer
  typealias OutboundIn = ByteBuffer
  typealias OutboundOut = ByteBuffer

  private let activity: RequestActivity

  init(activity: RequestActivity) { self.activity = activity }

  func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    if unwrapInboundIn(data).readableBytes > 0 { activity.record() }
    context.fireChannelRead(data)
  }

  func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
    var bytes = unwrapOutboundIn(data)
    // Heartbeats keep the remote timer alive, but cannot conceal stalled local work.
    if bytes.readableBytes == Frame.headerBytes,
      bytes.getInteger(at: bytes.readerIndex, as: UInt32.self) == 0,
      bytes.getInteger(at: bytes.readerIndex + 4, as: UInt32.self) == SetuWire.service,
      bytes.getInteger(at: bytes.readerIndex + 8, as: UInt32.self) == SetuWire.heartbeat
    {
      context.write(data, promise: promise)
      return
    }
    activity.record()
    var writes: [EventLoopFuture<Void>] = []
    // Completion of small writes measures socket progress even for a large binary frame.
    while let part = bytes.readSlice(length: min(bytes.readableBytes, 65_536)), !part.readableBytesView.isEmpty {
      let completion = context.write(wrapOutboundOut(part))
      completion.whenSuccess { [activity] in activity.record() }
      writes.append(completion)
    }
    let completion = EventLoopFuture.andAllSucceed(writes, on: context.eventLoop)
    if let promise {
      completion.cascade(to: promise)
    } else {
      let context = NIOLoopBound(context, eventLoop: context.eventLoop)
      completion.whenFailure { context.value.fireErrorCaught($0) }
    }
  }
}
