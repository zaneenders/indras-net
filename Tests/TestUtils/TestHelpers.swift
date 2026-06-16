import Foundation
import Logging
import NIOPosix
import Testing

@testable import IndrasNet

public enum TestHelpers {
  public static let quietLogger: Logger = {
    var logger = Logger(label: "indras-net.test")
    logger.logLevel = .error
    return logger
  }()
}

extension TestHelpers {
  public static func withEventLoopGroup<R>(
    numberOfThreads: Int = 2,
    _ body: (MultiThreadedEventLoopGroup) async throws -> R
  ) async rethrows -> R {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: numberOfThreads)
    defer {
      #expect(throws: Never.self) {
        try group.syncShutdownGracefully()
      }
    }
    return try await body(group)
  }

  /// Polls `condition` until it returns true or `timeout` elapses.
  /// Returns whether the condition was satisfied. This is the single polling
  /// primitive every other wait helper in the test suite is built on.
  @discardableResult
  public static func poll(
    timeout: Duration,
    pollInterval: Duration = .milliseconds(25),
    until condition: @Sendable () async -> Bool
  ) async -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if await condition() {
        return true
      }
      try? await Task.sleep(for: pollInterval)
    }
    return await condition()
  }

  /// Polls `produce` until it returns a non-nil value or `timeout` elapses.
  /// Returns the produced value, or `nil` if the timeout elapsed first.
  public static func pollForValue<Value: Sendable>(
    timeout: Duration,
    pollInterval: Duration = .milliseconds(25),
    produce: @Sendable () async -> Value?
  ) async -> Value? {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if let value = await produce() {
        return value
      }
      try? await Task.sleep(for: pollInterval)
    }
    return await produce()
  }

  /// Polls until `condition` is true, recording an `Issue` if the timeout elapses first.
  public static func waitUntil(
    timeout: Duration,
    pollInterval: Duration = .milliseconds(25),
    condition: @Sendable () async -> Bool
  ) async {
    if await !poll(timeout: timeout, pollInterval: pollInterval, until: condition) {
      Issue.record("Condition not met before timeout")
    }
  }
}

public actor MessageCollector {
  private var entries: [(RaftMessage, PeerId)] = []

  public init() {}

  public func record(_ message: RaftMessage, from peerID: PeerId) {
    self.entries.append((message, peerID))
  }

  private func firstMessage(matching predicate: (RaftMessage, PeerId) -> Bool) -> RaftMessage? {
    entries.first { predicate($0.0, $0.1) }?.0
  }

  public func waitForMessage(
    type: RaftMessage,
    from peerID: PeerId,
    timeout: Duration
  ) async throws -> RaftMessage {
    await TestHelpers.poll(timeout: timeout) {
      await self.firstMessage { $0 == type && $1 == peerID } != nil
    }
    if let match = firstMessage(matching: { $0 == type && $1 == peerID }) {
      return match
    }
    Issue.record("Did not receive \(type) from \(peerID)")
    throw MessageCollectorError.timeout
  }

  public func count(type: RaftMessage, from peerID: PeerId) -> Int {
    self.entries.lazy.filter { $0.0 == type && $0.1 == peerID }.count
  }

  public func waitForAnyMessage(from peerID: PeerId, timeout: Duration) async throws -> RaftMessage {
    await TestHelpers.poll(timeout: timeout) {
      await self.firstMessage { _, from in from == peerID } != nil
    }
    if let match = firstMessage(matching: { _, from in from == peerID }) {
      return match
    }
    Issue.record("Did not receive any message from \(peerID)")
    throw MessageCollectorError.timeout
  }
}

public enum MessageCollectorError: Error {
  case timeout
}

extension TestHelpers {
  /// Opaque app payload for transport tests — exercises send/receive without asserting Raft semantics.
  public static let transportProbe = RaftMessage.appendEntries(
    AppendEntries.Args(term: 0, leaderId: "transport-probe")
  )
}
