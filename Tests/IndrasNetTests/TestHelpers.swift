import Foundation
import Logging
import NIOPosix
import Testing

@testable import IndrasNet

enum TestHelpers {
  static let quietLogger: Logger = {
    var logger = Logger(label: "indras-net.test")
    logger.logLevel = .error
    return logger
  }()
}

extension TestHelpers {
  static func withEventLoopGroup<R>(
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

  static func waitUntil(
    timeout: Duration,
    pollInterval: Duration = .milliseconds(25),
    condition: () async -> Bool
  ) async {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if await condition() {
        return
      }
      try? await Task.sleep(for: pollInterval)
    }
    Issue.record("Condition not met before timeout")
  }
}

actor MessageCollector {
  private var entries: [(AppMessage, PeerId)] = []

  func record(_ message: AppMessage, from peerID: PeerId) {
    self.entries.append((message, peerID))
  }

  func waitForMessage(
    type: AppMessage,
    from peerID: PeerId,
    timeout: Duration
  ) async throws -> AppMessage {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if let match = self.entries.first(where: { $0.0 == type && $0.1 == peerID }) {
        return match.0
      }
      try? await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("Did not receive \(type) from \(peerID)")
    throw MessageCollectorError.timeout
  }

  func count(type: AppMessage, from peerID: PeerId) -> Int {
    self.entries.lazy.filter { $0.0 == type && $0.1 == peerID }.count
  }

  func waitForAnyMessage(from peerID: PeerId, timeout: Duration) async throws -> AppMessage {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if let match = self.entries.first(where: { $0.1 == peerID }) {
        return match.0
      }
      try? await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("Did not receive any message from \(peerID)")
    throw MessageCollectorError.timeout
  }
}

enum MessageCollectorError: Error {
  case timeout
}

extension TestHelpers {
  /// Opaque app payload for transport tests — exercises send/receive without asserting Raft semantics.
  static let transportProbe = AppMessage.appendEntries(
    AppendEntries.Args(term: 0, leaderId: "transport-probe")
  )
}
