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

  public static func waitUntil(
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

public actor MessageCollector {
  private var entries: [(RaftMessage, PeerId)] = []

  public init() {}

  public func record(_ message: RaftMessage, from peerID: PeerId) {
    self.entries.append((message, peerID))
  }

  public func waitForMessage(
    type: RaftMessage,
    from peerID: PeerId,
    timeout: Duration
  ) async throws -> RaftMessage {
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

  public func count(type: RaftMessage, from peerID: PeerId) -> Int {
    self.entries.lazy.filter { $0.0 == type && $0.1 == peerID }.count
  }

  public func waitForAnyMessage(from peerID: PeerId, timeout: Duration) async throws -> RaftMessage {
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

public enum MessageCollectorError: Error {
  case timeout
}

extension TestHelpers {
  /// Opaque app payload for transport tests — exercises send/receive without asserting Raft semantics.
  public static let transportProbe = RaftMessage.appendEntries(
    AppendEntries.Args(term: 0, leaderId: "transport-probe")
  )
}
