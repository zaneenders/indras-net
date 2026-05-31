import NIOPosix
import Testing

@testable import IndrasNet

enum TestHelpers {}

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
  private var entries: [(Message, PeerID)] = []

  func record(_ message: Message, from peerID: PeerID) {
    self.entries.append((message, peerID))
  }

  func waitForMessage(
    type: MessageType,
    from peerID: PeerID,
    timeout: Duration
  ) async throws -> Message {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if let match = self.entries.first(where: { $0.0.type == type && $0.1 == peerID }) {
        return match.0
      }
      try? await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("Did not receive \(type.name) from \(peerID)")
    throw MessageCollectorError.timeout
  }

  func count(type: MessageType, from peerID: PeerID) -> Int {
    self.entries.lazy.filter { $0.0.type == type && $0.1 == peerID }.count
  }

  func waitForCount(
    type: MessageType,
    from peerID: PeerID,
    atLeast minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if self.count(type: type, from: peerID) >= minimum {
        return
      }
      try? await Task.sleep(for: .milliseconds(25))
    }
    Issue.record(
      "Received \(self.count(type: type, from: peerID)) \(type.name) from \(peerID), expected \(minimum)"
    )
    throw MessageCollectorError.timeout
  }
}

enum MessageCollectorError: Error {
  case timeout
}
