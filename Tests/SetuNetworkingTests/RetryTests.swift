import NIOCore
import Networking
import Setu
import Synchronization
import Testing

@testable import SetuNetworking

@Suite(.timeLimit(.minutes(1)))
struct RetryTests {
  @Test func actualConnectionRefusalIsRetriedWithTheSameOperationBudget() async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    try await listener.channel.close()
    let failure: any Error
    do {
      let unexpected = try await TCP.connect(host: "127.0.0.1", port: port, connectTimeout: .seconds(1))
      try? await unexpected.channel.close()
      Issue.record("Expected the released listener port to refuse the connection")
      return
    } catch { failure = error }
    #expect(isTransient(failure))
    let attempts = AttemptCount()
    let result = try await retryTransient {
      let attempt = attempts.next()
      if attempt < 3 { throw failure }
      return attempt
    }
    #expect(result == 3)
    #expect(isTransient(FrameError.truncatedFrame))
    #expect(!isTransient(FrameError.payloadLengthOverflow))
    #expect(!isTransient(TransferError("Chunk integrity check failed")))
    #expect(!isTransient(CancellationError()))
  }
}

private final class AttemptCount: Sendable {
  private let value = Mutex(0)
  func next() -> Int {
    value.withLock { value in
      value += 1
      return value
    }
  }
}
