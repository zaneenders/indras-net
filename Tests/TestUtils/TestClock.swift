import Synchronization

public final class TestClock: Clock, Sendable {
  public struct Instant: InstantProtocol {
    public var offset: Duration

    public init(offset: Duration = .zero) {
      self.offset = offset
    }

    public func advanced(by duration: Duration) -> Instant {
      Instant(offset: offset + duration)
    }

    public func duration(to other: Instant) -> Duration {
      other.offset - offset
    }

    public static func < (lhs: Instant, rhs: Instant) -> Bool {
      lhs.offset < rhs.offset
    }
  }

  private struct Sleeper {
    let id: Int
    let deadline: Instant
    let continuation: CheckedContinuation<Void, any Error>
  }

  private struct State {
    var now = Instant()
    var pending: [Sleeper] = []
    var nextID = 0
  }

  private let state = Mutex(State())

  public init() {}

  public var now: Instant { state.withLock { $0.now } }
  public var minimumResolution: Duration { .zero }

  public func sleep(until deadline: Instant, tolerance: Duration? = nil) async throws {
    try Task.checkCancellation()

    let id = state.withLock { state -> Int in
      defer { state.nextID += 1 }
      return state.nextID
    }

    enum Outcome { case resumeImmediately, cancel, park }

    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
        let outcome: Outcome = state.withLock { state in
          if Task.isCancelled { return .cancel }
          if deadline <= state.now { return .resumeImmediately }
          state.pending.append(Sleeper(id: id, deadline: deadline, continuation: continuation))
          return .park
        }
        switch outcome {
        case .resumeImmediately: continuation.resume()
        case .cancel: continuation.resume(throwing: CancellationError())
        case .park: break
        }
      }
    } onCancel: {
      let continuation = state.withLock { state -> CheckedContinuation<Void, any Error>? in
        guard let index = state.pending.firstIndex(where: { $0.id == id }) else { return nil }
        return state.pending.remove(at: index).continuation
      }
      continuation?.resume(throwing: CancellationError())
    }
  }

  /// Advances the clock by `duration`, resuming every sleeper whose deadline has passed.
  public func advance(by duration: Duration) {
    let due = state.withLock { state -> [Sleeper] in
      state.now = state.now.advanced(by: duration)
      let ready = state.pending.filter { $0.deadline <= state.now }
      state.pending.removeAll { $0.deadline <= state.now }
      return ready
    }
    for sleeper in due {
      sleeper.continuation.resume()
    }
  }
}
