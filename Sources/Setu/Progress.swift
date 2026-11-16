public import Foundation

/// A serializable snapshot. Byte counts and throughput describe the current phase.
public struct TransferProgress: Codable, Equatable, Sendable {
  public enum Phase: String, Codable, Sendable {
    case connecting, hashing, transferring, verifying, publishing, completed
  }

  public let phase: Phase
  public let completedBytes: Int64
  public let totalBytes: Int64?
  public let bytesPerSecond: Double
  package init(phase: Phase, completedBytes: Int64, totalBytes: Int64?, bytesPerSecond: Double) {
    self.phase = phase
    self.completedBytes = completedBytes
    self.totalBytes = totalBytes
    self.bytesPerSecond = bytesPerSecond
  }
}

/// Tracks a transfer using Foundation's thread-safe ProgressManager on Darwin, glibc, and musl.
public actor TransferProgressTracker {
  private let manager = ProgressManager(totalCount: nil)
  private var phase = TransferProgress.Phase.connecting
  private var started = ContinuousClock.now
  private var initialBytes: Int64 = 0
  private let onUpdate: (@Sendable () -> Void)?

  public init(onUpdate: (@Sendable () -> Void)?) { self.onUpdate = onUpdate }

  public var progressReporter: ProgressReporter { manager.reporter }

  package func begin(_ phase: TransferProgress.Phase, totalBytes: Int64?, completedBytes: Int64) {
    self.phase = phase
    started = .now
    initialBytes = completedBytes
    manager.setCounts { completed, total in
      completed = Int(completedBytes)
      total = totalBytes.map(Int.init)
    }
    onUpdate?()
  }

  package func advance(by bytes: Int) {
    manager.complete(count: bytes)
    onUpdate?()
  }

  public func snapshot() -> TransferProgress {
    let reporter = manager.reporter
    let elapsed = started.duration(to: .now).components
    let seconds = max(0.000001, Double(elapsed.seconds) + Double(elapsed.attoseconds) / 1e18)
    let completed = Int64(reporter.completedCount)
    return TransferProgress(
      phase: phase, completedBytes: completed, totalBytes: reporter.totalCount.map(Int64.init),
      bytesPerSecond: phase == .completed ? 0 : Double(completed - initialBytes) / seconds)
  }
}
