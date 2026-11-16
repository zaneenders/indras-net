import Foundation
import Testing

@testable import Setu

struct ProgressTests: Sendable {
  @Test func hashingReportsItsByteCountUsingFoundationProgress() async throws(any Error) {
    let path = FileManager.default.temporaryDirectory.appendingPathComponent("setu-progress-\(UUID())")
    defer { try? FileManager.default.removeItem(at: path) }
    let data = Data(repeating: 1, count: 1_048_577)
    try data.write(to: path)
    let tracker = TransferProgressTracker(onUpdate: nil)
    _ = try await Manifest.scan(
      path: path.path, name: nil, chunkSize: 1_048_576, followSymbolicLinks: true, progress: tracker)
    let snapshot = await tracker.snapshot()
    #expect(snapshot.phase == .hashing)
    #expect(snapshot.completedBytes == data.count && snapshot.totalBytes == Int64(data.count))
    let reporter = await tracker.progressReporter
    #expect(reporter.isFinished && reporter.completedCount == data.count)
  }

  @Test func parallelChunkUpdatesAreCountedExactlyOnceAndReusedBytesDoNotInflateSpeed() async {
    let tracker = TransferProgressTracker(onUpdate: nil)
    await tracker.begin(.transferring, totalBytes: 1024, completedBytes: 512)
    #expect(await tracker.snapshot().bytesPerSecond == 0)
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<8 { group.addTask { await tracker.advance(by: 64) } }
    }
    let snapshot = await tracker.snapshot()
    #expect(snapshot.completedBytes == 1024 && snapshot.totalBytes == 1024)
    #expect(snapshot.bytesPerSecond > 0)
    await tracker.begin(.verifying, totalBytes: nil, completedBytes: 0)
    #expect(await tracker.snapshot().totalBytes == nil)
  }
}
