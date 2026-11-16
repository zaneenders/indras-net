import Foundation
import _NIOFileSystem

package actor TransferState {
  package let manifest: Manifest
  package let partial: String
  private let temporary: TemporaryFiles
  private var completed: Set<Int> = []
  private var busy: Set<Int> = []
  private var waiters: [Int: [UUID: CheckedContinuation<Void, Never>]] = [:]
  private var publishing = false
  private var finished = false
  private var lastActivity = ContinuousClock.now

  package init(manifest: Manifest, partial: String, temporary: TemporaryFiles) {
    self.manifest = manifest
    self.partial = partial
    self.temporary = temporary
  }

  package func status(id: String) -> UploadStatus {
    lastActivity = .now
    return UploadStatus(id: id, completed: completed.sorted(), committed: false, destination: nil)
  }

  package func reserve(_ index: Int) async throws(any Error) -> (Manifest, String)? {
    _ = try manifest.length(index)
    while busy.contains(index), !finished {
      let id = UUID()
      try await withTaskCancellationHandler {
        try Task.checkCancellation()
        await withCheckedContinuation { waiters[index, default: [:]][id] = $0 }
        try Task.checkCancellation()
      } onCancel: {
        Task { await self.cancelWaiter(id, index: index) }
      }
    }
    try Task.checkCancellation()
    if completed.contains(index) { return nil }
    guard !finished, !publishing else { throw TransferError("Transfer is already finished") }
    busy.insert(index)
    lastActivity = .now
    return (manifest, partial)
  }

  package func release(_ index: Int) {
    busy.remove(index)
    lastActivity = .now
    if let waiting = waiters.removeValue(forKey: index) {
      for waiter in waiting.values { waiter.resume() }
    }
  }

  package func complete(_ index: Int) throws(TransferError) {
    guard !finished, !publishing else { throw TransferError("Transfer is already finished") }
    _ = try manifest.length(index)
    completed.insert(index)
    release(index)
  }

  private func cancelWaiter(_ id: UUID, index: Int) {
    waiters[index]?.removeValue(forKey: id)?.resume()
    if waiters[index]?.isEmpty == true { waiters.removeValue(forKey: index) }
  }

  package func finish(destination: String, progress: TransferProgressTracker?) async throws(any Error) {
    guard !finished, !publishing, completed.count == manifest.chunks.count, busy.isEmpty else {
      throw TransferError("Transfer has missing or busy chunks")
    }
    publishing = true
    defer { publishing = false }
    try await publish(
      partial: partial, destination: destination, manifest: manifest, progress: progress, alreadyVerified: false)
    try await temporary.discard(partial)
    finished = true
  }

  package func discard() async throws(any Error) {
    finished = true
    for index in Array(waiters.keys) { release(index) }
    try await temporary.discard(partial)
  }

  // Reserve and expiry are serialized here: an upload being received or published cannot expire.

  package func expire(before cutoff: ContinuousClock.Instant) async throws(any Error) -> Bool {
    guard !finished, !publishing, busy.isEmpty, lastActivity <= cutoff else { return false }
    finished = true
    do {
      try await temporary.discard(partial)
      return true
    } catch {
      finished = false
      throw error
    }
  }
}
