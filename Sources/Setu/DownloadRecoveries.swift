package import Foundation

// A node shares this store across peer clients so its budget applies to all retained downloads.

package actor DownloadRecoveries {
  package struct Payload: Sendable {
    package let owner: UUID
    package let manifest: Manifest
    package let partial: String
    package let temporary: TemporaryFiles
    package let recovery: DownloadRecovery
    package let expiration: Task<Void, Never>
    package init(
      owner: UUID, manifest: Manifest, partial: String, temporary: TemporaryFiles, recovery: DownloadRecovery,
      expiration: Task<Void, Never>
    ) {
      self.owner = owner
      self.manifest = manifest
      self.partial = partial
      self.temporary = temporary
      self.recovery = recovery
      self.expiration = expiration
    }
  }

  private let budgetBytes: Int64
  private var retained: [String: Payload] = [:]
  private var retainedBytes: Int64 = 0

  package init(budgetBytes: Int64) {
    precondition(budgetBytes >= 0)
    self.budgetBytes = budgetBytes
  }

  package func retain(owner: UUID, manifest: Manifest, partial: String, temporary: TemporaryFiles)
    async throws(any Error) -> DownloadRecovery
  {
    guard budgetBytes > 0 else {
      throw TransferError(
        "Verified download was not retained because the download recovery budget is 0 bytes (disabled)")
    }
    guard manifest.size <= budgetBytes else {
      throw TransferError(
        "Verified download was not retained: its size (\(manifest.size) bytes) exceeds the download recovery budget (\(budgetBytes) bytes)"
      )
    }
    try await expire(at: Date().timeIntervalSince1970)
    // Discard suspends this actor. Recheck the budget after every eviction, then reserve
    // the new payload without another suspension so concurrent failures cannot overfill it.
    while retainedBytes > budgetBytes - manifest.size {
      guard let oldest = retained.values.min(by: { $0.recovery.expiresAt < $1.recovery.expiresAt }) else {
        throw TransferError("Download recovery budget is unavailable")
      }
      try await discard(oldest.recovery.id, owner: nil)
    }
    let recovery = DownloadRecovery(
      id: UUID().uuidString, expiresAt: Date().timeIntervalSince1970 + 900,
      sha256: manifest.digest, size: manifest.size)
    // Clients discard their entries on close, cancelling these timers and releasing the store.
    let expiration = Task {
      do { try await Task.sleep(for: .seconds(900)) } catch { return }
      try? await discard(recovery.id, owner: nil)
    }
    retained[recovery.id] = Payload(
      owner: owner, manifest: manifest, partial: partial, temporary: temporary,
      recovery: recovery, expiration: expiration)
    retainedBytes += manifest.size
    return recovery
  }

  package func contains(_ id: String, owner: UUID) -> Bool {
    guard let payload = retained[id], payload.owner == owner else { return false }
    return payload.recovery.expiresAt > Date().timeIntervalSince1970
  }

  package func take(_ id: String, owner: UUID) async throws(any Error) -> Payload {
    try await expire(at: Date().timeIntervalSince1970)
    guard retained[id]?.owner == owner, let payload = remove(id) else {
      throw TransferError("Download recovery is unknown or expired")
    }
    return payload
  }

  private func remove(_ id: String) -> Payload? {
    guard let payload = retained.removeValue(forKey: id) else { return nil }
    payload.expiration.cancel()
    retainedBytes -= payload.manifest.size
    return payload
  }

  package func discard(_ id: String, owner: UUID?) async throws(any Error) {
    if let owner, retained[id]?.owner != owner { return }
    guard let payload = remove(id) else { return }
    try await payload.temporary.discard(payload.partial)
  }

  package func discardAll(owner: UUID) async {
    for payload in retained.values where payload.owner == owner {
      try? await discard(payload.recovery.id, owner: owner)
    }
  }

  package func expire(at time: Double) async throws(any Error) {
    for payload in retained.values where payload.recovery.expiresAt <= time {
      try await discard(payload.recovery.id, owner: nil)
    }
  }
}
