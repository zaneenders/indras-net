import Foundation
import Testing

@testable import Setu

struct DownloadRecoveryTests {
  @Test func byteBudgetEvictsOldestPayloadsUntilTheNewFileFits() async throws(any Error) {
    try await withTemporary { temporary in
      let store = DownloadRecoveries(budgetBytes: 10)
      let owner = UUID()
      let first = try await retain(size: 4, owner: owner, temporary: temporary, store: store)
      let second = try await retain(size: 5, owner: owner, temporary: temporary, store: store)
      let third = try await retain(size: 7, owner: owner, temporary: temporary, store: store)
      #expect(!(await store.contains(first.id, owner: owner)))
      #expect(!(await store.contains(second.id, owner: owner)))
      #expect(await store.contains(third.id, owner: owner))
      #expect(try payloadCount(temporary) == 1)
      try await store.expire(at: .greatestFiniteMagnitude)
      #expect(try payloadCount(temporary) == 0)
    }
  }

  @Test(arguments: [Int64(8), Int64(12)])
  func concurrentFailuresShareTheByteBudgetAcrossOwners(budget: Int64) async throws(any Error) {
    try await withTemporary { temporary in
      let store = DownloadRecoveries(budgetBytes: budget)
      let owners = [UUID(), UUID()]
      for index in 0..<Int(budget) {
        _ = try await retain(size: 1, owner: owners[index % 2], temporary: temporary, store: store)
      }
      try await withThrowingTaskGroup(of: Void.self) { group in
        for index in 0..<48 {
          group.addTask {
            _ = try await retain(size: 1, owner: owners[index % 2], temporary: temporary, store: store)
          }
        }
        try await group.waitForAll()
      }
      // One-byte files fill the budget; there is no separate eight-file cap.
      #expect(try payloadCount(temporary) == Int(budget))
      for owner in owners { await store.discardAll(owner: owner) }
      #expect(try payloadCount(temporary) == 0)
    }
  }

  @Test func takingAndClosingOneOwnersRecoveriesPreservesOtherOwnersAndReleasesBudget() async throws(any Error) {
    try await withTemporary { temporary in
      let store = DownloadRecoveries(budgetBytes: 8)
      let firstOwner = UUID()
      let secondOwner = UUID()
      let first = try await retain(size: 4, owner: firstOwner, temporary: temporary, store: store)
      let second = try await retain(size: 4, owner: secondOwner, temporary: temporary, store: store)
      await #expect(throws: TransferError.self) { try await store.take(second.id, owner: firstOwner) }
      await store.discardAll(owner: firstOwner)
      #expect(!(await store.contains(first.id, owner: firstOwner)))
      #expect(await store.contains(second.id, owner: secondOwner))
      #expect(try payloadCount(temporary) == 1)
      let payload = try await store.take(second.id, owner: secondOwner)
      try await temporary.discard(payload.partial)
      let replacement = try await retain(size: 8, owner: firstOwner, temporary: temporary, store: store)
      #expect(await store.contains(replacement.id, owner: firstOwner))
      await store.discardAll(owner: firstOwner)
      #expect(try payloadCount(temporary) == 0)
    }
  }
}

private func retain(size: Int, owner: UUID, temporary: TemporaryFiles, store: DownloadRecoveries)
  async throws(any Error) -> DownloadRecovery
{
  let data = Data(repeating: 0x42, count: size)
  let manifest = Manifest(
    name: UUID().uuidString, size: Int64(size), chunkSize: defaultChunkSize,
    digest: sha256(data), chunks: [sha256(data)])
  let partial = try await temporary.allocate(size: Int64(size))
  try await writeRange(path: partial, offset: 0, data: data)
  return try await store.retain(owner: owner, manifest: manifest, partial: partial, temporary: temporary)
}

private func payloadCount(_ temporary: TemporaryFiles) throws(any Error) -> Int {
  try FileManager.default.contentsOfDirectory(atPath: temporary.directory).filter { $0.hasSuffix(".partial") }.count
}

private func withTemporary(_ body: @Sendable (TemporaryFiles) async throws(any Error) -> Void) async throws(any Error) {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent("setu-recovery-\(UUID())")
  defer { try? FileManager.default.removeItem(at: directory) }
  let temporary = try await TemporaryFiles(directory: directory.path, protecting: [])
  do {
    try await body(temporary)
    await temporary.close()
  } catch {
    await temporary.close()
    throw error
  }
}
