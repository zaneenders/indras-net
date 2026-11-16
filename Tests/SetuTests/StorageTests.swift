import Foundation
import Testing

@testable import Setu

struct StorageTests: Sendable {
  private let owner = UUID()

  func temporaryDirectory() throws(any Error) -> String {
    let path = FileManager.default.temporaryDirectory.appendingPathComponent("transfer-tests-\(UUID().uuidString)").path
    try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: false)
    return path
  }

  func oneByteManifest(name: String, byte: UInt8) -> Manifest {
    let digest = sha256(Data([byte]))
    return Manifest(name: name, size: 1, chunkSize: 1_048_576, digest: digest, chunks: [digest])
  }

  @Test func rejectsUnsafeNamesAndMalformedManifests() throws(any Error) {
    for name in ["", ".", "..", "../escape", "/absolute", ".transfer", "a\\b", "a\nb"] {
      #expect(!validName(name))
    }
    #expect(validName("vacation photo.zip"))
    let invalid = Manifest(
      name: "file", size: 1, chunkSize: 1_048_576, digest: String(repeating: "a", count: 64), chunks: [])
    #expect(throws: TransferError.self) { try invalid.validate() }
    let badIndex = Manifest(name: "file", size: 0, chunkSize: 1_048_576, digest: sha256(Data()), chunks: [])
    #expect(throws: TransferError.self) { try badIndex.length(-1) }
    #expect(throws: TransferError.self) { try badIndex.length(Int.max) }
  }

  @Test(arguments: [1_048_576, 1_048_577])
  func manifestHashesWholeFileAndPartialLastChunk(chunkSize: Int) async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let data = Data(repeating: 0xa5, count: 1_048_576 + 137)
    let path = root + "/source"
    try data.write(to: URL(fileURLWithPath: path))
    let manifest = try await Manifest.scan(
      path: path, name: nil, chunkSize: chunkSize, followSymbolicLinks: true, progress: nil)
    #expect(manifest.digest == sha256(data))
    #expect(manifest.chunks.count == 2)
    #expect(try manifest.length(1) == data.count - chunkSize)
    #expect(manifest.chunks[0] == sha256(data.prefix(chunkSize)))
    #expect(manifest.chunks[1] == sha256(data.suffix(data.count - chunkSize)))
    #expect(try manifest.id == manifest.id)
  }

  @Test func restartDropsUploadProgressAndCompletionLeavesOnlyTheFile() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let data = Data(repeating: 0x42, count: 1_048_577)
    let source = root + "/source"
    let received = root + "/received"
    try data.write(to: URL(fileURLWithPath: source))
    let manifest = try await Manifest.scan(
      path: source, name: "result", chunkSize: 1_048_576, followSymbolicLinks: true, progress: nil)
    let store = try await makeStore(
      root: received, quota: 10_000_000, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let status = try await store.begin(
      manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
    let (_, partial) = try #require(try await store.reserve(status.id, index: 0))
    try await writeRange(path: partial, offset: 0, data: data.prefix(manifest.chunkSize))
    try await store.complete(status.id, index: 0)
    #expect(
      try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil).completed
        == [0])
    await store.close()
    #expect(try FileManager.default.contentsOfDirectory(atPath: received).isEmpty)

    let restarted = try await makeStore(
      root: received, quota: 10_000_000, maxActiveUploads: 128, idleTimeout: .seconds(900))
    #expect(
      try await restarted.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
        .completed.isEmpty)
    for index in manifest.chunks.indices {
      let (_, path) = try #require(try await restarted.reserve(status.id, index: index))
      let offset = index * manifest.chunkSize
      try await writeRange(
        path: path, offset: Int64(offset), data: data.subdata(in: offset..<offset + manifest.length(index)))
      try await restarted.complete(status.id, index: index)
    }
    try await restarted.commit(status.id, progress: nil)
    await #expect(throws: TransferError.self) { try await restarted.reserve(status.id, index: 0) }
    #expect(
      try await restarted.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
        .committed)
    #expect(try Data(contentsOf: URL(fileURLWithPath: received + "/result")) == data)
    #expect(try FileManager.default.contentsOfDirectory(atPath: received) == ["result"])
    await restarted.close()
  }

  @Test(arguments: [false, true])
  func acceptsMoreThan128UploadsAndReleasesCompletedQuota(committed: Bool) async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let received = root + "/received"
    let store = try await makeStore(
      root: received, quota: committed ? 1 : 1_000, maxActiveUploads: 256, idleTimeout: .seconds(900))
    for index in 0..<129 {
      let manifest = oneByteManifest(name: "file-\(index)", byte: 1)
      let status = try await store.begin(
        manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
      if committed {
        let (_, path) = try #require(try await store.reserve(status.id, index: 0))
        try await writeRange(path: path, offset: 0, data: Data([1]))
        try await store.complete(status.id, index: 0)
        try await store.commit(status.id, progress: nil)
      }
    }
    await store.close()
    #expect(try FileManager.default.contentsOfDirectory(atPath: received).count == (committed ? 129 : 0))
    let restarted = try await makeStore(root: received, quota: 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    #expect(
      try await restarted.begin(
        oneByteManifest(name: "after-restart", byte: 2), destination: nil, createDestination: false, owner: owner,
        progress: nil
      ).completed.isEmpty)
    await restarted.close()
    #expect(try FileManager.default.contentsOfDirectory(atPath: received).count == (committed ? 129 : 0))
  }

  @Test(arguments: [8, 65])
  func parallelChunksPublishWithoutTransferMetadata(chunks: Int) async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let data = Data(repeating: 0x42, count: chunks * 1_048_576)
    let source = root + "/source"
    try data.write(to: URL(fileURLWithPath: source))
    let manifest = try await Manifest.scan(
      path: source, name: "result", chunkSize: 1_048_576, followSymbolicLinks: true, progress: nil)
    let store = try await makeStore(
      root: root + "/received", quota: 100_000_000, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let id = try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil).id
    try await parallel(items: Array(manifest.chunks.indices), jobs: 8) { index in
      let (_, path) = try #require(try await store.reserve(id, index: index))
      let offset = index * manifest.chunkSize
      try await writeRange(
        path: path, offset: Int64(offset), data: data.subdata(in: offset..<offset + manifest.chunkSize))
      try await store.complete(id, index: index)
    }
    #expect(
      try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil).completed
        == Array(manifest.chunks.indices))
    try await store.commit(id, progress: nil)
    #expect(try Data(contentsOf: URL(fileURLWithPath: root + "/received/result")) == data)
    #expect(try FileManager.default.contentsOfDirectory(atPath: root + "/received") == ["result"])
    await store.close()
  }

  @Test func discardedUploadsReleaseTheirNameQuotaAndPayload() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let received = root + "/received"
    let store = try await makeStore(root: received, quota: 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let first = oneByteManifest(name: "result", byte: 1)
    let id = try await store.begin(first, destination: nil, createDestination: false, owner: owner, progress: nil).id
    let (_, partial) = try #require(try await store.reserve(id, index: 0))
    try await writeRange(path: partial, offset: 0, data: Data([1]))
    try await store.complete(id, index: 0)
    try await store.discard(id, owner: owner)
    #expect(try FileManager.default.contentsOfDirectory(atPath: received).isEmpty)
    #expect(
      try await store.begin(
        oneByteManifest(name: "result", byte: 2), destination: nil, createDestination: false, owner: owner,
        progress: nil
      )
      .completed.isEmpty)
    await store.close()
    #expect(try FileManager.default.contentsOfDirectory(atPath: received).isEmpty)
  }

  @Test func oneUploaderCannotDiscardAnotherUploadersChunks() async throws {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(
      root: root + "/received", quota: 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let manifest = oneByteManifest(name: "result", byte: 7)
    let other = UUID()
    let id = try await store.begin(
      manifest, destination: nil, createDestination: false, owner: owner, progress: nil
    ).id
    _ = try await store.begin(manifest, destination: nil, createDestination: false, owner: other, progress: nil)
    let (_, partial) = try #require(try await store.reserve(id, index: 0))
    let duplicate = Task { try await store.reserve(id, index: 0) }
    try await store.discard(id, owner: UUID())
    try await store.discard(id, owner: owner)
    try await store.discard(id, owner: owner)
    #expect(FileManager.default.fileExists(atPath: partial))
    try await writeRange(path: partial, offset: 0, data: Data([7]))
    try await store.complete(id, index: 0)
    #expect(try await duplicate.value == nil)
    try await store.commit(id, progress: nil)
    #expect(try Data(contentsOf: URL(fileURLWithPath: root + "/received/result")) == Data([7]))
    await store.close()
  }

  @Test(arguments: [true, false])
  func concurrentBeginsReserveNameOrQuota(sameName: Bool) async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(
      root: root + "/received", quota: sameName ? 2 : 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let manifests = [
      oneByteManifest(name: sameName ? "result" : "one", byte: 1),
      oneByteManifest(name: sameName ? "result" : "two", byte: 2)
    ]
    let successful = await withTaskGroup(of: Bool.self, returning: Int.self) { group in
      for manifest in manifests {
        group.addTask {
          do {
            _ = try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
            return true
          } catch {
            return false
          }
        }
      }
      var count = 0
      for await result in group where result {
        count += 1
      }
      return count
    }
    #expect(successful == 1)
  }

  @Test func publicationNeverOverwritesExistingDestination() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let source = root + "/partial"
    let destination = root + "/existing"
    try Data("new".utf8).write(to: URL(fileURLWithPath: source))
    try Data("original".utf8).write(to: URL(fileURLWithPath: destination))
    let manifest = try await Manifest.scan(
      path: source, name: "existing", chunkSize: 1_048_576, followSymbolicLinks: true, progress: nil)
    await #expect(throws: (any Error).self) {
      try await publish(
        partial: source, destination: destination, manifest: manifest, progress: nil, alreadyVerified: false)
    }
    #expect(try String(contentsOfFile: destination, encoding: .utf8) == "original")
  }

  @Test(arguments: [false, true])
  func publicationRejectsChangedDiskContents(changeSize: Bool) async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let partial = root + "/partial"
    let destination = root + "/result"
    try Data("original".utf8).write(to: URL(fileURLWithPath: partial))
    let manifest = try await Manifest.scan(
      path: partial, name: "result", chunkSize: 1_048_576, followSymbolicLinks: true, progress: nil)
    try await writeRange(path: partial, offset: changeSize ? manifest.size : 0, data: Data([0]))
    await #expect(throws: TransferError.self) {
      try await publish(
        partial: partial, destination: destination, manifest: manifest, progress: nil, alreadyVerified: false)
    }
    #expect(!FileManager.default.fileExists(atPath: destination))
    #expect(FileManager.default.fileExists(atPath: partial))
  }

  @Test func catalogCannotExportSymlinks() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    try Data("shared".utf8).write(to: URL(fileURLWithPath: root + "/file.bin"))
    let catalog = FileCatalog(root: root, node: UUID())
    let entry = try #require(try await catalog.list().first)
    let manifest = try await catalog.manifest(fileID: entry.id, chunkSize: defaultChunkSize, progress: nil)
    #expect(manifest.name == entry.name)
    try FileManager.default.removeItem(atPath: root + "/file.bin")
    try FileManager.default.createSymbolicLink(atPath: root + "/file.bin", withDestinationPath: "/etc/passwd")
    #expect(try await catalog.list().isEmpty)
    await #expect(throws: TransferError.self) {
      try await catalog.manifest(fileID: entry.id, chunkSize: defaultChunkSize, progress: nil)
    }
    await #expect(throws: TransferError.self) {
      try await catalog.chunk(name: entry.name, index: 0, chunkSize: defaultChunkSize)
    }
  }

  @Test func failedBeginReleasesNameAndQuota() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(
      root: root + "/received", quota: 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let manifest = oneByteManifest(name: "result", byte: 1)
    let destination = URL(fileURLWithPath: root + "/received/result")
    try Data([2]).write(to: destination)
    await #expect(throws: TransferError.self) {
      try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
    }
    try FileManager.default.removeItem(at: destination)
    #expect(
      try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil).id
        == manifest.id)
  }

  @Test func duplicateBeginsShareOnlyActiveTransfers() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(
      root: root + "/received", quota: 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let manifest = oneByteManifest(name: "result", byte: 1)
    try await withThrowingTaskGroup(of: UploadStatus.self) { group in
      for _ in 0..<8 {
        group.addTask {
          try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
        }
      }
      for try await status in group { #expect(try status.id == manifest.id) }
    }
    let id = try manifest.id
    let (_, path) = try #require(try await store.reserve(id, index: 0))
    let waiting = Task { try await store.reserve(id, index: 0) }
    waiting.cancel()
    await #expect(throws: CancellationError.self) { try await waiting.value }
    await #expect(throws: TransferError.self) { try await store.commit(id, progress: nil) }
    try await writeRange(path: path, offset: 0, data: Data([1]))
    try await store.complete(id, index: 0)
    #expect(try await store.reserve(id, index: 0) == nil)
    try await store.commit(id, progress: nil)
    await #expect(throws: TransferError.self) { try await store.commit(id, progress: nil) }
    #expect(
      try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil).committed
    )

    try FileManager.default.removeItem(atPath: root + "/received/result")
    try await withThrowingTaskGroup(of: UploadStatus.self) { group in
      for _ in 0..<8 {
        group.addTask {
          try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
        }
      }
      for try await status in group { #expect(!status.committed && status.completed.isEmpty) }
    }
    let replacement = oneByteManifest(name: "result", byte: 2)
    await #expect(throws: TransferError.self) {
      try await store.begin(replacement, destination: nil, createDestination: false, owner: owner, progress: nil)
    }
    try await store.discard(id, owner: owner)
    let replacementID = try await store.begin(
      replacement, destination: nil, createDestination: false, owner: owner, progress: nil
    ).id
    await #expect(throws: TransferError.self) {
      try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
    }
    let (_, replacementPath) = try #require(try await store.reserve(replacementID, index: 0))
    try await writeRange(path: replacementPath, offset: 0, data: Data([2]))
    try await store.complete(replacementID, index: 0)
    try await store.commit(replacementID, progress: nil)
    #expect(try Data(contentsOf: URL(fileURLWithPath: root + "/received/result")) == Data([2]))
    #expect(try FileManager.default.contentsOfDirectory(atPath: root + "/received") == ["result"])
    await store.close()
  }

  @Test func activeLimitIncludesEmptyUploadsAndIsReleasedOnCommit() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(root: root + "/received", quota: 1, maxActiveUploads: 2, idleTimeout: .seconds(900))
    let accepted = try await withThrowingTaskGroup(of: Bool.self) { group in
      for index in 0..<8 {
        group.addTask {
          let manifest = Manifest(
            name: "empty-\(index)", size: 0, chunkSize: defaultChunkSize, digest: sha256(Data()), chunks: [])
          do {
            _ = try await store.begin(manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
            return true
          } catch let error as TransferError {
            #expect(error.description == "Receiver active upload limit reached")
            return false
          }
        }
      }
      var count = 0
      for try await success in group { if success { count += 1 } }
      return count
    }
    #expect(accepted == 2)
    try await store.expireIdleUploads(at: .now + .seconds(901))
    let replacement = oneByteManifest(name: "replacement", byte: 1)
    let id = try await store.begin(replacement, destination: nil, createDestination: false, owner: owner, progress: nil)
      .id
    let (_, path) = try #require(try await store.reserve(id, index: 0))
    try await writeRange(path: path, offset: 0, data: Data([1]))
    try await store.complete(id, index: 0)
    try await store.commit(id, progress: nil)
    #expect(
      try await store.begin(
        oneByteManifest(name: "next", byte: 2), destination: nil, createDestination: false, owner: owner, progress: nil
      )
      .completed.isEmpty)
    await store.close()
  }

  @Test func expiryReleasesPayloadNameAndQuotaButProtectsBusyUploads() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(
      root: root + "/received", quota: 1, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let original = oneByteManifest(name: "result", byte: 1)
    let id = try await store.begin(original, destination: nil, createDestination: false, owner: owner, progress: nil).id
    let (_, path) = try #require(try await store.reserve(id, index: 0))
    try await store.expireIdleUploads(at: .now + .seconds(901))
    #expect(FileManager.default.fileExists(atPath: path))
    await store.release(id, index: 0)
    try await store.expireIdleUploads(at: .now)
    #expect(FileManager.default.fileExists(atPath: path))
    try await store.expireIdleUploads(at: .now + .seconds(901))
    #expect(!FileManager.default.fileExists(atPath: path))
    await #expect(throws: TransferError.self) { try await store.reserve(id, index: 0) }
    #expect(
      try await store.begin(
        oneByteManifest(name: "result", byte: 2), destination: nil, createDestination: false, owner: owner,
        progress: nil
      )
      .completed.isEmpty)
    await store.close()
  }

  @Test func cleanupRunsWithoutFurtherRequests() async throws(any Error) {
    let root = try temporaryDirectory()
    defer { try? FileManager.default.removeItem(atPath: root) }
    let store = try await makeStore(
      root: root + "/received", quota: 1, maxActiveUploads: 128, idleTimeout: .milliseconds(30))
    let id = try await store.begin(
      oneByteManifest(name: "abandoned", byte: 1), destination: nil, createDestination: false, owner: owner,
      progress: nil
    ).id
    let (_, path) = try #require(try await store.reserve(id, index: 0))
    await store.release(id, index: 0)
    await store.startCleanup()
    let deadline = ContinuousClock.now + .seconds(3)
    while FileManager.default.fileExists(atPath: path), ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(10))
    }
    #expect(!FileManager.default.fileExists(atPath: path))
    #expect(
      try await store.begin(
        oneByteManifest(name: "abandoned", byte: 2), destination: nil, createDestination: false, owner: owner,
        progress: nil
      )
      .completed.isEmpty)
    await store.close()
  }
}

private func makeStore(
  root: String, quota: Int64, maxActiveUploads: Int, idleTimeout: Duration
) async throws(any Error) -> UploadStore {
  let directory = URL(fileURLWithPath: root).deletingLastPathComponent()
    .appendingPathComponent("temporary-\(UUID().uuidString)").path
  let temporary = try await TemporaryFiles(directory: directory, protecting: [])
  return try await UploadStore(
    root: root, quota: quota, temporary: temporary, maxActiveUploads: maxActiveUploads, idleTimeout: idleTimeout)
}
