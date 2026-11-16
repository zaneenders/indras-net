import Foundation
import Testing

@testable import Setu

struct TemporaryFilesTests {
  private let owner = UUID()

  @Test func closingAClientScopeClearsItsPayloadsAndPreservesOtherTransfers() async throws(any Error) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-temporary-scopes-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let temporary = try await TemporaryFiles(directory: root.path, protecting: [])
    let first = try await temporary.scope()
    let second = try await temporary.scope()
    let firstPayload = try await first.allocate(size: 1)
    let secondPayload = try await second.allocate(size: 1)
    try await writeRange(path: secondPayload, offset: 0, data: Data([7]))
    await first.close()
    #expect(!FileManager.default.fileExists(atPath: firstPayload))
    #expect(try Data(contentsOf: URL(fileURLWithPath: secondPayload)) == Data([7]))
    await #expect(throws: TransferError.self) { try await TemporaryFiles(directory: root.path, protecting: []) }
    await second.close()
    #expect(try FileManager.default.contentsOfDirectory(atPath: root.path) == [".lock"])
    let next = try await temporary.allocate(size: 1)
    try await temporary.discard(next)
    await temporary.close()
  }

  @Test func cleanupFailuresAreReportedAndCanBeRetried() async throws(any Error) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-temporary-cleanup-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let temporary = try await TemporaryFiles(directory: root.path, protecting: [])
    let partial = try await temporary.allocate(size: 1)
    try FileManager.default.removeItem(atPath: partial)
    try FileManager.default.createDirectory(atPath: partial, withIntermediateDirectories: false)
    let obstruction = URL(fileURLWithPath: partial).appendingPathComponent("obstruction")
    try Data([1]).write(to: obstruction)
    await #expect(throws: (any Error).self) { try await temporary.discard(partial) }
    try FileManager.default.removeItem(at: obstruction)
    try await temporary.discard(partial)
    #expect(!FileManager.default.fileExists(atPath: partial))
    await temporary.close()
  }

  @Test func startupRemovesAbandonedPayloadsAndPreservesOtherFiles() async throws(any Error) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-temporary-tests-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let directory = root.appendingPathComponent("temporary").path
    let first = try await TemporaryFiles(directory: directory, protecting: [])
    let abandoned = try await first.allocate(size: 1)
    await first.close()
    try Data([1]).write(to: URL(fileURLWithPath: abandoned))
    let notes = directory + "/notes.txt"
    try Data("keep".utf8).write(to: URL(fileURLWithPath: notes))
    let outside = root.appendingPathComponent("outside")
    try Data("outside".utf8).write(to: outside)
    let symlink = directory + "/.transfer-\(UUID().uuidString).partial"
    try FileManager.default.createSymbolicLink(atPath: symlink, withDestinationPath: outside.path)

    let restarted = try await TemporaryFiles(directory: directory, protecting: [])
    #expect(!FileManager.default.fileExists(atPath: abandoned))
    #expect(!FileManager.default.fileExists(atPath: symlink))
    #expect(try String(contentsOfFile: notes, encoding: .utf8) == "keep")
    #expect(try String(contentsOf: outside, encoding: .utf8) == "outside")
    #expect(try FileManager.default.contentsOfDirectory(atPath: directory).sorted() == [".lock", "notes.txt"])
    await restarted.close()
  }

  @Test func anotherOwnerCannotClearActivePayloads() async throws(any Error) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-temporary-lock-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let temporary = try await TemporaryFiles(directory: root.path, protecting: [])
    let partial = try await temporary.allocate(size: 1)
    try await writeRange(path: partial, offset: 0, data: Data([7]))
    await #expect(throws: TransferError.self) { try await TemporaryFiles(directory: root.path, protecting: []) }
    #expect(try Data(contentsOf: URL(fileURLWithPath: partial)) == Data([7]))
    await temporary.close()
    #expect(try FileManager.default.contentsOfDirectory(atPath: root.path) == [".lock"])
  }

  @Test func refusesSharedPublicAndSymlinkDirectories() async throws(any Error) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-temporary-permissions-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createDirectory(
      at: root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    await #expect(throws: TransferError.self) {
      try await TemporaryFiles(directory: root.path, protecting: [root.path])
    }
    let exposed = root.appendingPathComponent("exposed")
    try FileManager.default.createDirectory(
      at: exposed, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o755])
    await #expect(throws: TransferError.self) { try await TemporaryFiles(directory: exposed.path, protecting: []) }
    let symlink = root.appendingPathComponent("symlink")
    try FileManager.default.createSymbolicLink(at: symlink, withDestinationURL: root)
    await #expect(throws: TransferError.self) { try await TemporaryFiles(directory: symlink.path, protecting: []) }
  }

  @Test func uploadsStayInConfiguredTemporaryDirectoryUntilVerified() async throws(any Error) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-temporary-upload-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let temporary = try await TemporaryFiles(directory: root.appendingPathComponent("temporary").path, protecting: [])
    let shared = root.appendingPathComponent("shared")
    let store = try await UploadStore(
      root: shared.path, quota: 10, temporary: temporary, maxActiveUploads: 128, idleTimeout: .seconds(900))
    let data = Data([7])
    let digest = sha256(data)
    let manifest = Manifest(name: "result", size: 1, chunkSize: 1_048_576, digest: digest, chunks: [digest])
    let status = try await store.begin(
      manifest, destination: nil, createDestination: false, owner: owner, progress: nil)
    let (_, partial) = try #require(try await store.reserve(status.id, index: 0))
    #expect(URL(fileURLWithPath: partial).deletingLastPathComponent().path == temporary.directory)
    #expect(try FileManager.default.contentsOfDirectory(atPath: shared.path).isEmpty)
    try await writeRange(path: partial, offset: 0, data: data)
    try await store.complete(status.id, index: 0)
    #expect(!FileManager.default.fileExists(atPath: shared.appendingPathComponent("result").path))
    try await store.commit(status.id, progress: nil)
    #expect(try Data(contentsOf: shared.appendingPathComponent("result")) == data)
    #expect(try FileManager.default.contentsOfDirectory(atPath: temporary.directory) == [".lock"])
    await store.close()
    await temporary.close()
  }

  @Test func publishesFromSystemTemporaryDirectoryToHomeFilesystemWithoutOverwriting() async throws(any Error) {
    let root = URL(fileURLWithPath: FileManager.default.homeDirectoryForCurrentUser.path).appendingPathComponent(
      ".cache/setu-publication-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createDirectory(
      at: root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    let temporaryRoot = FileManager.default.temporaryDirectory.appendingPathComponent(
      "setu-publication-source-\(UUID())")
    defer { try? FileManager.default.removeItem(at: temporaryRoot) }
    let temporary = try await TemporaryFiles(directory: temporaryRoot.path, protecting: [])
    let data = Data(repeating: 0x42, count: 1_048_577)
    let source = try await temporary.allocate(size: Int64(data.count))
    try await writeRange(path: source, offset: 0, data: data)
    let manifest = try await Manifest.scan(
      path: source, name: "result", chunkSize: 1_048_576, followSymbolicLinks: true, progress: nil)
    let destination = root.appendingPathComponent("result")
    try await publish(
      partial: source, destination: destination.path, manifest: manifest, progress: nil, alreadyVerified: false)
    #expect(try Data(contentsOf: destination) == data)
    #expect(!FileManager.default.fileExists(atPath: source))
    try await temporary.discard(source)

    let second = try await temporary.allocate(size: Int64(data.count))
    try await writeRange(path: second, offset: 0, data: data)
    await #expect(throws: (any Error).self) {
      try await publish(
        partial: second, destination: destination.path, manifest: manifest, progress: nil, alreadyVerified: false)
    }
    #expect(try Data(contentsOf: destination) == data)
    #expect(try FileManager.default.contentsOfDirectory(atPath: root.path) == ["result"])
    await temporary.close()
  }
}
