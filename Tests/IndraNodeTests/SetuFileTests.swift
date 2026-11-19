import Foundation
import NIOCore
import SystemPackage
import TCPActorSystem
import Testing

@testable import IndraNode
@testable import IndraSetu

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

@Suite(.timeLimit(.minutes(1)))
struct SetuFileTests {
  @Test func commandPathsPreserveParentComponentsForFilesystemResolution() async throws {
    let relative = FilePath("link/../file")
    let absolute = FilePath(FileManager.default.currentDirectoryPath).pushing(relative)
    #expect(try await absolutePath(relative) == absolute)
    #expect(try await absolutePath(absolute) == absolute)
  }

  @Test func fileListingCursorsKeepIndependentSnapshotsAcrossDirectoryChanges() async throws {
    let manager = FileManager.default
    let root = manager.temporaryDirectory.appending(path: "indra-pages-\(UUID())", directoryHint: .isDirectory)
    defer { try? manager.removeItem(at: root) }
    let config = makeConfiguration(name: "setu", seeds: [])
    try await withTestRuntime(
      configuration: config, configurationPath: root.appending(path: "config.json").path,
      services: [SetuService.installer(directory: root.path)]
    ) { runtime in
      let services = try await runtime.receptionist.lookup(SetuService.key)
      let service = try #require(services.first)
      let empty = try await service.list(after: nil, cluster: config.cluster)
      #expect(empty.files.isEmpty && empty.next == nil)

      for index in 0..<1024 { try Data().write(to: root.appending(path: "entry-\(index)")) }
      let fullPage = try await service.list(after: nil, cluster: config.cluster)
      #expect(fullPage.files.count == 1024 && fullPage.next == nil)
      let last = root.appending(path: "z-old")
      try Data([1]).write(to: last)
      let first = try await service.list(after: nil, cluster: config.cluster)
      let cursor = try #require(first.next)
      #expect(first.files.map(\.name) == fullPage.files.map(\.name))
      try manager.moveItem(at: last, to: root.appending(path: "z-new"))
      let fresh = try await service.list(after: nil, cluster: config.cluster)
      let other = try #require(fresh.next)
      for _ in 0..<2 {
        let tail = try await service.list(after: cursor, cluster: config.cluster)
        #expect(tail.files.map(\.name) == ["z-old"] && tail.next == nil)
      }
      try await service.closeListing(cursor.listing, cluster: config.cluster)
      await #expect(throws: IndraError.self) { try await service.list(after: cursor, cluster: config.cluster) }
      let tail = try await service.list(after: other, cluster: config.cluster)
      #expect(tail.files.map(\.name) == ["z-new"] && tail.next == nil)
      try await service.closeListing(other.listing, cluster: config.cluster)
    }
  }

  @Test(arguments: [false, true])
  func sharedDirectoryWithParentComponentsMustExist(relativeLink: Bool) async throws {
    let manager = FileManager.default
    let base = manager.temporaryDirectory.appending(path: "indra-shared-\(UUID())", directoryHint: .isDirectory)
    let child = base.appending(path: "nested/child", directoryHint: .isDirectory)
    try manager.createDirectory(at: child, withIntermediateDirectories: true, attributes: nil)
    defer { try? manager.removeItem(at: base) }
    let link = base.appending(path: "link", directoryHint: .isDirectory)
    try manager.createSymbolicLink(atPath: link.path, withDestinationPath: relativeLink ? "nested/child" : child.path)

    let path = link.path + "/../shared/files"
    let resolved = base.appending(path: "nested/shared/files", directoryHint: .isDirectory)
    await #expect(throws: IndraError.self) {
      try await SharedDirectory(path: path, journal: StagingJournal(parent: FilePath(base.path)))
    }
    #expect(!manager.fileExists(atPath: resolved.path))
    _ = try await SharedDirectory(path: resolved.path, journal: StagingJournal(parent: FilePath(base.path)))
    let directory = try await SharedDirectory(path: path, journal: StagingJournal(parent: FilePath(base.path)))
    let fixture = resolved.appending(path: "file", directoryHint: .notDirectory)
    try Data([1]).write(to: fixture, options: [])
    try manager.createSymbolicLink(atPath: directory.root.appending("alias").string, withDestinationPath: "file")
    let hiddenName = ".\u{0301}hidden"
    try Data([2]).write(to: fixture.deletingLastPathComponent().appending(path: hiddenName), options: [])
    #expect(throws: IndraError.self) { try directory.path(for: hiddenName) }
    let files = try await directory.list()
    #expect(files.map(\.name) == ["file"])
    #expect(files.map(\.size) == [1])
    #expect(!manager.fileExists(atPath: base.path + "/shared"))
  }

  @Test func competingSinksPublishOnceInTheOriginallyOpenedDirectory() async throws {
    let manager = FileManager.default
    let root = manager.temporaryDirectory.appending(path: UUID().uuidString)
    let original = root.appending(path: "shared")
    let moved = root.appending(path: "moved")
    try manager.createDirectory(at: original, withIntermediateDirectories: true)
    defer { try? manager.removeItem(at: root) }
    let path = FilePath(original.appending(path: "result").path)
    let first = try await FileSink(path: path, journal: StagingJournal(parent: FilePath(root.path)))
    defer { await first.abort() }
    let second = try await FileSink(path: path, journal: StagingJournal(parent: FilePath(root.path)))
    defer { await second.abort() }
    try await first.write(ByteBuffer(string: "first"), at: 0)
    try await second.write(ByteBuffer(string: "second"), at: 0)
    try manager.moveItem(at: original, to: moved)
    try manager.createDirectory(at: original, withIntermediateDirectories: false)

    let winners = await withTaskGroup(of: String?.self) { group in
      for (sink, value) in [(first, "first"), (second, "second")] {
        group.addTask {
          do {
            try await sink.commit()
            return value
          } catch {
            await sink.abort()
            return nil
          }
        }
      }
      var winners: [String] = []
      for await winner in group { if let winner { winners.append(winner) } }
      return winners
    }
    #expect(try winners == [String(contentsOf: moved.appending(path: "result"), encoding: .utf8)])
    #expect(try manager.contentsOfDirectory(atPath: moved.path) == ["result"])
    #expect(try manager.contentsOfDirectory(atPath: original.path).isEmpty)
  }

  @Test func recoveryReclaimsInterruptedStagesAndPreservesTheActiveWriter() async throws {
    let manager = FileManager.default
    let root = manager.temporaryDirectory.appending(path: UUID().uuidString)
    let output = root.appending(path: "external")
    try manager.createDirectory(at: output, withIntermediateDirectories: true)
    defer { try? manager.removeItem(at: root) }
    let rootPath = root.path
    let journal = StagingJournal(parent: FilePath(rootPath))
    let live = try await StagedFile(
      path: FilePath(output.appending(path: "live").path), publication: .replace, journal: journal)
    defer { await live.abort() }
    try await live.write(ByteBuffer(string: "live"), at: 0)

    try await #require(processExitsWith: .exitCode(42)) { [rootPath = rootPath as String] in
      let journal = StagingJournal(parent: FilePath(rootPath))
      let output = FilePath(rootPath).appending("external")
      let transfer = try await FileSink(path: output.appending("download"), journal: journal)
      let named = try await StagedFile(path: output.appending("replace"), publication: .replace, journal: journal)
      try await transfer.write(ByteBuffer(string: "partial download"), at: 0)
      try await named.write(ByteBuffer(string: "partial replacement"), at: 0)
      _exit(42)
    }
    try await journal.recover()
    try await journal.recover()
    try await live.commit()
    #expect(try manager.contentsOfDirectory(atPath: output.path) == ["live"])
    #expect(try String(contentsOf: output.appending(path: "live"), encoding: .utf8) == "live")
    #expect(try manager.contentsOfDirectory(atPath: root.appending(path: ".indra-staging").path).isEmpty)
  }

  @Test(arguments: [false, true])
  func directorySyncFailurePreservesPublicationAndReportsUncertainty(receiverDials: Bool) async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
    defer { try? FileManager.default.removeItem(at: root) }
    let destination = root.appending(path: "copy")
    let source = root.appending(path: "original")
    try Data([7]).write(to: source)
    let sender = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0,
      callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await sender.shutdown() }
    let receiver = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0,
      callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await receiver.shutdown() }
    let openSource: @Sendable () async throws -> TransferBody = {
      .source(try await FileSource(path: FilePath(source.path), followLinks: false))
    }
    let openSink: @Sendable () async throws -> TransferBody = {
      var file = try await StagedFile(
        path: FilePath(destination.path), publication: .createOnly,
        journal: StagingJournal(parent: FilePath(root.path)))
      file.synchronizeDirectory = { _ in throw IndraError("Injected directory sync failure") }
      return .sink(StagedSink(file: file))
    }
    let server = receiverDials ? sender : receiver
    let client = receiverDials ? receiver : sender
    let ticket = try await server.transfers.offer(
      to: client.endpoint, byteCount: 1,
      open: receiverDials ? openSource : openSink)
    do {
      _ = try await client.transfers.transfer(ticket, open: receiverDials ? openSink : openSource)
      Issue.record("Expected an unconfirmed commit")
    } catch let error as CommitUnconfirmed {
      #expect(error.expected.id == ticket.id)
    }
    #expect(try Data(contentsOf: destination) == Data([7]))
    #expect(await receiver.transfers.receipt(for: ticket.id) == nil)
  }

  @Test func sourceRejectsFIFOWithoutWaitingForAWriter() async throws {
    let root = FileManager.default.temporaryDirectory.appending(
      path: "indra-fifo-\(UUID())", directoryHint: .isDirectory)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false, attributes: nil)
    defer { try? FileManager.default.removeItem(at: root) }
    let path = FilePath(root.appending(path: "source", directoryHint: .notDirectory).path)
    let result = unsafe path.withPlatformString { unsafe mkfifo($0, 0o600) }
    try #require(result == 0)

    // Release a blocking open so a regression can fail without hanging the test runner.
    let watchdog = Task {
      try await Task.sleep(for: .seconds(2))
      Issue.record("Opening a FIFO as a source waited for a writer")
      let writer = try FileDescriptor.open(
        path, .writeOnly, options: [.nonBlocking, .closeOnExec], permissions: nil, retryOnInterrupt: true)
      try writer.close()
    }
    defer {
      watchdog.cancel()
      _ = await watchdog.result
    }
    await #expect(throws: IndraError.self) {
      let source = try await FileSource(path: path, followLinks: false)
      await source.close()
    }
  }
}

private struct StagedSink: DataSink {
  let file: StagedFile
  func write(_ bytes: ByteBuffer, at offset: Int64) async throws { try await file.write(bytes, at: offset) }
  func commit() async throws { try await file.commit() }
  func abort() async { await file.abort() }
}
