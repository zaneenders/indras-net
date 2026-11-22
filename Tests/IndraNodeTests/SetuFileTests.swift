import NIOCore
import TCPActorSystem
import Testing
import _NIOFileSystem

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
    let absolute = try await FileSystem.shared.currentWorkingDirectory.pushing(relative)
    #expect(try await absolutePath(relative) == absolute)
    #expect(try await absolutePath(absolute) == absolute)
  }

  @Test func fileListingCursorsKeepIndependentSnapshotsAcrossDirectoryChanges() async throws {
    let files = FileSystem.shared
    let base = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: base, strategy: .sequential, recursively: true) }
    let root = base.appending("shared")
    let config = makeConfiguration(name: "setu", seeds: [])
    try await withTestRuntime(
      configuration: config, configurationPath: root.appending("config.json").string,
      services: [SetuService.installer(directory: root.string)]
    ) { runtime in
      let services = try await runtime.receptionist.lookup(SetuService.key)
      let service = try #require(services.first)
      let empty = try await service.list(after: nil, cluster: config.cluster)
      #expect(empty.files.isEmpty && empty.next == nil)

      for index in 0..<1024 {
        try await ByteBuffer().write(
          toFileAt: root.appending("entry-\(index)"), absoluteOffset: 0,
          options: .newFile(replaceExisting: false, permissions: nil))
      }
      let fullPage = try await service.list(after: nil, cluster: config.cluster)
      #expect(fullPage.files.count == 1024 && fullPage.next == nil)
      let last = root.appending("z-old")
      try await [UInt8(1)].write(
        toFileAt: last, absoluteOffset: 0, options: .newFile(replaceExisting: false, permissions: nil))
      let first = try await service.list(after: nil, cluster: config.cluster)
      let cursor = try #require(first.next)
      #expect(first.files.map(\.name) == fullPage.files.map(\.name))
      try await files.moveItem(at: last, to: root.appending("z-new"))
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
    let files = FileSystem.shared
    let base = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: base, strategy: .sequential, recursively: true) }
    let child = base.appending("nested/child")
    try await files.createDirectory(at: child, withIntermediateDirectories: true, permissions: .ownerReadWriteExecute)
    let link = base.appending("link")
    try await files.createSymbolicLink(at: link, withDestination: relativeLink ? "nested/child" : child)

    let path = link.appending("../shared/files").string
    let resolved = base.appending("nested/shared/files")
    await #expect(throws: IndraError.self) {
      try await SharedDirectory(path: path, journal: StagingJournal(parent: base))
    }
    #expect(try await files.info(forFileAt: resolved, infoAboutSymbolicLink: true) == nil)
    _ = try await SharedDirectory(path: resolved.string, journal: StagingJournal(parent: base))
    let directory = try await SharedDirectory(path: path, journal: StagingJournal(parent: base))
    try await [UInt8(1)].write(
      toFileAt: resolved.appending("file"), absoluteOffset: 0,
      options: .newFile(replaceExisting: false, permissions: nil))
    try await files.createSymbolicLink(at: directory.root.appending("alias"), withDestination: "file")
    let hiddenName = ".\u{0301}hidden"
    try await [UInt8(2)].write(
      toFileAt: resolved.appending(hiddenName), absoluteOffset: 0,
      options: .newFile(replaceExisting: false, permissions: nil))
    #expect(throws: IndraError.self) { try directory.path(for: hiddenName) }
    let listing = try await directory.list()
    #expect(listing.map(\.name) == ["file"])
    #expect(listing.map(\.size) == [1])
    #expect(try await files.info(forFileAt: base.appending("shared"), infoAboutSymbolicLink: true) == nil)
  }

  @Test func competingSinksPublishOnceInTheOriginallyOpenedDirectory() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let original = root.appending("shared")
    let moved = root.appending("moved")
    try await files.createDirectory(
      at: original, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
    let path = original.appending("result")
    let first = try await FileSink(path: path, journal: StagingJournal(parent: root))
    defer { await first.abort() }
    let second = try await FileSink(path: path, journal: StagingJournal(parent: root))
    defer { await second.abort() }
    try await first.write(ByteBuffer(string: "first"), at: 0)
    try await second.write(ByteBuffer(string: "second"), at: 0)
    try await files.moveItem(at: original, to: moved)
    try await files.createDirectory(
      at: original, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)

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
    #expect(try await winners == [String(contentsOf: moved.appending("result"), maximumSizeAllowed: .unlimited)])
    #expect(try await directoryNames(at: moved) == ["result"])
    #expect(try await directoryNames(at: original).isEmpty)
  }

  @Test func recoveryReclaimsInterruptedStagesAndPreservesTheActiveWriter() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let output = root.appending("external")
    try await files.createDirectory(at: output, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
    let rootPath = root.string
    let journal = StagingJournal(parent: root)
    let live = try await StagedFile(
      path: output.appending("live"), publication: .replace, journal: journal)
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
    #expect(try await directoryNames(at: output) == ["live"])
    #expect(try await String(contentsOf: output.appending("live"), maximumSizeAllowed: .unlimited) == "live")
    #expect(try await directoryNames(at: root.appending(".indra-staging")).isEmpty)
  }

  @Test(arguments: [false, true])
  func directorySyncFailurePreservesPublicationAndReportsUncertainty(receiverDials: Bool) async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let destination = root.appending("copy")
    let source = root.appending("original")
    try await [UInt8(7)].write(
      toFileAt: source, absoluteOffset: 0, options: .newFile(replaceExisting: false, permissions: nil))
    let sender = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0,
      callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await sender.shutdown() }
    let receiver = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0,
      callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await receiver.shutdown() }
    let openSource: @Sendable () async throws -> TransferBody = {
      .source(try await FileSource(path: source, followLinks: false))
    }
    let openSink: @Sendable () async throws -> TransferBody = {
      var file = try await StagedFile(
        path: destination, publication: .createOnly, journal: StagingJournal(parent: root))
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
    #expect(try await [UInt8](contentsOf: destination, maximumSizeAllowed: .unlimited) == [7])
    #expect(await receiver.transfers.receipt(for: ticket.id) == nil)
  }

  @Test func sourceRejectsFIFOWithoutWaitingForAWriter() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("source")
    let result = unsafe path.withPlatformString { unsafe mkfifo($0, 0o600) }
    try #require(result == 0)

    // Release a blocking open so a regression can fail without hanging the test runner.
    let watchdog = Task {
      try await Task.sleep(for: .seconds(2))
      Issue.record("Opening a FIFO as a source waited for a writer")
      try await files.withDirectoryHandle(atPath: root, options: .init(followSymbolicLinks: false, closeOnExec: true)) {
        directory in
        try await directory.withFileHandle(
          forWritingAt: "source",
          options: .init(existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)
        ) { _ in }
      }
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

func directoryNames(at path: FilePath) async throws -> [String] {
  try await FileSystem.shared.withDirectoryHandle(
    atPath: path, options: .init(followSymbolicLinks: false, closeOnExec: true)
  ) { directory in
    try await directory.listContents(recursive: false).map { $0.name.string }.reduce(into: []) { $0.append($1) }
  }
}

private struct StagedSink: DataSink {
  let file: StagedFile
  func write(_ bytes: ByteBuffer, at offset: Int64) async throws { try await file.write(bytes, at: offset) }
  func commit() async throws { try await file.commit() }
  func abort() async { await file.abort() }
}
