import Foundation
import IndraNode
import NIOCore
import Synchronization
import SystemPackage
import Testing

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
    await #expect(throws: IndraError.self) { try await SharedDirectory(path: path) }
    #expect(!manager.fileExists(atPath: resolved.path))
    _ = try await SharedDirectory(path: resolved.path)
    let directory = try await SharedDirectory(path: path)
    let fixture = resolved.appending(path: "file", directoryHint: .notDirectory)
    try Data([1]).write(to: fixture, options: [])
    try manager.createSymbolicLink(atPath: directory.root.appending("alias").string, withDestinationPath: "file")
    let hiddenName = ".\u{0301}hidden"
    try Data([2]).write(to: fixture.deletingLastPathComponent().appending(path: hiddenName), options: [])
    #expect(throws: IndraError.self) { try directory.path(for: hiddenName) }
    let listing = Mutex<[SharedFile]>([])
    try await directory.list { page in listing.withLock { $0.append(contentsOf: page) } }
    let files = listing.withLock { $0 }
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
    let first = try await FileSink(path: path)
    defer { await first.abort() }
    let second = try await FileSink(path: path)
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
