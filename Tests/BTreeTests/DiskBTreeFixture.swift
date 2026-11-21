import Testing
import _NIOFileSystem

@testable import BTree

enum DiskBTreeFixture {
  enum Abort: Error, Equatable {
    case requested
  }

  static let schema = DiskBTree<String, Int, String>.persistedSchema

  static func corruption(in error: any Error) -> DiskBTreeError.Corruption? {
    guard case DiskBTreeError.corruptStore(let cause) = error else { return nil }
    return cause
  }

  static func root(in directory: DirectoryFileHandle) async throws -> Int64 {
    let manifest = try await DiskTreeManifest.read(in: directory)
    return try #require(manifest?.rootPageID)
  }

  static func writeStore(
    pages: [[UInt8]], root: Int64, schema: DiskTreeSchema, in directory: DirectoryFileHandle
  ) async throws {
    try await createFixtureFile(named: "pages", bytes: pages.flatMap { $0 }, in: directory)
    try await writeManifest(
      DiskTreeManifest(rootPageID: root, pageCount: Int64(pages.count), schema: schema),
      named: "manifest", in: directory)
  }

  static func writeManifest(
    _ manifest: DiskTreeManifest, named name: String, in directory: DirectoryFileHandle
  ) async throws {
    if name == DiskTreeManifest.name {
      try await createFixtureFile(named: DiskPager.publicationLockName.string, bytes: [], in: directory)
    }
    try await directory.withFileHandle(
      forWritingAt: FilePath(name),
      options: .init(
        existingFile: .none, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
        followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      try await manifest.write(to: file)
      try await file.synchronize()
    }
  }

  static func createFixtureFile(
    named name: String, bytes: [UInt8], in directory: DirectoryFileHandle
  ) async throws {
    try await directory.withFileHandle(
      forWritingAt: FilePath(name),
      options: .init(
        existingFile: .none, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
        followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      let written = try await file.write(contentsOf: bytes, toAbsoluteOffset: 0)
      try #require(written == bytes.count)
      try await file.synchronize()
    }
  }
}
