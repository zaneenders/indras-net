import NIOCore
import Testing
import _NIOFileSystem

@testable import BTree

@Suite
struct DiskBTreePagerTests {
  private typealias Store = DiskBTree<String, Int, String>
  private static let directoryOptions = OpenOptions.Directory(followSymbolicLinks: false, closeOnExec: true)
  private static let modify = OpenOptions.Write(
    existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)

  @Test func creationPublishesAnEmptyStoreAndRefusesToReplaceIt() async throws {
    let fs = FileSystem.shared
    try await fs.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { directory, path in
      let database = try await Store.create(in: path)
      let manifest = try await DiskTreeManifest.read(in: directory)
      let header = try #require(manifest)
      #expect(header.rootPageID == nil)
      #expect(header.pageCount == 0)
      #expect(header.schema == DiskBTreeFixture.schema)
      let pages = try await fs.info(forFileAt: path.appending("pages"), infoAboutSymbolicLink: true)
      #expect(pages?.type == .regular)
      #expect(pages?.size == 0)
      let publicationLock = try await fs.info(
        forFileAt: path.appending(DiskPager.publicationLockName.string), infoAboutSymbolicLink: true)
      #expect(publicationLock?.type == .regular)
      #expect(publicationLock?.size == 0)

      let reopened = try await Store.open(in: path)
      let empty = try await reopened.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: 1)
      }
      #expect(empty.entries.isEmpty)
      #expect(empty.nextAfter == nil)
      try await database.withWriteTransaction { store in
        let previous = try await store.updateValue("original", for: "p", sortKey: 1)
        #expect(previous == nil)
      }
      await #expect {
        _ = try await Store.create(in: path)
      } throws: {
        if case DiskBTreeError.storeAlreadyExists = $0 { true } else { false }
      }
      let retained = try await reopened.value(for: "p", sortKey: 1)
      #expect(retained == "original")
    }
  }

  @Test func openingRequiresThePagesFileEvenForAnEmptyStore() async throws {
    let fs = FileSystem.shared
    try await fs.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { _, path in
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        if case DiskBTreeError.storeNotFound = $0 { true } else { false }
      }
      let database = try await Store.create(in: path)
      let pages = path.appending("pages")
      try await fs.moveItem(at: pages, to: path.appending("saved-pages"))
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingPages
      }
      await #expect {
        try await database.withWriteTransaction { _ in }
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingPages
      }
      let info = try await fs.info(forFileAt: pages, infoAboutSymbolicLink: true)
      #expect(info == nil)
    }
  }

  @Test func aMissingPublicationLockPreventsReadsAndWrites() async throws {
    let fs = FileSystem.shared
    try await fs.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { _, path in
      let database = try await Store.create(in: path)
      let lock = path.appending(DiskPager.publicationLockName.string)
      try await fs.moveItem(at: lock, to: path.appending("saved-lock"))
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingPublicationLock
      }
      await #expect {
        try await database.withWriteTransaction { _ in }
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingPublicationLock
      }
      let info = try await fs.info(forFileAt: lock, infoAboutSymbolicLink: true)
      #expect(info == nil)
    }
  }

  @Test func creationCanRetryAfterFailingBeforePublication() async throws {
    let fs = FileSystem.shared
    try await fs.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { _, path in
      let pending = path.appending(DiskTreeManifest.pendingName)
      try await fs.createDirectory(at: pending, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      await #expect {
        _ = try await Store.create(in: path)
      } throws: {
        if case DiskBTreeError.io = $0 { true } else { false }
      }
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        if case DiskBTreeError.storeNotFound = $0 { true } else { false }
      }

      _ = try await fs.removeItem(at: pending, strategy: .platformDefault, recursively: false)
      _ = try await Store.create(in: path)
      let reopened = try await Store.open(in: path)
      let value = try await reopened.value(for: "p", sortKey: 1)
      #expect(value == nil)
    }
  }

  @Test func emptyManifestPrecedesTheFirstPrivatePageAndSurvivesAbort() async throws {
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { directory, path in
      let database = try await Store.create(in: path)
      await #expect {
        try await database.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            if step == .writePage(index: 0) {
              let header = try await DiskTreeManifest.read(in: directory)
              try #require(header != nil)
              #expect(header?.rootPageID == nil)
              #expect(header?.pageCount == 0)
            }
          }
          try await store.updateValue("unpublished", for: "p", sortKey: 1)
          throw DiskBTreeFixture.Abort.requested
        }
      } throws: {
        $0 as? DiskBTreeFixture.Abort == .requested
      }
      let missing = try await database.value(for: "p", sortKey: 1)
      #expect(missing == nil)
      try await database.withWriteTransaction { store in
        try await store.updateValue("first", for: "p", sortKey: 2)
        try await store.updateValue("second", for: "p", sortKey: 3)
      }
      let header = try await DiskTreeManifest.read(in: directory)
      #expect(header?.pageCount == 2)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("third", for: "p", sortKey: 4)
      }
      let next = try await DiskTreeManifest.read(in: directory)
      #expect(next?.pageCount == 3)
    }
  }

  @Test func losingTheManifestRefusesReadsAndWritesAndPreservesExistingPages() async throws {
    let fs = FileSystem.shared
    try await fs.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("original", for: "p", sortKey: 1)
      }
      let manifest = path.appending("manifest")
      let backup = path.appending("saved-manifest")
      try await fs.moveItem(at: manifest, to: backup)
      await #expect {
        _ = try await database.value(for: "p", sortKey: 1)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingManifest
      }
      await #expect {
        try await database.withWriteTransaction { store in
          _ = try await store.updateValue("replacement", for: "p", sortKey: 1)
        }
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingManifest
      }
      await #expect {
        _ = try await Store.create(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .missingManifest
      }
      try await fs.moveItem(at: backup, to: manifest)
      let value = try await database.value(for: "p", sortKey: 1)
      #expect(value == "original")
    }
  }

  @Test func aTruncatedCommittedFileCannotBeHiddenByAnUnrelatedInsert() async throws {
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { directory, path in
      let database = try await Store.create(in: path)
      let large = String(repeating: "v", count: 1_900)
      try await database.withWriteTransaction { store in
        for key in [1, 2, 3, 1, 3, 1] { try await store.updateValue(large, for: "p", sortKey: key) }
      }
      let stored = try await DiskTreeManifest.read(in: directory)
      let header = try #require(stored)
      let partialSize = DiskPageFormat.offset(for: header.pageCount - 1) + 6
      try await directory.withFileHandle(forWritingAt: FilePath("pages"), options: Self.modify) { file in
        try await file.resize(to: .bytes(partialSize))
      }
      await #expect {
        try await database.withWriteTransaction { store in
          _ = try await store.updateValue(large, for: "p", sortKey: 4)
        }
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .incompletePage
      }
      await #expect {
        _ = try await database.value(for: "p", sortKey: 1)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .incompletePage
      }
      let size = try await FileSystem.shared.info(forFileAt: path.appending("pages"), infoAboutSymbolicLink: false)?
        .size
      #expect(size == partialSize)
    }
  }

  @Test func checksumsDetectAValidCBORValueChangedByOneBit() async throws {
    typealias IntStore = DiskBTree<String, Int, Int>
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.directoryOptions) { directory, path in
      let database = try await IntStore.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue(42, for: "p", sortKey: 1)
      }
      let root = try await DiskBTreeFixture.root(in: directory)
      let offset = DiskPageFormat.offset(for: root)
      try await directory.withFileHandle(forReadingAndWritingAt: FilePath("pages"), options: Self.modify) { file in
        let bytes = try await file.readChunk(
          fromAbsoluteOffset: offset, length: .bytes(Int64(DiskPageFormat.pageSize - DiskPageFormat.checksumSize)))
        let index = try #require(bytes.readableBytesView.lastIndex(of: 0x2a))
        let count = try await file.write(contentsOf: [UInt8(0x2b)], toAbsoluteOffset: offset + Int64(index))
        try #require(count == 1)
      }
      await #expect {
        _ = try await database.value(for: "p", sortKey: 1)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: root)
      }
    }
  }
}
