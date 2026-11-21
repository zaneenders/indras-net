import NIOCore
import Testing
import _NIOFileSystem

@testable import BTree

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

@Suite(.timeLimit(.minutes(1)))
struct DiskBTreeCommitTests {
  private typealias Store = DiskBTree<String, Int, String>
  private typealias Page = DiskPage<String, Int>

  private static let partition = String(repeating: "p", count: 960)
  private static let keys = Array(stride(from: 10, through: 140, by: 10))
  private static let insertedKey = 15
  private static let stepsAfterPageWrites: [DiskCommitStep] = [
    .syncPages, .writeManifest, .syncManifest, .syncDirectoryBeforeRename, .renameManifest, .syncDirectoryAfterRename
  ]
  private static let failuresBeforePublication =
    (0..<5).map { DiskCommitStep.writePage(index: $0) } + stepsAfterPageWrites.dropLast()

  private static let injectedIO = DiskBTreeError.IOFailure.fileSystem(message: "Injected commit failure")

  private actor CommitTrace {
    private(set) var steps: [DiskCommitStep] = []

    func record(_ step: DiskCommitStep) {
      steps.append(step)
    }
  }

  private struct PathPage: Equatable, Sendable {
    let id: Int64
    let siblings: [Int64]
  }

  private struct LookupPath: Equatable, Sendable {
    let pages: [PathPage]
    let value: String?
  }

  @Test func updatingOneLeafCopiesItsPathAndSharesSiblingPages() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let database = try await Store.open(in: path)
      let key = 60
      let originalRoot = try await DiskBTreeFixture.root(in: directory)
      let originalSize = try await Self.pagesSize(in: directory)
      let originalBytes = try await Self.pagesPrefix(length: originalSize, in: directory)
      let original = try await Self.lookupPath(to: key, root: originalRoot, in: directory)
      try #require(original.pages.count == 3)
      #expect(original.value == Self.value(for: key))

      let trace = CommitTrace()
      let newValue = String(repeating: "w", count: Self.value(for: key).utf8.count)
      try await database.withWriteTransaction { store in
        store.beforeCommitStep = { await trace.record($0) }
        let previous = try await store.updateValue(newValue, for: Self.partition, sortKey: key)
        #expect(previous == original.value)
      }

      let appendedCount = original.pages.count
      let expectedSteps = (0..<appendedCount).map { DiskCommitStep.writePage(index: $0) } + Self.stepsAfterPageWrites
      #expect(await trace.steps == expectedSteps)
      let newRoot = try await DiskBTreeFixture.root(in: directory)
      let newSize = try await Self.pagesSize(in: directory)
      let updated = try await Self.lookupPath(to: key, root: newRoot, in: directory)
      let retained = try await Self.lookupPath(to: key, root: originalRoot, in: directory)
      let retainedBytes = try await Self.pagesPrefix(length: originalSize, in: directory)
      let retainedPagesAreIntact = retainedBytes == originalBytes
      #expect(retainedPagesAreIntact)
      #expect(retained == original)
      #expect(updated.value == newValue)
      #expect(updated.pages.map(\.siblings) == original.pages.map(\.siblings))
      #expect(newSize == originalSize + Int64(appendedCount * DiskPageFormat.pageSize))
      let firstAppend = originalSize / Int64(DiskPageFormat.pageSize)
      #expect(updated.pages.map(\.id) == (0..<appendedCount).reversed().map { firstAppend + Int64($0) })

      try await database.withWriteTransaction { store in
        let page = try await store.scan(partition: Self.partition, after: nil, limit: .max)
        #expect(page.entries.map(\.sortKey) == Self.keys)
        #expect(page.entries.map(\.value) == Self.keys.map { $0 == key ? newValue : Self.value(for: $0) })
      }
    }
  }

  @Test(arguments: DiskBTreeCommitTests.failuresBeforePublication)
  func failuresBeforePublicationLeaveTheWholeOldTree(fault: DiskCommitStep) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let database = try await Store.open(in: path)
      let original = try await DiskBTreeFixture.root(in: directory)
      await #expect {
        try await database.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            if step == fault { throw DiskBTreeError.io(cause: Self.injectedIO) }
          }
          try await store.updateValue(Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        }
      } throws: {
        $0 as? DiskBTreeError == .io(cause: Self.injectedIO)
      }

      let reopenedRoot = try await DiskBTreeFixture.root(in: directory)
      #expect(reopenedRoot == original)
      try await database.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: false)
        let previous = try await store.updateValue(
          Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        #expect(previous == nil)
      }
      try await database.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: true)
      }
    }
  }

  @Test func aPartialSecondPageAndTheAbandonedFirstPageAreReusedOnRetry() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let database = try await Store.open(in: path)
      let originalSize = try await Self.pagesSize(in: directory)
      let originalRoot = try await DiskBTreeFixture.root(in: directory)
      try await database.withWriteTransaction { store in
        store.beforeCommitStep = { step in
          guard step == .writePage(index: 1) else { return }
          try await directory.withFileHandle(
            forWritingAt: FilePath("pages"),
            options: .init(existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)
          ) { pages in
            let offset = try await pages.info().size
            let written = try await pages.write(contentsOf: [UInt8(0x42), 0x54], toAbsoluteOffset: offset)
            try #require(written == 2)
          }
          throw DiskBTreeError.io(cause: .incompleteWrite)
        }
        await #expect {
          _ = try await store.updateValue(
            Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        } throws: {
          $0 as? DiskBTreeError == .io(cause: .incompleteWrite)
        }
      }
      let partialSize = try await Self.pagesSize(in: directory)
      let rootBeforeRetry = try await DiskBTreeFixture.root(in: directory)
      #expect(partialSize == originalSize + Int64(DiskPageFormat.pageSize) + 2)
      #expect(rootBeforeRetry == originalRoot)

      try await database.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: false)
        let previous = try await store.updateValue(
          Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        #expect(previous == nil)
      }
      let publishedSize = try await Self.pagesSize(in: directory)
      let publishedRoot = try await DiskBTreeFixture.root(in: directory)
      #expect(publishedSize == originalSize + Int64(5 * DiskPageFormat.pageSize))
      #expect(DiskPageFormat.offset(for: publishedRoot) == publishedSize - Int64(DiskPageFormat.pageSize))
      try await database.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: true)
      }
    }
  }

  @Test func aPartialWriteIntoARecycledPagePreservesEarlierWritesAndSnapshots() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("published", for: "p", sortKey: 1)
      }
      let recycledOffset = try await Self.pagesSize(in: directory)
      let expectedSize = recycledOffset + Int64(2 * DiskPageFormat.pageSize)
      let original = try await Self.pagesPrefix(length: recycledOffset, in: directory)

      try await database.withReadSnapshot { snapshot in
        try await database.withWriteTransaction { store in
          try await store.updateValue("first", for: "p", sortKey: 1)
          try await store.updateValue("second", for: "p", sortKey: 1)
          #expect(store.pageWriteCount == 2)
          store.beforeCommitStep = { step in
            guard step == .writePage(index: 0) else { return }
            try await directory.withFileHandle(
              forWritingAt: FilePath("pages"),
              options: .init(existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)
            ) { pages in
              let written = try await pages.write(contentsOf: [UInt8(0), 0], toAbsoluteOffset: recycledOffset)
              try #require(written == 2)
            }
            throw DiskBTreeError.io(cause: .incompleteWrite)
          }
          await #expect(throws: DiskBTreeError.io(cause: .incompleteWrite)) {
            _ = try await store.updateValue("failed", for: "p", sortKey: 1)
          }
          #expect(try await store.value(for: "p", sortKey: 1) == "second")
          #expect(store.pageWriteCount == 2)

          store.beforeCommitStep = nil
          let previous = try await store.updateValue("retried", for: "p", sortKey: 1)
          #expect(previous == "second")
          #expect(store.pageWriteCount == 3)
        }
        #expect(try await snapshot.value(for: "p", sortKey: 1) == "published")
      }

      #expect(try await Self.pagesSize(in: directory) == expectedSize)
      #expect(try await Self.pagesPrefix(length: recycledOffset, in: directory) == original)
      let root = try await DiskBTreeFixture.root(in: directory)
      #expect(DiskPageFormat.offset(for: root) == recycledOffset)
      let reopened = try await Store.open(in: path)
      #expect(try await reopened.value(for: "p", sortKey: 1) == "retried")
    }
  }

  @Test func finalDirectorySyncFailureRestoresThePreviousRoot() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let database = try await Store.open(in: path)
      let original = try await DiskBTreeFixture.root(in: directory)
      await #expect {
        try await database.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            if step == .syncDirectoryAfterRename { throw DiskBTreeError.io(cause: Self.injectedIO) }
          }
          try await store.updateValue(Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        }
      } throws: {
        guard case DiskBTreeError.io(let cause) = $0 else { return false }
        return cause == Self.injectedIO
      }

      let reopenedRoot = try await DiskBTreeFixture.root(in: directory)
      #expect(reopenedRoot == original)
      try await database.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: false)
        let previous = try await store.updateValue(
          Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        #expect(previous == nil)
      }
      let value = try await database.value(for: Self.partition, sortKey: Self.insertedKey)
      #expect(value == Self.value(for: Self.insertedKey))
    }
  }

  @Test func rollbackSyncFailureIsRecoveredBeforeTheNextRead() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let database = try await Store.open(in: path)
      await #expect {
        try await database.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            if step == .syncDirectoryAfterRename || step == .syncDirectoryAfterRollback {
              throw DiskBTreeError.io(cause: Self.injectedIO)
            }
          }
          try await store.updateValue(Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        }
      } throws: {
        guard case DiskBTreeError.recoveryRequired(let cause) = $0 else { return false }
        return cause == Self.injectedIO
      }
      try await directory.withFileHandle(
        forReadingAt: FilePath(DiskTreeManifest.recoveryName),
        options: .init(followSymbolicLinks: false, closeOnExec: true)
      ) { _ in }
      let reopened = try await Store.open(in: path)
      await #expect {
        try await directory.withFileHandle(
          forReadingAt: FilePath(DiskTreeManifest.recoveryName),
          options: .init(followSymbolicLinks: false, closeOnExec: true)
        ) { _ in }
      } throws: {
        ($0 as? FileSystemError)?.code == .notFound
      }
      let value = try await reopened.value(for: Self.partition, sortKey: Self.insertedKey)
      #expect(value == nil)
    }
  }

  @Test func closeFailureAfterPublicationPreservesSuccess() async throws {
    let result = try await DiskPager.finishFileScope(
      execute: { (42, true) }, close: { throw DiskBTreeError.io(cause: Self.injectedIO) })
    #expect(result.value == 42)
    #expect(result.published)
  }

  @Test func closeFailureBeforePublicationReturnsAnIOError() async {
    await #expect {
      _ = try await DiskPager.finishFileScope(
        execute: { ((), false) }, close: { throw DiskBTreeError.io(cause: Self.injectedIO) })
    } throws: {
      guard case DiskBTreeError.io(let cause) = $0 else { return false }
      return cause == Self.injectedIO
    }
  }

  @Test func closeFailurePreservesThePrimaryError() async {
    await #expect {
      _ = try await DiskPager.finishFileScope(
        execute: { () -> (Void, Bool) in throw DiskBTreeFixture.Abort.requested },
        close: { throw CancellationError() })
    } throws: {
      $0 as? DiskBTreeFixture.Abort == .requested
    }
  }

  enum Interruption: String, CaseIterable, Codable, Sendable {
    case partialBatch
    case preparedManifest
    case visibleManifest
    case confirmedManifest

    var step: DiskCommitStep? {
      switch self {
      case .partialBatch: return .writePage(index: 1)
      case .preparedManifest: return .renameManifest
      case .visibleManifest: return .syncDirectoryAfterRename
      case .confirmedManifest: return nil
      }
    }

    var publishesRoot: Bool {
      self == .visibleManifest || self == .confirmedManifest
    }
  }

  @Test(arguments: Interruption.allCases)
  func reopeningAfterAnAbruptProcessExitFindsACompleteTree(interruption: Interruption) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let original = try await DiskBTreeFixture.root(in: directory)
      let storePath = path.string
      try await #require(processExitsWith: .exitCode(42)) {
        [storePath = storePath as String, interruption = interruption as Interruption] in
        let reopened = try await Store.open(in: FilePath(storePath))
        try await reopened.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            if step == interruption.step { _exit(42) }
          }
          _ = try await store.updateValue(
            DiskBTreeCommitTests.value(for: DiskBTreeCommitTests.insertedKey),
            for: DiskBTreeCommitTests.partition, sortKey: DiskBTreeCommitTests.insertedKey)
        }
        if interruption == .confirmedManifest { _exit(42) }
      }
      let reopenedRoot = try await DiskBTreeFixture.root(in: directory)
      #expect((reopenedRoot != original) == interruption.publishesRoot)
      let reopened = try await Store.open(in: path)
      let value = try await reopened.value(for: Self.partition, sortKey: Self.insertedKey)
      #expect(value == (interruption.publishesRoot ? Self.value(for: Self.insertedKey) : nil))
      try await reopened.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: interruption.publishesRoot)
        if !interruption.publishesRoot {
          try await store.updateValue(Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        }
      }
      try await reopened.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: true)
      }
    }
  }

  @Test(arguments: [false, true])
  func cancellationHonorsThePublicationBoundary(afterRename: Bool) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(in: directory)
      let database = try await Store.open(in: path)
      let checkpoint: DiskCommitStep = afterRename ? .syncDirectoryAfterRename : .renameManifest
      let reached = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
      let resume = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
      let writer = Task {
        defer { reached.continuation.finish() }
        try await database.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            guard step == checkpoint else { return }
            reached.continuation.yield(())
            for await _ in resume.stream { break }
            if afterRename { try Task.checkCancellation() }
          }
          _ = try await store.updateValue(
            Self.value(for: Self.insertedKey), for: Self.partition, sortKey: Self.insertedKey)
        }
      }
      defer {
        writer.cancel()
        resume.continuation.finish()
      }
      var checkpoints = reached.stream.makeAsyncIterator()
      let arrived = await checkpoints.next() != nil
      try #require(arrived)
      writer.cancel()
      resume.continuation.finish()
      if afterRename {
        try await writer.value
      } else {
        await #expect {
          try await writer.value
        } throws: {
          if case DiskBTreeError.cancelled = $0 { true } else { false }
        }
      }
      try await database.withWriteTransaction { store in
        try await Self.expectContents(in: store, inserted: afterRename)
      }
    }
  }

  private static func value(for key: Int) -> String {
    String(repeating: "v", count: 1_000) + String(key)
  }

  private static func seed(in directory: DirectoryFileHandle) async throws {
    // The first branch has five full leaves, so inserting 15 splits both its leaf and its parent.
    var pages: [[UInt8]] = []
    for start in stride(from: 0, to: keys.count, by: 2) {
      let entries = try keys[start..<(start + 2)].map { sortKey in
        let key = Self.key(sortKey)
        let record = try Page.record(for: key, value: value(for: sortKey))
        return Page.Leaf.Entry(key: key, value: record)
      }
      let leaf = try #require(Page.Leaf(validating: entries))
      pages.append(try wholePage(Page.encode(leaf)))
    }
    let left = Page.Branch(level: 1, separators: [30, 50, 70, 90].map(Self.key), children: [0, 1, 2, 3, 4])
    let right = Page.Branch(level: 1, separators: [key(130)], children: [5, 6])
    let root = Page.Branch(level: 2, separators: [key(110)], children: [7, 8])
    for branch in [left, right, root] {
      pages.append(try wholePage(Page.encode(branch)))
    }
    try await DiskBTreeFixture.writeStore(
      pages: pages, root: Int64(pages.count - 1), schema: Store.persistedSchema, in: directory)
    try await directory.synchronize()
  }

  private static func key(_ sortKey: Int) -> Page.Key {
    Page.Key(partition: partition, sortKey: sortKey)
  }

  private static func wholePage(_ encoded: Page.Encoded) throws -> [UInt8] {
    try #require(encoded.split == nil)
    return Array(encoded.left.readableBytesView)
  }

  private static func expectContents(in store: borrowing Store.WriteTransaction, inserted: Bool)
    async throws(DiskBTreeError)
  {
    let expected = inserted ? (keys + [insertedKey]).sorted() : keys
    let page = try await store.scan(partition: partition, after: nil, limit: .max)
    #expect(page.entries.map(\.sortKey) == expected)
    #expect(page.entries.map(\.value) == expected.map { value(for: $0) })
    #expect(page.nextAfter == nil)
  }

  private static func pagesSize(in directory: DirectoryFileHandle) async throws -> Int64 {
    try await directory.withFileHandle(
      forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      try await file.info().size
    }
  }

  private static func pagesPrefix(length: Int64, in directory: DirectoryFileHandle) async throws -> ByteBuffer {
    try await directory.withFileHandle(
      forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      let bytes = try await file.readChunk(fromAbsoluteOffset: 0, length: .bytes(length))
      try #require(bytes.readableBytes == length)
      return bytes
    }
  }

  private static func lookupPath(to sortKey: Int, root: Int64, in directory: DirectoryFileHandle) async throws
    -> LookupPath
  {
    try await directory.withFileHandle(
      forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      let key = Self.key(sortKey)
      var id = root
      var pages: [PathPage] = []
      while true {
        let bytes = try await file.readChunk(
          fromAbsoluteOffset: DiskPageFormat.offset(for: id), length: .bytes(Int64(DiskPageFormat.pageSize)))
        try #require(bytes.readableBytes == DiskPageFormat.pageSize)
        switch try Page.decode(bytes, at: id) {
        case .branch(let branch):
          let index = branch.separators.prefix(while: { $0 <= key }).count
          let siblings = branch.children.enumerated().compactMap { $0.offset == index ? nil : $0.element }
          pages.append(PathPage(id: id, siblings: siblings))
          id = branch.children[index]
        case .leaf(let leaf):
          pages.append(PathPage(id: id, siblings: []))
          return LookupPath(pages: pages, value: try leaf.value(for: key)?.decoded(as: String.self, at: id))
        }
      }
    }
  }
}
