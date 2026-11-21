import CBOR
import Testing
import _NIOFileSystem

@testable import BTree

@Suite
struct DiskBTreeTests {
  private typealias Store = DiskBTree<String, Int, String>

  @Test(arguments: [0, -1])
  func scansRejectNonpositiveLimits(limit: Int) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withReadSnapshot { snapshot in
        _ = await #expect(throws: DiskBTreeError.invalidScanLimit(limit)) {
          _ = try await snapshot.scan(partition: "p", after: nil, limit: limit)
        }
      }
      try await database.withWriteTransaction { transaction in
        try await transaction.updateValue("first", for: "p", sortKey: 1)
        _ = await #expect(throws: DiskBTreeError.invalidScanLimit(limit)) {
          _ = try await transaction.scan(partition: "p", after: nil, limit: limit)
        }
      }
    }
  }

  @Test func reopeningReadsTheCommittedLeaf() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == nil)
        try await store.updateValue("first", for: "p", sortKey: 1)
      }

      let reopened = try await Store.open(in: path)
      try await reopened.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == "first")
        let previous = try await store.updateValue("replacement", for: "p", sortKey: 1)
        #expect(previous == "first")
        try await store.updateValue("second", for: "p", sortKey: 2)
        let removed = try await store.removeValue(for: "p", sortKey: 2)
        #expect(removed == "second")
      }

      try await reopened.withWriteTransaction { store in
        let page = try await store.scan(partition: "p", after: nil, limit: 2)
        #expect(page.entries.map(\.sortKey) == [1])
        #expect(page.entries.map(\.value) == ["replacement"])
        #expect(page.nextAfter == nil)
      }
    }
  }

  @Test func ignoresAnUnpublishedPartialPageAndCanAppendAgain() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("committed", for: "p", sortKey: 1)
      }

      try await directory.withFileHandle(
        forWritingAt: FilePath("pages"),
        options: .init(existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)
      ) { pages in
        let end = try await pages.info().size
        let written = try await pages.write(contentsOf: [UInt8(0)], toAbsoluteOffset: end)
        try #require(written == 1)
        try await pages.synchronize()
      }

      let reopened = try await Store.open(in: path)
      try await reopened.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == "committed")
        try await store.updateValue("new commit", for: "p", sortKey: 1)
      }

      try await reopened.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == "new commit")
      }
    }
  }

  @Test func fillsAPageWithTwoMaximumRecordsAndRejectsOneMoreBytePerRecord() async throws {
    let encoder = CBOREncoder()
    let keyBytes = try encoder.encode(DiskKey(partition: "p", sortKey: 1)).count
    let sample = String(repeating: "x", count: 1_900)
    let overhead = try encoder.encode(sample).count - sample.utf8.count
    let maximumValue = String(repeating: "x", count: DiskPageFormat.recordLimit - 4 - keyBytes - overhead)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        try await store.updateValue(maximumValue, for: "p", sortKey: 1)
        try await store.updateValue(maximumValue, for: "p", sortKey: 2)
        await #expect {
          try await store.updateValue(maximumValue + "x", for: "p", sortKey: 1)
        } throws: {
          guard case DiskBTreeError.recordTooLarge(let count, let limit) = $0 else { return false }
          return limit == DiskPageFormat.recordLimit && count == limit + 1
        }
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == maximumValue)
      }

      try await database.withWriteTransaction { store in
        let page = try await store.scan(partition: "p", after: nil, limit: 2)
        #expect(page.entries.map(\.value) == [maximumValue, maximumValue])
        #expect(page.nextAfter == nil)
      }
      let fileSize = try await directory.withFileHandle(
        forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
      ) { file in
        try await file.info().size
      }
      #expect(fileSize == 2 * 4_096)
    }
  }

  @Test(arguments: ["p", "é", "💾", "👩‍💻"])
  func boundsTheEncodedCompositeKey(text: String) async throws {
    let sample = String(repeating: "p", count: 960)
    let encoder = CBOREncoder()
    let overhead = try encoder.encode(DiskKey(partition: sample, sortKey: 1)).count - sample.utf8.count
    let capacity = DiskPageFormat.keyLimit - overhead
    let (copies, remaining) = capacity.quotientAndRemainder(dividingBy: text.utf8.count)
    let maximumPartition = String(repeating: text, count: copies) + String(repeating: "p", count: remaining)
    let maximumKey = try encoder.encode(DiskKey(partition: maximumPartition, sortKey: 1))
    #expect(maximumKey.count == DiskPageFormat.keyLimit)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        try await store.updateValue("valid", for: maximumPartition, sortKey: 1)
        await #expect {
          try await store.updateValue("too long", for: maximumPartition + "p", sortKey: 1)
        } throws: {
          if case DiskBTreeError.keyTooLarge(encodedBytes: 1_025, maximumBytes: 1_024) = $0 { true } else { false }
        }
      }
      try await database.withWriteTransaction { store in
        let previous = try await store.value(for: maximumPartition, sortKey: 1)
        let oversized = try await store.value(for: maximumPartition + "p", sortKey: 1)
        #expect(previous == "valid")
        #expect(oversized == nil)
      }
    }
  }

  @Test func replacingAMiddleRecordCanSplitTheLeafByBytes() async throws {
    let large = String(repeating: "x", count: 1_900)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        try await store.updateValue(large, for: "p", sortKey: 1)
        try await store.updateValue("middle", for: "p", sortKey: 2)
        try await store.updateValue(large, for: "p", sortKey: 3)
        let previous = try await store.updateValue(large, for: "p", sortKey: 2)
        #expect(previous == "middle")
      }
      try await database.withWriteTransaction { store in
        let page = try await store.scan(partition: "p", after: nil, limit: 3)
        #expect(page.entries.map(\.sortKey) == [1, 2, 3])
        #expect(page.entries.map(\.value) == [large, large, large])
        #expect(page.nextAfter == nil)
      }
    }
  }

  @Test func fileListingReopensAfterLeafAndBranchSplits() async throws {
    let archive = String(repeating: "a", count: 960)
    let inbox = String(repeating: "i", count: 960)
    let sent = String(repeating: "s", count: 960)
    let payload = String(repeating: "v", count: 1_000)
    let keys = [80, 30, 110, 10, 60, 120, 40, 90, 20, 100, 50, 70]
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      // These records fit two per leaf, and their separators fit four per branch.
      let nextAfter = try await database.withWriteTransaction { store in
        for key in keys {
          let previous = try await store.updateValue(payload + String(key), for: inbox, sortKey: key)
          #expect(previous == nil)
        }
        try await store.updateValue(payload + "archive", for: archive, sortKey: 10)
        try await store.updateValue(payload + "sent", for: sent, sortKey: 10)
        let page = try await store.scan(partition: inbox, after: nil, limit: 5)
        #expect(page.entries.map(\.sortKey) == [10, 20, 30, 40, 50])
        return page.nextAfter
      }
      let cursor = try #require(nextAfter)
      #expect(cursor == 50)

      let reopened = try await Store.open(in: path)
      try await reopened.withWriteTransaction { store in
        for key in keys {
          let value = try await store.value(for: inbox, sortKey: key)
          #expect(value == payload + String(key))
        }
        let previous = try await store.removeValue(for: inbox, sortKey: cursor)
        #expect(previous == payload + "50")
        try await store.updateValue(payload + "45", for: inbox, sortKey: 45)
        try await store.updateValue(payload + "55", for: inbox, sortKey: 55)
      }

      try await reopened.withWriteTransaction { store in
        let next = try await store.scan(partition: inbox, after: cursor, limit: 5)
        #expect(next.entries.map(\.sortKey) == [55, 60, 70, 80, 90])
        #expect(next.nextAfter == 90)
        let last = try await store.scan(partition: inbox, after: next.nextAfter, limit: 5)
        #expect(last.entries.map(\.sortKey) == [100, 110, 120])
        #expect(last.nextAfter == nil)
        let archiveValue = try await store.value(for: archive, sortKey: 10)
        let sentValue = try await store.value(for: sent, sortKey: 10)
        #expect(archiveValue == payload + "archive")
        #expect(sentValue == payload + "sent")
      }
    }
  }

  @Test func mixedWritesAndReopensMatchADictionary() async throws {
    let partitions = ["a", "b", "c"].map { String(repeating: $0, count: 900) }
    let large = String(repeating: "x", count: 1_020)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      var model: [String: [Int: String]] = [:]
      for round in 0...3 {
        let committed = model
        model = try await database.withWriteTransaction { store in
          for partition in partitions {
            let expected = (committed[partition] ?? [:]).sorted { $0.key < $1.key }
            let page = try await store.scan(partition: partition, after: nil, limit: .max)
            #expect(page.entries.map(\.sortKey) == expected.map(\.key))
            #expect(page.entries.map(\.value) == expected.map(\.value))
            #expect(page.nextAfter == nil)
          }
          if round == 3 { return committed }

          var working = committed
          for index in 0..<42 {
            let id = (index * 17 + round * 5) % 42
            let partition = partitions[id % partitions.count]
            let key = id / partitions.count
            if round > 0, (id + round).isMultiple(of: 4) {
              let previous = try await store.removeValue(for: partition, sortKey: key)
              let expected = working[partition, default: [:]].removeValue(forKey: key)
              #expect(previous == expected)
            } else {
              let value = (id + round).isMultiple(of: 3) ? large + String(id) : "\(round)/\(id)"
              let previous = try await store.updateValue(value, for: partition, sortKey: key)
              let expected = working[partition, default: [:]].updateValue(value, forKey: key)
              #expect(previous == expected)
            }
            if index.isMultiple(of: 11) {
              let cursor = key - 1
              let ordered = working[partition, default: [:]].filter { $0.key > cursor }.sorted { $0.key < $1.key }
              let expected = ordered.prefix(4)
              let page = try await store.scan(partition: partition, after: cursor, limit: 4)
              #expect(page.entries.map(\.sortKey) == expected.map(\.key))
              #expect(page.entries.map(\.value) == expected.map(\.value))
              #expect(page.nextAfter == (ordered.count > 4 ? expected.last?.key : nil))
            }
          }
          return working
        }
      }
    }
  }

  @Test func storedNilSurvivesAReopenAndCanBeRemoved() async throws {
    typealias OptionalStore = DiskBTree<String, Int, String?>
    let payload = String(repeating: "v", count: 1_900)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await OptionalStore.create(in: path)
      try await database.withWriteTransaction { store in
        try await store.updateValue(nil, for: "p", sortKey: 0)
        for key in 1...3 {
          try await store.updateValue(payload, for: "p", sortKey: key)
        }
      }
      let reopened = try await OptionalStore.open(in: path)
      try await reopened.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 0)
        #expect(value == .some(nil))
        let previous = try await store.removeValue(for: "p", sortKey: 0)
        #expect(previous == .some(nil))
      }
      try await reopened.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 0)
        #expect(value == nil)
      }
    }
  }

  @Test func rejectsASecondOpenWhileTheStoreIsInUse() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { _ in
        _ = await #expect {
          try await database.withWriteTransaction { _ in }
        } throws: {
          if case DiskBTreeError.storeInUse = $0 { true } else { false }
        }
      }
    }
  }

  @Test(arguments: [Int64(-1), Int64.max])
  func rejectsOutOfRangeManifestPageIDs(pageID: Int64) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await DiskBTreeFixture.writeManifest(
        DiskTreeManifest(
          rootPageID: pageID, pageCount: 1, schema: DiskBTreeFixture.schema),
        named: "manifest", in: directory)

      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidManifest
      }
    }
  }

  @Test func writesStayInTheOpenedDirectoryAfterItMoves() async throws {
    let fileSystem = FileSystem.shared
    try await fileSystem.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, parent in
      let original = parent.appending("store")
      let relocated = parent.appending("relocated")
      try await fileSystem.createDirectory(
        at: original, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      let database = try await Store.create(in: original)

      try await database.withWriteTransaction { store in
        try await store.updateValue("before move", for: "p", sortKey: 1)
        try await fileSystem.moveItem(at: original, to: relocated)
        try await fileSystem.createDirectory(
          at: original, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
        try await store.updateValue("after move", for: "p", sortKey: 1)
      }

      let reopened = try await Store.open(in: relocated)
      let value = try await reopened.value(for: "p", sortKey: 1)
      #expect(value == "after move")
      await #expect {
        _ = try await database.value(for: "p", sortKey: 1)
      } throws: {
        if case DiskBTreeError.storeNotFound = $0 { true } else { false }
      }
    }
  }

  @Test func failedManifestWriteLeavesThePreviousCommitReadable() async throws {
    let fileSystem = FileSystem.shared
    try await fileSystem.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      let pendingManifest = path.appending("manifest.next")
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("committed", for: "p", sortKey: 1)
      }
      try await fileSystem.createDirectory(
        at: pendingManifest, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      await #expect {
        try await database.withWriteTransaction { store in
          _ = try await store.updateValue("unpublished", for: "p", sortKey: 1)
        }
      } throws: {
        if case DiskBTreeError.io = $0 { true } else { false }
      }
      let value = try await database.value(for: "p", sortKey: 1)
      #expect(value == "committed")

      _ = try await fileSystem.removeItem(at: pendingManifest, strategy: .platformDefault, recursively: false)
      try await database.withWriteTransaction { store in
        let previous = try await store.updateValue("retry", for: "p", sortKey: 1)
        #expect(previous == "committed")
      }
      try await database.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == "retry")
      }
    }
  }
}
