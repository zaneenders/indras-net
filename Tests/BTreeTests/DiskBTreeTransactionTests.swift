import Testing
import _NIOFileSystem

@testable import BTree

@Suite(.timeLimit(.minutes(1)))
struct DiskBTreeTransactionTests {
  private typealias Store = DiskBTree<String, Int, String>

  private static let writeFailure = DiskBTreeError.IOFailure.fileSystem(message: "Injected page write failure")
  private static let publicationFailure = DiskBTreeError.IOFailure.fileSystem(message: "Injected publication failure")

  private actor CommitTrace {
    private(set) var steps: [DiskCommitStep] = []

    func record(_ step: DiskCommitStep) {
      steps.append(step)
    }
  }

  @Test func publishesABatchOnceAndReadsItsOwnWrites() async throws {
    let payload = String(repeating: "v", count: 1_900)
    let trace = CommitTrace()
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      let missing = try await database.value(for: "p", sortKey: 30)
      #expect(missing == nil)
      let result = try await database.withWriteTransaction { store in
        store.beforeCommitStep = { await trace.record($0) }
        for key in [30, 10, 50, 20, 40, 60] {
          let previous = try await store.updateValue(payload + String(key), for: "p", sortKey: key)
          #expect(previous == nil)
        }
        let previous = try await store.updateValue("replacement", for: "p", sortKey: 30)
        #expect(previous == payload + "30")
        let removed = try await store.removeValue(for: "p", sortKey: 20)
        #expect(removed == payload + "20")
        let updated = try await store.value(for: "p", sortKey: 30)
        let missing = try await store.value(for: "p", sortKey: 20)
        #expect(updated == "replacement")
        #expect(missing == nil)
        let page = try await store.scan(partition: "p", after: nil, limit: .max)
        #expect(page.entries.map(\.sortKey) == [10, 30, 40, 50, 60])
        #expect(
          page.entries.map(\.value) == [payload + "10", "replacement", payload + "40", payload + "50", payload + "60"]
        )

        let committed = try await database.withReadSnapshot { snapshot in
          try await snapshot.scan(partition: "p", after: nil, limit: .max)
        }
        #expect(committed.entries.isEmpty)
        #expect(committed.nextAfter == nil)
        let steps = await trace.steps
        #expect(!steps.contains(.syncPages))
        #expect(!steps.contains(.renameManifest))
        return page.entries.count
      }
      #expect(result == 5)

      let steps = await trace.steps
      #expect(steps.filter { $0 == .syncPages }.count == 1)
      #expect(steps.filter { $0 == .writeManifest }.count == 1)
      #expect(steps.filter { $0 == .renameManifest }.count == 1)
      #expect(steps.last == .syncDirectoryAfterRename)

      let page = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: .max)
      }
      #expect(page.entries.map(\.sortKey) == [10, 30, 40, 50, 60])
      #expect(
        page.entries.map(\.value) == [payload + "10", "replacement", payload + "40", payload + "50", payload + "60"])
      #expect(page.nextAfter == nil)
    }
  }

  @Test func abortLeavesTheCommittedRootVisibleAndReleasesTheWriter() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await Self.seed(database)
      let original = try await DiskBTreeFixture.root(in: directory)
      await #expect {
        try await database.withWriteTransaction { store in
          _ = try await store.removeValue(for: "p", sortKey: 1)
          _ = try await store.updateValue("unpublished", for: "p", sortKey: 3)
          throw DiskBTreeFixture.Abort.requested
        }
      } throws: {
        $0 as? DiskBTreeFixture.Abort == .requested
      }
      let afterAbort = try await DiskBTreeFixture.root(in: directory)
      #expect(afterAbort == original)
      try await database.withWriteTransaction { store in
        let page = try await store.scan(partition: "p", after: nil, limit: .max)
        #expect(page.entries.map(\.value) == ["one", "two"])
        let previous = try await store.updateValue("published", for: "p", sortKey: 3)
        #expect(previous == nil)
      }
      let page = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: .max)
      }
      #expect(page.entries.map(\.value) == ["one", "two", "published"])
    }
  }

  @Test func snapshotPropagatesCallerErrors() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      await #expect(throws: DiskBTreeFixture.Abort.requested) {
        try await database.withReadSnapshot { _ in
          throw DiskBTreeFixture.Abort.requested
        }
      }
    }
  }

  @Test func aCaughtMutationFailurePreservesEarlierWritesInTheBatch() async throws {
    let value = String(repeating: "v", count: 1_900)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        try await store.updateValue(value, for: "p", sortKey: 1)
        try await store.updateValue(value, for: "p", sortKey: 2)
        store.beforeCommitStep = { step in
          if step == .writePage(index: 1) { throw DiskBTreeError.io(cause: Self.writeFailure) }
        }
        await #expect {
          try await store.updateValue(value, for: "p", sortKey: 3)
        } throws: {
          $0 as? DiskBTreeError == .io(cause: Self.writeFailure)
        }
        store.beforeCommitStep = nil
        let working = try await store.scan(partition: "p", after: nil, limit: .max)
        #expect(working.entries.map(\.sortKey) == [1, 2])
        let previous = try await store.updateValue("four", for: "p", sortKey: 4)
        #expect(previous == nil)
      }
      let committed = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: .max)
      }
      #expect(committed.entries.map(\.sortKey) == [1, 2, 4])
      #expect(committed.entries.map(\.value) == [value, value, "four"])
    }
  }

  @Test func aNewSnapshotUsesTheLatestCommit() async throws {
    let payload = String(repeating: "r", count: 1_850)
    let original = Array(stride(from: 10, through: 80, by: 10))
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { writer in
        for key in original {
          try await writer.updateValue(payload + String(key), for: "p", sortKey: key)
        }
      }
      let first = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: 3)
      }
      #expect(first.entries.map(\.sortKey) == [10, 20, 30])
      let cursor = try #require(first.nextAfter)
      try await database.withWriteTransaction { writer in
        try await writer.removeValue(for: "p", sortKey: 30)
        try await writer.removeValue(for: "p", sortKey: 40)
        try await writer.updateValue(payload + "25", for: "p", sortKey: 25)
        try await writer.updateValue(payload + "35", for: "p", sortKey: 35)
        try await writer.updateValue("replacement", for: "p", sortKey: 50)
      }

      try await database.withReadSnapshot { snapshot in
        let second = try await snapshot.scan(partition: "p", after: cursor, limit: 3)
        #expect(second.entries.map(\.sortKey) == [35, 50, 60])
        #expect(second.entries.map(\.value) == [payload + "35", "replacement", payload + "60"])
        let last = try await snapshot.scan(partition: "p", after: second.nextAfter, limit: 3)
        #expect(last.entries.map(\.sortKey) == [70, 80])
        #expect(last.nextAfter == nil)
      }
      let updated = try await database.value(for: "p", sortKey: 50)
      #expect(updated == "replacement")
    }
  }

  @Test(arguments: [false, true])
  func freshReadersWaitForPublicationOrRollback(rollsBack: Bool) async throws {
    // At most two records fit per leaf, so the first and last keys occupy different leaves.
    let payload = String(repeating: "v", count: 1_900)
    let keys = [10, 20, 30, 40]
    let original = keys.map { payload + "old\($0)" }
    let updated = [payload + "new10", original[1], original[2], payload + "new40"]

    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { writer in
        for (key, value) in zip(keys, original) {
          try await writer.updateValue(value, for: "p", sortKey: key)
        }
      }

      let reached = AsyncStream.makeStream(of: DiskCommitStep.self, bufferingPolicy: .bufferingNewest(1))
      let beforeManifestSync = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
      let afterRename = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
      try await database.withReadSnapshot { snapshot in
        try await withThrowingTaskGroup(of: Bool.self) { group in
          defer {
            beforeManifestSync.continuation.finish()
            afterRename.continuation.finish()
          }
          group.addTask {
            defer { reached.continuation.finish() }
            do {
              try await database.withWriteTransaction { writer in
                writer.beforeCommitStep = { step in
                  let resume: AsyncStream<Void>
                  switch step {
                  case .syncManifest: resume = beforeManifestSync.stream
                  case .syncDirectoryAfterRename: resume = afterRename.stream
                  default: return
                  }
                  reached.continuation.yield(step)
                  var iterator = resume.makeAsyncIterator()
                  _ = await iterator.next()
                  if rollsBack, step == .syncDirectoryAfterRename {
                    throw DiskBTreeError.io(cause: Self.publicationFailure)
                  }
                }
                try await writer.updateValue(updated[0], for: "p", sortKey: 10)
                try await writer.updateValue(updated[3], for: "p", sortKey: 40)
              }
              #expect(!rollsBack)
            } catch DiskBTreeError.io(let cause) where cause == Self.publicationFailure {
              #expect(rollsBack)
            }
            return false
          }

          var checkpoints = reached.stream.makeAsyncIterator()
          let before = await checkpoints.next()
          try #require(before == .syncManifest)
          let oldPage = try await database.withReadSnapshot { snapshot in
            try await snapshot.scan(partition: "p", after: nil, limit: .max)
          }
          #expect(oldPage.entries.map(\.sortKey) == keys)
          #expect(oldPage.entries.map(\.value) == original)
          beforeManifestSync.continuation.finish()

          let after = await checkpoints.next()
          try #require(after == .syncDirectoryAfterRename)
          let pinned = try await snapshot.scan(partition: "p", after: nil, limit: .max)
          #expect(pinned.entries.map(\.value) == original)
          group.addTask {
            let page = try await database.withReadSnapshot { snapshot in
              try await snapshot.scan(partition: "p", after: nil, limit: .max)
            }
            #expect(page.entries.map(\.sortKey) == keys)
            #expect(page.entries.map(\.value) == (rollsBack ? original : updated))
            return false
          }
          group.addTask {
            try await Task.sleep(for: .milliseconds(50))
            return true
          }
          let timerReturnedFirst = try await group.next()
          #expect(timerReturnedFirst == true)
          afterRename.continuation.finish()
          try await group.waitForAll()
        }
      }

      let committedPage = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: .max)
      }
      #expect(committedPage.entries.map(\.sortKey) == keys)
      #expect(committedPage.entries.map(\.value) == (rollsBack ? original : updated))
    }
  }

  @Test func scopedPaginationKeepsItsSnapshotAcrossACommit() async throws {
    let payload = String(repeating: "r", count: 1_850)
    let original = [10, 20, 30, 40]
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { writer in
        for key in original {
          try await writer.updateValue(payload + String(key), for: "p", sortKey: key)
        }
      }

      try await database.withReadSnapshot { reader in
        let first = try await reader.scan(partition: "p", after: nil, limit: 2)
        #expect(first.entries.map(\.sortKey) == [10, 20])
        let cursor = try #require(first.nextAfter)

        try await database.withWriteTransaction { writer in
          try await writer.updateValue(payload + "15", for: "p", sortKey: 15)
          try await writer.removeValue(for: "p", sortKey: 30)
        }

        let fresh = try await database.withReadSnapshot { snapshot in
          try await snapshot.scan(partition: "p", after: nil, limit: .max)
        }
        let updated = [10, 15, 20, 40]
        #expect(fresh.entries.map(\.sortKey) == updated)
        #expect(fresh.entries.map(\.value) == updated.map { payload + String($0) })

        let last = try await reader.scan(partition: "p", after: cursor, limit: 2)
        let snapshot = first.entries + last.entries
        #expect(snapshot.map(\.sortKey) == original)
        #expect(snapshot.map(\.value) == original.map { payload + String($0) })
        #expect(last.nextAfter == nil)
      }
    }
  }

  @Test(arguments: [false, true])
  func publicationFailuresPreserveTheOldRoot(afterRename: Bool) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await Self.seed(database)
      let original = try await DiskBTreeFixture.root(in: directory)
      let checkpoint: DiskCommitStep = afterRename ? .syncDirectoryAfterRename : .renameManifest
      do {
        try await database.withWriteTransaction { writer in
          writer.beforeCommitStep = { step in
            if step == checkpoint { throw DiskBTreeError.io(cause: Self.publicationFailure) }
          }
          try await writer.updateValue("replacement", for: "p", sortKey: 1)
          try await writer.updateValue("three", for: "p", sortKey: 3)
        }
        Issue.record("Expected the publication checkpoint to fail")
      } catch DiskBTreeError.io(let cause) where cause == Self.publicationFailure {
      }

      let recovered = try await DiskBTreeFixture.root(in: directory)
      #expect(recovered == original)
      let page = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: .max)
      }
      #expect(page.entries.map(\.value) == ["one", "two"])
    }
  }

  @Test func aReadOnlyWriteTransactionSkipsPublication() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await Self.seed(database)
      let original = try await DiskBTreeFixture.root(in: directory)
      let value = try await database.withWriteTransaction { writer in
        writer.beforeCommitStep = { _ in
          Issue.record("A read-only transaction attempted publication")
          throw DiskBTreeError.io(cause: Self.publicationFailure)
        }
        let missing = try await writer.removeValue(for: "p", sortKey: 99)
        #expect(missing == nil)
        return try await writer.value(for: "p", sortKey: 1)
      }
      #expect(value == "one")
      let current = try await DiskBTreeFixture.root(in: directory)
      #expect(current == original)
    }
  }

  @Test func cancellationBeforeTheBodyReturnsAbortsTheBatch() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let database = try await Store.create(in: path)
      try await Self.seed(database)
      let staged = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
      let resume = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
      let writer = Task {
        defer { staged.continuation.finish() }
        try await database.withWriteTransaction { store in
          try await store.updateValue("unpublished", for: "p", sortKey: 3)
          staged.continuation.yield(())
          for await _ in resume.stream { break }
        }
      }
      defer {
        writer.cancel()
        resume.continuation.finish()
      }
      var ready = staged.stream.makeAsyncIterator()
      let hasStaged = await ready.next() != nil
      try #require(hasStaged)
      writer.cancel()
      resume.continuation.finish()
      await #expect {
        try await writer.value
      } throws: {
        if case DiskBTreeError.cancelled = $0 { true } else { false }
      }
      let page = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: .max)
      }
      #expect(page.entries.map(\.value) == ["one", "two"])
    }
  }

  private static func seed(_ database: Store) async throws {
    try await database.withWriteTransaction { writer in
      try await writer.updateValue("one", for: "p", sortKey: 1)
      try await writer.updateValue("two", for: "p", sortKey: 2)
    }
  }
}
