import NIOCore
import Testing
import _NIOFileSystem

@testable import BTree

@Suite(.timeLimit(.minutes(1)))
struct DiskBTreeStressTests {
  private typealias Store = DiskBTree<String, Int, String>
  private typealias Page = DiskPage<String, Int>

  private static let partitions = [128, 360, 712, 896].enumerated().map {
    String($0.offset) + String(repeating: "p", count: $0.element)
  }

  private struct Mutation: Sendable {
    let partition: String
    let key: Int
    let value: String?
    let previous: String?
  }

  private struct SeededGenerator: RandomNumberGenerator {
    var state: UInt64

    mutating func next() -> UInt64 {
      state &+= 0x9e37_79b9_7f4a_7c15
      var result = state
      result = (result ^ (result >> 30)) &* 0xbf58_476d_1ce4_e5b9
      result = (result ^ (result >> 27)) &* 0x94d0_49bb_1331_11eb
      return result ^ (result >> 31)
    }
  }

  @Test(arguments: [UInt64(74), 0x1234_5678, 0xcafe_f00d])
  func mixedWritesMatchTheModelAndKeepValidPageReferences(seed: UInt64) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      let batchCount = 200
      let batchSize = 25
      let valueLengths = [40, 180, 640, 1_010]
      var random = SeededGenerator(state: seed)
      var model = [[Int: String]](repeating: [:], count: Self.partitions.count)

      for batch in 0..<batchCount {
        var nextModel = model
        let mutations = (0..<batchSize).map { offset in
          let partition = Int.random(in: Self.partitions.indices, using: &random)
          let key = Int.random(in: 0..<48, using: &random)
          let value: String?
          let previous: String?
          if Int.random(in: 0..<5, using: &random) < 2 {
            value = nil
            previous = nextModel[partition].removeValue(forKey: key)
          } else {
            let length = Int.random(in: valueLengths.indices, using: &random)
            let payload = String(repeating: "v", count: valueLengths[length]) + String(batch * batchSize + offset)
            value = payload
            previous = nextModel[partition].updateValue(payload, forKey: key)
          }
          return Mutation(partition: Self.partitions[partition], key: key, value: value, previous: previous)
        }
        let abort = batch > 0 && batch.isMultiple(of: 13)
        let committed: Bool
        do {
          try await database.withWriteTransaction { store in
            for (offset, mutation) in mutations.enumerated() {
              let previous: String?
              if let value = mutation.value {
                previous = try await store.updateValue(value, for: mutation.partition, sortKey: mutation.key)
              } else {
                previous = try await store.removeValue(for: mutation.partition, sortKey: mutation.key)
              }
              guard previous == mutation.previous else {
                Issue.record("Unexpected previous value for seed \(seed), batch \(batch), operation \(offset)")
                throw DiskBTreeFixture.Abort.requested
              }
            }
            if abort { throw DiskBTreeFixture.Abort.requested }
          }
          committed = true
        } catch DiskBTreeFixture.Abort.requested {
          committed = false
        }
        try #require(committed == !abort)
        if committed { model = nextModel }

        if abort || batch.isMultiple(of: 16) || batch == batchCount - 1 {
          try await Self.checkContents(in: database, against: model)
        }
        if batch.isMultiple(of: 32) || batch == batchCount - 1 {
          try await Self.checkTree(in: directory)
        }
      }
    }
  }

  @Test func committedUpdatesAppendOnePageToASingleLeafStore() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("0", for: "p", sortKey: 0)
      }

      for version in 1...16 {
        try await database.withWriteTransaction { store in
          let previous = try await store.updateValue(version.description, for: "p", sortKey: 0)
          #expect(previous == (version - 1).description)
        }
        let value = try await database.value(for: "p", sortKey: 0)
        #expect(value == version.description)
        let size = try await Self.pagesSize(in: directory)
        #expect(size == Int64((version + 1) * DiskPageFormat.pageSize))
      }

      let published = try await database.value(for: "p", sortKey: 0)
      #expect(published == "16")
    }
  }

  @Test func bulkInsertReusesPagesBetweenMutations() async throws {
    typealias IntegerStore = DiskBTree<Int, Int, Int>
    let keys = Array(0..<1_000)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await IntegerStore.create(in: path)
      try await database.withWriteTransaction { store in
        for key in keys { try await store.updateValue(key, for: 0, sortKey: key) }
      }

      let manifest = try await DiskTreeManifest.read(in: directory)
      let pageCount = try #require(manifest?.pageCount)
      // The final tree has 15 pages and a mutation can replace two of them at once.
      #expect(pageCount <= 17)
      #expect(try await Self.pagesSize(in: directory) == pageCount * Int64(DiskPageFormat.pageSize))

      let reopened = try await IntegerStore.open(in: path)
      try await reopened.withReadSnapshot { snapshot in
        let page = try await snapshot.scan(partition: 0, after: nil, limit: keys.count)
        #expect(page.entries.map(\.sortKey) == keys)
        #expect(page.entries.map(\.value) == keys)
        #expect(page.nextAfter == nil)
      }
    }
  }

  @Test func rolledBackTransactionsOverwriteOnlyTheUnpublishedTail() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("published", for: "p", sortKey: 0)
      }
      let root = try await DiskBTreeFixture.root(in: directory)

      for _ in 0..<4 {
        await #expect {
          try await database.withWriteTransaction { store in
            for version in 1...32 {
              try await store.updateValue(version.description, for: "p", sortKey: 0)
            }
            let working = try await store.value(for: "p", sortKey: 0)
            #expect(working == "32")
            throw DiskBTreeFixture.Abort.requested
          }
        } throws: {
          $0 as? DiskBTreeFixture.Abort == .requested
        }
        let stable = try await database.value(for: "p", sortKey: 0)
        #expect(stable == "published")
        let size = try await Self.pagesSize(in: directory)
        #expect(size == Int64(3 * DiskPageFormat.pageSize))
        let published = try await DiskTreeManifest.read(in: directory)
        #expect(published?.pageCount == 1)
        #expect(published?.rootPageID == root)
      }
    }
  }

  private static func checkContents(in database: Store, against model: [[Int: String]]) async throws {
    let limit = 7
    try await database.withReadSnapshot { snapshot in
      for (index, partition) in partitions.enumerated() {
        let expected = model[index].sorted { $0.key < $1.key }
        var cursor: Int?
        for offset in stride(from: 0, to: max(1, expected.count), by: limit) {
          let window = expected.dropFirst(offset).prefix(limit)
          let page = try await snapshot.scan(partition: partition, after: cursor, limit: limit)
          try #require(page.entries.map(\.sortKey) == window.map(\.key))
          try #require(page.entries.map(\.value) == window.map(\.value))
          let next = offset + limit < expected.count ? window.last?.key : nil
          try #require(page.nextAfter == next)
          cursor = page.nextAfter
        }
      }
    }
  }

  private static func checkTree(in directory: DirectoryFileHandle) async throws {
    let published = try await DiskTreeManifest.read(in: directory)
    let manifest = try #require(published)
    try await directory.withFileHandle(
      forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      var pending: [(id: Int64, level: UInt8?)] = [(try #require(manifest.rootPageID), nil)]
      var visited = Set<Int64>()
      while let node = pending.popLast() {
        try #require(DiskPageFormat.validPageIDs.contains(node.id) && node.id < manifest.pageCount)
        let inserted = visited.insert(node.id).inserted
        try #require(inserted, "Page \(node.id) was referenced more than once")
        let bytes = try await file.readChunk(
          fromAbsoluteOffset: DiskPageFormat.offset(for: node.id), length: .bytes(Int64(DiskPageFormat.pageSize)))
        try #require(bytes.readableBytes == DiskPageFormat.pageSize)
        let page = try Page.decode(bytes, at: node.id)
        if let level = node.level { try #require(page.level == level) }
        if case .branch(let branch) = page {
          pending.append(contentsOf: branch.children.map { ($0, branch.level - 1) })
        }
      }
    }
  }

  private static func pagesSize(in directory: DirectoryFileHandle) async throws -> Int64 {
    try await directory.withFileHandle(
      forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      try await file.info().size
    }
  }
}
