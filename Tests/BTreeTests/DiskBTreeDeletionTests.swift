import NIOCore
import Testing
import _NIOFileSystem

@testable import BTree

@Suite(.timeLimit(.minutes(1)))
struct DiskBTreeDeletionTests {
  private typealias Store = DiskBTree<String, Int, String>
  private typealias Page = DiskPage<String, Int>

  private struct Shape: Sendable {
    var rootLevel: UInt8
    var leafDepths: Set<Int>
    var leafKeys: [[Int]]
    var fanouts: [Int: [Int]]
  }

  private struct Child {
    let firstKey: Page.Key
    let id: Int64
  }

  enum DeletionOrder: CaseIterable, Sendable {
    case ascending
    case descending
    case interleaved

    func order(_ keys: [Int]) -> [Int] {
      switch self {
      case .ascending: return keys
      case .descending: return Array(keys.reversed())
      case .interleaved: return keys.indices.map { keys[$0 * 17 % keys.count] }
      }
    }
  }

  @Test(arguments: DeletionOrder.allCases)
  func deletesFromSeveralLevelsBackDownToOneLeaf(order: DeletionOrder) async throws {
    let partition = String(repeating: "p", count: 960)
    let keys = Array(0..<32)
    let payload = String(repeating: "v", count: 1_000)
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        for key in DeletionOrder.interleaved.order(keys) {
          try await store.updateValue(payload + String(key), for: partition, sortKey: key)
        }
      }
      let initial = try await Self.shape(in: directory)
      #expect(initial.rootLevel >= 2)
      var remaining = Set(keys)
      var rootLevel = initial.rootLevel

      for key in order.order(keys) {
        remaining.remove(key)
        let expected = remaining.sorted()
        try await database.withWriteTransaction { store in
          let removed = try await store.removeValue(for: partition, sortKey: key)
          #expect(removed == payload + String(key))
          let missing = try await store.value(for: partition, sortKey: key)
          #expect(missing == nil)
        }
        try await database.withWriteTransaction { store in
          let page = try await store.scan(partition: partition, after: nil, limit: .max)
          #expect(page.entries.map(\.sortKey) == expected)
          #expect(page.entries.map(\.value) == expected.map { payload + String($0) })
          #expect(page.nextAfter == nil)
          for survivor in expected {
            let value = try await store.value(for: partition, sortKey: survivor)
            #expect(value == payload + String(survivor))
          }
        }
        let current = try await Self.shape(in: directory)
        #expect(current.leafKeys.flatMap { $0 } == expected)
        #expect(current.leafDepths == [Int(current.rootLevel)])
        #expect(current.rootLevel <= rootLevel)
        if expected.count <= 1 {
          #expect(current.rootLevel == 0)
          #expect(current.leafKeys == [expected])
        }
        rootLevel = current.rootLevel
      }
    }
  }

  @Test(arguments: [false, true])
  func redistributesLeafRecordsByBytes(donorOnLeft: Bool) async throws {
    let lengths = donorOnLeft ? [1_400, 1_400, 1_060, 1_060, 1_360] : [1_360, 1_060, 1_060, 1_400, 1_400]
    let keys = [10, 20, 30, 40, 50]
    let values = Dictionary(uniqueKeysWithValues: zip(keys, lengths.map { String(repeating: "v", count: $0) }))
    let leaves = donorOnLeft ? [[10, 20, 30], [40, 50]] : [[10, 20], [30, 40, 50]]
    let deleted = donorOnLeft ? 50 : 10
    let expected = donorOnLeft ? [[10, 20], [30, 40]] : [[20, 30], [40, 50]]
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(
        leaves: leaves, branchFanouts: [], values: values, partitionForKey: { _ in "p" }, in: directory)
      let database = try await Store.open(in: path)
      try await database.withWriteTransaction { store in
        let removed = try await store.removeValue(for: "p", sortKey: deleted)
        #expect(removed == values[deleted])
      }
      let current = try await Self.shape(in: directory)
      #expect(current.leafKeys == expected)
      try await database.withWriteTransaction { store in
        for key in expected.flatMap({ $0 }) {
          let value = try await store.value(for: "p", sortKey: key)
          #expect(value == values[key])
        }
      }
    }
  }

  @Test func mergesWithTheRightSiblingWhenTheLeftWouldRequireRedistribution() async throws {
    let values = [
      10: String(repeating: "l", count: 1_900), 20: String(repeating: "l", count: 1_900),
      30: String(repeating: "m", count: 1_400), 40: String(repeating: "m", count: 1_000),
      50: String(repeating: "r", count: 900)
    ]
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(
        leaves: [[10, 20], [30, 40], [50]], branchFanouts: [], values: values, partitionForKey: { _ in "p" },
        in: directory)
      let database = try await Store.open(in: path)
      try await database.withWriteTransaction { store in
        let removed = try await store.removeValue(for: "p", sortKey: 30)
        #expect(removed == values[30])
      }
      let current = try await Self.shape(in: directory)
      #expect(current.leafKeys == [[10, 20], [40, 50]])
    }
  }

  @Test func redistributesBranchChildrenWhenTheirSeparatorsWouldOverflowAMerge() async throws {
    let partition = String(repeating: "p", count: 960)
    let leaves = [[10, 20], [30, 40], [50, 60], [70, 80], [90, 100], [110, 120], [130, 140]]
    let keys = leaves.flatMap { $0 }
    let value = String(repeating: "v", count: 1_000)
    let values = Dictionary(uniqueKeysWithValues: keys.map { ($0, value) })
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(
        leaves: leaves, branchFanouts: [5, 2], values: values, partitionForKey: { _ in partition }, in: directory)
      let database = try await Store.open(in: path)
      try await database.withWriteTransaction { store in
        for key in [110, 120] {
          let removed = try await store.removeValue(for: partition, sortKey: key)
          #expect(removed == value)
        }
      }
      let current = try await Self.shape(in: directory)
      #expect(current.fanouts[1] == [3, 3])
      #expect(current.leafDepths == [2])
      #expect(current.leafKeys == leaves.filter { $0 != [110, 120] })
      try await database.withWriteTransaction { store in
        for key in keys {
          let actual = try await store.value(for: partition, sortKey: key)
          #expect(actual == ([110, 120].contains(key) ? nil : value))
        }
      }
    }
  }

  @Test func aLargerSeparatorCanSplitTheParentDuringDeletion() async throws {
    let keys = Array(stride(from: 10, through: 90, by: 10))
    let partitionNames =
      ["a", "a", "b", "b" + String(repeating: "x", count: 959)]
      + ["c", "d", "e", "f", "g"].map { String(repeating: $0, count: 960) }
    let partitions = Dictionary(uniqueKeysWithValues: zip(keys, partitionNames))
    let lengths = [1_360, 1_060, 1_060, 450, 450, 1, 1, 1, 1]
    let values = Dictionary(uniqueKeysWithValues: zip(keys, lengths.map { String(repeating: "v", count: $0) }))
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(
        leaves: [[10, 20], [30, 40, 50], [60], [70], [80], [90]], branchFanouts: [], values: values,
        partitionForKey: { key in try #require(partitions[key]) }, in: directory)
      let database = try await Store.open(in: path)
      let initial = try await Self.shape(in: directory)
      #expect(initial.rootLevel == 1)
      try await database.withWriteTransaction { store in
        let removed = try await store.removeValue(for: "a", sortKey: 10)
        #expect(removed == values[10])
      }
      let current = try await Self.shape(in: directory)
      #expect(current.rootLevel == 2)
      #expect(current.leafDepths == [2])
      #expect(current.leafKeys == [[20, 30], [40, 50], [60], [70], [80], [90]])
      try await database.withWriteTransaction { store in
        for key in keys {
          guard let partition = partitions[key] else {
            Issue.record("Missing partition for key \(key)")
            throw DiskBTreeFixture.Abort.requested
          }
          let value = try await store.value(for: partition, sortKey: key)
          #expect(value == (key == 10 ? nil : values[key]))
        }
      }
    }
  }

  private static let renameFailure = DiskBTreeError.IOFailure.fileSystem(message: "Injected rename failure")

  @Test func interruptedRootContractionCanBeRetried() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await Self.seed(
        leaves: [[10], [20]], branchFanouts: [], values: [10: "first", 20: "second"],
        partitionForKey: { _ in "p" }, in: directory)
      let database = try await Store.open(in: path)
      let originalRoot = try await DiskBTreeFixture.root(in: directory)
      await #expect {
        try await database.withWriteTransaction { store in
          store.beforeCommitStep = { step in
            if step == .renameManifest { throw DiskBTreeError.io(cause: Self.renameFailure) }
          }
          try await store.removeValue(for: "p", sortKey: 10)
        }
      } throws: {
        guard case DiskBTreeError.io(let cause) = $0 else { return false }
        return cause == Self.renameFailure
      }
      let recoveredRoot = try await DiskBTreeFixture.root(in: directory)
      #expect(recoveredRoot == originalRoot)

      try await database.withWriteTransaction { store in
        let page = try await store.scan(partition: "p", after: nil, limit: .max)
        #expect(page.entries.map(\.sortKey) == [10, 20])
        let removed = try await store.removeValue(for: "p", sortKey: 10)
        #expect(removed == "first")
      }
      let published = try await Self.shape(in: directory)
      #expect(published.rootLevel == 0)
      #expect(published.leafKeys == [[20]])
    }
  }

  private static func seed(
    leaves: [[Int]], branchFanouts: [Int], values: [Int: String], partitionForKey: (Int) throws -> String,
    in directory: DirectoryFileHandle
  ) async throws {
    var pages: [[UInt8]] = []
    var children: [Child] = []
    for keys in leaves {
      let entries = try keys.map { key in
        let diskKey = try Page.Key(partition: partitionForKey(key), sortKey: key)
        let record = try Page.record(for: diskKey, value: #require(values[key]))
        return Page.Leaf.Entry(key: diskKey, value: record)
      }
      let leaf = try #require(Page.Leaf(validating: entries))
      let first = try #require(entries.first)
      children.append(Child(firstKey: first.key, id: Int64(pages.count)))
      pages.append(try wholePage(Page.encode(leaf)))
    }
    if !branchFanouts.isEmpty {
      var start = 0
      var branches: [Child] = []
      for fanout in branchFanouts {
        let group = children[start..<(start + fanout)]
        branches.append(Child(firstKey: children[start].firstKey, id: Int64(pages.count)))
        pages.append(try branch(group, level: 1))
        start += fanout
      }
      children = branches
    }
    let rootID = Int64(pages.count)
    pages.append(try branch(children[...], level: branchFanouts.isEmpty ? 1 : 2))
    try await DiskBTreeFixture.writeStore(pages: pages, root: rootID, schema: Store.persistedSchema, in: directory)
  }

  private static func branch(_ children: ArraySlice<Child>, level: UInt8) throws -> [UInt8] {
    let separators = children.dropFirst().map(\.firstKey)
    return try wholePage(Page.encode(Page.Branch(level: level, separators: separators, children: children.map(\.id))))
  }

  private static func wholePage(_ encoded: Page.Encoded) throws -> [UInt8] {
    try #require(encoded.split == nil)
    return Array(encoded.left.readableBytesView)
  }

  private static func shape(in directory: DirectoryFileHandle) async throws -> Shape {
    let root = try await DiskBTreeFixture.root(in: directory)
    return try await directory.withFileHandle(
      forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      var pending = [(id: root, depth: 0)]
      var shape = Shape(rootLevel: 0, leafDepths: [], leafKeys: [], fanouts: [:])
      while let node = pending.popLast() {
        let bytes = try await file.readChunk(
          fromAbsoluteOffset: DiskPageFormat.offset(for: node.id), length: .bytes(Int64(DiskPageFormat.pageSize)))
        try #require(bytes.readableBytes == DiskPageFormat.pageSize)
        let page = try Page.decode(bytes, at: node.id)
        if node.depth == 0 { shape.rootLevel = page.level }
        #expect(Int(page.level) + node.depth == Int(shape.rootLevel))
        switch page {
        case .leaf(let leaf):
          #expect(node.depth == 0 || !leaf.entries.isEmpty)
          shape.leafDepths.insert(node.depth)
          shape.leafKeys.append(leaf.entries.map(\.key.sortKey))
        case .branch(let branch):
          shape.fanouts[node.depth, default: []].append(branch.children.count)
          pending.append(contentsOf: branch.children.reversed().map { (id: $0, depth: node.depth + 1) })
        }
      }
      return shape
    }
  }
}
