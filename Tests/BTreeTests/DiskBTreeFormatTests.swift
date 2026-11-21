import CBOR
import NIOCore
import Testing
import _NIOFileSystem

@testable import BTree

@Suite
struct DiskBTreeFormatTests {
  private typealias Store = DiskBTree<String, Int, String>

  @Test func theManifestSelectsTheCommittedRootEvenWithForwardPageReferences() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let previous = try Self.leaf(records: [(Self.key(1), CBOREncoder().encode("previous"))])
      let left = try Self.leaf(records: [(Self.key(1), CBOREncoder().encode("committed"))])
      let right = try Self.leaf(records: [(Self.key(2), CBOREncoder().encode("neighbor"))])
      let root = try Self.branch(separators: [Self.key(2)], children: [2, 3])
      let pending = try Self.leaf(records: [(Self.key(1), CBOREncoder().encode("pending"))])
      try await DiskBTreeFixture.writeStore(
        pages: [previous, root, left, right, pending], root: 1, schema: Store.persistedSchema, in: directory)
      try await DiskBTreeFixture.writeManifest(
        DiskTreeManifest(
          rootPageID: 4, pageCount: 5, schema: DiskBTreeFixture.schema),
        named: "manifest.next", in: directory)

      let database = try await Store.open(in: path)
      try await database.withWriteTransaction { store in
        let first = try await store.scan(partition: "p", after: nil, limit: 1)
        #expect(first.entries.map(\.value) == ["committed"])
        #expect(first.nextAfter == 1)
        let second = try await store.scan(partition: "p", after: first.nextAfter, limit: 1)
        #expect(second.entries.map(\.value) == ["neighbor"])
        #expect(second.nextAfter == nil)
      }
    }
  }

  @Test func aPendingFirstCommitLeavesTheStoreEmpty() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let pending = try Self.leaf(records: [(Self.key(1), CBOREncoder().encode("pending"))])
      try await DiskBTreeFixture.createFixtureFile(named: "pages", bytes: pending, in: directory)
      try await DiskBTreeFixture.writeManifest(
        DiskTreeManifest(
          rootPageID: nil, pageCount: 0, schema: DiskBTreeFixture.schema),
        named: "manifest", in: directory)
      try await DiskBTreeFixture.writeManifest(
        DiskTreeManifest(
          rootPageID: 0, pageCount: 1, schema: DiskBTreeFixture.schema),
        named: "manifest.next", in: directory)

      let database = try await Store.open(in: path)
      try await database.withWriteTransaction { store in
        let page = try await store.scan(partition: "p", after: nil, limit: 1)
        #expect(page.entries.isEmpty)
        let previous = try await store.updateValue("committed", for: "p", sortKey: 2)
        #expect(previous == nil)
      }
      try await database.withWriteTransaction { store in
        let first = try await store.value(for: "p", sortKey: 1)
        let second = try await store.value(for: "p", sortKey: 2)
        #expect(first == nil)
        #expect(second == "committed")
      }
    }
  }

  @Test(arguments: [0, 1, 4_095])
  func rejectsAnIncompleteReferencedPage(byteCount: Int) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let page = Self.leaf(records: [])
      try await DiskBTreeFixture.writeStore(
        pages: [page, Array(page.prefix(byteCount))], root: 1, schema: Store.persistedSchema, in: directory)
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .incompletePage
      }
    }
  }

  @Test func aNewStoreWritesManifestAndPageMagic() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue("first", for: "p", sortKey: 1)
      }
      let options = OpenOptions.Read(followSymbolicLinks: false, closeOnExec: true)
      try await directory.withFileHandle(forReadingAt: FilePath("manifest"), options: options) { file in
        let header = try await file.readChunk(fromAbsoluteOffset: 0, length: .bytes(4))
        #expect(header.readableBytesView.elementsEqual("BTM0".utf8))
      }
      try await directory.withFileHandle(forReadingAt: FilePath("pages"), options: options) { file in
        let header = try await file.readChunk(fromAbsoluteOffset: 0, length: .bytes(4))
        #expect(Array(header.readableBytesView) == [0x42, 0x54, 0x50, 0x33])
      }
    }
  }

  @Test(
    arguments: [
      [], Array("BTM0".utf8), Array("BTM0".utf8) + [0, 0, 0, 1], Array(repeating: 0, count: 124)
    ] as [[UInt8]])
  func rejectsMalformedManifests(manifest: [UInt8]) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      try await DiskBTreeFixture.createFixtureFile(named: "pages", bytes: Self.leaf(records: []), in: directory)
      try await DiskBTreeFixture.createFixtureFile(named: "manifest", bytes: manifest, in: directory)

      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidManifest
      }
    }
  }

  @Test(arguments: [0, 7, 15, 20, 51, 52, 83, 84, 115, 123])
  func rejectsCorruptedManifests(byteOffset: Int) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let page = Self.leaf(records: [])
      try await DiskBTreeFixture.createFixtureFile(named: "pages", bytes: page + page + page, in: directory)
      let original = DiskTreeManifest(
        rootPageID: 2, pageCount: 3, schema: DiskBTreeFixture.schema)
      try await DiskBTreeFixture.writeManifest(original, named: "manifest", in: directory)

      try await directory.withFileHandle(
        forReadingAndWritingAt: FilePath("manifest"),
        options: .init(existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)
      ) { file in
        let offset = Int64(byteOffset)
        let current = try await file.readChunk(fromAbsoluteOffset: offset, length: .bytes(1))
        let byte = try #require(current.readableBytesView.first)
        let written = try await file.write(contentsOf: [byte ^ 1], toAbsoluteOffset: offset)
        try #require(written == 1)
      }

      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidManifest
      }
    }
  }

  @Test(arguments: [0, 4, 5, 7, 8, 10, 4_095])
  func rejectsCorruptPageChecksums(byteOffset: Int) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      var page = try Self.leaf(records: [(Self.key(1), CBOREncoder().encode("first"))])
      page[byteOffset] ^= 0xff
      try await DiskBTreeFixture.writeStore(pages: [page], root: 0, schema: Store.persistedSchema, in: directory)
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 0)
      }
    }
  }

  @Test(arguments: [0, DiskPageFormat.payloadCapacity - 1])
  func rejectsNonzeroPaddingWithAValidChecksum(paddingOffset: Int) {
    var padding = [UInt8](repeating: 0, count: paddingOffset + 1)
    padding[paddingOffset] = 1
    let bytes = ByteBuffer(bytes: Self.page(level: 0, count: 0, payload: padding))
    #expect {
      _ = try DiskPage<String, Int>.decode(bytes, at: 0)
    } throws: {
      DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 0)
    }
  }

  enum LeafKeyDefect: CaseIterable {
    case malformed, descendingSortKeys, duplicate, descendingPartitions
  }

  @Test(arguments: LeafKeyDefect.allCases)
  func rejectsMalformedOrUnorderedLeafKeys(defect: LeafKeyDefect) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let keys: [[UInt8]] =
        switch defect {
        case .malformed: [[0xa2]]
        case .descendingSortKeys: [try Self.key(2), try Self.key(1)]
        case .duplicate: [try Self.key(1), try Self.key(1)]
        case .descendingPartitions:
          [try CBOREncoder().encode(DiskKey(partition: "q", sortKey: 1)), try Self.key(1)]
        }
      let value = try CBOREncoder().encode("value")
      let page = Self.leaf(records: keys.map { ($0, value) })
      try await DiskBTreeFixture.writeStore(pages: [page], root: 0, schema: Store.persistedSchema, in: directory)
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 0)
      }
    }
  }

  enum TextLocation: CaseIterable, Sendable {
    case leafKey, leafValue, branchSeparator
  }

  @Test(
    arguments: [
      [0x61, 0x80],  // Unexpected continuation byte.
      [0x62, 0xc0, 0xaf],  // Overlong scalar.
      [0x63, 0xed, 0xa0, 0x80],  // Surrogate code point.
      [0x64, 0xf4, 0x90, 0x80, 0x80],  // Above U+10FFFF.
      [0x62, 0xe2, 0x82],  // Incomplete scalar at the end of a string.
      [0x7f, 0x61, 0xc3, 0x61, 0xa9, 0xff],  // Valid only if the chunks are concatenated before validation.
      [0x7f, 0x62, 0x65, 0xcc, 0x61, 0x81, 0xff],  // A combining scalar split across chunks.
      [0x7f, 0x61, 0x61],  // Missing break marker.
      [0x7f, 0x7b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]  // A chunk length that cannot fit in Int.
    ] as [[UInt8]], TextLocation.allCases)
  func rejectsMalformedUTF8(text: [UInt8], location: TextLocation) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let value = try CBOREncoder().encode("value")
      let badKey = try Self.key(partition: text, sortKey: 1)
      let pages: [[UInt8]]
      switch location {
      case .leafKey:
        pages = [Self.leaf(records: [(badKey, value)])]
      case .leafValue:
        pages = [try Self.leaf(records: [(Self.key(1), text)])]
      case .branchSeparator:
        pages = [
          try Self.leaf(records: [(Self.key(0), value)]),
          try Self.leaf(records: [(Self.key(1), value)]),
          Self.branch(separators: [badKey], children: [0, 1])
        ]
      }
      let root = Int64(pages.count - 1)
      try await DiskBTreeFixture.writeStore(pages: pages, root: root, schema: Store.persistedSchema, in: directory)
      await #expect {
        let database = try await Store.open(in: path)
        _ = try await database.value(for: "p", sortKey: 1)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: root)
      }
    }
  }

  @Test func acceptsValidTextChunksAndPreservesTheirBytes() async throws {
    let text: [UInt8] = [0x7f, 0x60, 0x61, 0x65, 0x62, 0xcc, 0x81, 0xff]
    let expectedBytes: [UInt8] = [0x65, 0xcc, 0x81]
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let page = try Self.leaf(records: [(Self.key(partition: text, sortKey: 1), text)])
      try await DiskBTreeFixture.writeStore(pages: [page], root: 0, schema: Store.persistedSchema, in: directory)
      let database = try await Store.open(in: path)
      let result = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "\u{e9}", after: nil, limit: 1)
      }
      let entry = try #require(result.entries.first)
      #expect(entry.value.utf8.elementsEqual(expectedBytes))
      guard case .leaf(let leaf) = try DiskPage<String, Int>.decode(ByteBuffer(bytes: page), at: 0) else {
        Issue.record("Expected a leaf page")
        return
      }
      let stored = try #require(leaf.entries.first)
      #expect(stored.key.partition.utf8.elementsEqual(expectedBytes))
    }
  }

  @Test func rejectsMalformedTextNestedInContainers() async throws {
    typealias Store = DiskBTree<String, Int, [String: [String]]>
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let value: [UInt8] = [0xa1, 0x61, 0x6b, 0x81, 0x7f, 0x61, 0xc3, 0x61, 0xa9, 0xff]
      let page = try Self.leaf(records: [(Self.key(1), value)])
      try await DiskBTreeFixture.writeStore(pages: [page], root: 0, schema: Store.persistedSchema, in: directory)
      let database = try await Store.open(in: path)
      await #expect {
        _ = try await database.withReadSnapshot { snapshot in
          try await snapshot.scan(partition: "p", after: nil, limit: 1)
        }
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 0)
      }
    }
  }

  @Test(arguments: [[-1, 0], [0, 2], [0, 3], [0, 0]] as [[Int64]])
  func rejectsInvalidChildPageReferences(children: [Int64]) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let root = try Self.branch(separators: [Self.key(2)], children: children)
      let empty = Self.leaf(records: [])
      try await DiskBTreeFixture.writeStore(
        pages: [empty, empty, root], root: 2, schema: Store.persistedSchema, in: directory)
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 2)
      }
    }
  }

  @Test(arguments: [[2, 1], [1, 1]])
  func rejectsUnorderedBranchSeparators(keys: [Int]) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let root = try Self.branch(separators: keys.map(Self.key), children: [0, 1, 2])
      let empty = Self.leaf(records: [])
      try await DiskBTreeFixture.writeStore(
        pages: [empty, empty, empty, root], root: 3, schema: Store.persistedSchema, in: directory)
      await #expect {
        _ = try await Store.open(in: path)
      } throws: {
        DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 3)
      }
    }
  }

  enum ChildDefect: CaseIterable, Sendable {
    case malformedValue
    case wrongLevel
    case keyOutsideFence
  }

  @Test(arguments: ChildDefect.allCases)
  func readsAndValidatesChildrenOnlyWhenVisited(defect: ChildDefect) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { directory, path in
      let left = try Self.leaf(records: [(Self.key(1), CBOREncoder().encode("left"))])
      let other = try Self.leaf(records: [(Self.key(10), CBOREncoder().encode("other"))])
      let right: [UInt8]
      switch defect {
      case .malformedValue: right = try Self.leaf(records: [(Self.key(10), [0x18])])
      case .wrongLevel: right = try Self.branch(separators: [Self.key(11)], children: [0, 1])
      case .keyOutsideFence: right = try Self.leaf(records: [(Self.key(9), CBOREncoder().encode("out of range"))])
      }
      let root = try Self.branch(separators: [Self.key(10)], children: [0, 2])
      try await DiskBTreeFixture.writeStore(
        pages: [left, other, right, root], root: 3, schema: Store.persistedSchema, in: directory)

      let database = try await Store.open(in: path)
      try await database.withWriteTransaction { store in
        let value = try await store.value(for: "p", sortKey: 1)
        #expect(value == "left")
        await #expect {
          try await store.value(for: "p", sortKey: 10)
        } throws: {
          DiskBTreeFixture.corruption(in: $0) == .invalidPage(pageID: 2)
        }
      }
    }
  }

  private static func key(_ sortKey: Int) throws -> [UInt8] {
    try CBOREncoder().encode(DiskKey(partition: "p", sortKey: sortKey))
  }

  private static func key(partition: [UInt8], sortKey: Int) throws -> [UInt8] {
    let encoder = CBOREncoder()
    return try [0xa2] + encoder.encode("partition") + partition + encoder.encode("sortKey") + encoder.encode(sortKey)
  }

  private static func leaf(records: [(key: [UInt8], value: [UInt8])]) -> [UInt8] {
    let payload = records.flatMap { key, value in
      integerBytes(UInt16(key.count)) + integerBytes(UInt16(value.count)) + key + value
    }
    return page(level: 0, count: records.count, payload: payload)
  }

  private static func branch(separators: [[UInt8]], children: [Int64]) -> [UInt8] {
    var payload = integerBytes(children[0])
    for (key, child) in zip(separators, children.dropFirst()) {
      payload += integerBytes(UInt16(key.count)) + key + integerBytes(child)
    }
    return page(level: 1, count: separators.count, payload: payload)
  }

  private static func page(level: UInt8, count: Int, payload: [UInt8]) -> [UInt8] {
    let bytes: [UInt8] = [0x42, 0x54, 0x50, 0x33, level] + integerBytes(UInt16(count)) + payload
    var page = ByteBuffer(bytes: bytes)
    DiskPageFormat.seal(&page)
    return Array(page.readableBytesView)
  }

  private static func integerBytes<Integer: FixedWidthInteger>(_ value: Integer) -> [UInt8] {
    (0..<(Integer.bitWidth / 8)).reversed().map { UInt8(truncatingIfNeeded: value >> ($0 * 8)) }
  }
}
