import CBOR
import Foundation
import Testing
import _NIOFileSystem

@testable import BTree

@Suite
struct DiskBTreeRecordTests {
  private struct Named: DiskBTreeValue {
    static let diskSchemaID = "btree-tests:name"
    let name: String
  }

  private struct Annotated: DiskBTreeValue, Equatable {
    static let diskSchemaID = Named.diskSchemaID
    let name: String
    let comment: String?
  }

  private struct BinaryValue: DiskBTreeValue, Equatable {
    static let diskSchemaID = "btree-tests:binary"
    let bytes: Data
    let counter: UInt64
    let labels: [String?]
  }

  private struct Timestamp: DiskBTreeKey {
    static let diskOrderingID = "btree-tests:timestamp"
    let date: Date

    static func < (lhs: Self, rhs: Self) -> Bool { lhs.date < rhs.date }
  }

  private protocol DictionaryKeySpelling {
    static var prefix: String { get }
  }

  private enum OriginalSpelling: DictionaryKeySpelling { static let prefix = "old-" }
  private enum NewSpelling: DictionaryKeySpelling { static let prefix = "new-" }

  private struct DictionaryKey<Spelling: DictionaryKeySpelling>: DiskBTreeValue, Hashable, CodingKeyRepresentable {
    static var diskSchemaID: String { "btree-tests:dictionary-key" }
    let diskRepresentation: Int

    init(diskRepresentation: Int) { self.diskRepresentation = diskRepresentation }

    var codingKey: any CodingKey { "\(Spelling.prefix)\(diskRepresentation)".codingKey }

    init?(codingKey: some CodingKey) {
      let spelling = codingKey.stringValue
      guard spelling.hasPrefix(Spelling.prefix), let number = Int(spelling.dropFirst(Spelling.prefix.count), radix: 10)
      else {
        return nil
      }
      diskRepresentation = number
    }
  }

  private struct NestedValue: DiskBTreeValue {
    static let diskSchemaID = "btree-tests:nested"
    let depth: Int

    init(depth: Int) { self.depth = depth }

    init(from decoder: any Decoder) throws {
      var container = try decoder.unkeyedContainer()
      var depth = 0
      while !container.isAtEnd {
        container = try container.nestedUnkeyedContainer()
        depth += 1
      }
      self.depth = depth
    }

    func encode(to encoder: any Encoder) throws {
      var container = encoder.unkeyedContainer()
      for _ in 0..<depth { container = container.nestedUnkeyedContainer() }
    }
  }

  private static let options = OpenOptions.Directory(followSymbolicLinks: false, closeOnExec: true)

  @Test func documentAttributesRoundTripAcrossReopen() async throws {
    typealias Store = DiskBTree<String, Int, Document>
    let expected: Document = [
      "caf\u{e9}": .string("open"),
      "numbers": .list([.int64(1), .uint64(1), .float32(1), .float64(1), .int64(.min), .uint64(.max)]),
      "details": .map(["verified": .bool(true), "bytes": .bytes(Data([0, 0xff]))])
    ]

    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { transaction in
        try await transaction.updateValue(expected, for: "p", sortKey: 1)
        try await transaction.updateValue([:], for: "p", sortKey: 2)
      }

      let reopened = try await Store.open(in: path)
      let document = try await reopened.value(for: "p", sortKey: 1)
      #expect(document == expected)
      #expect(document?["cafe\u{301}"] == .string("open"))
      #expect(try await reopened.value(for: "p", sortKey: 2) == [:])
      #expect(try await reopened.value(for: "p", sortKey: 3) == nil)
    }
  }

  @Test func attributeEncodingUsesStableCaseTags() throws {
    let encoder = CBOREncoder()
    let payload: CBOR = .map([.textString("_0"): .unsignedInt(1)])
    let signed: CBOR = .map([.textString("i"): payload])
    let unsigned: CBOR = .map([.textString("u"): payload])
    #expect(try encoder.encode(AttributeValue.int64(1)) == signed.encode())
    #expect(try encoder.encode(AttributeValue.uint64(1)) == unsigned.encode())

    let unknown: CBOR = .map([.textString("?"): payload])
    #expect(throws: DecodingError.self) {
      try DiskCBOR.decode(AttributeValue.self, from: unknown.encode())
    }
    let ambiguous: CBOR = .map([.textString("i"): payload, .textString("u"): payload])
    #expect(throws: DecodingError.self) {
      try DiskCBOR.decode(AttributeValue.self, from: ambiguous.encode())
    }
  }

  @Test(arguments: [UInt64(127), UInt64.max])
  func binaryCodableRecordsRoundTrip(counter: UInt64) async throws {
    typealias Store = DiskBTree<String, UInt64, BinaryValue>
    let expected = BinaryValue(
      bytes: Data([0, 0x7f, 0x61, 0xc3, 0x61, 0xa9, 0xff, 0x80]), counter: counter,
      labels: [nil, "", "$null", "e\u{301}", "before\0after", "\u{7f}", "\u{feff}lead", "\u{fffd}", "👩🏽‍💻", "🇱🇺"])
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue(expected, for: "p", sortKey: .max)
      }
      let value = try await database.value(for: "p", sortKey: .max)
      #expect(value == expected)
      #expect(value?.labels.map { $0.map { Array($0.utf8) } } == expected.labels.map { $0.map { Array($0.utf8) } })
    }
  }

  @Test func utf8KeyValidatesBytesAndOwnsTheExactSpelling() throws {
    let bytes = Array("e\u{301}\0👩🏽‍💻".utf8)
    let key = try UTF8Key(validating: bytes.span)
    #expect(key.string.utf8.elementsEqual(bytes))
    let invalid: [UInt8] = [0x61, 0xe2, 0x82]
    do {
      _ = try UTF8Key(validating: invalid.span)
      Issue.record("Accepted a truncated UTF-8 scalar")
    } catch {
      #expect(error.kind == .truncatedScalar)
      #expect(error.byteOffsets == 1..<3)
    }
  }

  @Test func stringKeysShareOneRecordForCanonicallyEquivalentSpellings() async throws {
    typealias Store = DiskBTree<String, String, String>
    let composed = "\u{e9}"
    let decomposed = "e\u{301}"
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        let first = try await store.updateValue("first", for: composed, sortKey: composed)
        #expect(first == nil)
        let previous = try await store.updateValue("second", for: decomposed, sortKey: decomposed)
        #expect(previous == "first")
      }
      let value = try await database.value(for: composed, sortKey: composed)
      #expect(value == "second")
      let page = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: composed, after: nil, limit: 2)
      }
      let entry = try #require(page.entries.first)
      #expect(page.entries.count == 1)
      #expect(entry.sortKey.utf8.elementsEqual(decomposed.utf8))
    }
  }

  @Test func rejectsValuesDeeperThanTheDecoderSupports() async throws {
    typealias Store = DiskBTree<String, Int, NestedValue>
    let depthLimit = CBORDecodingOptions.default.maximumDepth
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        try await store.updateValue(NestedValue(depth: 3), for: "p", sortKey: 1)
        await #expect {
          _ = try await store.updateValue(NestedValue(depth: depthLimit + 1), for: "p", sortKey: 1)
        } throws: {
          $0 as? DiskBTreeError == .valueEncodingFailed
        }
      }
      let value = try await database.value(for: "p", sortKey: 1)
      #expect(value?.depth == 3)
    }
  }

  @Test func rejectsAKeyThatChangesWhenDecoded() async throws {
    typealias Store = DiskBTree<String, Timestamp, Int>
    let original = Timestamp(date: Date(timeIntervalSinceReferenceDate: 0))
    let lossy = Timestamp(date: Date(timeIntervalSinceReferenceDate: 0.000_000_01))
    try #require(original < lossy)

    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { transaction in
        _ = try await transaction.updateValue(1, for: "p", sortKey: original)
      }
      try await database.withWriteTransaction { transaction -> Void in
        await #expect(throws: DiskBTreeError.keyEncodingFailed) {
          _ = try await transaction.updateValue(2, for: "p", sortKey: lossy)
        }
      }

      let reopened = try await Store.open(in: path)
      #expect(try await reopened.value(for: "p", sortKey: original) == 1)
      #expect(try await reopened.value(for: "p", sortKey: lossy) == nil)
    }
  }

  @Test func nestedOptionalValuesRoundTrip() async throws {
    typealias Store = DiskBTree<String, Int, String??>
    let values: [String??] = [.none, .some(.none), .some(.some("text"))]
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        for (key, value) in values.enumerated() {
          let previous = try await store.updateValue(value, for: "p", sortKey: key)
          #expect(previous == nil)
        }
      }

      let reopened = try await Store.open(in: path)
      try await reopened.withReadSnapshot { snapshot in
        for (key, expected) in values.enumerated() {
          let actual = try await snapshot.value(for: "p", sortKey: key)
          #expect(actual == .some(expected))
        }
        let missing = try await snapshot.value(for: "p", sortKey: values.count)
        #expect(missing == nil)
      }
    }
  }

  @Test func nestedOptionalDictionaryKeysAndArrayElementsRoundTrip() async throws {
    typealias Store = DiskBTree<String, Int, [String??: [Int??]]>
    let expected: [String??: [Int??]] = [
      .none: [.some(.none)],
      .some(.none): [.none, .some(.some(1))],
      .some(.some("key")): [.some(.some(2))]
    ]
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await Store.create(in: path)
      try await database.withWriteTransaction { store in
        _ = try await store.updateValue(expected, for: "p", sortKey: 1)
      }
      let reopened = try await Store.open(in: path)
      let actual = try await reopened.value(for: "p", sortKey: 1)
      #expect(actual == expected)
    }
  }

  @Test func dictionaryKeysUseTheDiskRepresentationAcrossCodingKeyChanges() async throws {
    typealias OriginalKey = DictionaryKey<OriginalSpelling>
    typealias NewKey = DictionaryKey<NewSpelling>
    typealias OriginalStore = DiskBTree<String, Int, [OriginalKey: String]>
    typealias NewStore = DiskBTree<String, Int, [NewKey: String]>
    let original = OriginalKey(diskRepresentation: -7)
    let new = NewKey(diskRepresentation: -7)
    #expect(original.codingKey.stringValue != new.codingKey.stringValue)

    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let database = try await OriginalStore.create(in: path)
      try await database.withWriteTransaction { transaction in
        _ = try await transaction.updateValue([original: "old"], for: "p", sortKey: 1)
      }

      let reopened = try await NewStore.open(in: path)
      try await reopened.withWriteTransaction { transaction in
        let previous = try await transaction.updateValue([new: "new"], for: "p", sortKey: 1)
        #expect(previous == [new: "old"])
      }
      #expect(try await database.value(for: "p", sortKey: 1) == [original: "new"])
    }
  }

  @Test func neighboringValuesPreserveUnknownFieldsThroughEditsAndSplits() async throws {
    typealias Older = DiskBTree<String, Int, Named>
    typealias Newer = DiskBTree<String, Int, Annotated>
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let newer = try await Newer.create(in: path)
      let original = Annotated(name: "one", comment: "written by the newer schema")
      try await newer.withWriteTransaction { store in
        try await store.updateValue(original, for: "p", sortKey: 1)
        try await store.updateValue(Annotated(name: "two", comment: nil), for: "p", sortKey: 2)
      }
      let older = try await Older.open(in: path)
      try await older.withWriteTransaction { store in
        let previous = try await store.updateValue(Named(name: "replaced"), for: "p", sortKey: 2)
        #expect(previous?.name == "two")
        let large = Named(name: String(repeating: "v", count: 1_850))
        for key in 3...12 { try await store.updateValue(large, for: "p", sortKey: key) }
        for key in 3...12 { try await store.removeValue(for: "p", sortKey: key) }
      }
      let page = try await newer.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: "p", after: nil, limit: 2)
      }
      #expect(page.entries.map(\.value) == [original, Annotated(name: "replaced", comment: nil)])
    }
  }

  @Test(arguments: [false, true])
  func incompatibleValueSchemaIsRejectedOnOpen(populated: Bool) async throws {
    typealias Strings = DiskBTree<String, Int, String>
    typealias Integers = DiskBTree<String, Int, Int>
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let strings = try await Strings.create(in: path)
      if populated {
        try await strings.withWriteTransaction { store in
          _ = try await store.updateValue("text", for: "p", sortKey: 1)
        }
      }
      await #expect(throws: DiskBTreeError.incompatibleValueSchema) {
        _ = try await Integers.open(in: path)
      }
      let reopened = try await Strings.open(in: path)
      let retained = try await reopened.value(for: "p", sortKey: 1)
      #expect(retained == (populated ? "text" : nil))
    }
  }

  @Test func reloadingAReplacedStoreChecksTheNestedValueSchema() async throws {
    typealias Strings = DiskBTree<String, Int, [String: [String?]]>
    typealias Integers = DiskBTree<String, Int, [String: [Int?]]>
    let fs = FileSystem.shared
    try await fs.withTemporaryDirectory(prefix: nil, options: Self.options) { _, parent in
      let path = parent.appending("store")
      try await fs.createDirectory(at: path, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      let strings = try await Strings.create(in: path)
      let expected: [String: [String?]] = ["word": [nil, "text"]]
      try await strings.withWriteTransaction { store in
        _ = try await store.updateValue(expected, for: "p", sortKey: 1)
      }
      let originalPath = parent.appending("original")
      try await fs.moveItem(at: path, to: originalPath)
      try await fs.createDirectory(at: path, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      _ = try await Integers.create(in: path)

      await #expect(throws: DiskBTreeError.incompatibleValueSchema) {
        _ = try await strings.value(for: "p", sortKey: 1)
      }
      await #expect(throws: DiskBTreeError.incompatibleValueSchema) {
        try await strings.withReadSnapshot { _ in }
      }
      await #expect(throws: DiskBTreeError.incompatibleValueSchema) {
        try await strings.withWriteTransaction { _ in }
      }
      let reopened = try await Strings.open(in: originalPath)
      let actual = try await reopened.value(for: "p", sortKey: 1)
      #expect(actual == expected)
    }
  }

  @Test func shrinkingReplacementsCoalesceTheLiveLeaves() async throws {
    typealias Store = DiskBTree<Int, Int, String>
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { directory, path in
      let database = try await Store.create(in: path)
      let keys = Array(0..<96)
      try await database.withWriteTransaction { store in
        let large = String(repeating: "v", count: 1_900)
        for key in keys { try await store.updateValue(large, for: 0, sortKey: key) }
      }
      try await database.withWriteTransaction { store in
        for key in keys { try await store.updateValue("v", for: 0, sortKey: key) }
      }
      let root = try await DiskBTreeFixture.root(in: directory)
      try await directory.withFileHandle(
        forReadingAt: FilePath("pages"), options: .init(followSymbolicLinks: false, closeOnExec: true)
      ) { pages in
        let bytes = try await pages.readChunk(
          fromAbsoluteOffset: DiskPageFormat.offset(for: root), length: .bytes(Int64(DiskPageFormat.pageSize)))
        let page = try DiskPage<Int, Int>.decode(bytes, at: root)
        #expect(page.level == 0)
      }
      let page = try await database.withReadSnapshot { snapshot in
        try await snapshot.scan(partition: 0, after: nil, limit: keys.count)
      }
      #expect(page.entries.map(\.sortKey) == keys)
      #expect(page.entries.allSatisfy { $0.value == "v" })
    }
  }

  @Test func filenameIndexRetainsByteDistinctSpellingsAndChecksItsOrderingOnOpen() async throws {
    typealias Files = DiskBTree<String, UTF8Key, Int>
    typealias OtherSortKeyOrder = DiskBTree<String, String, Int>
    typealias OtherPartitionOrder = DiskBTree<UTF8Key, UTF8Key, Int>
    let composed = UTF8Key("\u{e9}")
    let decomposed = UTF8Key("e\u{301}")
    try await FileSystem.shared.withTemporaryDirectory(prefix: nil, options: Self.options) { _, path in
      let files = try await Files.create(in: path)
      try await files.withWriteTransaction { store in
        try await store.updateValue(1, for: "files", sortKey: composed)
        try await store.updateValue(2, for: "files", sortKey: decomposed)
      }
      let reopened = try await Files.open(in: path)
      try await reopened.withReadSnapshot { snapshot in
        let first = try await snapshot.scan(partition: "files", after: nil, limit: 1)
        #expect(first.entries.map(\.sortKey) == [decomposed])
        #expect(first.entries.map(\.value) == [2])
        let second = try await snapshot.scan(partition: "files", after: first.nextAfter, limit: 1)
        #expect(second.entries.map(\.sortKey) == [composed])
        #expect(second.entries.map(\.value) == [1])
      }
      await #expect(throws: DiskBTreeError.incompatibleKeyOrdering) {
        _ = try await OtherSortKeyOrder.open(in: path)
      }
      await #expect(throws: DiskBTreeError.incompatibleKeyOrdering) {
        _ = try await OtherPartitionOrder.open(in: path)
      }
    }
  }
}
