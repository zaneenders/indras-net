import CBOR
import Crypto
import NIOCore

enum DiskPageFormat {
  static let magic: UInt32 = 0x4254_5033  // BTP3
  static let pageSize = 4_096
  static let headerSize = 7
  static let checksumSize = SHA256.byteCount
  static let payloadCapacity = pageSize - headerSize - checksumSize
  static let keyLimit = 1_024
  static let recordLimit = payloadCapacity / 2
  // A level-50 tree with two children per branch already needs 2^51 - 1 pages.
  static let maxLevel: UInt8 = 50
  static let maxPageCount = Int64.max / Int64(pageSize)
  static let validPageIDs = 0..<maxPageCount
  private static let zeroPadding = ByteBuffer(repeating: 0, count: payloadCapacity)

  static func validateFileSize(_ size: Int64, pageCount: Int64) throws(DiskBTreeError) {
    guard size >= pageCount * Int64(pageSize) else { throw .corruptStore(cause: .incompletePage) }
    guard size <= maxPageCount * Int64(pageSize) else { throw .storeFull }
  }

  static func seal(_ bytes: inout ByteBuffer) {
    bytes.writeRepeatingByte(0, count: pageSize - checksumSize - bytes.readableBytes)
    let checksum = digest(bytes)
    bytes.writeBytes(checksum)
  }

  static func verifiedContents(_ bytes: ByteBuffer, at id: Int64) throws(DiskBTreeError) -> ByteBuffer {
    var buffer = bytes
    guard buffer.readableBytes == pageSize,
      let contents = buffer.readSlice(length: pageSize - checksumSize),
      buffer.readableBytesView.elementsEqual(digest(contents))
    else { throw .corruptStore(cause: .invalidPage(pageID: id)) }
    return contents
  }

  static func hasValidPadding(_ bytes: ByteBuffer) -> Bool {
    guard let expected = zeroPadding.getSlice(at: 0, length: bytes.readableBytes) else { return false }
    return bytes == expected
  }

  private static func digest(_ bytes: ByteBuffer) -> SHA256.Digest {
    var hash = SHA256()
    hash.update(bytes: bytes.readableBytesSpan)
    return hash.finalize()
  }

  static func offset(for pageID: Int64) -> Int64 {
    precondition(validPageIDs.contains(pageID), "The page ID must address a full page")
    return pageID * Int64(pageSize)
  }
}

struct DiskRecord {
  let key: [UInt8]
  let value: [UInt8]

  var size: Int { 4 + key.count + value.count }

  func decoded<Value: DiskBTreeValue>(as _: Value.Type, at pageID: Int64) throws(DiskBTreeError) -> Value {
    do {
      let representation = try DiskCBOR.decode(Value.DiskRepresentation.self, from: value)
      return try Value(diskRepresentation: representation)
    } catch is CBORError {
      throw .corruptStore(cause: .invalidPage(pageID: pageID))
    } catch {
      throw .valueDecodingFailed(pageID: pageID)
    }
  }
}

enum DiskPage<Partition: DiskBTreeKey, SortKey: DiskBTreeKey> {
  typealias Key = DiskKey<Partition, SortKey>
  private typealias Format = DiskPageFormat

  struct Branch {
    let level: UInt8
    var separators: [Key]
    var children: [Int64]
  }

  struct Encoded {
    let left: ByteBuffer
    let split: (separator: Key, right: ByteBuffer)?
    let payloadSize: Int
  }

  case leaf(Leaf)
  case branch(Branch)

  static func partitioningIndex<Element>(in elements: [Element], where isInSuffix: (Element) -> Bool) -> Int {
    var lower = elements.startIndex
    var upper = elements.endIndex
    while lower < upper {
      let middle = lower + (upper - lower) / 2
      if isInSuffix(elements[middle]) {
        upper = middle
      } else {
        lower = middle + 1
      }
    }
    return lower
  }

  var level: UInt8 {
    switch self {
    case .leaf: return 0
    case .branch(let branch): return branch.level
    }
  }

  func encode() throws(DiskBTreeError) -> Encoded {
    switch self {
    case .leaf(let leaf): return try Self.encode(leaf)
    case .branch(let branch): return try Self.encode(branch)
    }
  }

  func joined(with right: Self, separator: Key) -> Self? {
    switch (self, right) {
    case (.leaf(var leaf), .leaf(let right)):
      leaf.append(contentsOf: right)
      return .leaf(leaf)
    case (.branch(var branch), .branch(let right)):
      guard branch.level == right.level else { return nil }
      branch.separators.append(separator)
      branch.separators.append(contentsOf: right.separators)
      branch.children.append(contentsOf: right.children)
      return .branch(branch)
    default:
      return nil
    }
  }

  static func decode(_ bytes: ByteBuffer, at pageID: Int64) throws(DiskBTreeError) -> Self {
    var buffer = try Format.verifiedContents(bytes, at: pageID)
    let invalid = DiskBTreeError.corruptStore(cause: .invalidPage(pageID: pageID))
    guard buffer.readInteger(endianness: .big, as: UInt32.self) == Format.magic,
      let level = buffer.readInteger(endianness: .big, as: UInt8.self), level <= Format.maxLevel,
      let count = buffer.readInteger(endianness: .big, as: UInt16.self)
    else { throw invalid }

    let page: Self
    do {
      if level == 0 {
        var entries: [Leaf.Entry] = []
        for _ in 0..<count {
          guard let keyLength = buffer.readInteger(endianness: .big, as: UInt16.self),
            let valueLength = buffer.readInteger(endianness: .big, as: UInt16.self),
            keyLength > 0, Int(keyLength) <= Format.keyLimit, valueLength > 0,
            4 + Int(keyLength) + Int(valueLength) <= Format.recordLimit,
            let keyBytes = buffer.readBytes(length: Int(keyLength)),
            let valueBytes = buffer.readBytes(length: Int(valueLength))
          else { throw invalid }
          let record = DiskRecord(key: keyBytes, value: valueBytes)
          let key = try DiskCBOR.decode(Key.self, from: record.key)
          entries.append(Leaf.Entry(key: key, value: record))
        }
        guard let leaf = Leaf(validating: entries) else { throw invalid }
        page = .leaf(leaf)
      } else {
        guard count > 0, let first = buffer.readInteger(endianness: .big, as: Int64.self) else { throw invalid }
        var separators: [Key] = []
        var children = [first]
        for _ in 0..<count {
          guard let keyLength = buffer.readInteger(endianness: .big, as: UInt16.self),
            keyLength > 0, Int(keyLength) <= Format.keyLimit,
            let keyBytes = buffer.readBytes(length: Int(keyLength)),
            let child = buffer.readInteger(endianness: .big, as: Int64.self)
          else { throw invalid }
          separators.append(try DiskCBOR.decode(Key.self, from: keyBytes))
          children.append(child)
        }
        guard zip(separators, separators.dropFirst()).allSatisfy({ $0 < $1 }),
          children.allSatisfy({ Format.validPageIDs.contains($0) && $0 != pageID }),
          Set(children).count == children.count
        else { throw invalid }
        page = .branch(Branch(level: level, separators: separators, children: children))
      }
    } catch {
      throw invalid
    }
    guard Format.hasValidPadding(buffer) else { throw invalid }
    return page
  }

  static func record<Value: DiskBTreeValue>(for key: Key, value: Value) throws(DiskBTreeError) -> DiskRecord {
    let encoder = CBOREncoder()
    let keyBytes = try encodeKey(key, using: encoder)
    let valueBytes: [UInt8]
    do {
      valueBytes = try encoder.encode(value.diskRepresentation)
    } catch {
      throw .valueEncodingFailed
    }
    let record = DiskRecord(key: keyBytes, value: valueBytes)
    guard record.size <= Format.recordLimit else {
      throw .recordTooLarge(encodedBytes: record.size, maximumBytes: Format.recordLimit)
    }
    try validateValueEncoding(record.value)
    return record
  }

  static func encode(_ leaf: Leaf) throws(DiskBTreeError) -> Encoded {
    let records = leaf.entries.map(\.value)
    let sizes = records.map(\.size)
    let size = sizes.reduce(0, +)
    guard size > Format.payloadCapacity else {
      return Encoded(left: leafPage(records[...]), split: nil, payloadSize: size)
    }

    let split = try splitIndex(sizes, total: size, capacity: Format.payloadCapacity, promotesSeparator: false)
    return Encoded(
      left: leafPage(records[..<split]),
      split: (leaf.entries[split].key, leafPage(records[split...])),
      payloadSize: size)
  }

  static func encode(_ branch: Branch) throws(DiskBTreeError) -> Encoded {
    let encoder = CBOREncoder()
    let keys = try branch.separators.map { key throws(DiskBTreeError) in try encodeKey(key, using: encoder) }
    let sizes = keys.map { 10 + $0.count }
    let size = sizes.reduce(0, +)
    let capacity = Format.payloadCapacity - 8
    guard size > capacity else {
      return Encoded(
        left: branchPage(level: branch.level, keys: keys[...], children: branch.children[...]),
        split: nil, payloadSize: size + 8)
    }

    let promoted = try splitIndex(sizes, total: size, capacity: capacity, promotesSeparator: true)
    let rightIndex = promoted + 1
    return Encoded(
      left: branchPage(level: branch.level, keys: keys[..<promoted], children: branch.children[..<rightIndex]),
      split: (
        branch.separators[promoted],
        branchPage(level: branch.level, keys: keys[rightIndex...], children: branch.children[rightIndex...])
      ),
      payloadSize: size + 8)
  }

  private static func splitIndex(
    _ sizes: [Int], total: Int, capacity: Int, promotesSeparator: Bool
  ) throws(DiskBTreeError) -> Int {
    // A promoted separator leaves at least one separator and two children on each side.
    let end = sizes.count - (promotesSeparator ? 1 : 0)
    guard end > 1 else { throw .storeFull }
    var leftSize = 0
    var bestIndex: Int?
    var bestBalance = Int.max
    for index in 1..<end {
      leftSize += sizes[index - 1]
      let rightSize = total - leftSize - (promotesSeparator ? sizes[index] : 0)
      let balance = abs(leftSize - rightSize)
      if leftSize <= capacity, rightSize <= capacity, balance < bestBalance {
        bestIndex = index
        bestBalance = balance
      }
    }
    guard let bestIndex else { throw .storeFull }
    return bestIndex
  }

  private static func encodeKey(_ key: Key, using encoder: CBOREncoder) throws(DiskBTreeError) -> [UInt8] {
    let bytes: [UInt8]
    do {
      bytes = try encoder.encode(key)
    } catch {
      throw .keyEncodingFailed
    }
    guard bytes.count <= Format.keyLimit else {
      throw .keyTooLarge(encodedBytes: bytes.count, maximumBytes: Format.keyLimit)
    }
    guard let decoded = try? DiskCBOR.decode(Key.self, from: bytes), decoded == key else {
      throw .keyEncodingFailed
    }
    return bytes
  }

  private static func validateValueEncoding(_ bytes: [UInt8]) throws(DiskBTreeError) {
    let options = CBORDecodingOptions.default
    // Each nested level takes at least one byte. Reject an over-depth encoding before writing it.
    if bytes.count > options.maximumDepth {
      do {
        _ = try CBOR.decode(bytes, options: options)
      } catch {
        throw .valueEncodingFailed
      }
    }
  }

  private static func leafPage(_ records: ArraySlice<DiskRecord>) -> ByteBuffer {
    var buffer = header(level: 0, count: records.count)
    for record in records {
      buffer.writeInteger(UInt16(record.key.count), endianness: .big, as: UInt16.self)
      buffer.writeInteger(UInt16(record.value.count), endianness: .big, as: UInt16.self)
      buffer.writeBytes(record.key)
      buffer.writeBytes(record.value)
    }
    Format.seal(&buffer)
    return buffer
  }

  private static func branchPage(level: UInt8, keys: ArraySlice<[UInt8]>, children: ArraySlice<Int64>) -> ByteBuffer {
    var buffer = header(level: level, count: keys.count)
    buffer.writeInteger(children[children.startIndex], endianness: .big, as: Int64.self)
    for (key, child) in zip(keys, children.dropFirst()) {
      buffer.writeInteger(UInt16(key.count), endianness: .big, as: UInt16.self)
      buffer.writeBytes(key)
      buffer.writeInteger(child, endianness: .big, as: Int64.self)
    }
    Format.seal(&buffer)
    return buffer
  }

  private static func header(level: UInt8, count: Int) -> ByteBuffer {
    var buffer = ByteBufferAllocator().buffer(capacity: Format.pageSize)
    buffer.writeInteger(Format.magic, endianness: .big, as: UInt32.self)
    buffer.writeInteger(level, endianness: .big, as: UInt8.self)
    buffer.writeInteger(UInt16(count), endianness: .big, as: UInt16.self)
    return buffer
  }
}
