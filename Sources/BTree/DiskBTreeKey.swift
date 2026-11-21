/// A persisted key with a stable total order and a lossless Codable round trip.
package protocol DiskBTreeKey: Comparable, Codable, Sendable {
  /// A stable, namespaced ID for the encoding and comparison rules. The `swift:` prefix is reserved.
  static var diskOrderingID: String { get }
}

extension String: DiskBTreeKey {
  package static var diskOrderingID: String { "swift:String" }
}

extension Int: DiskBTreeKey {
  package static var diskOrderingID: String { "swift:Int" }
}

extension Int64: DiskBTreeKey {
  package static var diskOrderingID: String { "swift:Int64" }
}

extension UInt64: DiskBTreeKey {
  package static var diskOrderingID: String { "swift:UInt64" }
}

struct UTF8Key: DiskBTreeKey, Sendable {
  let string: String
  static var diskOrderingID: String { "swift:UTF8Key" }

  init(_ string: String) {
    self.string = string
  }

  init(validating bytes: Span<UInt8>) throws(UTF8.ValidationError) {
    let utf8 = try UTF8Span(validating: bytes)
    string = String(copying: utf8)
  }

  static func == (lhs: Self, rhs: Self) -> Bool {
    lhs.string.utf8.elementsEqual(rhs.string.utf8)
  }

  static func < (lhs: Self, rhs: Self) -> Bool {
    lhs.string.utf8.lexicographicallyPrecedes(rhs.string.utf8)
  }

  init(from decoder: any Decoder) throws {
    string = try decoder.singleValueContainer().decode(String.self)
  }

  func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(string)
  }
}

struct DiskKey<Partition: DiskBTreeKey, SortKey: DiskBTreeKey>: Comparable, Codable, Sendable {
  let partition: Partition
  let sortKey: SortKey

  static func < (lhs: Self, rhs: Self) -> Bool {
    (lhs.partition, lhs.sortKey) < (rhs.partition, rhs.sortKey)
  }
}
