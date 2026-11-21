package protocol DiskBTreeValue: Codable, Sendable {
  associatedtype DiskRepresentation: Codable = Self

  /// A stable ID shared by compatible encodings. The `swift:` prefix is reserved.
  static var diskSchemaID: String { get }

  var diskRepresentation: DiskRepresentation { get }
  init(diskRepresentation: DiskRepresentation) throws
}

extension DiskBTreeValue where DiskRepresentation == Self {
  package var diskRepresentation: Self { self }

  package init(diskRepresentation: Self) {
    self = diskRepresentation
  }
}

extension String: DiskBTreeValue {
  package static var diskSchemaID: String { "swift:String" }
}

extension Int: DiskBTreeValue {
  package static var diskSchemaID: String { "swift:Int" }
}

extension Int64: DiskBTreeValue {
  package static var diskSchemaID: String { "swift:Int64" }
}

extension UInt64: DiskBTreeValue {
  package static var diskSchemaID: String { "swift:UInt64" }
}

extension Optional: DiskBTreeValue where Wrapped: DiskBTreeValue {
  package static var diskSchemaID: String { "swift:OptionalV2:\(Wrapped.diskSchemaID)" }

  package var diskRepresentation: [Wrapped.DiskRepresentation] {
    map { [$0.diskRepresentation] } ?? []
  }

  package init(diskRepresentation: [Wrapped.DiskRepresentation]) throws {
    switch diskRepresentation.count {
    case 0:
      self = .none
    case 1:
      self = .some(try Wrapped(diskRepresentation: diskRepresentation[0]))
    default:
      throw DecodingError.dataCorrupted(
        .init(codingPath: [], debugDescription: "An optional disk value must have at most one element."))
    }
  }
}

extension Array: DiskBTreeValue where Element: DiskBTreeValue {
  package static var diskSchemaID: String { "swift:Array:\(Element.diskSchemaID)" }

  package var diskRepresentation: [Element.DiskRepresentation] {
    map(\.diskRepresentation)
  }

  package init(diskRepresentation: [Element.DiskRepresentation]) throws {
    self = try diskRepresentation.map { try Element(diskRepresentation: $0) }
  }
}

extension Dictionary: DiskBTreeValue where Key: DiskBTreeValue, Value: DiskBTreeValue {
  package static var diskSchemaID: String {
    let key = Key.diskSchemaID
    return "swift:Dictionary:\(key.utf8.count):\(key)\(Value.diskSchemaID)"
  }

  package var diskRepresentation: [DiskDictionaryKey<Key>: Value.DiskRepresentation] {
    var representation = [DiskDictionaryKey<Key>: Value.DiskRepresentation](minimumCapacity: count)
    for (key, value) in self {
      representation.updateValue(value.diskRepresentation, forKey: DiskDictionaryKey(value: key))
    }
    return representation
  }

  package init(diskRepresentation: [DiskDictionaryKey<Key>: Value.DiskRepresentation]) throws {
    self.init(minimumCapacity: diskRepresentation.count)
    for (key, representation) in diskRepresentation {
      updateValue(try Value(diskRepresentation: representation), forKey: key.value)
    }
  }
}

package struct DiskDictionaryKey<Key: DiskBTreeValue & Hashable>: Codable, Hashable, Sendable {
  let value: Key

  init(value: Key) {
    self.value = value
  }

  package init(from decoder: any Decoder) throws {
    value = try Key(diskRepresentation: Key.DiskRepresentation(from: decoder))
  }

  package func encode(to encoder: any Encoder) throws {
    try value.diskRepresentation.encode(to: encoder)
  }
}
