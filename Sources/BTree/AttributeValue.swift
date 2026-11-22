package import struct Foundation.Data

package typealias Document = [String: AttributeValue]

package enum AttributeValue: DiskBTreeValue, Equatable {
  package static let diskSchemaID = "indras:attribute-value:v1"

  case string(String)
  case bool(Bool)
  case null
  case int64(Int64)
  case uint64(UInt64)
  case float32(Float)
  case float64(Double)
  case bytes(Data)
  case list([AttributeValue])
  case map(Document)

  // These tags are part of the v1 encoding.
  private enum CodingKeys: String, CodingKey {
    case string = "s"
    case bool = "b"
    case null = "n"
    case int64 = "i"
    case uint64 = "u"
    case float32 = "f"
    case float64 = "d"
    case bytes = "x"
    case list = "l"
    case map = "m"
  }
}
