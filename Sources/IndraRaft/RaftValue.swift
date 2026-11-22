package import BTree
import Foundation

package enum RaftValue: Codable, Sendable, Equatable {
  case string(String)
  case document(Document)

  // Text keeps its original wire and disk encoding, including log receipt digests.
  package init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if let text = try? container.decode(String.self) {
      self = .string(text)
    } else {
      self = .document(try container.decode(Document.self))
    }
  }

  package func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .string(let text): try container.encode(text)
    case .document(let document): try container.encode(document)
    }
  }

  package init(documentJSON: String) throws {
    let entries = try JSONDecoder().decode([String: JSONAttribute].self, from: Data(documentJSON.utf8))
    self = .document(entries.mapValues(\.value))
  }

  package func formatted(asJSON: Bool) throws -> String {
    if case .string(let text) = self, !asJSON { return text }
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    let bytes: Data
    switch self {
    case .string(let text): bytes = try encoder.encode(text)
    case .document(let document): bytes = try encoder.encode(document.mapValues(JSONAttribute.init(value:)))
    }
    return String(decoding: bytes, as: UTF8.self)
  }
}

private struct JSONAttribute: Codable {
  let value: AttributeValue

  init(value: AttributeValue) { self.value = value }

  init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if container.decodeNil() { value = .null }
    else if let text = try? container.decode(String.self) { value = .string(text) }
    else if let flag = try? container.decode(Bool.self) { value = .bool(flag) }
    else if let integer = try? container.decode(Int64.self) { value = .int64(integer) }
    else if let integer = try? container.decode(UInt64.self) { value = .uint64(integer) }
    else if let number = try? container.decode(Double.self) { value = .float64(number) }
    else if let list = try? container.decode([Self].self) { value = .list(list.map(\.value)) }
    else { value = .map(try container.decode([String: Self].self).mapValues(\.value)) }
  }

  func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    switch value {
    case .string(let text): try container.encode(text)
    case .bool(let flag): try container.encode(flag)
    case .null: try container.encodeNil()
    case .int64(let integer): try container.encode(integer)
    case .uint64(let integer): try container.encode(integer)
    case .float32(let number): try container.encode(number)
    case .float64(let number): try container.encode(number)
    case .bytes(let bytes): try container.encode(bytes.base64EncodedString())
    case .list(let list): try container.encode(list.map(Self.init(value:)))
    case .map(let document): try container.encode(document.mapValues(Self.init(value:)))
    }
  }
}
