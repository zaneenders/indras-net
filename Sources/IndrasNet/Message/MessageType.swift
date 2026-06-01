struct MessageType: RawRepresentable, Hashable, Sendable {
  let rawValue: UInt16

  init(rawValue: UInt16) {
    self.rawValue = rawValue
  }

  static let hello = MessageType(rawValue: 0x0001)
  static let ping = MessageType(rawValue: 0x0002)
  static let pong = MessageType(rawValue: 0x0003)

  private static let names: [MessageType: String] = [
    .hello: "hello",
    .ping: "ping",
    .pong: "pong",
  ]

  private static let byName: [String: MessageType] = [
    "hello": .hello,
    "ping": .ping,
    "pong": .pong,
  ]

  var name: String {
    if let name = Self.names[self] { return name }
    let hex = String(rawValue, radix: 16)
    return "0x" + String(repeating: "0", count: max(0, 4 - hex.count)) + hex
  }

  init?(name: String) {
    guard let type = Self.byName[name.lowercased()] else { return nil }
    self = type
  }
}

extension MessageType: Codable {
  init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    self.init(rawValue: try container.decode(UInt16.self))
  }

  func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(rawValue)
  }
}
