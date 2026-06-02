import NIOCore

enum WireProtocol: Sendable {
  static let magic: UInt8 = 0x66
  static let version: UInt8 = 0
  /// Bytes exchanged once per connection (`magic` + `version`) ahead of any
  /// framed message. Validated once by `ProtocolPreambleHandler`, then never
  /// sent again — protocol/version compatibility is a property of the
  /// connection, not of every message.
  static let preambleLength: Int = 2
  /// Per-message framing header: type (`UInt16`) + payload length (`UInt32`).
  static let headerLength: Int = 6
  static let defaultMaxPayloadLength: UInt32 = .max
}
