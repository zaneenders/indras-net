import NIOCore

enum WireProtocol: Sendable {
  static let magic: UInt8 = 0x66
  static let version: UInt8 = 0
  static let headerLength: Int = 8
  static let defaultMaxPayloadLength: UInt32 = .max
}
