import NIOCore

enum WireProtocol: Sendable {
  static let magic: UInt8 = 0x66
  static let version: UInt8 = 0
  static let headerLength: Int = 6
  static let defaultMaxPayloadLength: UInt32 = UInt32(UInt16.max)
}
