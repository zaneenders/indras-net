import NIOCore

public enum WireProtocol: Sendable {
  public static let magic: UInt8 = 0x66
  public static let version: UInt8 = 0
  public static let headerLength: Int = 8
  public static let defaultMaxPayloadLength: UInt32 = .max
}
