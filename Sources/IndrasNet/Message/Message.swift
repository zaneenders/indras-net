import NIOCore

// NOTE: I think we could have transport level message and user messages inside of that kinda like TCP/IP nesting
struct Message: Sendable, Equatable {
  var type: MessageType
  var payload: ByteBuffer

  static let headerLength: Int = 6
  static let defaultMaxPayloadLength: UInt32 = UInt32(UInt16.max)

  init(
    type: MessageType,
    payload: ByteBuffer
  ) {
    self.type = type
    self.payload = payload
  }
}
