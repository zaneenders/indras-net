import NIOCore

struct Message: Sendable, Equatable {
  var type: MessageType
  var payload: ByteBuffer

  var payloadString: String {
    var copy = payload
    return copy.readString(length: copy.readableBytes) ?? ""
  }

  init(
    type: MessageType,
    payload: ByteBuffer
  ) {
    self.type = type
    self.payload = payload
  }
}
