import NIOCore

public struct Message: Sendable, Equatable {
  public var type: MessageType
  public var payload: ByteBuffer

  public var payloadString: String {
    var copy = payload
    return copy.readString(length: copy.readableBytes) ?? ""
  }

  public init(
    type: MessageType,
    payload: ByteBuffer
  ) {
    self.type = type
    self.payload = payload
  }
}
