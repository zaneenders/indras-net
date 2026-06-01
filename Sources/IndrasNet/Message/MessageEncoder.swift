import NIOCore

struct MessageEncoder: MessageToByteEncoder {
  typealias OutboundIn = Message

  init() {}

  func encode(data message: Message, out: inout ByteBuffer) throws {
    let payloadLength = UInt32(message.payload.readableBytes)
    out.writeInteger(WireProtocol.magic)
    out.writeInteger(WireProtocol.version)
    out.writeInteger(message.type.rawValue)
    out.writeInteger(payloadLength)
    var payload = message.payload
    out.writeBuffer(&payload)
  }
}
