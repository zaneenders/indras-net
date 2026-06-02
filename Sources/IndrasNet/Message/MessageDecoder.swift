import NIOCore

struct MessageDecoder: ByteToMessageDecoder {
  typealias InboundOut = Message

  private let maxPayloadLength: UInt32

  init(maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength) {
    self.maxPayloadLength = maxPayloadLength
  }

  mutating func decode(
    context: ChannelHandlerContext,
    buffer: inout ByteBuffer
  ) throws -> DecodingState {
    guard let message = try decodeNextMessage(buffer: &buffer) else {
      return .needMoreData
    }
    context.fireChannelRead(wrapInboundOut(message))
    return .continue
  }

  mutating func decodeLast(
    context: ChannelHandlerContext,
    buffer: inout ByteBuffer,
    seenEOF: Bool
  ) throws -> DecodingState {
    while try decode(context: context, buffer: &buffer) == .continue {}
    if buffer.readableBytes > 0 {
      throw MessageDecodeError.incompleteMessageOnClose(remainingBytes: buffer.readableBytes)
    }
    return .needMoreData
  }

  private mutating func decodeNextMessage(buffer: inout ByteBuffer) throws -> Message? {
    guard buffer.readableBytes >= WireProtocol.headerLength else {
      return nil
    }

    guard var header = buffer.getSlice(at: buffer.readerIndex, length: WireProtocol.headerLength) else {
      return nil
    }
    let parsed = try parseHeader(&header)

    guard parsed.payloadLength <= maxPayloadLength else {
      throw MessageDecodeError.messageTooLarge(length: parsed.payloadLength, max: maxPayloadLength)
    }

    let totalLength = WireProtocol.headerLength + Int(parsed.payloadLength)
    guard buffer.readableBytes >= totalLength else {
      return nil
    }

    _ = buffer.readSlice(length: WireProtocol.headerLength)
    let payload = buffer.readSlice(length: Int(parsed.payloadLength)) ?? ByteBuffer()

    return Message(
      type: parsed.type,
      payload: payload
    )
  }

  private func parseHeader(_ header: inout ByteBuffer) throws -> ParsedHeader {
    // Magic/version are validated once per connection by `ProtocolPreambleHandler`,
    // so the per-message header is just type + payload length.
    let typeRaw = try header.readRequiredInteger(as: UInt16.self)
    let payloadLength = try header.readRequiredInteger(as: UInt32.self)
    return ParsedHeader(type: MessageType(rawValue: typeRaw), payloadLength: payloadLength)
  }
}

extension ByteBuffer {
  fileprivate mutating func readRequiredInteger<T: FixedWidthInteger>(as type: T.Type) throws -> T {
    guard let value = readInteger(as: type) else {
      throw MessageDecodeError.truncatedHeader(remainingBytes: readableBytes)
    }
    return value
  }
}
