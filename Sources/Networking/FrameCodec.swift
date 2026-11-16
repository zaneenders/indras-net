public import NIOCore

/// Encodes a 12-byte header followed by the payload's readable bytes.
/// All header fields are unsigned 32-bit integers in network (big-endian) order:
/// payload length, target, opcode. The length excludes the header.
public struct FrameEncoder: MessageToByteEncoder, Sendable {
  public typealias OutboundIn = Frame

  public init() {}

  public func encode(data: Frame, out: inout ByteBuffer) throws {
    guard let count = UInt32(exactly: data.payload.readableBytes) else {
      throw FrameError.payloadLengthOverflow
    }
    out.writeInteger(count, endianness: .big)
    out.writeInteger(data.target, endianness: .big)
    out.writeInteger(data.opcode, endianness: .big)
    out.writeImmutableBuffer(data.payload)
  }
}

/// Reassembles frames independently of socket read boundaries.
public struct FrameDecoder: ByteToMessageDecoder, Sendable {
  public typealias InboundOut = Frame

  public init() {}

  public mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
    guard let count = buffer.getInteger(at: buffer.readerIndex, endianness: .big, as: UInt32.self) else {
      return .needMoreData
    }
    guard buffer.readableBytes >= Frame.headerBytes + Int(count) else {
      return .needMoreData
    }

    // The complete frame was checked above; every read is now within its bounds.
    buffer.moveReaderIndex(forwardBy: 4)
    let target = buffer.readInteger(endianness: .big, as: UInt32.self)!
    let opcode = buffer.readInteger(endianness: .big, as: UInt32.self)!
    let payload = buffer.readSlice(length: Int(count))!
    context.fireChannelRead(wrapInboundOut(Frame(target: target, opcode: opcode, payload: payload)))
    return .continue
  }

  public mutating func decodeLast(
    context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool
  ) throws -> DecodingState {
    let state = try decode(context: context, buffer: &buffer)
    if state == .needMoreData, seenEOF, buffer.readableBytes > 0 {
      throw FrameError.truncatedFrame
    }
    return state
  }
}
