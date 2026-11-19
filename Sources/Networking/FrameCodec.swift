public import NIOCore

/// Encodes a 12-byte header followed by the payload's readable bytes.
/// All header fields are unsigned 32-bit integers in network (big-endian) order:
/// payload length, target, opcode. The length excludes the header.
public struct FrameEncoder: MessageToByteEncoder, Sendable {
  public typealias OutboundIn = Frame
  private let maximumPayloadBytes: UInt32

  public init(maximumPayloadBytes: UInt32) {
    self.maximumPayloadBytes = maximumPayloadBytes
  }

  public func encode(data: Frame, out: inout ByteBuffer) throws {
    guard let count = UInt32(exactly: data.payload.readableBytes), count <= maximumPayloadBytes else {
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

  /// An application can narrow the receive limit for the rest of the socket when a target first appears.
  private let connectionLimits: [UInt32: UInt32]
  private var maximum: UInt32

  public init(maximumPayloadBytes: UInt32, connectionLimits: [UInt32: UInt32]) {
    self.maximum = maximumPayloadBytes
    self.connectionLimits = connectionLimits
  }

  public mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
    guard let count = buffer.getInteger(at: buffer.readerIndex, endianness: .big, as: UInt32.self) else {
      return .needMoreData
    }
    guard count <= maximum else { throw FrameError.payloadLengthOverflow }
    guard let target = buffer.getInteger(at: buffer.readerIndex + 4, endianness: .big, as: UInt32.self) else {
      return .needMoreData
    }
    if let limit = connectionLimits[target] {
      maximum = min(maximum, limit)
      guard count <= maximum else { throw FrameError.payloadLengthOverflow }
    }
    guard UInt64(buffer.readableBytes) >= UInt64(Frame.headerBytes) + UInt64(count) else {
      return .needMoreData
    }

    // The complete frame was checked above; every read is now within its bounds.
    buffer.moveReaderIndex(forwardBy: 8)
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
