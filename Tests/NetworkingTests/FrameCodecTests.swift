import Networking
import NIOCore
import NIOEmbedded
import Testing

@Suite
struct FrameCodecTests {
  // A hand-written fixture, independent of the production encoder.
  private static let wire: [UInt8] = [
    0, 0, 0, 3,  // payload length
    1, 2, 3, 4,  // target
    0x80, 0x90, 0xA0, 0xB0,  // opcode
    0, 0x80, 0xFF  // opaque payload
  ]

  private static var frame: Frame {
    Frame(target: 0x0102_0304, opcode: 0x8090_A0B0, payload: ByteBuffer(bytes: [0, 0x80, 0xFF]))
  }

  @Test func encoderMatchesWireFixtureAndUsesOnlyReadableBytes() throws {
    var payload = ByteBuffer(bytes: [0xEE, 0, 0x80, 0xFF])
    payload.moveReaderIndex(forwardBy: 1)
    let input = Frame(target: 0x0102_0304, opcode: 0x8090_A0B0, payload: payload)
    var output = ByteBuffer()
    try FrameEncoder().encode(data: input, out: &output)
    #expect(Array(output.readableBytesView) == Self.wire)
    #expect(input.payload.readerIndex == 1)
  }

  @Test func decoderMatchesIndependentWireFixture() throws {
    let channel = EmbeddedChannel(
      handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    try channel.writeInbound(ByteBuffer(bytes: Self.wire))
    #expect(try channel.readInbound(as: Frame.self) == Self.frame)
    #expect(try channel.readInbound(as: Frame.self) == nil)
  }

  @Test(arguments: 1..<15)
  func everyHeaderAndBodySplitIsReassembled(split: Int) throws {
    let channel = EmbeddedChannel(
      handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    try channel.writeInbound(ByteBuffer(bytes: Self.wire.prefix(split)))
    #expect(try channel.readInbound(as: Frame.self) == nil)
    try channel.writeInbound(ByteBuffer(bytes: Self.wire.dropFirst(split)))
    #expect(try channel.readInbound(as: Frame.self) == Self.frame)
    #expect(try channel.readInbound(as: Frame.self) == nil)
  }

  @Test func coalescedFramesAndByteAtATimeReadsPreserveOrder() throws {
    for fragmentSize in [1, Self.wire.count * 2] {
      let channel = EmbeddedChannel(
        handler: ByteToMessageHandler(FrameDecoder()))
      defer { _ = try? channel.finish() }
      let wire = Self.wire + Self.wire
      for start in stride(from: 0, to: wire.count, by: fragmentSize) {
        try channel.writeInbound(ByteBuffer(bytes: wire[start..<min(start + fragmentSize, wire.count)]))
      }
      #expect(try channel.readInbound(as: Frame.self) == Self.frame)
      #expect(try channel.readInbound(as: Frame.self) == Self.frame)
      #expect(try channel.readInbound(as: Frame.self) == nil)
    }
  }

  @Test func emptyPayloadAndUninterpretedMaximumIdentifiersAreValid() throws {
    let channel = EmbeddedChannel(handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    // A zero-length frame with target and opcode both UInt32.max.
    try channel.writeInbound(ByteBuffer(bytes: [0, 0, 0, 0] + Array(repeating: 0xFF, count: 8)))
    #expect(try channel.readInbound(as: Frame.self) == Frame(target: .max, opcode: .max, payload: ByteBuffer()))
  }

  @Test(arguments: [0, 3, 65_536, 1_048_577])
  func variedPayloadSizesAreAccepted(bytes: Int) throws {
    let frame = Frame(target: 1, opcode: 2, payload: ByteBuffer(repeating: 0xAB, count: bytes))
    var wire = ByteBuffer()
    try FrameEncoder().encode(data: frame, out: &wire)
    #expect(wire.readableBytes == Frame.headerBytes + bytes)
    let channel = EmbeddedChannel(handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    try channel.writeInbound(wire)
    #expect(try channel.readInbound(as: Frame.self) == frame)
  }

  @Test(arguments: [UInt32(4), UInt32.max])
  func announcedLengthWaitsForItsPayload(length: UInt32) throws {
    let channel = EmbeddedChannel(handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    var wire = ByteBuffer()
    wire.writeInteger(length, endianness: .big)
    wire.writeInteger(UInt32(1), endianness: .big)
    wire.writeInteger(UInt32(2), endianness: .big)
    try channel.writeInbound(wire)
    #expect(try channel.readInbound(as: Frame.self) == nil)
    #expect(throws: FrameError.truncatedFrame) { try channel.finish() }
  }

  @Test(arguments: [1, 3, 4, 8, 11, 12, 14])
  func eofWithinAHeaderOrPayloadIsAnError(prefix: Int) throws {
    let channel = EmbeddedChannel(
      handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    try channel.writeInbound(ByteBuffer(bytes: Self.wire.prefix(prefix)))
    #expect(throws: FrameError.truncatedFrame) { try channel.finish() }
    #expect(try channel.readInbound(as: Frame.self) == nil)
  }

  @Test func eofAfterACompleteFrameIsClean() throws {
    let channel = EmbeddedChannel(
      handler: ByteToMessageHandler(FrameDecoder()))
    try channel.writeInbound(ByteBuffer(bytes: Self.wire))
    #expect(try channel.readInbound(as: Frame.self) == Self.frame)
    #expect(try channel.finish().isClean)
  }

  @Test func aCompleteFrameBeforeATruncatedFrameRemainsAvailable() throws {
    let channel = EmbeddedChannel(
      handler: ByteToMessageHandler(FrameDecoder()))
    defer { _ = try? channel.finish() }
    try channel.writeInbound(ByteBuffer(bytes: Self.wire + Self.wire.prefix(13)))
    #expect(try channel.readInbound(as: Frame.self) == Self.frame)
    #expect(throws: FrameError.truncatedFrame) { try channel.finish() }
    #expect(try channel.readInbound(as: Frame.self) == nil)
  }
}
