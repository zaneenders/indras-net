import NIOCore
import NIOEmbedded
import Testing

@testable import IndrasNet

@Suite struct MessageCodecTests {
  @Test func encodeDecodeRoundTripPreservesFields() throws {
    var payload = ByteBuffer()
    payload.writeString("hello-wire")
    let original = Message(
      type: .ping,
      payload: payload
    )

    let decoded = try decodeInbound(original.encodeToByteBuffer())

    #expect(decoded == original)
  }

  @Test func encodeDecodeRoundTripHelloPayloadBody() throws {
    var payload = ByteBuffer()
    payload.writeString("ok")
    let original = Message(type: .hello, payload: payload)

    let decoded = try decodeInbound(original.encodeToByteBuffer())

    #expect(decoded.type == .hello)
    var body = decoded.payload
    #expect(body.readString(length: body.readableBytes) == "ok")
  }

  @Test func encodeDecodeRoundTripAllMessageTypes() throws {
    for type in [MessageType.hello, .ping, .pong] {
      var payload = ByteBuffer()
      payload.writeInteger(type.rawValue)
      let original = Message(type: type, payload: payload)
      let decoded = try decodeInbound(original.encodeToByteBuffer())
      #expect(decoded.type == type)
      #expect(decoded.payload == original.payload)
    }
  }

  @Test func encodedWireFormatMatchesProtocolLayout() throws {
    var payload = ByteBuffer()
    payload.writeString("x")
    let message = Message(
      type: .pong,
      payload: payload
    )
    var wire = message.encodeToByteBuffer()

    #expect(wire.readableBytes == WireProtocol.headerLength + 1)
    #expect(wire.readInteger(as: UInt8.self) == WireProtocol.magic)
    #expect(wire.readInteger(as: UInt8.self) == WireProtocol.version)
    #expect(wire.readInteger(as: UInt16.self) == MessageType.pong.rawValue)
    #expect(wire.readInteger(as: UInt32.self) == 1)
    #expect(wire.readString(length: 1) == "x")
  }

  @Test func decoderWaitsForFullFrameBeforeEmitting() throws {
    let wire = Message(type: .hello, payload: ByteBuffer()).encodeToByteBuffer()
    let partial = wire.getSlice(at: wire.readerIndex, length: WireProtocol.headerLength - 1)!

    let channel = try makeCodecChannel()
    try channel.writeInbound(partial)
    #expect(try channel.readInbound(as: Message.self) == nil)

    try channel.writeInbound(wire.getSlice(at: wire.readerIndex + WireProtocol.headerLength - 1, length: 1)!)
    #expect(try channel.readInbound(as: Message.self) != nil)
  }

  @Test func decodeRejectsInvalidMagic() throws {
    var wire = validHeaderWire(type: .hello, payloadLength: 0)
    wire.setInteger(0x00, at: wire.readerIndex, as: UInt8.self)

    #expect(throws: MessageDecodeError.invalidMagic(got: 0x00)) {
      _ = try decodeInbound(wire)
    }
  }

  @Test func decodeRejectsUnsupportedVersion() throws {
    var wire = validHeaderWire(type: .hello, payloadLength: 0)
    wire.setInteger(UInt8(99), at: wire.readerIndex + 1, as: UInt8.self)

    #expect(throws: MessageDecodeError.unsupportedVersion(got: 99, expected: WireProtocol.version)) {
      _ = try decodeInbound(wire)
    }
  }

  @Test func decodeAcceptsUnknownMessageTypeForExtensibility() throws {
    // Unknown type bytes round-trip rather than being rejected, so new protocols
    // (e.g. Raft RPCs) can be mixed in without touching the framing layer.
    let wire = validHeaderWire(typeRaw: 0xFFFF, payloadLength: 0)

    let decoded = try decodeInbound(wire)

    #expect(decoded.type == MessageType(rawValue: 0xFFFF))
  }

  @Test func decodeLastRejectsPartialFrameOnClose() throws {
    var wire = ByteBuffer()
    wire.writeInteger(WireProtocol.magic)
    wire.writeInteger(WireProtocol.version)
    wire.writeInteger(UInt16(0x0001))

    let channel = try makeCodecChannel()
    try channel.writeInbound(wire)

    #expect(throws: MessageDecodeError.incompleteMessageOnClose(remainingBytes: 4)) {
      _ = try channel.finish(acceptAlreadyClosed: false)
    }
  }

  @Test func decodeRejectsPayloadLengthAboveMax() throws {
    let max: UInt32 = 64
    let wire = validHeaderWire(type: .hello, payloadLength: max + 1)

    #expect(throws: MessageDecodeError.messageTooLarge(length: max + 1, max: max)) {
      _ = try decodeInbound(wire, maxPayloadLength: max)
    }
  }

  @Test func decodeLastRejectsTrailingBytesOnClose() throws {
    var wire = Message(type: .hello, payload: ByteBuffer()).encodeToByteBuffer()
    wire.writeInteger(UInt8(0xFF))

    let channel = try makeCodecChannel()
    try channel.writeInbound(wire)
    _ = try channel.readInbound(as: Message.self)

    #expect(throws: MessageDecodeError.incompleteMessageOnClose(remainingBytes: 1)) {
      _ = try channel.finish(acceptAlreadyClosed: false)
    }
  }
}

extension MessageCodecTests {
  private func makeCodecChannel(maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength) throws
    -> EmbeddedChannel
  {
    let channel = EmbeddedChannel()
    try channel.pipeline.syncOperations.addHandler(
      ByteToMessageHandler(MessageDecoder(maxPayloadLength: maxPayloadLength))
    )
    return channel
  }

  private func decodeInbound(
    _ wire: ByteBuffer,
    maxPayloadLength: UInt32 = WireProtocol.defaultMaxPayloadLength
  ) throws -> Message {
    let channel = try makeCodecChannel(maxPayloadLength: maxPayloadLength)
    try channel.writeInbound(wire)
    return try #require(try channel.readInbound(as: Message.self))
  }

  private func validHeaderWire(
    type: MessageType,
    payloadLength: UInt32
  ) -> ByteBuffer {
    validHeaderWire(typeRaw: type.rawValue, payloadLength: payloadLength)
  }

  private func validHeaderWire(
    typeRaw: UInt16,
    payloadLength: UInt32
  ) -> ByteBuffer {
    var buffer = ByteBuffer()
    buffer.writeInteger(WireProtocol.magic)
    buffer.writeInteger(WireProtocol.version)
    buffer.writeInteger(typeRaw)
    buffer.writeInteger(payloadLength)
    return buffer
  }
}

extension Message {
  func encodeToByteBuffer(allocator: ByteBufferAllocator = ByteBufferAllocator()) -> ByteBuffer {
    var buffer = allocator.buffer(capacity: WireProtocol.headerLength + payload.readableBytes)
    let encoder = MessageEncoder()
    try? encoder.encode(data: self, out: &buffer)
    return buffer
  }
}
