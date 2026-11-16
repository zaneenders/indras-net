import Foundation
import NIOCore
import Testing

@testable import Setu

struct CompressionTests: Sendable {
  @Test func gzipAlwaysEncodesAndRoundTripsCompressibleAndRandomChunks() async throws(any Error) {
    var randomState: UInt32 = 0xC0FFEE
    let random = (0..<65_536).map { _ -> UInt8 in
      randomState ^= randomState << 13
      randomState ^= randomState >> 17
      randomState ^= randomState << 5
      return UInt8(truncatingIfNeeded: randomState)
    }
    for original in [Data(repeating: 0x41, count: 1_048_576), Data(random)] {
      let source = ByteBuffer(bytes: original)
      let encoded = try await encodeChunk(source)
      #expect(encoded.readableBytesView.starts(with: [0x1F, 0x8B]))
      #expect(encoded.readableBytes <= maximumGzipChunkSize(source.readableBytes))
      let decoded = try await decodeChunk(encoded, encoding: .gzip, expectedLength: source.readableBytes)
      #expect(Data(decoded.readableBytesView) == original)
    }
  }

  @Test func gzipEnforcesOriginalLengthAndRejectsInvalidStreams() async throws(any Error) {
    let source = ByteBuffer(bytes: Data(repeating: 0x41, count: 65_536))
    let encoded = try await encodeChunk(source)
    await #expect(throws: (any Error).self) {
      _ = try await decodeChunk(encoded, encoding: .gzip, expectedLength: 32_768)
    }
    await #expect(throws: (any Error).self) {
      _ = try await decodeChunk(
        ByteBuffer(bytes: [0x1F, 0x8B, 0]), encoding: .gzip, expectedLength: 65_536)
    }
  }

  @Test func parsesGzipNegotiation() throws(any Error) {
    #expect(acceptsGzip("gzip"))
    #expect(acceptsGzip("identity, gzip; q=0.5"))
    #expect(!acceptsGzip("gzip; q=0"))
    #expect(!acceptsGzip("gzip; Q=0"))
    #expect(!acceptsGzip("identity"))
    #expect(try ChunkContentEncoding(header: nil) == .identity)
    #expect(try ChunkContentEncoding(header: "GZip") == .gzip)
    #expect(throws: TransferError.self) { try ChunkContentEncoding(header: "br") }
  }
}
