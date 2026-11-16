import Foundation
package import NIOCore
import Zlib

package let gzipChunkFeature = "gzip-chunks-v1"
public let defaultChunkSize = 8_388_608

package enum ChunkContentEncoding: String, Codable, Sendable {
  case identity
  case gzip

  package init(header: String?) throws(TransferError) {
    let value = header?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    switch value {
    case nil, "", "identity": self = .identity
    case "gzip": self = .gzip
    default: throw TransferError("Unsupported chunk content encoding")
    }
  }
}

package func maximumGzipChunkSize(_ originalLength: Int) -> Int {
  // Automatic encoding never exceeds stored DEFLATE blocks plus the gzip wrapper.
  originalLength + (originalLength / 65_535 + 1) * 5 + 18
}

@concurrent
package func encodeChunk(_ source: ByteBuffer) async throws(any Error) -> ByteBuffer {
  let encoded = try ZLib.deflate(
    input: source.readableBytesUInt8Span, format: .gzip, strategy: .automatic)
  var buffer = ByteBuffer()
  buffer.writeBytes(encoded.span.bytes)
  return buffer
}

@concurrent
package func decodeChunk(
  _ source: ByteBuffer, encoding: ChunkContentEncoding, expectedLength: Int
) async throws(any Error) -> ByteBuffer {
  switch encoding {
  case .identity:
    guard source.readableBytes == expectedLength else {
      throw TransferError("Incorrect chunk content length")
    }
    return source
  case .gzip:
    guard source.readableBytes > 0, source.readableBytes <= maximumGzipChunkSize(expectedLength) else {
      throw TransferError("Incorrect compressed chunk content length")
    }
    let decoded = try ZLib.inflate(
      input: source.readableBytesUInt8Span, format: .gzip, maximumOutputSize: expectedLength)
    guard decoded.count == expectedLength else {
      throw TransferError("Incorrect decompressed chunk length")
    }
    var buffer = ByteBuffer()
    buffer.writeBytes(decoded.span.bytes)
    return buffer
  }
}

package func acceptsGzip(_ header: String?) -> Bool {
  guard let header else { return false }
  for value in header.split(separator: ",") {
    let parameters = value.split(separator: ";")
    guard parameters.first?.trimmingCharacters(in: .whitespaces).lowercased() == "gzip" else {
      continue
    }
    for parameter in parameters.dropFirst() {
      let parameter = parameter.trimmingCharacters(in: .whitespaces).lowercased()
      if parameter.hasPrefix("q=") {
        return (Double(parameter.dropFirst(2)) ?? 0) > 0
      }
    }
    return true
  }
  return false
}
