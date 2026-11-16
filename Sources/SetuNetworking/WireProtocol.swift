import Foundation
import NIOCore
import Networking
import Setu

enum TransferRequest: Codable, Sendable {
  case health
  case restart
  case list
  case manifest(fileID: String, chunkSize: Int)
  case begin(manifest: Manifest, destination: String?, createDestination: Bool, owner: UUID)
  case uploadChunk(id: String, index: Int, encoding: ChunkContentEncoding)
  case downloadChunk(name: String, index: Int, chunkSize: Int, length: Int)
  case commit(id: String)
  case discard(id: String, owner: UUID)

  var retryable: Bool {
    switch self {
    case .health, .list, .manifest, .begin, .uploadChunk, .downloadChunk: true
    case .restart, .commit, .discard: false
    }
  }
}

enum TransferResponse: Codable, Sendable {
  case health(UUID)
  case restart(RestartAcknowledgement)
  case files([FileEntry])
  case manifest(Manifest)
  case status(UploadStatus)
  case chunk(encoding: ChunkContentEncoding, length: Int)
  case acknowledged
  case failure(String)
}

private struct Envelope<Value: Codable & Sendable>: Codable, Sendable {
  let version: UInt32
  let value: Value
}

enum SetuWire {
  static let service: UInt32 = 0x5345_5455
  static let control: UInt32 = 0x5345_5443
  static let version: UInt32 = 2
  static let request: UInt32 = 1
  static let response: UInt32 = 2
  static let chunk: UInt32 = 3
  static let heartbeat: UInt32 = 4

  static func isHeartbeat(_ frame: Frame) -> Bool {
    frame.target == service && frame.opcode == heartbeat && frame.payload.readableBytes == 0
  }

  static func encode<Value: Codable & Sendable>(_ value: Value, target: UInt32, opcode: UInt32) throws -> Frame {
    Frame(
      target: target, opcode: opcode,
      payload: ByteBuffer(bytes: try jsonData(Envelope(version: version, value: value))))
  }

  static func decode<Value: Codable & Sendable>(
    _ frame: Frame, target: UInt32, opcode: UInt32, as type: Value.Type
  ) throws -> Value {
    guard frame.target == target, frame.opcode == opcode else { throw TransferError("Unexpected Setu frame") }
    let envelope = try JSONDecoder().decode(Envelope<Value>.self, from: Data(frame.payload.readableBytesView))
    guard envelope.version == version else { throw TransferError("Unsupported Setu protocol version") }
    return envelope.value
  }

  static func bytes(_ payload: ByteBuffer) -> Frame {
    Frame(target: service, opcode: chunk, payload: payload)
  }

  static func payload(_ frame: Frame) throws -> ByteBuffer {
    guard frame.target == service, frame.opcode == chunk else { throw TransferError("Expected Setu chunk") }
    return frame.payload
  }
}

enum PeerTransportError: Error, LocalizedError, Sendable {
  case disconnected
  case timedOut
  case closed

  var errorDescription: String? {
    switch self {
    case .disconnected: "Setu peer disconnected before acknowledging the request"
    case .timedOut: "Setu peer request timed out"
    case .closed: "Setu client is closed"
    }
  }
}

func withDeadline<Value: Sendable>(
  timeout: Duration, operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { tasks in
    tasks.addTask { try await operation() }
    tasks.addTask {
      try await Task.sleep(for: timeout)
      throw PeerTransportError.timedOut
    }
    defer { tasks.cancelAll() }
    return try await tasks.next()!
  }
}
