import Foundation
import NIOCore
import Networking

struct RemoteInvocation: Codable, Sendable {
  let requestID: UUID
  let actorID: ActorAddress
  let actorType: String
  let target: String
  let arguments: [Data]
  let substitutions: [String]
  let errorTypeName: String?
  let returnTypeName: String?
}

enum CallResult: Codable, Sendable {
  case value(Data)
  case void
  case systemError(RPCError)
  case codedError(code: String, detail: String?)
  case applicationError(ApplicationErrorReply)
}

struct ApplicationErrorReply: Codable, Sendable {
  let type: String
  let message: String
  let mangledTypeName: String?
  let payload: Data?
}

extension ApplicationErrorReply {
  init(error: any Error, encoder: JSONEncoder) {
    let errorType = Swift.type(of: error)
    self.type = String(reflecting: errorType)
    self.message = String(describing: error)
    if let error = error as? any CodableRPCError,
      let name = _mangledTypeName(errorType),
      let bytes = try? encoder.encode(error)
    {
      self.mangledTypeName = name
      self.payload = bytes
    } else {
      self.mangledTypeName = nil
      self.payload = nil
    }
  }

  init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.init(
      type: try container.decode(String.self, forKey: .type),
      message: try container.decode(String.self, forKey: .message),
      mangledTypeName: try? container.decodeIfPresent(String.self, forKey: .mangledTypeName),
      payload: try? container.decodeIfPresent(Data.self, forKey: .payload))
  }
}

enum WireMessage: Codable, Sendable {
  static let version = 1

  static let codedErrors = "rpc-error-codes-v1"

  case hello(version: Int, endpoint: ActorEndpoint, capabilities: [String]?)
  case call(RemoteInvocation)
  case reply(UUID, CallResult)

  static func reply(to id: UUID, result: CallResult, supportsErrorCodes: Bool) -> Self {
    guard case .systemError(let error) = result else { return .reply(id, result) }
    if supportsErrorCodes {
      let (code, detail) = error.wireCode
      return .reply(id, .codedError(code: code, detail: detail))
    }
    switch error {
    case .invalidConfiguration, .shutDown, .disconnected, .timedOut, .unknownActor, .actorTypeMismatch, .invalidMessage,
      .unknownType:
      return .reply(id, result)
    default:
      let fallback = ApplicationErrorReply(
        type: String(reflecting: RPCError.self), message: String(describing: error), mangledTypeName: nil, payload: nil)
      return .reply(id, .applicationError(fallback))
    }
  }

  func frame() throws -> Frame {
    let encoder = JSONEncoder()
    var data = try encoder.encode(self)
    if data.count > ActorFraming.maximumPayloadBytes,
      case .reply(let id, .applicationError(let error)) = self, error.payload != nil
    {
      let fallback = ApplicationErrorReply(type: error.type, message: error.message, mangledTypeName: nil, payload: nil)
      data = try encoder.encode(Self.reply(id, .applicationError(fallback)))
    }
    guard data.count <= ActorFraming.maximumPayloadBytes else { throw FrameError.payloadLengthOverflow }
    return Frame(target: 0, opcode: 0, payload: ByteBuffer(bytes: data))
  }

  init(frame: Frame) throws {
    guard frame.target == 0, frame.opcode == 0 else { throw RPCError.invalidMessage }
    self = try JSONDecoder().decode(Self.self, from: Data(frame.payload.readableBytesView))
  }
}
