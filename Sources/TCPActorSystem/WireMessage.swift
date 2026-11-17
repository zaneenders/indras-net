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
}

enum CallResult: Codable, Sendable {
  case value(Data)
  case void
  case systemError(RPCError)
  case applicationError(RemoteActorError)
}

enum WireMessage: Codable, Sendable {
  static let version = 1

  case hello(version: Int, endpoint: ActorEndpoint)
  case call(RemoteInvocation)
  case reply(UUID, CallResult)

  func frame() throws -> Frame {
    Frame(target: 0, opcode: 0, payload: ByteBuffer(bytes: try JSONEncoder().encode(self)))
  }

  init(frame: Frame) throws {
    guard frame.target == 0, frame.opcode == 0 else { throw RPCError.invalidMessage }
    self = try JSONDecoder().decode(Self.self, from: Data(frame.payload.readableBytesView))
  }
}
