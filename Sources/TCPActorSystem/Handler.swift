public import Distributed
import Foundation

extension TCPActorSystem {
  public struct Handler: DistributedTargetInvocationResultHandler, Sendable {
    public typealias SerializationRequirement = TCPActorSystem.SerializationRequirement

    let requestID: UUID
    let sessionID: UUID
    let transport: RPCTransport
    let encoder: JSONEncoder

    public func onReturn<Success: SerializationRequirement>(value: Success) async throws {
      try await transport.reply(.value(encoder.encode(value)), requestID: requestID, sessionID: sessionID)
    }

    public func onReturnVoid() async throws {
      try await transport.reply(.void, requestID: requestID, sessionID: sessionID)
    }

    public func onThrow<Err: Error>(error: Err) async throws {
      let result: CallResult
      if let error = error as? RPCError {
        result = .systemError(error)
      } else {
        result = .applicationError(ApplicationErrorReply(error: error, encoder: encoder))
      }
      try await transport.reply(result, requestID: requestID, sessionID: sessionID)
    }
  }
}
