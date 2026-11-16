public import Distributed
import Foundation

public struct CodableInvocationEncoder: DistributedTargetInvocationEncoder, Sendable {
  public typealias SerializationRequirement = Codable

  var arguments: [Data] = []
  var substitutions: [String] = []

  public mutating func recordGenericSubstitution<T>(_ type: T.Type) throws {
    guard let name = _mangledTypeName(type) else {
      throw RPCError.unknownType(String(reflecting: type))
    }
    substitutions.append(name)
  }

  public mutating func recordArgument<Value: Codable>(_ argument: RemoteCallArgument<Value>) throws {
    arguments.append(try JSONEncoder().encode(argument.value))
  }

  public mutating func recordErrorType<E: Error>(_ type: E.Type) throws {}

  public mutating func recordReturnType<R: Codable>(_ type: R.Type) throws {}

  public mutating func doneRecording() throws {}
}

public struct CodableInvocationDecoder: DistributedTargetInvocationDecoder, Sendable {
  public typealias SerializationRequirement = Codable

  let arguments: [Data]
  let substitutions: [String]
  let system: TCPActorSystem
  var index = 0

  public mutating func decodeGenericSubstitutions() throws -> [Any.Type] {
    try substitutions.map { name in
      guard let type = _typeByName(name) else { throw RPCError.unknownType(name) }
      return type
    }
  }

  public mutating func decodeNextArgument<Argument: Codable>() throws -> Argument {
    guard index < arguments.count else { throw RPCError.invalidMessage }
    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = system
    let argument = try decoder.decode(Argument.self, from: arguments[index])
    index += 1
    return argument
  }

  public mutating func decodeErrorType() throws -> Any.Type? { nil }

  public mutating func decodeReturnType() throws -> Any.Type? { nil }
}

public struct CodableResultHandler: DistributedTargetInvocationResultHandler, Sendable {
  public typealias SerializationRequirement = Codable

  let requestID: UUID
  let sessionID: UUID
  let transport: RPCTransport

  public func onReturn<Success: Codable>(value: Success) async throws {
    try await transport.reply(
      .value(JSONEncoder().encode(value)), requestID: requestID, sessionID: sessionID)
  }

  public func onReturnVoid() async throws {
    try await transport.reply(.void, requestID: requestID, sessionID: sessionID)
  }

  public func onThrow<Err: Error>(error: Err) async throws {
    let result: CallResult
    if let error = error as? RPCError {
      result = .systemError(error)
    } else {
      result = .applicationError(RemoteActorError(error: error))
    }
    try await transport.reply(result, requestID: requestID, sessionID: sessionID)
  }
}
