public import Distributed
import Foundation
import Logging

extension TCPActorSystem {
  public struct CallDecoder: DistributedTargetInvocationDecoder, Sendable {
    public typealias SerializationRequirement = TCPActorSystem.SerializationRequirement

    private static let logger = Logger(label: "TCPActorSystem.CallDecoder")
    private let message: RemoteInvocation
    private let decoder: JSONDecoder
    private let returnTypeName: String?
    private var argumentsIterator: [Data].Iterator

    init(message: RemoteInvocation, decoder: JSONDecoder, returnTypeName: String?) {
      self.message = message
      self.decoder = decoder
      self.returnTypeName = returnTypeName
      self.argumentsIterator = message.arguments.makeIterator()
    }

    public mutating func decodeGenericSubstitutions() throws -> [Any.Type] {
      Self.logger.trace(#function)
      return try message.substitutions.map { name in
        guard let type = _typeByName(name) else { throw RPCError.unknownType(name) }
        return type
      }
    }

    public mutating func decodeErrorType() throws -> Any.Type? {
      Self.logger.trace("\(#function) \(message.errorTypeName ?? "nil")")
      return message.errorTypeName.flatMap(_typeByName)
    }

    public mutating func decodeReturnType() throws -> Any.Type? {
      Self.logger.trace("\(#function) \(returnTypeName ?? "nil")")
      return returnTypeName.flatMap(_typeByName)
    }

    public mutating func decodeNextArgument<Argument: SerializationRequirement>() throws -> Argument {
      guard let data = argumentsIterator.next() else {
        Self.logger.error("\(#function) ran out of arguments")
        throw RPCError.invalidMessage
      }
      do {
        let value = try decoder.decode(Argument.self, from: data)
        Self.logger.trace("\(#function) decoded \(Argument.self)")
        return value
      } catch {
        Self.logger.error("\(#function) \(error)")
        if error is DecodingError { throw RPCError.invalidMessage }
        throw error
      }
    }
  }
}
