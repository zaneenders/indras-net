public import Distributed
import Foundation

extension TCPActorSystem {
  public struct CallEncoder: DistributedTargetInvocationEncoder, Sendable {
    public typealias SerializationRequirement = TCPActorSystem.SerializationRequirement

    private let encoder: JSONEncoder
    private(set) var arguments: [Data]
    private(set) var substitutions: [String]
    private(set) var errorTypeName: String?
    private(set) var returnTypeName: String?

    init(encoder: JSONEncoder) {
      self.encoder = encoder
      self.arguments = []
      self.substitutions = []
      self.errorTypeName = nil
      self.returnTypeName = nil
    }

    public mutating func recordGenericSubstitution<T>(_ type: T.Type) throws {
      guard let name = _mangledTypeName(type) else {
        throw RPCError.unknownType(String(reflecting: type))
      }
      substitutions.append(name)
    }

    public mutating func recordArgument<Value: SerializationRequirement>(_ argument: RemoteCallArgument<Value>) throws {
      arguments.append(try encoder.encode(argument.value))
    }

    public mutating func recordErrorType<E: Error>(_ type: E.Type) throws {
      errorTypeName = _mangledTypeName(type)
    }

    public mutating func recordReturnType<R: SerializationRequirement>(_ type: R.Type) throws {
      returnTypeName = _mangledTypeName(type)
    }

    public mutating func doneRecording() throws {}
  }
}
