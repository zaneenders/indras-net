public import Distributed

public protocol CodableRPCError: Error, Codable {}

public enum RPCError: DistributedActorSystemError, Codable, Sendable, Equatable {
  case invalidConfiguration
  case shutDown
  case disconnected
  case timedOut
  case overloaded
  case unknownActor
  case actorTypeMismatch
  case invalidMessage
  case unknownType(String)
  case unknownRemoteError(String)

  var wireCode: (String, String?) {
    switch self {
    case .invalidConfiguration: ("invalid-configuration", nil)
    case .shutDown: ("shut-down", nil)
    case .disconnected: ("disconnected", nil)
    case .timedOut: ("timed-out", nil)
    case .overloaded: ("overloaded", nil)
    case .unknownActor: ("unknown-actor", nil)
    case .actorTypeMismatch: ("actor-type-mismatch", nil)
    case .invalidMessage: ("invalid-message", nil)
    case .unknownType(let name): ("unknown-type", name)
    case .unknownRemoteError(let code): (code, nil)
    }
  }

  init(code: String, detail: String?) {
    switch code {
    case "invalid-configuration": self = .invalidConfiguration
    case "shut-down": self = .shutDown
    case "disconnected": self = .disconnected
    case "timed-out": self = .timedOut
    case "overloaded": self = .overloaded
    case "unknown-actor": self = .unknownActor
    case "actor-type-mismatch": self = .actorTypeMismatch
    case "invalid-message": self = .invalidMessage
    case "unknown-type": self = detail.map(Self.unknownType) ?? .invalidMessage
    default: self = .unknownRemoteError(code)
    }
  }
}

public struct RemoteActorError: Error, Codable, Sendable, Equatable {
  public let type: String
  public let message: String
}
