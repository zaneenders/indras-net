public import Distributed

public enum RPCError: DistributedActorSystemError, Codable, Sendable, Equatable {
  case invalidConfiguration
  case shutDown
  case disconnected
  case timedOut
  case unknownActor
  case actorTypeMismatch
  case invalidMessage
  case unknownType(String)
}

public struct RemoteActorError: Error, Codable, Sendable, Equatable {
  public let type: String
  public let message: String

  init(error: any Error) {
    self.type = String(reflecting: Swift.type(of: error))
    self.message = String(describing: error)
  }
}
