public import Distributed
public import Foundation

public enum RPCError: DistributedActorSystemError, Codable, Sendable, Equatable {
  case invalidConfiguration
  case shutDown
  case disconnected
  case timedOut
  case unknownActor
  case unknownService(String)
  case serviceAlreadyPublished(String)
  case actorTypeMismatch
  case invalidMessage
  case unknownType(String)
}

public struct RemoteActorError: Error, Codable, Sendable, Equatable {
  public let type: String
  public let message: String
  public let payload: Data?

  init(error: any Error) {
    self.type = String(reflecting: Swift.type(of: error))
    self.message = String(describing: error)
    self.payload = (error as? any Encodable).flatMap { try? JSONEncoder().encode($0) }
  }
}
