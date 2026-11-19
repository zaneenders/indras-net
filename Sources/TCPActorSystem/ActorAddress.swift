public import Foundation

public struct ActorAddress: Codable, Hashable, Sendable {
  public let endpoint: ActorEndpoint
  public let id: UUID

  public init(endpoint: ActorEndpoint, id: UUID) {
    self.endpoint = endpoint
    self.id = id
  }
}

public struct ActorEndpoint: Codable, Hashable, Sendable {
  public let host: String
  public let port: Int
  public let systemID: UUID

  public init(host: String, port: Int, systemID: UUID) {
    self.host = host
    self.port = port
    self.systemID = systemID
  }

  private enum CodingKeys: String, CodingKey {
    case host
    case port
    case systemID = "node"
  }
}
