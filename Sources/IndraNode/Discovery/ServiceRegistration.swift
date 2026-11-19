public import Distributed
public import TCPActorSystem

public struct ServiceKey<Act>: Hashable, Sendable
where Act: DistributedActor, Act.ActorSystem == TCPActorSystem {
  public let name: String

  public init(_ name: String) {
    self.name = name
  }
}

public struct ServiceRegistration: Codable, Hashable, Sendable {
  public let key: String
  public let actorType: String
  public let address: ActorAddress

  func validate(owner: NodeIdentity) throws {
    guard !key.isEmpty, key.utf8.count <= 128,
      !actorType.isEmpty, actorType.utf8.count <= 1024,
      address.endpoint == owner.endpoint
    else { throw IndraError("Invalid service registration") }
  }
}

public struct DiscoveredService: Codable, Hashable, Sendable {
  public let node: NodeIdentity
  public let registration: ServiceRegistration
}
