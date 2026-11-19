public import Foundation
public import TCPActorSystem

public struct NodeIdentity: Codable, Hashable, Sendable {
  public let id: UUID
  public let name: String
  public let endpoint: ActorEndpoint
  public let build: BuildIdentity?

  var address: PeerAddress { PeerAddress(host: endpoint.host, port: endpoint.port) }

  func validate() throws {
    try address.validate()
    try build?.validate()
    guard !name.isEmpty, name.utf8.count <= 128 else { throw IndraError("Invalid peer name") }
  }
}

public struct Member: Codable, Sendable {
  public enum State: String, Codable, Sendable { case alive, suspect, unreachable, dead }
  public let node: NodeIdentity
  public var state: State
}

public struct NodeStatus: Codable, Sendable {
  public let node: NodeIdentity
  public let cluster: String
  public let members: [Member]
  public let seeds: [PeerAddress]
  public let configurationError: String?
  public let discoveryErrors: [String: String]
}

public struct PingResult: Codable, Sendable {
  public let node: NodeIdentity
  public let milliseconds: Double
}
