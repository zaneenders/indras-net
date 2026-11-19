import ClusterMembership
import Foundation
import TCPActorSystem

/// All mutations run on NodeRuntime, in the same actor turn as the corresponding SWIM updates.
struct PeerDirectory: Sendable {
  struct Verification: Sendable {
    fileprivate var byID: [UUID: ActorEndpoint] = [:]
    fileprivate var byAddress: [PeerAddress: ActorEndpoint] = [:]

    func unchanged(at address: PeerAddress, since earlier: Self) -> Bool {
      byAddress[address] == earlier.byAddress[address]
    }
  }

  private(set) var verification = Verification()
  private var identities: [ClusterMembership.Node: NodeIdentity] = [:]
  private var current: [UUID: NodeIdentity] = [:]
  private(set) var retired: Set<ActorEndpoint> = []

  subscript(node: ClusterMembership.Node) -> NodeIdentity? { identities[node] }

  func incumbent(for peer: NodeIdentity) -> NodeIdentity? {
    guard let node = current[peer.id], node.endpoint != peer.endpoint else { return nil }
    return node
  }

  func accepts(_ peer: NodeIdentity) -> Bool {
    !retired.contains(peer.endpoint) && incumbent(for: peer) == nil
      && (verification.byAddress[peer.address].map { $0 == peer.endpoint } ?? true)
  }

  mutating func learn(_ peer: NodeIdentity) -> Bool {
    guard accepts(peer) else { return false }
    let key = peer.swimNode
    let previous = identities[key]
    // Gossip from an older node can omit build metadata learned directly from the peer.
    let identity = NodeIdentity(
      id: peer.id, name: peer.name, endpoint: peer.endpoint, build: peer.build ?? previous?.build)
    guard previous != identity else { return false }
    identities[key] = identity
    current[peer.id] = identity
    return true
  }

  mutating func verify(_ peer: NodeIdentity, observed: Verification, checkedIncumbent: ActorEndpoint?) throws
    -> (retired: [NodeIdentity], identityChanged: Bool)
  {
    guard !retired.contains(peer.endpoint),
      verification.unchanged(at: peer.address, since: observed)
        || verification.byAddress[peer.address] == peer.endpoint,
      observed.byID[peer.id] == verification.byID[peer.id] || verification.byID[peer.id] == peer.endpoint,
      incumbent(for: peer)?.endpoint == checkedIncumbent || current[peer.id]?.endpoint == peer.endpoint
    else { throw IndraError("Peer process changed while the request was in flight") }

    if verification.byID[peer.id] == peer.endpoint { return ([], learn(peer)) }
    let replaced = identities.values.filter {
      $0.endpoint != peer.endpoint && !retired.contains($0.endpoint) && ($0.id == peer.id || $0.address == peer.address)
    }
    for old in replaced {
      retired.insert(old.endpoint)
      if current[old.id]?.endpoint == old.endpoint { current[old.id] = nil }
      if verification.byID[old.id] == old.endpoint { verification.byID[old.id] = nil }
      if verification.byAddress[old.address] == old.endpoint { verification.byAddress[old.address] = nil }
    }
    verification.byID[peer.id] = peer.endpoint
    verification.byAddress[peer.address] = peer.endpoint
    return (replaced, learn(peer))
  }

  func resolve(_ target: String, local: NodeIdentity, isMember: (NodeIdentity) -> Bool) throws -> NodeIdentity? {
    if let uuid = UUID(uuidString: target) {
      if local.id == uuid { return local }
      if let node = current[uuid], isMember(node) { return node }
    }
    var match: NodeIdentity? = local.name == target ? local : nil
    for peer in current.values where peer.id != local.id && peer.name == target && isMember(peer) {
      guard match == nil else { throw IndraError("Ambiguous node name; use its UUID or host:port") }
      match = peer
    }
    return match
  }
}
