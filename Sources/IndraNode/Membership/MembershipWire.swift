import ClusterMembership
import Foundation
import SWIM
import TCPActorSystem

struct MembershipRequest: Codable, Sendable {
  let version: Int
  let cluster: String
  let node: NodeIdentity
  let probe: MembershipProbe
}

enum MembershipProbe: Codable, Sendable {
  case ping(sequenceNumber: UInt32, payload: [GossipMember])
  case pingRequest(target: NodeIdentity, sequenceNumber: UInt32, payload: [GossipMember])

  var payload: [GossipMember] {
    switch self {
    case .ping(_, let payload), .pingRequest(_, _, let payload): return payload
    }
  }

  var sequenceNumber: UInt32 {
    switch self {
    case .ping(let number, _), .pingRequest(_, let number, _): return number
    }
  }
}

struct MembershipReply: Codable, Sendable {
  let version: Int
  let cluster: String
  let node: NodeIdentity
  let response: MembershipResponse
}

enum MembershipResponse: Codable, Sendable {
  case ack(target: NodeIdentity, incarnation: UInt64, payload: [GossipMember], sequenceNumber: UInt32)
  case nack(target: NodeIdentity, sequenceNumber: UInt32)

  var target: NodeIdentity {
    switch self {
    case .ack(let target, _, _, _), .nack(let target, _): return target
    }
  }

  var sequenceNumber: UInt32 {
    switch self {
    case .ack(_, _, _, let number), .nack(_, let number): return number
    }
  }

  var payload: [GossipMember] {
    switch self {
    case .ack(_, _, let payload, _): return payload
    case .nack: return []
    }
  }

  func swimResponse(excludingStaleIncarnationsOf identity: NodeIdentity) -> SWIM.PingResponse {
    switch self {
    case .ack(let target, let incarnation, let payload, let sequenceNumber):
      return .ack(
        target: target.swimNode, incarnation: incarnation,
        payload: .membership(
          payload.filter { $0.node.id != identity.id || $0.node.endpoint == identity.endpoint }.map(\.member)),
        sequenceNumber: sequenceNumber)
    case .nack(let target, let sequenceNumber):
      return .nack(target: target.swimNode, sequenceNumber: sequenceNumber)
    }
  }
}

struct GossipMember: Codable, Sendable {
  let node: NodeIdentity
  let status: GossipStatus
  let protocolPeriod: UInt64

  var member: SWIM.Member {
    SWIM.Member(node: node.swimNode, status: status.status, protocolPeriod: protocolPeriod, suspicionStartedAt: nil)
  }
}

enum GossipStatus: Codable, Sendable {
  case alive(UInt64)
  case suspect(UInt64, [GossipAddress])
  case unreachable(UInt64)
  case dead

  init(_ status: SWIM.Status) {
    switch status {
    case .alive(let incarnation): self = .alive(incarnation)
    case .suspect(let incarnation, let suspectedBy):
      self = .suspect(incarnation, suspectedBy.map { GossipAddress($0) })
    case .unreachable(let incarnation): self = .unreachable(incarnation)
    case .dead: self = .dead
    }
  }

  var status: SWIM.Status {
    switch self {
    case .alive(let incarnation): return .alive(incarnation: incarnation)
    case .suspect(let incarnation, let suspectedBy):
      return .suspect(incarnation: incarnation, suspectedBy: Set(suspectedBy.map(\.node)))
    case .unreachable(let incarnation): return .unreachable(incarnation: incarnation)
    case .dead: return .dead
    }
  }

  func validate() throws {
    if case .suspect(_, let suspectedBy) = self {
      guard suspectedBy.count <= 8 else { throw IndraError("Too many suspicion sources") }
      for address in suspectedBy { try PeerAddress(host: address.host, port: address.port).validate() }
    }
  }
}

struct GossipAddress: Codable, Sendable {
  let host: String
  let port: Int
  let uid: UInt64?

  init(_ node: ClusterMembership.Node) {
    host = node.host
    port = node.port
    uid = node.uid
  }

  var node: ClusterMembership.Node {
    ClusterMembership.Node(protocol: "indra", host: host, port: port, uid: uid)
  }
}

extension NodeIdentity {
  var swimNode: ClusterMembership.Node {
    // The actor endpoint changes on every daemon start; the configured UUID stays stable.
    let uid = endpoint.systemID.uuidString.prefix(18).filter { $0 != "-" }
    return ClusterMembership.Node(
      protocol: "indra", host: endpoint.host, port: endpoint.port,
      uid: UInt64(uid, radix: 16))
  }
}
