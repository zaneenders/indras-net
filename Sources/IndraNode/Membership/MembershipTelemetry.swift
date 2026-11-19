import DequeModule
import SWIM

public struct ProbeTotals: Codable, Sendable {
  public private(set) var ping: UInt64 = 0
  public private(set) var pingRequest: UInt64 = 0
  public private(set) var ack: UInt64 = 0
  public private(set) var nack: UInt64 = 0
  public private(set) var failures: UInt64 = 0

  public var messages: UInt64 { ping &+ pingRequest &+ ack &+ nack }

  mutating func record(_ kind: ProbeEvent.Kind) {
    switch kind {
    case .ping: ping &+= 1
    case .pingRequest: pingRequest &+= 1
    case .ack: ack &+= 1
    case .nack: nack &+= 1
    case .failure: failures &+= 1
    }
  }
}

public struct ProbeEvent: Codable, Sendable {
  public enum Direction: String, Codable, Sendable { case inbound, outbound }
  public enum Kind: String, Codable, Sendable { case ping, pingRequest, ack, nack, failure }
  public let id: UInt64
  public let uptimeSeconds: Double
  public let direction: Direction
  public let kind: Kind
  public let peer: PeerAddress
  public let target: PeerAddress?
  public let sequenceNumber: UInt32
  public let gossipMembers: Int
  public let roundTripMilliseconds: Double?
  public let error: String?
}

public struct MembershipChange: Codable, Sendable {
  public let uptimeSeconds: Double
  public let node: NodeIdentity
  public let previous: Member.State?
  public let current: Member.State
}

public struct MonitorSnapshot: Codable, Sendable {
  public let status: NodeStatus
  public let uptimeSeconds: Double
  public let protocolPeriod: UInt64
  public let localHealthMultiplier: Int
  public let probeIntervalMilliseconds: Int
  public let requestTimeoutMilliseconds: Int
  public let outbound: ProbeTotals
  public let inbound: ProbeTotals
  public let inFlight: Int
  public let events: [ProbeEvent]
  public let changes: [MembershipChange]
}

struct MembershipTelemetry: Sendable {
  let started = ContinuousClock.now
  private let capacity: Int
  private var nextID: UInt64 = 0
  private(set) var outbound = ProbeTotals()
  private(set) var inbound = ProbeTotals()
  private(set) var events: Deque<ProbeEvent> = []
  private(set) var changes: Deque<MembershipChange> = []
  var inFlight = 0

  init(capacity: Int) {
    precondition(capacity > 0)
    self.capacity = capacity
  }

  var uptimeSeconds: Double { started.duration(to: .now).secondsValue }

  mutating func record(
    direction: ProbeEvent.Direction, kind: ProbeEvent.Kind, peer: PeerAddress, target: PeerAddress?,
    sequenceNumber: UInt32, gossipMembers: Int, roundTripMilliseconds: Double?, error: String?
  ) {
    switch direction {
    case .outbound: outbound.record(kind)
    case .inbound: inbound.record(kind)
    }
    nextID &+= 1
    if events.count == capacity { events.removeFirst() }
    events.append(
      ProbeEvent(
        id: nextID, uptimeSeconds: uptimeSeconds, direction: direction, kind: kind, peer: peer, target: target,
        sequenceNumber: sequenceNumber, gossipMembers: gossipMembers,
        roundTripMilliseconds: roundTripMilliseconds, error: error.map { String($0.prefix(512)) }))
  }

  mutating func recordChange(node: NodeIdentity, previous: Member.State?, current: Member.State) {
    guard previous != current else { return }
    if changes.count == capacity { changes.removeFirst() }
    changes.append(
      MembershipChange(
        uptimeSeconds: uptimeSeconds, node: node, previous: previous, current: current))
  }
}

extension Member.State {
  init(_ status: SWIM.Status) {
    switch status {
    case .alive: self = .alive
    case .suspect: self = .suspect
    case .unreachable: self = .unreachable
    case .dead: self = .dead
    }
  }
}

extension Duration {
  var secondsValue: Double { Double(components.seconds) + Double(components.attoseconds) / 1e18 }
}
