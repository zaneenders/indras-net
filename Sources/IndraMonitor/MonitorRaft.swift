import Foundation
import IndraNode
import IndraRaft

struct MonitorRaftGroup: Sendable {
  enum Membership: String, Sendable {
    case voter = "Voter"
    case previousVoter = "Previous voter"
    case learner = "Learner"
  }

  struct Replica: Sendable {
    let id: String
    let name: String
    let status: RaftStatus?
    let membership: Membership?
    let unavailable: String

    var role: String {
      guard let status else { return unavailable }
      if membership == .learner, status.role == "follower" { return "learner" }
      return status.role
    }

    func lag(behind commit: UInt64) -> UInt64? {
      status.map { commit - min(commit, $0.appliedIndex) }
    }

    func progress(to commit: UInt64, width: Int) -> String? {
      guard let status, commit > 0 else { return nil }
      func position(_ index: UInt64) -> Int {
        Int(Double(min(index, commit)) / Double(commit) * Double(width))
      }
      let committed = position(status.commitIndex)
      let applied = min(committed, position(status.appliedIndex))
      let snapshotted = min(applied, position(status.snapshotIndex))
      return String(repeating: "░", count: snapshotted)
        + String(repeating: "█", count: applied - snapshotted)
        + String(repeating: "▒", count: committed - applied)
        + String(repeating: "·", count: width - committed)
    }
  }

  private typealias Report = ServiceReport<RaftStatus>

  let id: String
  let name: String
  let term: UInt64
  let leader: String?
  let conflictingLeaders: Bool
  let commit: UInt64
  let configuration: RaftConfigurationStatus?
  let voters: Int
  let learners: Int
  let replicas: [Replica]

  var responding: Set<String> { Set(replicas.compactMap { $0.status == nil ? nil : $0.id }) }

  static func collect(reports: [ServiceReport<RaftStatus>], status: NodeStatus) -> [Self] {
    let nodes = Dictionary(
      ([status.node] + status.members.map(\.node)).map { ($0.id.uuidString, $0) },
      uniquingKeysWith: { first, _ in first })
    let latest = Dictionary(
      reports.compactMap { report -> (String, Report.Outcome)? in
        let id = report.node.id.uuidString
        guard nodes[id]?.endpoint == report.node.endpoint else { return nil }
        return (id, report.outcome)
      }, uniquingKeysWith: { first, _ in first })
    let responding = latest.compactMap { id, outcome -> RaftStatus? in
      guard case .responded(let status) = outcome, status.node == id else { return nil }
      return status
    }
    let byGroup = Dictionary(grouping: responding, by: \.group)
    return byGroup.keys.sorted().compactMap { id in
      guard let reports = byGroup[id] else { return nil }
      return Self(id: id, reports: reports, nodes: nodes, outcomes: latest)
    }
  }

  private init?(id: String, reports: [RaftStatus], nodes: [String: NodeIdentity], outcomes: [String: Report.Outcome]) {
    guard let newest = reports.max(by: { ($0.term, $0.commitIndex, $0.node) < ($1.term, $1.commitIndex, $1.node) })
    else { return nil }
    let claims = reports.filter { $0.term == newest.term && $0.role == "leader" && $0.leader == $0.node }
    let leader = claims.count == 1 ? claims.first : nil
    let reference = leader ?? newest
    let configuration = reference.configuration
    let voters = Set(configuration?.voters ?? [])
    let previousVoters = Set(configuration?.previousVoters ?? [])
    let readers = Set(configuration?.readers ?? [])
    let byNode = Dictionary(reports.map { ($0.node, $0) }, uniquingKeysWith: { first, _ in first })

    self.id = id
    self.name = nodes[id]?.name ?? String(id.prefix(8))
    self.term = newest.term
    self.leader = leader.map { nodes[$0.node]?.name ?? String($0.node.prefix(8)) }
    self.conflictingLeaders = claims.count > 1
    self.commit = reports.map(\.commitIndex).max() ?? reference.commitIndex
    self.configuration = configuration
    self.voters = reference.voters
    self.learners = reference.readers
    self.replicas = voters.union(previousVoters).union(readers).union(byNode.keys).map { peer in
      let membership: Membership?
      if voters.contains(peer) { membership = .voter }
      else if previousVoters.contains(peer) { membership = .previousVoter }
      else if readers.contains(peer) { membership = .learner }
      else { membership = nil }
      return Replica(
        id: peer, name: nodes[peer]?.name ?? String(peer.prefix(8)), status: byNode[peer], membership: membership,
        unavailable: Self.unavailable(outcomes[peer]))
    }.sorted { left, right in
      let first = leader?.node
      if (left.id == first) != (right.id == first) { return left.id == first }
      return (left.name, left.id) < (right.name, right.id)
    }
  }

  private static func unavailable(_ outcome: Report.Outcome?) -> String {
    switch outcome {
    case .timedOut: "timeout"
    case .unreachable: "offline"
    case .failed: "error"
    case .responded: "elsewhere"
    case .unavailable, nil: "no report"
    }
  }
}
