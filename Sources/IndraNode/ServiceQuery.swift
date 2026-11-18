import Distributed
import TCPActorSystem

public struct ServiceReport<Value: Codable & Sendable>: Codable, Sendable {
  public enum Outcome: Codable, Sendable {
    case responded(metrics: Value)
    case timedOut
    case unreachable
    case unavailable
    case failed(message: String)
  }

  public let node: NodeIdentity
  public let outcome: Outcome
}

func collectReports<Act, Value: Codable & Sendable>(
  actors: [Act], status: NodeStatus, timeout: Duration, sample: @escaping @Sendable (Act, String) async throws -> Value
) async throws -> [ServiceReport<Value>] where Act: DistributedActor, Act.ActorSystem == TCPActorSystem {
  let byEndpoint = Dictionary(actors.map { ($0.id.endpoint, $0) }, uniquingKeysWith: { first, _ in first })
  let members = [Member(node: status.node, state: .alive)] + status.members
  let cluster = status.cluster
  let deadline = ContinuousClock.now.advanced(by: timeout)
  return try await withThrowingTaskGroup(of: (Int, ServiceReport<Value>.Outcome).self) { tasks in
    var outcomes = Array(repeating: ServiceReport<Value>.Outcome.unavailable, count: members.count)
    for (index, member) in members.enumerated() {
      if member.state == .unreachable || member.state == .dead {
        outcomes[index] = .unreachable
        continue
      }
      guard let actor = byEndpoint[member.node.endpoint] else { continue }
      tasks.addTask {
        do {
          let value = try await withDeadline(deadline: deadline, timeoutError: RPCError.timedOut) {
            try await sample(actor, cluster)
          }
          return (index, .responded(metrics: value))
        } catch RPCError.timedOut {
          return (index, .timedOut)
        } catch {
          try Task.checkCancellation()
          return (index, .failed(message: String(describing: error)))
        }
      }
    }
    for try await (index, outcome) in tasks { outcomes[index] = outcome }
    try Task.checkCancellation()
    return zip(members, outcomes).map { ServiceReport(node: $0.node, outcome: $1) }
  }
}
