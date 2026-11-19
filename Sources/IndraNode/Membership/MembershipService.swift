import Distributed
import Foundation
import TCPActorSystem

distributed actor MembershipService: FixedActorID {
  typealias ActorSystem = TCPActorSystem
  static let fixedID = UUID(uuid: (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1))
  static let serviceName = "indra.membership.swim.v1"
  private let runtime: NodeRuntime

  init(actorSystem: TCPActorSystem, runtime: NodeRuntime) {
    self.actorSystem = actorSystem
    self.runtime = runtime
  }

  distributed func probe(_ message: MembershipRequest) async throws -> MembershipReply {
    try await runtime.receive(message)
  }
}
