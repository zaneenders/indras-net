import Distributed
import Foundation
import TCPActorSystem

struct ServiceSnapshot: Codable, Sendable {
  let version: Int
  let cluster: String
  let node: NodeIdentity
  let revision: UInt64
  let registrations: [ServiceRegistration]
}

distributed actor ReceptionistService: FixedActorID {
  typealias ActorSystem = TCPActorSystem
  static let fixedID = UUID(uuid: (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2))
  static let serviceName = "indra.receptionist.v1"
  private let receptionist: Receptionist

  init(actorSystem: TCPActorSystem, receptionist: Receptionist) {
    self.actorSystem = actorSystem
    self.receptionist = receptionist
  }

  distributed func snapshot(version: Int, cluster: String) async throws -> ServiceSnapshot {
    try await receptionist.snapshot(version: version, cluster: cluster)
  }
}
