package import Distributed
import Foundation
package import IndraNode
package import TCPActorSystem

package struct HealthMetrics: Codable, Sendable {
  package let uptimeSeconds: Double
  package let freeDiskBytes: UInt64
}

package distributed actor HealthService {
  package typealias ActorSystem = TCPActorSystem
  package static let key = ServiceKey<HealthService>("indra.health.v1")

  private let cluster: String
  private let configurationPath: String
  private let started: ContinuousClock.Instant

  package static func install(_ context: ServiceContext) async throws {
    let service = HealthService(
      actorSystem: context.actorSystem, cluster: context.cluster, configurationPath: context.configurationPath,
      started: context.started)
    try await context.receptionist.registerQuery(service, with: key, timeout: .seconds(3)) { service, cluster in
      try await service.sample(cluster: cluster)
    }
  }

  init(actorSystem: TCPActorSystem, cluster: String, configurationPath: String, started: ContinuousClock.Instant) {
    self.actorSystem = actorSystem
    self.cluster = cluster
    self.configurationPath = configurationPath
    self.started = started
  }

  distributed func sample(cluster: String) throws -> HealthMetrics {
    guard cluster == self.cluster else { throw IndraError("Peer uses a different cluster") }
    let directory = URL(fileURLWithPath: configurationPath).deletingLastPathComponent()
    guard
      let capacity = (try? directory.resourceValues(forKeys: [.volumeAvailableCapacityKey]))?.volumeAvailableCapacity,
      let freeDiskBytes = UInt64(exactly: capacity)
    else { throw IndraError("Could not read free space on the configuration filesystem") }
    return HealthMetrics(uptimeSeconds: started.duration(to: .now) / .seconds(1), freeDiskBytes: freeDiskBytes)
  }
}
