public import TCPActorSystem

package import struct Foundation.UUID

public typealias ServiceInstaller = @Sendable (ServiceContext) async throws -> Void

public struct ServiceContext: Sendable {
  public let actorSystem: TCPActorSystem
  public let receptionist: Receptionist
  public let cluster: String
  public let configurationPath: String
  public let started: ContinuousClock.Instant
  package let nodeID: UUID
  let runtime: NodeRuntime

  public func onShutdown(_ action: @escaping @Sendable () async -> Void) async throws {
    try await runtime.addServiceShutdown(action)
  }
}
