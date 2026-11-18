public import TCPActorSystem

public typealias ServiceInstaller = @Sendable (ServiceContext) async throws -> Void

public struct ServiceContext: Sendable {
  public let actorSystem: TCPActorSystem
  public let receptionist: Receptionist
  public let cluster: String
  public let configurationPath: String
  public let started: ContinuousClock.Instant
}
