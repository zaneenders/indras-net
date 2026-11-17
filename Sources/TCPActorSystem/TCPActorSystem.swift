public import Distributed
package import Foundation
public import NIOCore
import Networking
import Synchronization

package protocol FixedActorID {
  static var fixedID: UUID { get }
}

public final class TCPActorSystem: DistributedActorSystem, Sendable {
  public typealias ActorID = ActorAddress
  public typealias InvocationEncoder = CodableInvocationEncoder
  public typealias InvocationDecoder = CodableInvocationDecoder
  public typealias ResultHandler = CodableResultHandler
  public typealias SerializationRequirement = Codable

  public let endpoint: ActorEndpoint
  private let registry = Mutex(Registry())
  private let transport: RPCTransport

  public init(host: String, advertisedHost: String, port: Int, callTimeout: Duration, connectTimeout: TimeAmount)
    async throws
  {
    guard !host.isEmpty, !advertisedHost.isEmpty, (0...65535).contains(port), callTimeout > .zero,
      connectTimeout.nanoseconds > 0
    else {
      throw RPCError.invalidConfiguration
    }
    let listener = try await TCP.listen(host: host, port: port)
    guard let boundPort = listener.channel.localAddress?.port else {
      try? await listener.channel.close()
      throw RPCError.invalidConfiguration
    }
    self.endpoint = ActorEndpoint(host: advertisedHost, port: boundPort, node: UUID())
    self.transport = RPCTransport(listener: listener, callTimeout: callTimeout, connectTimeout: connectTimeout)
    await transport.start(system: self)
  }

  package func peerEndpoint(host: String, port: Int) async throws -> ActorEndpoint {
    try await transport.peerEndpoint(host: host, port: port, system: self)
  }

  package func reserveFixedActorIDs(_ ids: Set<UUID>) throws {
    try registry.withLock {
      guard !$0.shutDown else { throw RPCError.shutDown }
      guard $0.fixedIDs.isDisjoint(with: ids) else { throw RPCError.invalidConfiguration }
      $0.fixedIDs.formUnion(ids)
    }
  }

  public func assignID<Act>(_ actorType: Act.Type) -> ActorID where Act: DistributedActor, Act.ID == ActorID {
    ActorID(endpoint: endpoint, id: (actorType as? any FixedActorID.Type)?.fixedID ?? UUID())
  }

  public func actorReady<Act>(_ actor: Act) where Act: DistributedActor, Act.ID == ActorID {
    registry.withLock {
      precondition($0.actors[actor.id] == nil, "An actor ID is already registered")
      $0.actors[actor.id] = WeakActor(actor: actor)
    }
  }

  public func resignID(_ id: ActorID) {
    registry.withLock { $0.actors[id] = nil }
  }

  public func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act?
  where Act: DistributedActor, Act.ID == ActorID {
    let actor = try registry.withLock { state -> (any DistributedActor)? in
      guard !state.shutDown else { throw RPCError.shutDown }
      guard id.endpoint == endpoint else { return nil }
      guard let actor = state.actors[id]?.actor else { throw RPCError.unknownActor }
      return actor
    }
    guard let actor else { return nil }
    guard let typed = actor as? Act else { throw RPCError.actorTypeMismatch }
    return typed
  }

  public func makeInvocationEncoder() -> InvocationEncoder {
    InvocationEncoder()
  }

  public func remoteCall<Act, Err, Res>(
    on actor: Act, target: RemoteCallTarget, invocation: inout InvocationEncoder,
    throwing: Err.Type, returning: Res.Type
  ) async throws -> Res where Act: DistributedActor, Act.ID == ActorID, Err: Error, Res: Codable {
    let result = try await call(
      actorID: actor.id, actorType: String(reflecting: Act.self), target: target, invocation: invocation)
    try checkError(result)
    guard case .value(let data) = result else { throw RPCError.invalidMessage }
    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = self
    return try decoder.decode(Res.self, from: data)
  }

  public func remoteCallVoid<Act, Err>(
    on actor: Act, target: RemoteCallTarget, invocation: inout InvocationEncoder, throwing: Err.Type
  ) async throws where Act: DistributedActor, Act.ID == ActorID, Err: Error {
    let result = try await call(
      actorID: actor.id, actorType: String(reflecting: Act.self), target: target, invocation: invocation)
    try checkError(result)
    guard case .void = result else { throw RPCError.invalidMessage }
  }

  public func shutdown() async {
    registry.withLock { $0.shutDown = true }
    await transport.shutdown()
  }

  func execute(_ invocation: RemoteInvocation, sessionID: UUID) async {
    let handler = ResultHandler(requestID: invocation.requestID, sessionID: sessionID, transport: transport)
    do {
      let actor = try registry.withLock { state in
        guard !state.shutDown else { throw RPCError.shutDown }
        guard let actor = state.actors[invocation.actorID]?.actor else { throw RPCError.unknownActor }
        return actor
      }
      guard String(reflecting: Swift.type(of: actor)) == invocation.actorType else {
        throw RPCError.actorTypeMismatch
      }
      var decoder = InvocationDecoder(
        arguments: invocation.arguments, substitutions: invocation.substitutions, system: self)
      try await executeDistributedTarget(
        on: actor, target: RemoteCallTarget(invocation.target), invocationDecoder: &decoder, handler: handler)
    } catch {
      try? await handler.onThrow(error: error)
    }
  }

  private func call(
    actorID: ActorID, actorType: String, target: RemoteCallTarget, invocation: InvocationEncoder
  ) async throws -> CallResult {
    let call = RemoteInvocation(
      requestID: UUID(), actorID: actorID, actorType: actorType, target: target.identifier,
      arguments: invocation.arguments, substitutions: invocation.substitutions)
    return try await transport.call(call, system: self)
  }

  private func checkError(_ result: CallResult) throws {
    switch result {
    case .systemError(let error): throw error
    case .applicationError(let error): throw error
    case .value, .void: break
    }
  }
}

private struct WeakActor: Sendable {
  weak var actor: (any DistributedActor)?
}

private struct Registry: Sendable {
  var actors: [ActorAddress: WeakActor] = [:]
  var fixedIDs: Set<UUID> = []
  var shutDown = false
}
