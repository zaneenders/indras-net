public import Distributed
import Foundation
import NIOConcurrencyHelpers
public import NIOCore
import Networking

public final class TCPActorSystem: DistributedActorSystem, Sendable {
  public typealias ActorID = ActorAddress
  public typealias InvocationEncoder = CodableInvocationEncoder
  public typealias InvocationDecoder = CodableInvocationDecoder
  public typealias ResultHandler = CodableResultHandler
  public typealias SerializationRequirement = Codable

  public let endpoint: ActorEndpoint
  private let registry = NIOLockedValueBox(Registry())
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

  public func registerError<E: Error & Codable & Sendable>(_ type: E.Type) {
    registry.withLockedValue {
      $0.errors[String(reflecting: type)] = { try JSONDecoder().decode(type, from: $0) }
    }
  }

  public func publish<Act>(_ actor: Act, as name: String) throws
  where Act: DistributedActor, Act.ActorSystem == TCPActorSystem {
    try registry.withLockedValue { state in
      guard !state.shutDown else { throw RPCError.shutDown }
      guard !name.isEmpty else { throw RPCError.invalidConfiguration }
      guard actor.id.endpoint == endpoint, state.actors[actor.id]?.actor != nil else { throw RPCError.unknownActor }
      guard state.services[name] == nil else { throw RPCError.serviceAlreadyPublished(name) }
      state.services[name] = actor
    }
  }

  public func unpublish(_ name: String) {
    let actor = registry.withLockedValue { $0.services.removeValue(forKey: name) }
    withExtendedLifetime(actor) {}
  }

  public func service<Act>(named name: String, host: String, port: Int, as actorType: Act.Type) async throws -> Act
  where Act: DistributedActor, Act.ActorSystem == TCPActorSystem {
    let result = try await transport.lookup(
      name: name, actorType: String(reflecting: actorType), host: host, port: port, system: self)
    try checkError(result)
    guard case .value(let data) = result else { throw RPCError.invalidMessage }
    let address = try JSONDecoder().decode(ActorAddress.self, from: data)
    return try Act.resolve(id: address, using: self)
  }

  public func assignID<Act>(_ actorType: Act.Type) -> ActorID where Act: DistributedActor, Act.ID == ActorID {
    ActorID(endpoint: endpoint, id: UUID())
  }

  public func actorReady<Act>(_ actor: Act) where Act: DistributedActor, Act.ID == ActorID {
    registry.withLockedValue { $0.actors[actor.id] = WeakActor(actor: actor) }
  }

  public func resignID(_ id: ActorID) {
    registry.withLockedValue { $0.actors[id] = nil }
  }

  public func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act?
  where Act: DistributedActor, Act.ID == ActorID {
    let actor = try registry.withLockedValue { state -> (any DistributedActor)? in
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
    let services = registry.withLockedValue { state in
      state.shutDown = true
      let services = state.services
      state.services.removeAll()
      return services
    }
    await transport.shutdown()
    withExtendedLifetime(services) {}
  }

  func lookup(requestID: UUID, name: String, actorType: String, sessionID: UUID) async {
    let handler = ResultHandler(requestID: requestID, sessionID: sessionID, transport: transport)
    do {
      let actor = try registry.withLockedValue { state in
        guard !state.shutDown else { throw RPCError.shutDown }
        guard let actor = state.services[name] else { throw RPCError.unknownService(name) }
        return actor
      }
      guard String(reflecting: Swift.type(of: actor)) == actorType else { throw RPCError.actorTypeMismatch }
      guard let address = actor.id as? ActorAddress else { throw RPCError.actorTypeMismatch }
      try await handler.onReturn(value: address)
    } catch {
      try? await handler.onThrow(error: error)
    }
  }

  func execute(_ invocation: RemoteInvocation, sessionID: UUID) async {
    let handler = ResultHandler(requestID: invocation.requestID, sessionID: sessionID, transport: transport)
    do {
      let actor = try registry.withLockedValue { state in
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
    case .applicationError(let error):
      let decode = registry.withLockedValue { $0.errors[error.type] }
      if let decode, let payload = error.payload { throw try decode(payload) }
      throw error
    case .value, .void: break
    }
  }
}

private struct WeakActor {
  weak var actor: (any DistributedActor)?
}

private struct Registry {
  var actors: [ActorAddress: WeakActor] = [:]
  var services: [String: any DistributedActor] = [:]
  var errors: [String: @Sendable (Data) throws -> any Error] = [:]
  var shutDown = false
}
