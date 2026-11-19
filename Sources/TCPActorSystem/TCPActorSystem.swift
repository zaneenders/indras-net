public import Distributed
package import Foundation
import NIOCore
import NIOPosix
import Networking
import Synchronization
import SystemPackage

package protocol FixedActorID {
  static var fixedID: UUID { get }
}

public final class TCPActorSystem: DistributedActorSystem, Sendable {
  public typealias ActorID = ActorAddress
  public typealias InvocationEncoder = CallEncoder
  public typealias InvocationDecoder = CallDecoder
  public typealias ResultHandler = Handler
  public typealias SerializationRequirement = Sendable & Codable

  public let transfers: DataTransfers
  public let endpoint: ActorEndpoint
  private let registry = Mutex(Registry(actors: [:], fixedIDs: [], shutDown: false))
  private let transport: RPCTransport

  public init(host: String, advertisedHost: String, port: Int, callTimeout: Duration, connectTimeout: Duration)
    async throws
  {
    guard !host.isEmpty, !advertisedHost.isEmpty, (0...65535).contains(port), callTimeout > .zero,
      connectTimeout >= .nanoseconds(1)
    else {
      throw RPCError.invalidConfiguration
    }
    let transportTimeout = TimeAmount(min(connectTimeout, .nanoseconds(Int64.max)))
    let listener = try await FramedTCP<ActorFraming>.listen(host: host, port: port)
    guard let boundPort = listener.channel.localAddress?.port else {
      try? await listener.channel.close()
      throw RPCError.invalidConfiguration
    }
    self.endpoint = ActorEndpoint(host: advertisedHost, port: boundPort, systemID: UUID())
    self.transfers = DataTransfers(endpoint: endpoint, connectTimeout: transportTimeout)
    self.transport = RPCTransport(
      listener: listener, endpoint: endpoint, transfers: transfers, callTimeout: callTimeout,
      connectTimeout: transportTimeout, sessionLimit: 256)
    await transport.start(system: self)
  }

  deinit {
    guard !registry.withLock({ $0.shutDown }) else { return }
    let transfers = self.transfers
    let transport = self.transport
    Task {
      async let stopTransfers: Void = transfers.shutdown()
      await transport.shutdown()
      await stopTransfers
    }
  }

  package func peerEndpoint(host: String, port: Int, reuseConnection: Bool) async throws -> ActorEndpoint {
    guard !registry.withLock({ $0.shutDown }) else { throw RPCError.shutDown }
    return try await transport.peerEndpoint(host: host, port: port, reuseConnection: reuseConnection)
  }

  /// A fresh handshake distinguishes a live incumbent from a replacement at the same address.
  package func isListening(_ endpoint: ActorEndpoint) async throws -> Bool {
    do {
      return try await peerEndpoint(host: endpoint.host, port: endpoint.port, reuseConnection: false) == endpoint
    } catch let error as IOError {
      if error.errnoCode == Errno.connectionRefused.rawValue { return false }
      throw RPCError.disconnected
    } catch let error as NIOConnectionError {
      if !error.connectionErrors.isEmpty,
        error.connectionErrors.allSatisfy({
          ($0.error as? IOError)?.errnoCode == Errno.connectionRefused.rawValue
        })
      {
        return false
      }
      throw RPCError.disconnected
    }
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
    CallEncoder(encoder: makeJSONEncoder())
  }

  public func remoteCall<Act, Err, Res>(
    on actor: Act, target: RemoteCallTarget, invocation: inout InvocationEncoder,
    throwing: Err.Type, returning: Res.Type
  ) async throws -> Res where Act: DistributedActor, Act.ID == ActorID, Err: Error, Res: SerializationRequirement {
    let result = try await call(
      actorID: actor.id, actorType: String(reflecting: Act.self), target: target, invocation: invocation)
    try checkError(result)
    guard case .value(let data) = result else { throw RPCError.invalidMessage }
    return try makeJSONDecoder().decode(Res.self, from: data)
  }

  public func remoteCallVoid<Act, Err>(
    on actor: Act, target: RemoteCallTarget, invocation: inout InvocationEncoder, throwing: Err.Type
  ) async throws where Act: DistributedActor, Act.ID == ActorID, Err: Error {
    let result = try await call(
      actorID: actor.id, actorType: String(reflecting: Act.self), target: target, invocation: invocation)
    try checkError(result)
    guard case .void = result else { throw RPCError.invalidMessage }
  }

  public func close() async {
    registry.withLock { $0.shutDown = true }
    async let closeTransfers: Void = transfers.close()
    await transport.close()
    await closeTransfers
  }

  /// Closes the system and joins managed work when called outside its callbacks.
  public func shutdown() async {
    if ManagedActorSystemWork.systemIDs.contains(endpoint.systemID) {
      await close()
      return
    }
    registry.withLock { $0.shutDown = true }
    async let stopTransfers: Void = transfers.shutdown()
    await transport.shutdown()
    await stopTransfers
  }

  func execute(_ invocation: RemoteInvocation, sessionID: UUID) async {
    let handler = Handler(
      requestID: invocation.requestID, sessionID: sessionID, transport: transport, encoder: makeJSONEncoder())
    do {
      let actor = try registry.withLock { state in
        guard !state.shutDown else { throw RPCError.shutDown }
        guard let actor = state.actors[invocation.actorID]?.actor else { throw RPCError.unknownActor }
        return actor
      }
      guard String(reflecting: Swift.type(of: actor)) == invocation.actorType else {
        throw RPCError.actorTypeMismatch
      }
      // Swift allocates the result buffer using the local method's declared type.
      var decoder = CallDecoder(message: invocation, decoder: makeJSONDecoder(), returnTypeName: nil)
      // RPC assumes trusted clients. Swift-generated calls pair the actor type with its method.
      // Validate that the target belongs to the local actor type before exposing RPC to untrusted clients.
      // TODO(Swift 6.5): Revisit the runtime's target lookup and generic substitution checks after upgrading.
      let systemIDs = ManagedActorSystemWork.systemIDs.union([endpoint.systemID])
      try await ManagedActorSystemWork.$systemIDs.withValue(systemIDs) {
        try await executeDistributedTarget(
          on: actor, target: RemoteCallTarget(invocation.target), invocationDecoder: &decoder, handler: handler)
      }
    } catch {
      try? await handler.onThrow(error: error)
    }
  }

  private func call(
    actorID: ActorID, actorType: String, target: RemoteCallTarget, invocation: InvocationEncoder
  ) async throws -> CallResult {
    guard !registry.withLock({ $0.shutDown }) else { throw RPCError.shutDown }
    let call = RemoteInvocation(
      requestID: UUID(), actorID: actorID, actorType: actorType, target: target.identifier,
      arguments: invocation.arguments, substitutions: invocation.substitutions,
      errorTypeName: invocation.errorTypeName, returnTypeName: invocation.returnTypeName)
    return try await transport.call(call)
  }

  private func checkError(_ result: CallResult) throws {
    switch result {
    case .systemError(let error): throw error
    case .codedError(let code, let detail): throw RPCError(code: code, detail: detail)
    case .applicationError(let reply):
      if let payload = reply.payload,
        let name = reply.mangledTypeName,
        let type = _typeByName(name) as? any CodableRPCError.Type,
        let error = try? makeJSONDecoder().decode(type, from: payload)
      {
        throw error
      }
      throw RemoteActorError(type: reply.type, message: reply.message)
    case .value, .void: break
    }
  }

  private func makeJSONEncoder() -> JSONEncoder {
    let encoder = JSONEncoder()
    encoder.userInfo[.actorSystemKey] = self
    return encoder
  }

  private func makeJSONDecoder() -> JSONDecoder {
    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = self
    return decoder
  }
}

enum ManagedActorSystemWork {
  @TaskLocal static var systemIDs: Set<UUID> = []
}

private struct WeakActor: Sendable {
  weak var actor: (any DistributedActor)?
}

private struct Registry: Sendable {
  var actors: [ActorAddress: WeakActor]
  var fixedIDs: Set<UUID>
  var shutDown: Bool
}
