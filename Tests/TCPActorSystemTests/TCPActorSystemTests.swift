import Distributed
import Foundation
import NIOCore
import Networking
import Testing

@testable import TCPActorSystem

@Suite(.timeLimit(.minutes(1)))
struct TCPActorSystemTests {
  @Test func handshakeResolvesTheEndpointForAReservedActor() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      try server.reserveFixedActorIDs([ReservedCounter.fixedID])
      let counter = ReservedCounter(actorSystem: server)
      let endpoint = try await client.peerEndpoint(host: "localhost", port: server.endpoint.port, reuseConnection: true)
      #expect(endpoint == server.endpoint)
      let remote = try ReservedCounter.resolve(
        id: ActorAddress(endpoint: endpoint, id: ReservedCounter.fixedID), using: client)
      #expect(try await remote.value() == 42)
      #expect(remote.id == counter.id)
      withExtendedLifetime(counter) {}
    }
  }

  @Test func unsupportedHandshakeVersionClosesTheConnection() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let connection = try await TCP.connect(
        host: server.endpoint.host, port: server.endpoint.port, connectTimeout: .seconds(2))
      try await connection.executeThenClose { inbound, outbound in
        var frames = inbound.makeAsyncIterator()
        #expect(try await frames.next() != nil)
        try await outbound.write(WireMessage.hello(version: WireMessage.version + 1, endpoint: client.endpoint).frame())
        #expect(try await frames.next() == nil)
      }
      #expect(
        try await client.peerEndpoint(host: server.endpoint.host, port: server.endpoint.port, reuseConnection: true)
          == server.endpoint)
    }
  }

  @Test func localResolutionAndWeakRegistration() async throws {
    let system = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    do {
      var counter: TestCounter? = TestCounter(actorSystem: system, started: nil)
      let address = try #require(counter?.id)
      #expect(try TestCounter.resolve(id: address, using: system) === counter)
      #expect(throws: RPCError.actorTypeMismatch) { try OtherActor.resolve(id: address, using: system) }
      counter = nil
      #expect(throws: RPCError.unknownActor) { try TestCounter.resolve(id: address, using: system) }
      await system.shutdown()
      #expect(throws: RPCError.shutDown) { try TestCounter.resolve(id: address, using: system) }
    } catch {
      await system.shutdown()
      throw error
    }
  }

  @Test func valuesVoidGenericCallsAndRemoteErrors() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      #expect(try await remote.add(5) == 5)
      try await remote.reset()
      #expect(try await remote.value() == 0)
      #expect(try await remote.echo(["hello", "world"]) == ["hello", "world"])
      do {
        try await remote.fail()
        Issue.record("Expected a remote error")
      } catch let error as RemoteActorError {
        #expect(error.type == String(reflecting: CounterFailure.self))
        #expect(error.message == "rejected")
      }
      #expect(try await remote.add(1) == 1)
      withExtendedLifetime(local) {}
    }
  }

  @Test func concurrentRepliesCorrelateWithTheirRequests() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(5), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        for value in 0..<40 {
          tasks.addTask {
            let reply = try await remote.delayed(value, milliseconds: (40 - value) * 2)
            #expect(reply == value)
          }
        }
        try await tasks.waitForAll()
      }
      #expect(try await remote.add(1) == 1)
      withExtendedLifetime(local) {}
    }
  }

  @Test func actorReferencesSupportCallbacksAndReturnValues() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      let callback = TestCounter(actorSystem: client, started: nil)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      #expect(try await remote.callBack(callback, amount: 7) == 7)
      #expect(try await callback.value() == 7)
      let reference = try await remote.reference()
      #expect(reference.id == remote.id)
      #expect(try await reference.add(3) == 3)
      withExtendedLifetime((local, callback)) {}
    }
  }

  @Test func timeoutDiscardsLateReplyAndKeepsConnectionUsable() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .milliseconds(100), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      await #expect(throws: RPCError.timedOut) { try await remote.delayed(99, milliseconds: 250) }
      #expect(try await remote.add(1) == 1)
      try await Task.sleep(for: .milliseconds(300))
      #expect(try await remote.add(1) == 2)
      withExtendedLifetime(local) {}
    }
  }

  @Test func timedOutMutationIsNotReplayed() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .milliseconds(100), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      await #expect(throws: RPCError.timedOut) { try await remote.addThenPause(1, milliseconds: 250) }
      #expect(try await remote.value() == 1)
      withExtendedLifetime(local) {}
    }
  }

  @Test func cancellationReleasesPendingCall() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(30), connectTimeout: .seconds(2)) {
      server, client in
      let (started, signal) = AsyncStream<Int>.makeStream()
      let local = TestCounter(actorSystem: server, started: signal)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      let call = Task { try await remote.delayed(7, milliseconds: 30000) }
      var events = started.makeAsyncIterator()
      #expect(await events.next() == 7)
      call.cancel()
      await #expect(throws: CancellationError.self) { try await call.value }
      #expect(try await remote.add(1) == 1)
      withExtendedLifetime(local) {}
    }
  }

  @Test func disconnectFailsPendingCall() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(30), connectTimeout: .seconds(2)) {
      server, client in
      let (started, signal) = AsyncStream<Int>.makeStream()
      let local = TestCounter(actorSystem: server, started: signal)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      let call = Task { try await remote.delayed(7, milliseconds: 30000) }
      var events = started.makeAsyncIterator()
      #expect(await events.next() == 7)
      await server.shutdown()
      await #expect(throws: RPCError.disconnected) { try await call.value }
      withExtendedLifetime(local) {}
    }
  }

  @Test func shutdownFailsPendingCallsAndRejectsNewOnes() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(30), connectTimeout: .seconds(2)) {
      server, client in
      let (started, signal) = AsyncStream<Int>.makeStream()
      let local = TestCounter(actorSystem: server, started: signal)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      let call = Task { try await remote.delayed(7, milliseconds: 30000) }
      var events = started.makeAsyncIterator()
      #expect(await events.next() == 7)
      await client.shutdown()
      await #expect(throws: RPCError.shutDown) { try await call.value }
      await #expect(throws: RPCError.shutDown) { try await remote.value() }
      withExtendedLifetime(local) {}
    }
  }

  @Test func unknownRemoteActorReturnsAnError() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let address = ActorAddress(endpoint: server.endpoint, id: UUID())
      let remote = try TestCounter.resolve(id: address, using: client)
      await #expect(throws: RPCError.unknownActor) { try await remote.value() }
      let local = TestCounter(actorSystem: server, started: nil)
      let wrongType = try OtherActor.resolve(id: local.id, using: client)
      await #expect(throws: RPCError.actorTypeMismatch) { try await wrongType.value() }
      withExtendedLifetime(local) {}
    }
  }

  @Test func invalidConfigurationIsRejected() async throws {
    await #expect(throws: RPCError.invalidConfiguration) {
      try await TCPActorSystem(
        host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .zero, connectTimeout: .seconds(1))
    }
  }

  @Test func staleNodeIdentityIsRejectedAfterRestart() async throws {
    let original = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    let oldActor = TestCounter(actorSystem: original, started: nil)
    let address = oldActor.id
    await original.shutdown()
    let replacement = try await TCPActorSystem(
      host: address.endpoint.host, advertisedHost: address.endpoint.host, port: address.endpoint.port,
      callTimeout: .seconds(2), connectTimeout: .seconds(2))
    do {
      let client = try await TCPActorSystem(
        host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
      do {
        let stale = try TestCounter.resolve(id: address, using: client)
        await #expect(throws: RPCError.disconnected) { try await stale.value() }
        await client.shutdown()
      } catch {
        await client.shutdown()
        throw error
      }
      await replacement.shutdown()
    } catch {
      await replacement.shutdown()
      throw error
    }
    withExtendedLifetime(oldActor) {}
  }

  @Test func malformedPeerIsClosedWithoutStoppingTheSystem() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let connection = try await TCP.connect(
        host: server.endpoint.host, port: server.endpoint.port, connectTimeout: .seconds(2))
      try await connection.executeThenClose { inbound, outbound in
        var messages = inbound.makeAsyncIterator()
        #expect(try await messages.next() != nil)
        try await outbound.write(Frame(target: 0, opcode: 0, payload: ByteBuffer(string: "not JSON")))
        #expect(try await messages.next() == nil)
      }
      let local = TestCounter(actorSystem: server, started: nil)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      #expect(try await remote.add(1) == 1)
      withExtendedLifetime(local) {}
    }
  }

  @Test(arguments: [false, true])
  func timeoutIncludesHandshakeAndShutdownClosesTheDial(discoveringEndpoint: Bool) async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let client = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .milliseconds(100),
      connectTimeout: .seconds(2))
    let endpoint = ActorEndpoint(host: "127.0.0.1", port: port, node: UUID())
    let remote = try TestCounter.resolve(id: ActorAddress(endpoint: endpoint, id: UUID()), using: client)
    do {
      let call = Task {
        if discoveringEndpoint {
          _ = try await client.peerEndpoint(host: endpoint.host, port: endpoint.port, reuseConnection: true)
        } else {
          _ = try await remote.value()
        }
      }
      try await listener.executeThenClose { connections in
        var connections = connections.makeAsyncIterator()
        let connection = try #require(try await connections.next())
        try await connection.executeThenClose { inbound, _ in
          var frames = inbound.makeAsyncIterator()
          let hello = try await frames.next()
          #expect(hello != nil)
          await #expect(throws: RPCError.timedOut) { try await call.value }
          await client.shutdown()
          let end = try await frames.next()
          #expect(end == nil)
        }
      }
      await client.shutdown()
    } catch {
      await client.shutdown()
      try? await listener.channel.close()
      throw error
    }
  }
}

private func withSystems(
  host: String, port: Int, callTimeout: Duration, connectTimeout: TimeAmount,
  _ body: @Sendable (TCPActorSystem, TCPActorSystem) async throws -> Void
) async throws {
  let server = try await TCPActorSystem(
    host: host, advertisedHost: host, port: port, callTimeout: callTimeout, connectTimeout: connectTimeout)
  do {
    let client = try await TCPActorSystem(
      host: host, advertisedHost: host, port: port, callTimeout: callTimeout, connectTimeout: connectTimeout)
    do {
      try await body(server, client)
      await client.shutdown()
    } catch {
      await client.shutdown()
      throw error
    }
    await server.shutdown()
  } catch {
    await server.shutdown()
    throw error
  }
}

private enum CounterFailure: Error, Codable, Sendable {
  case rejected
}

private distributed actor ReservedCounter: FixedActorID {
  typealias ActorSystem = TCPActorSystem
  static let fixedID = UUID(uuid: (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3))

  distributed func value() -> Int { 42 }
}

distributed actor TestCounter {
  typealias ActorSystem = TCPActorSystem
  private var count = 0
  private let started: AsyncStream<Int>.Continuation?

  init(actorSystem: ActorSystem, started: AsyncStream<Int>.Continuation?) {
    self.actorSystem = actorSystem
    self.started = started
  }

  distributed func add(_ amount: Int) -> Int {
    count += amount
    return count
  }

  distributed func value() -> Int { count }

  distributed func reset() { count = 0 }

  distributed func fail() throws { throw CounterFailure.rejected }

  distributed func echo<Value: Codable & Sendable>(_ value: Value) -> Value { value }

  distributed func delayed(_ value: Int, milliseconds: Int) async throws -> Int {
    started?.yield(value)
    try await Task.sleep(for: .milliseconds(milliseconds))
    return value
  }

  distributed func addThenPause(_ amount: Int, milliseconds: Int) async throws -> Int {
    let value = add(amount)
    try await Task.sleep(for: .milliseconds(milliseconds))
    return value
  }

  distributed func callBack(_ counter: TestCounter, amount: Int) async throws -> Int {
    try await counter.add(amount)
  }

  distributed func reference() -> TestCounter { self }
}

distributed actor OtherActor {
  typealias ActorSystem = TCPActorSystem

  distributed func value() -> Int { 99 }
}
