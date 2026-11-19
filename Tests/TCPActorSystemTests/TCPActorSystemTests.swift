import Distributed
import Foundation
import NIOCore
import Networking
import Testing

@testable import TCPActorSystem

@Suite(.timeLimit(.minutes(1)))
struct TCPActorSystemTests {
  @Test(arguments: [Duration.seconds(2), .seconds(Int64.max) + .seconds(1)])
  func handshakeResolvesTheEndpointForAReservedActor(connectTimeout: Duration) async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: connectTimeout) {
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

  @Test(arguments: [0, WireMessage.version + 1])
  func unsupportedHandshakeVersionClosesTheConnection(version: Int) async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let connection = try await TCP.connect(
        host: server.endpoint.host, port: server.endpoint.port, connectTimeout: .seconds(2))
      try await connection.executeThenClose { inbound, outbound in
        var frames = inbound.makeAsyncIterator()
        #expect(try await frames.next() != nil)
        let hello = WireMessage.hello(version: version, endpoint: client.endpoint, capabilities: nil)
        try await outbound.write(hello.frame())
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
      #expect(try await remote.echo(42) == 42)
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

  @Test func codableRPCErrorsRoundTripThroughTCP() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      defer { withExtendedLifetime(local) {} }
      let remote = try TestCounter.resolve(id: local.id, using: client)
      await #expect(throws: CodableCounterFailure.rejected(code: 7)) {
        try await remote.failWithCodableError(code: 7)
      }
    }
  }

  enum ReplyKind: Sendable {
    case value
    case invalidErrorPayload
    case unknownErrorType
  }

  @Test(arguments: [ReplyKind.value, .invalidErrorPayload, .unknownErrorType])
  func callCarriesRecordedErrorAndReturnTypes(replyKind: ReplyKind) async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      _, client in
      let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
      let port = try #require(listener.channel.localAddress?.port)
      let endpoint = ActorEndpoint(host: "127.0.0.1", port: port, systemID: UUID())
      let remote = try TestCounter.resolve(id: ActorAddress(endpoint: endpoint, id: UUID()), using: client)
      async let result = remote.delayed(7, milliseconds: 0)

      try await listener.executeThenClose { connections in
        var connections = connections.makeAsyncIterator()
        let connection = try #require(try await connections.next())
        try await connection.executeThenClose { inbound, outbound in
          var frames = inbound.makeAsyncIterator()
          try await outbound.write(
            WireMessage.hello(version: WireMessage.version, endpoint: endpoint, capabilities: nil).frame())
          let hello = try WireMessage(frame: try #require(try await frames.next()))
          guard case .hello = hello else { throw RPCError.invalidMessage }
          let message = try WireMessage(frame: try #require(try await frames.next()))
          guard case .call(let invocation) = message else { throw RPCError.invalidMessage }
          let errorTypeName = try #require(_mangledTypeName((any Error).self))
          let returnTypeName = try #require(_mangledTypeName(Int.self))
          #expect(invocation.errorTypeName == errorTypeName)
          #expect(invocation.returnTypeName == returnTypeName)
          var decoder = TCPActorSystem.CallDecoder(
            message: invocation, decoder: JSONDecoder(), returnTypeName: invocation.returnTypeName)
          #expect(try decoder.decodeErrorType() == (any Error).self)
          #expect(try decoder.decodeReturnType() == Int.self)
          let reply: CallResult
          switch replyKind {
          case .value:
            reply = .value(try JSONEncoder().encode(7))
          case .invalidErrorPayload, .unknownErrorType:
            let mangledTypeName: String
            let payload: Data
            if replyKind == .invalidErrorPayload {
              mangledTypeName = try #require(_mangledTypeName(CodableCounterFailure.self))
              payload = Data("{}".utf8)
            } else {
              mangledTypeName = "8NoModule7MissingO"
              payload = try JSONEncoder().encode(CodableCounterFailure.rejected(code: 7))
            }
            reply = .applicationError(
              ApplicationErrorReply(
                type: String(reflecting: CodableCounterFailure.self), message: "rejected",
                mangledTypeName: mangledTypeName, payload: payload))
          }
          try await outbound.write(WireMessage.reply(invocation.requestID, reply).frame())
        }
      }
      if replyKind == .value {
        let value = try await result
        #expect(value == 7)
      } else {
        do {
          _ = try await result
          Issue.record("Expected RemoteActorError")
        } catch let error as RemoteActorError {
          #expect(error.type == String(reflecting: CodableCounterFailure.self))
          #expect(error.message == "rejected")
        }
      }
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
      let echoed = try await remote.echo(ContextualActorReference(actor: callback))
      #expect(echoed.actor === callback)
      withExtendedLifetime((local, callback)) {}
    }
  }

  @Test func overloadRejectsConsecutiveCallsWithoutDisconnecting() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(5), connectTimeout: .seconds(2)) {
      server, client in
      let (started, signal) = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingOldest(64))
      defer { signal.finish() }
      let local = TestCounter(actorSystem: server, started: signal)
      let remote = try TestCounter.resolve(id: local.id, using: client)
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        defer { tasks.cancelAll() }
        for value in 0..<64 {
          tasks.addTask { _ = try? await remote.delayed(value, milliseconds: 30_000) }
        }
        var events = started.makeAsyncIterator()
        for _ in 0..<64 { _ = try #require(await events.next()) }
        await #expect(throws: RPCError.overloaded) { try await remote.value() }
        await #expect(throws: RPCError.overloaded) { try await remote.value() }
      }
      withExtendedLifetime(local) {}
    }
  }

  @Test func payloadLimitRejectsLargeCallsWithoutDisconnecting() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(5), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      defer { withExtendedLifetime(local) {} }
      let remote = try TestCounter.resolve(id: local.id, using: client)
      let value = String(repeating: "a", count: Int(ActorFraming.maximumPayloadBytes))
      await #expect(throws: FrameError.payloadLengthOverflow) { try await remote.echo(value) }
      #expect(try await remote.value() == 0)
    }
  }

  @Test func peerWithoutCodedErrorsKeepsPendingCallsWhenOverloaded() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(10), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      defer { withExtendedLifetime(local) {} }
      let connection = try await TCP.connect(
        host: server.endpoint.host, port: server.endpoint.port, connectTimeout: .seconds(2))
      try await connection.executeThenClose { inbound, outbound in
        var frames = inbound.makeAsyncIterator()
        func send(_ message: WireMessage) async throws {
          try await outbound.write(message.frame())
        }
        func next() async throws -> WireMessage {
          guard let frame = try await frames.next(isolation: #isolation) else { throw RPCError.disconnected }
          return try WireMessage(frame: frame)
        }

        guard case .hello(WireMessage.version, server.endpoint, _) = try await next() else {
          throw RPCError.invalidMessage
        }
        try await send(.hello(version: WireMessage.version, endpoint: client.endpoint, capabilities: nil))
        let callback = ActorAddress(endpoint: client.endpoint, id: UUID())
        let arguments = [try JSONEncoder().encode(callback), try JSONEncoder().encode(7)]
        func call(_ id: UUID) -> WireMessage {
          .call(
            RemoteInvocation(
              requestID: id, actorID: local.id, actorType: String(reflecting: TestCounter.self),
              target: "$s19TCPActorSystemTests11TestCounterC8callBack_6amountSiAC_SitYaKFTE",
              arguments: arguments, substitutions: [], errorTypeName: nil, returnTypeName: nil))
        }
        var pending: Set<UUID> = []
        var callbacks: [UUID] = []
        for _ in 0..<64 {
          let id = UUID()
          pending.insert(id)
          try await send(call(id))
          guard case .call(let invocation) = try await next() else { throw RPCError.invalidMessage }
          callbacks.append(invocation.requestID)
        }
        let rejected = UUID()
        try await send(call(rejected))
        guard case .reply(rejected, .applicationError(let error)) = try await next() else {
          throw RPCError.invalidMessage
        }
        #expect(error.message == "overloaded")

        let value = try JSONEncoder().encode(7)
        for id in callbacks { try await send(.reply(id, .value(value))) }
        for _ in callbacks {
          guard case .reply(let id, .value(value)) = try await next() else { throw RPCError.invalidMessage }
          #expect(pending.remove(id) != nil)
        }
        #expect(pending.isEmpty)
      }
    }
  }

  @Test func blockedOverloadReplyLeavesTheSessionReading() async throws {
    let host = "127.0.0.1"
    try await withSystems(host: host, port: 0, callTimeout: .seconds(5), connectTimeout: .seconds(2)) {
      server, client in
      let listener = try await FramedTCP<ActorFraming>.listen(host: host, port: 0)
      let port = try #require(listener.channel.localAddress?.port)
      let (accepted, accept) = AsyncStream<any Channel>.makeStream(bufferingPolicy: .bufferingNewest(1))
      let channel = listener.channel
      try await channel.eventLoop.submit {
        try channel.pipeline.syncOperations.addHandler(CaptureAcceptedChannel(accepted: accept), position: .first)
      }.get()
      let transport = RPCTransport(
        listener: listener, endpoint: server.endpoint, transfers: server.transfers, callTimeout: .seconds(2),
        connectTimeout: .seconds(2), sessionLimit: 1)
      await transport.start(system: server)
      defer { await transport.shutdown() }
      #expect(try await client.peerEndpoint(host: host, port: port, reuseConnection: false) == server.endpoint)

      var children = accepted.makeAsyncIterator()
      let child = try #require(await children.next())
      accept.finish()
      let (blocked, block) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      try await child.eventLoop.submit {
        try child.pipeline.syncOperations.addHandler(HoldOverloadWrite(blocked: block), position: .last)
      }.get()

      let (started, signal) = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingOldest(64))
      defer { signal.finish() }
      let local = TestCounter(actorSystem: server, started: signal)
      let peer = TestCounter(actorSystem: client, started: nil)
      defer { withExtendedLifetime((local, peer)) {} }
      let remote = try TestCounter.resolve(id: local.id, using: client)
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        defer { tasks.cancelAll() }
        for value in 0..<64 {
          tasks.addTask { _ = try? await remote.delayed(value, milliseconds: 30_000) }
        }
        var executions = started.makeAsyncIterator()
        for _ in 0..<64 { _ = try #require(await executions.next()) }

        async let rejection = remote.value()
        var writes = blocked.makeAsyncIterator()
        _ = try #require(await writes.next())
        let invocation = RemoteInvocation(
          requestID: UUID(), actorID: peer.id, actorType: String(reflecting: TestCounter.self),
          target: "$s19TCPActorSystemTests11TestCounterC5valueSiyYaKFTE", arguments: [], substitutions: [],
          errorTypeName: nil, returnTypeName: nil)
        guard case .value(let data) = try await transport.call(invocation) else { throw RPCError.invalidMessage }
        #expect(try JSONDecoder().decode(Int.self, from: data) == 0)
        do {
          _ = try await rejection
          Issue.record("Expected the stalled session to close")
        } catch let error as RPCError {
          #expect(error == .disconnected)
        }
      }
    }
  }

  @Test func unknownWireErrorPreservesItsCode() {
    #expect(RPCError(code: "future-error", detail: nil) == .unknownRemoteError("future-error"))
  }

  @Test func reclaimsIdleSessionsForBothDialsAndAcceptedConnections() async throws {
    let host = "127.0.0.1"
    try await withSystems(host: host, port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) { first, second in
      let listener = try await FramedTCP<ActorFraming>.listen(host: host, port: 0)
      let endpoint = ActorEndpoint(
        host: host, port: try #require(listener.channel.localAddress?.port), systemID: UUID())
      let transport = RPCTransport(
        listener: listener, endpoint: endpoint, transfers: first.transfers, callTimeout: .seconds(2),
        connectTimeout: .seconds(2), sessionLimit: 1)
      await transport.start(system: first)
      defer { await transport.shutdown() }

      #expect(
        try await transport.peerEndpoint(host: host, port: first.endpoint.port, reuseConnection: true) == first.endpoint
      )
      #expect(
        try await transport.peerEndpoint(host: host, port: second.endpoint.port, reuseConnection: true)
          == second.endpoint)
      #expect(
        try await transport.peerEndpoint(host: host, port: first.endpoint.port, reuseConnection: true) == first.endpoint
      )
      #expect(try await second.peerEndpoint(host: host, port: endpoint.port, reuseConnection: false) == endpoint)
    }
  }

  @Test func droppingAnActorSystemReleasesItsListenerAndIdleSessions() async throws {
    var owner: TCPActorSystem? = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    let endpoint = try #require(owner?.endpoint)
    let client = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await client.shutdown() }
    #expect(try await client.peerEndpoint(host: endpoint.host, port: endpoint.port, reuseConnection: true) == endpoint)
    weak let released = owner
    owner = nil
    #expect(released == nil)
    await released?.shutdown()

    let deadline = ContinuousClock.now + .seconds(2)
    while true {
      do {
        let listener = try await TCP.listen(host: endpoint.host, port: endpoint.port)
        try await listener.channel.close()
        break
      } catch {
        guard ContinuousClock.now < deadline else { throw error }
        try await Task.sleep(for: .milliseconds(10))
      }
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

  @Test(arguments: [false, true])
  func distributedMethodCanStopItsOwnSystem(shutdown: Bool) async throws {
    let server = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    let client = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await client.shutdown() }

    let (returned, signal) = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
    let local = ShutdownActor(actorSystem: server, returned: signal)
    defer { withExtendedLifetime(local) {} }
    let remote = try ShutdownActor.resolve(id: local.id, using: client)
    let watchdog = Task {
      try await Task.sleep(for: .seconds(5))
      signal.finish()
    }
    defer {
      watchdog.cancel()
      signal.finish()
    }

    await #expect(throws: RPCError.disconnected) { try await remote.stop(shutdown: shutdown) }
    var events = returned.makeAsyncIterator()
    _ = try #require(await events.next(), "Stopping the system never returned to the distributed method")
    #expect(throws: RPCError.shutDown) { try ShutdownActor.resolve(id: local.id, using: server) }
    await server.shutdown()
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
    let invalidConnectTimeouts: [Duration] = [.zero, .nanoseconds(-1), .nanoseconds(1) / 2]
    for connectTimeout in invalidConnectTimeouts {
      await #expect(throws: RPCError.invalidConfiguration) {
        try await TCPActorSystem(
          host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(1),
          connectTimeout: connectTimeout)
      }
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

  @Test func malformedInvocationArgumentKeepsConnectionUsable() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let local = TestCounter(actorSystem: server, started: nil)
      defer { withExtendedLifetime(local) {} }
      let connection = try await TCP.connect(
        host: server.endpoint.host, port: server.endpoint.port, connectTimeout: .seconds(2))
      try await connection.executeThenClose { inbound, outbound in
        var frames = inbound.makeAsyncIterator()
        let hello = try WireMessage(frame: try #require(try await frames.next()))
        guard case .hello = hello else { throw RPCError.invalidMessage }
        try await outbound.write(
          WireMessage.hello(
            version: WireMessage.version, endpoint: client.endpoint, capabilities: [WireMessage.codedErrors]
          ).frame())

        func call(arguments: [Data], returnTypeName: String?) async throws -> CallResult {
          let id = UUID()
          let invocation = RemoteInvocation(
            requestID: id, actorID: local.id, actorType: String(reflecting: TestCounter.self),
            target: "$s19TCPActorSystemTests11TestCounterC3addyS2iYaKFTE", arguments: arguments, substitutions: [],
            errorTypeName: nil, returnTypeName: returnTypeName)
          try await outbound.write(WireMessage.call(invocation).frame())
          let frame = try #require(try await frames.next(isolation: #isolation))
          guard case .reply(id, let result) = try WireMessage(frame: frame) else { throw RPCError.invalidMessage }
          return result
        }

        let invalidArgument = try JSONEncoder().encode("not an integer")
        guard case .codedError(let code, let detail) = try await call(arguments: [invalidArgument], returnTypeName: nil)
        else { throw RPCError.invalidMessage }
        #expect(RPCError(code: code, detail: detail) == .invalidMessage)
        let validArgument = try JSONEncoder().encode(7)
        let unexpectedTypeName = try #require(_mangledTypeName(Void.self))
        guard case .value(let value) = try await call(arguments: [validArgument], returnTypeName: unexpectedTypeName)
        else { throw RPCError.invalidMessage }
        #expect(try JSONDecoder().decode(Int.self, from: value) == 7)
      }
    }
  }

  @Test(arguments: [false, true])
  func timeoutIncludesHandshakeAndShutdownClosesTheDial(discoveringEndpoint: Bool) async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let client = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .milliseconds(100),
      connectTimeout: .seconds(2))
    let endpoint = ActorEndpoint(host: "127.0.0.1", port: port, systemID: UUID())
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

func withSystems(
  host: String, port: Int, callTimeout: Duration, connectTimeout: Duration,
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

private final class CaptureAcceptedChannel: ChannelInboundHandler {
  typealias InboundIn = any Channel
  let accepted: AsyncStream<any Channel>.Continuation

  init(accepted: AsyncStream<any Channel>.Continuation) { self.accepted = accepted }

  func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    accepted.yield(unwrapInboundIn(data))
    context.fireChannelRead(data)
  }
}

private final class HoldOverloadWrite: ChannelOutboundHandler {
  typealias OutboundIn = Frame
  let blocked: AsyncStream<Void>.Continuation
  private var held: EventLoopPromise<Void>?

  init(blocked: AsyncStream<Void>.Continuation) { self.blocked = blocked }

  func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
    if case .reply(_, .codedError(code: "overloaded", detail: _))? = try? WireMessage(frame: unwrapOutboundIn(data)) {
      held = promise
      blocked.yield(())
    } else {
      context.write(data, promise: promise)
    }
  }

  func handlerRemoved(context: ChannelHandlerContext) {
    held?.fail(RPCError.disconnected)
    blocked.finish()
  }
}

private enum CounterFailure: Error, Codable, Sendable {
  case rejected
}

struct ContextualActorReference: Codable, Sendable {
  let actor: TestCounter

  init(actor: TestCounter) {
    self.actor = actor
  }

  init(from decoder: any Decoder) throws {
    self.actor = try decoder.singleValueContainer().decode(TestCounter.self)
  }

  func encode(to encoder: any Encoder) throws {
    guard (encoder.userInfo[.actorSystemKey] as? TCPActorSystem) === actor.actorSystem else {
      throw EncodingError.invalidValue(
        actor, EncodingError.Context(codingPath: encoder.codingPath, debugDescription: "Unexpected actor system"))
    }
    var container = encoder.singleValueContainer()
    try container.encode(actor)
  }
}

enum CodableCounterFailure: CodableRPCError, Equatable {
  case rejected(code: Int)
}

distributed actor ReservedCounter: FixedActorID {
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

  distributed func failWithCodableError(code: Int) throws { throw CodableCounterFailure.rejected(code: code) }

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

private distributed actor ShutdownActor {
  typealias ActorSystem = TCPActorSystem
  let returned: AsyncStream<Void>.Continuation

  init(actorSystem: TCPActorSystem, returned: AsyncStream<Void>.Continuation) {
    self.actorSystem = actorSystem
    self.returned = returned
  }

  distributed func stop(shutdown: Bool) async {
    if shutdown {
      await actorSystem.shutdown()
    } else {
      await actorSystem.close()
    }
    returned.yield(())
  }
}
