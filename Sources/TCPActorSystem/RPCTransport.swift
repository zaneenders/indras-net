import Foundation
import NIOCore
import Networking

actor RPCTransport {
  private let listener: Listener
  private let callTimeout: Duration
  private let connectTimeout: TimeAmount
  private var acceptTask: Task<Void, Never>?
  private var shutdownTask: Task<Void, Never>?
  private var stopped = false
  private var sessions: [UUID: Session] = [:]
  private var routes: [ActorEndpoint: UUID] = [:]
  private var dials: [DialDestination: Task<UUID, any Error>] = [:]
  private var pending: [UUID: PendingCall] = [:]

  init(listener: Listener, callTimeout: Duration, connectTimeout: TimeAmount) {
    self.listener = listener
    self.callTimeout = callTimeout
    self.connectTimeout = connectTimeout
  }

  func start(system: TCPActorSystem) {
    acceptTask = Task {
      do {
        try await listener.executeThenClose { connections in
          for try await connection in connections {
            self.accept(connection, system: system)
          }
        }
      } catch {}
    }
  }

  func call(_ invocation: RemoteInvocation, system: TCPActorSystem) async throws -> CallResult {
    let result = try await request(id: invocation.requestID) {
      let endpoint = invocation.actorID.endpoint
      let sessionID = try await self.session(
        host: endpoint.host, port: endpoint.port, expected: endpoint, system: system)
      try await self.sendRequest(.call(invocation), requestID: invocation.requestID, sessionID: sessionID)
    }
    guard case .reply(let reply) = result else { throw RPCError.invalidMessage }
    return reply
  }

  func peerEndpoint(host: String, port: Int, system: TCPActorSystem) async throws -> ActorEndpoint {
    try Task.checkCancellation()
    guard !stopped else { throw RPCError.shutDown }
    let destination = DialDestination(host: host, port: port, node: nil)
    if let session = sessionAtAddress(destination), let endpoint = session.value.endpoint { return endpoint }

    let requestID = UUID()
    let result = try await request(id: requestID) {
      try await self.discoverEndpoint(host: host, port: port, requestID: requestID, system: system)
    }
    guard case .endpoint(let endpoint) = result else { throw RPCError.invalidMessage }
    return endpoint
  }

  private func discoverEndpoint(
    host: String, port: Int, requestID: UUID, system: TCPActorSystem
  ) async throws {
    let sessionID = try await session(host: host, port: port, expected: nil, system: system)
    guard let endpoint = sessions[sessionID]?.endpoint else { throw RPCError.disconnected }
    complete(requestID, result: .success(.endpoint(endpoint)))
  }

  private func request(id requestID: UUID, send operation: @escaping @Sendable () async throws -> Void) async throws
    -> RequestResult
  {
    return try await withTaskCancellationHandler {
      try Task.checkCancellation()
      guard !stopped else { throw RPCError.shutDown }
      let deadline = ContinuousClock.now.advanced(by: callTimeout)
      return try await withCheckedThrowingContinuation { continuation in
        let timeout = Task {
          do {
            try await Task.sleep(until: deadline, clock: .continuous)
            complete(requestID, result: .failure(RPCError.timedOut))
          } catch {}
        }
        let send = Task {
          do {
            try await operation()
          } catch {
            complete(requestID, result: .failure(error))
          }
        }
        pending[requestID] = PendingCall(continuation: continuation, timeout: timeout, send: send, sessionID: nil)
      }
    } onCancel: {
      Task { await self.complete(requestID, result: .failure(CancellationError())) }
    }
  }

  func reply(_ result: CallResult, requestID: UUID, sessionID: UUID) async throws {
    guard let writer = sessions[sessionID]?.writer else { throw RPCError.disconnected }
    try await writer.write(WireMessage.reply(requestID, result).frame())
  }

  func shutdown() async {
    if let shutdownTask {
      await shutdownTask.value
      return
    }
    stopped = true
    let task = Task { await stopResources() }
    shutdownTask = task
    await task.value
  }

  private func stopResources() async {
    let active = Array(sessions.values)
    let dialing = Array(dials.values)
    let accepting = acceptTask
    for id in Array(pending.keys) { complete(id, result: .failure(RPCError.shutDown)) }
    accepting?.cancel()
    for dial in dialing { dial.cancel() }
    try? await listener.channel.close()
    for session in active {
      session.task.cancel()
      session.handshakeTimeout.cancel()
      for task in session.executions.values { task.cancel() }
      try? await session.connection.channel.close()
    }
    await accepting?.value
    for dial in dialing { _ = try? await dial.value }
    for session in active { await session.task.value }
    acceptTask = nil
    routes.removeAll()
    dials.removeAll()
  }

  private func accept(_ connection: Connection, system: TCPActorSystem) {
    if stopped {
      Task { try? await connection.channel.close() }
      return
    }
    install(connection, destination: nil, expected: nil, ready: nil, system: system)
  }

  private func session(host: String, port: Int, expected: ActorEndpoint?, system: TCPActorSystem) async throws -> UUID {
    guard !stopped else { throw RPCError.shutDown }
    guard !host.isEmpty, (1...65535).contains(port) else { throw RPCError.invalidMessage }
    let destination = DialDestination(host: host, port: port, node: expected?.node)
    if let expected {
      if let id = routes[expected], sessions[id]?.connection.channel.isActive == true { return id }
    } else if let entry = sessionAtAddress(destination) {
      return entry.key
    }
    if let dial = dials[destination] { return try await dial.value }
    let task = Task {
      let connection = try await TCP.connect(host: host, port: port, connectTimeout: connectTimeout)
      guard !stopped, !Task.isCancelled else {
        try? await connection.channel.close()
        throw RPCError.shutDown
      }
      return try await withCheckedThrowingContinuation { continuation in
        install(connection, destination: destination, expected: expected, ready: continuation, system: system)
      }
    }
    dials[destination] = task
    defer { dials[destination] = nil }
    return try await task.value
  }

  private func sessionAtAddress(_ destination: DialDestination) -> (key: UUID, value: Session)? {
    sessions.first { _, session in
      guard let endpoint = session.endpoint, session.connection.channel.isActive else { return false }
      return session.destination == destination
        || (endpoint.host == destination.host && endpoint.port == destination.port)
    }
  }

  private func install(
    _ connection: Connection, destination: DialDestination?, expected: ActorEndpoint?,
    ready: CheckedContinuation<UUID, any Error>?,
    system: TCPActorSystem
  ) {
    let id = UUID()
    let task = Task {
      do {
        try await connection.executeThenClose { inbound, outbound in
          await self.opened(id, writer: outbound)
          try await outbound.write(WireMessage.hello(version: WireMessage.version, endpoint: system.endpoint).frame())
          for try await frame in inbound {
            try await self.receive(WireMessage(frame: frame), sessionID: id, system: system)
          }
        }
      } catch {}
      await closed(id)
    }
    let handshakeTimeout = Task {
      do {
        try await Task.sleep(for: .nanoseconds(connectTimeout.nanoseconds))
        try? await connection.channel.close()
      } catch {}
    }
    sessions[id] = Session(
      connection: connection, task: task, handshakeTimeout: handshakeTimeout,
      destination: destination, expected: expected, endpoint: nil, writer: nil, ready: ready, executions: [:])
  }

  private func opened(_ id: UUID, writer: NIOAsyncChannelOutboundWriter<Frame>) {
    sessions[id]?.writer = writer
  }

  private func receive(_ message: WireMessage, sessionID: UUID, system: TCPActorSystem) throws {
    guard var session = sessions[sessionID] else { throw RPCError.disconnected }
    switch message {
    case .hello(let version, let endpoint):
      guard version == WireMessage.version, session.endpoint == nil,
        session.expected == nil || session.expected == endpoint,
        !endpoint.host.isEmpty, (1...65535).contains(endpoint.port)
      else { throw RPCError.invalidMessage }
      session.endpoint = endpoint
      session.handshakeTimeout.cancel()
      session.ready?.resume(returning: sessionID)
      session.ready = nil
      sessions[sessionID] = session
      if routes[endpoint].flatMap({ sessions[$0]?.connection.channel.isActive }) != true {
        routes[endpoint] = sessionID
      }
    case .reply(let id, let result):
      guard session.endpoint != nil else { throw RPCError.invalidMessage }
      guard pending[id]?.sessionID == sessionID else { return }
      complete(id, result: .success(.reply(result)))
    case .call(let invocation):
      guard session.endpoint != nil, session.executions[invocation.requestID] == nil else {
        throw RPCError.invalidMessage
      }
      let task = Task {
        await system.execute(invocation, sessionID: sessionID)
        executionFinished(invocation.requestID, sessionID: sessionID)
      }
      session.executions[invocation.requestID] = task
      sessions[sessionID] = session
    }
  }

  private func executionFinished(_ id: UUID, sessionID: UUID) {
    sessions[sessionID]?.executions[id] = nil
  }

  private func sendRequest(_ message: WireMessage, requestID: UUID, sessionID: UUID) async throws {
    guard pending[requestID] != nil else { return }
    guard let writer = sessions[sessionID]?.writer else { throw RPCError.disconnected }
    pending[requestID]?.sessionID = sessionID
    try await writer.write(message.frame())
  }

  private func closed(_ id: UUID) async {
    guard let session = sessions.removeValue(forKey: id) else { return }
    session.handshakeTimeout.cancel()
    session.ready?.resume(throwing: RPCError.disconnected)
    if let endpoint = session.endpoint, routes[endpoint] == id {
      routes[endpoint] =
        sessions.first {
          $0.value.endpoint == endpoint && $0.value.connection.channel.isActive
        }?.key
    }
    for requestID in Array(pending.keys) where pending[requestID]?.sessionID == id {
      complete(requestID, result: .failure(RPCError.disconnected))
    }
    for task in session.executions.values { task.cancel() }
    for task in session.executions.values { await task.value }
  }

  private func complete(_ id: UUID, result: Result<RequestResult, any Error>) {
    guard let call = pending.removeValue(forKey: id) else { return }
    call.timeout.cancel()
    call.send.cancel()
    call.continuation.resume(with: result)
  }
}

private enum RequestResult: Sendable {
  case endpoint(ActorEndpoint)
  case reply(CallResult)
}

private struct PendingCall {
  let continuation: CheckedContinuation<RequestResult, any Error>
  let timeout: Task<Void, Never>
  let send: Task<Void, Never>
  var sessionID: UUID?
}

private struct DialDestination: Hashable {
  let host: String
  let port: Int
  let node: UUID?
}

private struct Session {
  let connection: Connection
  let task: Task<Void, Never>
  let handshakeTimeout: Task<Void, Never>
  let destination: DialDestination?
  let expected: ActorEndpoint?
  var endpoint: ActorEndpoint?
  var writer: NIOAsyncChannelOutboundWriter<Frame>?
  var ready: CheckedContinuation<UUID, any Error>?
  var executions: [UUID: Task<Void, Never>]
}
