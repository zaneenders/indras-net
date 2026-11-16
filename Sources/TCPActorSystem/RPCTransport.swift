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
  private var dials: [DialDestination: Dial] = [:]
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
    try await request(id: invocation.requestID) {
      let endpoint = invocation.actorID.endpoint
      let sessionID = try await self.session(
        host: endpoint.host, port: endpoint.port, expected: endpoint, system: system)
      try await self.sendRequest(.call(invocation), requestID: invocation.requestID, sessionID: sessionID)
    }
  }

  func lookup(name: String, actorType: String, host: String, port: Int, system: TCPActorSystem) async throws
    -> CallResult
  {
    let id = UUID()
    return try await request(id: id) {
      let sessionID = try await self.session(host: host, port: port, expected: nil, system: system)
      try await self.sendRequest(
        .lookup(requestID: id, name: name, actorType: actorType), requestID: id, sessionID: sessionID)
    }
  }

  private func request(id requestID: UUID, send operation: @escaping @Sendable () async throws -> Void) async throws
    -> CallResult
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
    for dial in dialing { dial.task.cancel() }
    try? await listener.channel.close()
    for session in active {
      session.task.cancel()
      session.handshakeTimeout.cancel()
      for task in session.executions.values { task.cancel() }
      try? await session.connection.channel.close()
    }
    await accepting?.value
    for dial in dialing { _ = try? await dial.task.value }
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
    install(connection, expected: nil, ready: nil, system: system)
  }

  private func session(host: String, port: Int, expected: ActorEndpoint?, system: TCPActorSystem) async throws -> UUID {
    guard !stopped else { throw RPCError.shutDown }
    guard !host.isEmpty, (1...65535).contains(port) else { throw RPCError.invalidMessage }
    if let expected {
      if let id = routes[expected], sessions[id] != nil { return id }
    } else if let route = routes.first(where: { $0.key.host == host && $0.key.port == port }),
      sessions[route.value] != nil
    {
      return route.value
    }
    let destination = DialDestination(host: host, port: port, node: expected?.node)
    if let dial = dials[destination] { return try await dial.task.value }
    let token = UUID()
    let task = Task {
      let connection = try await TCP.connect(host: host, port: port, connectTimeout: connectTimeout)
      guard !stopped, !Task.isCancelled else {
        try? await connection.channel.close()
        throw RPCError.shutDown
      }
      return try await withCheckedThrowingContinuation { continuation in
        install(connection, expected: expected, ready: continuation, system: system)
      }
    }
    dials[destination] = Dial(token: token, task: task)
    defer { if dials[destination]?.token == token { dials[destination] = nil } }
    return try await task.value
  }

  private func install(
    _ connection: Connection, expected: ActorEndpoint?, ready: CheckedContinuation<UUID, any Error>?,
    system: TCPActorSystem
  ) {
    let id = UUID()
    let task = Task {
      do {
        try await connection.executeThenClose { inbound, outbound in
          await self.opened(id, writer: outbound)
          try await outbound.write(WireMessage.hello(system.endpoint).frame())
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
      expected: expected, endpoint: nil, writer: nil, ready: ready, executions: [:])
  }

  private func opened(_ id: UUID, writer: NIOAsyncChannelOutboundWriter<Frame>) {
    sessions[id]?.writer = writer
  }

  private func receive(_ message: WireMessage, sessionID: UUID, system: TCPActorSystem) throws {
    guard var session = sessions[sessionID] else { throw RPCError.disconnected }
    switch message {
    case .hello(let endpoint):
      guard session.endpoint == nil, session.expected == nil || session.expected == endpoint,
        !endpoint.host.isEmpty, (1...65535).contains(endpoint.port)
      else { throw RPCError.invalidMessage }
      session.endpoint = endpoint
      session.handshakeTimeout.cancel()
      session.ready?.resume(returning: sessionID)
      session.ready = nil
      sessions[sessionID] = session
      if routes[endpoint] == nil { routes[endpoint] = sessionID }
    case .reply(let id, let result):
      guard session.endpoint != nil else { throw RPCError.invalidMessage }
      guard pending[id]?.sessionID == sessionID else { return }
      complete(id, result: .success(result))
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
    case .lookup(let requestID, let name, let actorType):
      guard session.endpoint != nil, session.executions[requestID] == nil else { throw RPCError.invalidMessage }
      let task = Task {
        await system.lookup(requestID: requestID, name: name, actorType: actorType, sessionID: sessionID)
        executionFinished(requestID, sessionID: sessionID)
      }
      session.executions[requestID] = task
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
    if let endpoint = session.endpoint, routes[endpoint] == id { routes[endpoint] = nil }
    for requestID in Array(pending.keys) where pending[requestID]?.sessionID == id {
      complete(requestID, result: .failure(RPCError.disconnected))
    }
    for task in session.executions.values { task.cancel() }
    for task in session.executions.values { await task.value }
  }

  private func complete(_ id: UUID, result: Result<CallResult, any Error>) {
    guard let call = pending.removeValue(forKey: id) else { return }
    call.timeout.cancel()
    call.send.cancel()
    call.continuation.resume(with: result)
  }
}

private struct PendingCall {
  let continuation: CheckedContinuation<CallResult, any Error>
  let timeout: Task<Void, Never>
  let send: Task<Void, Never>
  var sessionID: UUID?
}

private struct Dial {
  let token: UUID
  let task: Task<UUID, any Error>
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
  let expected: ActorEndpoint?
  var endpoint: ActorEndpoint?
  var writer: NIOAsyncChannelOutboundWriter<Frame>?
  var ready: CheckedContinuation<UUID, any Error>?
  var executions: [UUID: Task<Void, Never>]
}
