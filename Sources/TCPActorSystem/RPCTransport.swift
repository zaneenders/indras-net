import Foundation
import NIOCore
import Networking

actor RPCTransport {
  private static let handshakeLimit = 64
  private static let sessionExecutionLimit = 64
  private static let executionLimit = 512

  private let listener: Listener
  private let endpoint: ActorEndpoint
  private let transfers: DataTransfers
  private let callTimeout: Duration
  private let connectTimeout: TimeAmount
  private let sessionLimit: Int
  private weak var system: TCPActorSystem?
  private var acceptTask: Task<Void, Never>?
  private var closingTask: Task<[Task<Void, Never>], Never>?
  private var stopped: Bool { closingTask != nil }
  private var sessions: [UUID: Session] = [:]
  private var routes: [ActorEndpoint: UUID] = [:]
  private var dials: [DialDestination: Task<UUID, any Error>] = [:]
  private var pending: [UUID: PendingCall] = [:]
  private var runningExecutions = 0

  init(
    listener: Listener, endpoint: ActorEndpoint, transfers: DataTransfers, callTimeout: Duration,
    connectTimeout: TimeAmount, sessionLimit: Int
  ) {
    self.listener = listener
    self.endpoint = endpoint
    self.transfers = transfers
    self.callTimeout = callTimeout
    self.connectTimeout = connectTimeout
    self.sessionLimit = sessionLimit
  }

  func start(system: TCPActorSystem) {
    self.system = system
    acceptTask = Task {
      do {
        try await listener.executeThenClose { connections in
          for try await connection in connections {
            await self.accept(connection)
          }
        }
      } catch {}
    }
  }

  func call(_ invocation: RemoteInvocation) async throws -> CallResult {
    let result = try await request(id: invocation.requestID) {
      try await self.sendInvocation(invocation)
    }
    guard case .reply(let reply) = result else { throw RPCError.invalidMessage }
    return reply
  }

  private func sendInvocation(_ invocation: RemoteInvocation) async throws {
    let endpoint = invocation.actorID.endpoint
    let sessionID = try await session(
      host: endpoint.host, port: endpoint.port, expected: endpoint, reuseConnection: true)
    try await sendRequest(.call(invocation), requestID: invocation.requestID, sessionID: sessionID)
  }

  func peerEndpoint(host: String, port: Int, reuseConnection: Bool) async throws
    -> ActorEndpoint
  {
    try Task.checkCancellation()
    guard !stopped else { throw RPCError.shutDown }
    if reuseConnection, let session = sessionAtAddress(host: host, port: port), let endpoint = session.value.endpoint {
      return endpoint
    }

    let requestID = UUID()
    let result = try await request(id: requestID) {
      try await self.discoverEndpoint(
        host: host, port: port, reuseConnection: reuseConnection, requestID: requestID)
    }
    guard case .endpoint(let endpoint) = result else { throw RPCError.invalidMessage }
    return endpoint
  }

  private func discoverEndpoint(
    host: String, port: Int, reuseConnection: Bool, requestID: UUID
  ) async throws {
    let sessionID = try await session(
      host: host, port: port, expected: nil, reuseConnection: reuseConnection)
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
    guard let session = sessions[sessionID], let writer = session.writer else { throw RPCError.disconnected }
    try await writer.write(
      WireMessage.reply(to: requestID, result: result, supportsErrorCodes: session.supportsErrorCodes).frame())
  }

  func close() async {
    _ = await startClosing().value
  }

  func shutdown() async {
    let sessionTasks = await startClosing().value
    for task in sessionTasks { await task.value }
  }

  private func startClosing() -> Task<[Task<Void, Never>], Never> {
    if let closingTask { return closingTask }
    let task = Task { await closeConnections() }
    closingTask = task
    return task
  }

  private func closeConnections() async -> [Task<Void, Never>] {
    let active = Array(sessions.values)
    let dialing = Array(dials.values)
    let accepting = acceptTask
    for id in Array(pending.keys) { complete(id, result: .failure(RPCError.shutDown)) }
    accepting?.cancel()
    for dial in dialing { dial.cancel() }
    listener.channel.close(promise: nil)
    for session in active {
      session.task.cancel()
      session.handshakeTimeout?.cancel()
      for task in session.executions.values { task.cancel() }
      session.connection.channel.close(promise: nil)
    }
    await accepting?.value
    try? await listener.channel.closeFuture.get()
    for dial in dialing { _ = try? await dial.value }
    for session in active { try? await session.connection.channel.closeFuture.get() }
    acceptTask = nil
    routes.removeAll()
    dials.removeAll()
    return active.map(\.task)
  }

  private var hasHandshakeCapacity: Bool {
    sessions.values.lazy.filter { $0.handshakeTimeout != nil }.prefix(Self.handshakeLimit).count < Self.handshakeLimit
  }

  private func makeRoomForSession() async -> Bool {
    guard !stopped, hasHandshakeCapacity else { return false }
    if sessions.count < sessionLimit { return true }

    let busy = Set(pending.values.compactMap(\.sessionID))
    let candidates = sessions.filter { id, session in
      if let destination = session.destination, dials[destination] != nil { return false }
      return session.endpoint != nil && session.executions.isEmpty && session.pendingOverloadReplies == 0
        && !busy.contains(id)
    }.sorted { $0.value.lastActivity < $1.value.lastActivity }

    for (id, candidate) in candidates {
      // Earlier async-writer writes have reached the event loop by the time it samples the buffer.
      let buffered = try? await candidate.connection.channel.getOption(ChannelOptions.bufferedWritableBytes).get()
      guard !stopped, hasHandshakeCapacity else { return false }
      if sessions.count < sessionLimit { return true }
      guard buffered == 0, let current = sessions[id], current.endpoint != nil,
        current.lastActivity == candidate.lastActivity, current.executions.isEmpty, current.pendingOverloadReplies == 0,
        !pending.values.contains(where: { $0.sessionID == id })
      else { continue }
      sessions.removeValue(forKey: id)
      removeRoute(current.endpoint, sessionID: id)
      current.connection.channel.close(promise: nil)
      current.task.cancel()
      return true
    }
    return false
  }

  private func accept(_ connection: Connection) async {
    guard await makeRoomForSession() else {
      try? await connection.channel.close()
      return
    }
    install(connection, destination: nil, expected: nil, ready: nil)
  }

  private func session(
    host: String, port: Int, expected: ActorEndpoint?, reuseConnection: Bool
  ) async throws -> UUID {
    guard !stopped else { throw RPCError.shutDown }
    guard !host.isEmpty, (1...65535).contains(port) else { throw RPCError.invalidMessage }
    let destination = DialDestination(
      host: host, port: port, systemID: expected?.systemID, reuseConnection: reuseConnection)
    if reuseConnection {
      if let expected {
        if let id = routes[expected], sessions[id]?.connection.channel.isActive == true { return id }
      } else if let entry = sessionAtAddress(host: host, port: port) {
        return entry.key
      }
    }
    if let dial = dials[destination] { return try await dial.value }
    guard dials.count < Self.handshakeLimit, hasHandshakeCapacity else { throw RPCError.overloaded }
    let task = Task {
      let connection = try await FramedTCP<ActorFraming>.connect(host: host, port: port, connectTimeout: connectTimeout)
      guard !stopped, !Task.isCancelled else {
        try? await connection.channel.close()
        throw RPCError.shutDown
      }
      guard await makeRoomForSession() else {
        try? await connection.channel.close()
        throw stopped ? RPCError.shutDown : RPCError.overloaded
      }
      return try await withCheckedThrowingContinuation { continuation in
        install(connection, destination: destination, expected: expected, ready: continuation)
      }
    }
    dials[destination] = task
    defer { dials[destination] = nil }
    return try await task.value
  }

  private func sessionAtAddress(host: String, port: Int) -> (key: UUID, value: Session)? {
    sessions.first { _, session in
      guard let endpoint = session.endpoint, session.connection.channel.isActive else { return false }
      return (session.destination?.host == host && session.destination?.port == port)
        || (endpoint.host == host && endpoint.port == port)
    }
  }

  private func install(
    _ connection: Connection, destination: DialDestination?, expected: ActorEndpoint?,
    ready: CheckedContinuation<UUID, any Error>?
  ) {
    let id = UUID()
    let (overloadReplies, enqueueReply) = AsyncStream<Frame>.makeStream(
      bufferingPolicy: .bufferingOldest(Self.sessionExecutionLimit))
    let task = Task {
      async let writes: Void = writeOverloadReplies(overloadReplies, sessionID: id, channel: connection.channel)
      do {
        try await connection.executeThenClose { inbound, outbound in
          await self.opened(id, writer: outbound)
          try await outbound.write(
            WireMessage.hello(
              version: WireMessage.version, endpoint: self.endpoint, capabilities: [WireMessage.codedErrors]
            ).frame())
          var frames = inbound.makeAsyncIterator()
          guard let first = try await frames.next() else { throw RPCError.disconnected }
          if destination == nil, first.target == DataWire.target {
            guard let owner = await self.beginDataSession(id) else { throw RPCError.disconnected }
            try await DataWire.serve(
              first, frames: &frames, connection: connection, transfers: self.transfers, cancel: owner.cancel)
            return
          }
          try await self.receive(WireMessage(frame: first), sessionID: id)
          while let frame = try await frames.next() {
            try await self.receive(WireMessage(frame: frame), sessionID: id)
          }
          throw RPCError.disconnected
        }
      } catch {}
      enqueueReply.finish()
      await writes
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
      destination: destination, expected: expected, endpoint: nil, supportsErrorCodes: false, writer: nil, ready: ready,
      executions: [:], enqueueOverloadReply: enqueueReply, pendingOverloadReplies: 0,
      lastActivity: .now)
  }

  private func beginDataSession(_ id: UUID) -> Task<Void, Never>? {
    finishHandshake(id)
    return sessions[id]?.task
  }

  private func finishHandshake(_ id: UUID) {
    sessions[id]?.handshakeTimeout?.cancel()
    sessions[id]?.handshakeTimeout = nil
  }

  private func opened(_ id: UUID, writer: NIOAsyncChannelOutboundWriter<Frame>) {
    sessions[id]?.writer = writer
  }

  private func receive(_ message: WireMessage, sessionID: UUID) throws {
    guard !stopped else { throw RPCError.shutDown }
    guard var session = sessions[sessionID] else { throw RPCError.disconnected }
    switch message {
    case .hello(let version, let endpoint, let capabilities):
      guard version == WireMessage.version, session.endpoint == nil,
        session.expected == nil || session.expected == endpoint,
        !endpoint.host.isEmpty, (1...65535).contains(endpoint.port)
      else { throw RPCError.invalidMessage }
      session.supportsErrorCodes = capabilities?.contains(WireMessage.codedErrors) == true
      session.endpoint = endpoint
      session.lastActivity = .now
      session.handshakeTimeout?.cancel()
      session.handshakeTimeout = nil
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
      guard session.executions.count < Self.sessionExecutionLimit, runningExecutions < Self.executionLimit else {
        let reply = try WireMessage.reply(
          to: invocation.requestID, result: .systemError(.overloaded), supportsErrorCodes: session.supportsErrorCodes
        ).frame()
        guard case .enqueued = session.enqueueOverloadReply.yield(reply) else { throw RPCError.overloaded }
        session.pendingOverloadReplies += 1
        session.lastActivity = .now
        sessions[sessionID] = session
        return
      }
      let task = Task { [weak system] in
        await system?.execute(invocation, sessionID: sessionID)
        executionFinished(invocation.requestID, sessionID: sessionID)
      }
      runningExecutions += 1
      session.executions[invocation.requestID] = task
      session.lastActivity = .now
      sessions[sessionID] = session
    }
  }

  private func writeOverloadReplies(_ replies: AsyncStream<Frame>, sessionID: UUID, channel: any Channel) async {
    for await reply in replies {
      let deadline = ContinuousClock.now + callTimeout
      let timeout = Task {
        do { try await Task.sleep(until: deadline, clock: .continuous) } catch { return }
        channel.close(promise: nil)
      }
      defer { timeout.cancel() }
      do {
        try await channel.writeAndFlush(reply).getAbandoningOnCancel()
      } catch {
        channel.close(promise: nil)
        return
      }
      sessions[sessionID]?.pendingOverloadReplies -= 1
    }
  }

  private func executionFinished(_ id: UUID, sessionID: UUID) {
    sessions[sessionID]?.executions[id] = nil
    sessions[sessionID]?.lastActivity = .now
    runningExecutions -= 1
  }

  private func sendRequest(_ message: WireMessage, requestID: UUID, sessionID: UUID) async throws {
    guard pending[requestID] != nil else { return }
    guard let session = sessions[sessionID], let writer = session.writer else { throw RPCError.disconnected }
    pending[requestID]?.sessionID = sessionID
    sessions[sessionID]?.lastActivity = .now
    do {
      try await writer.write(message.frame())
    } catch ChannelError.ioOnClosedChannel, is NIOAsyncWriterError {
      try? await session.connection.channel.close()
      throw RPCError.disconnected
    }
  }

  private func closed(_ id: UUID) async {
    guard let session = sessions[id] else { return }
    finishHandshake(id)
    session.ready?.resume(throwing: RPCError.disconnected)
    sessions[id]?.ready = nil
    sessions[id]?.endpoint = nil
    sessions[id]?.writer = nil
    removeRoute(session.endpoint, sessionID: id)
    for requestID in Array(pending.keys) where pending[requestID]?.sessionID == id {
      complete(requestID, result: .failure(RPCError.disconnected))
    }
    for task in session.executions.values { task.cancel() }
    for task in session.executions.values { await task.value }
    sessions[id] = nil
  }

  private func removeRoute(_ endpoint: ActorEndpoint?, sessionID: UUID) {
    guard let endpoint, routes[endpoint] == sessionID else { return }
    routes[endpoint] = sessions.first { $0.value.endpoint == endpoint && $0.value.connection.channel.isActive }?.key
  }

  private func complete(_ id: UUID, result: Result<RequestResult, any Error>) {
    guard let call = pending.removeValue(forKey: id) else { return }
    if let sessionID = call.sessionID { sessions[sessionID]?.lastActivity = .now }
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
  let systemID: UUID?
  let reuseConnection: Bool
}

private struct Session {
  let connection: Connection
  let task: Task<Void, Never>
  var handshakeTimeout: Task<Void, Never>?
  let destination: DialDestination?
  let expected: ActorEndpoint?
  var endpoint: ActorEndpoint?
  var supportsErrorCodes: Bool
  var writer: NIOAsyncChannelOutboundWriter<Frame>?
  var ready: CheckedContinuation<UUID, any Error>?
  var executions: [UUID: Task<Void, Never>]
  let enqueueOverloadReply: AsyncStream<Frame>.Continuation
  var pendingOverloadReplies: Int
  var lastActivity: ContinuousClock.Instant
}
