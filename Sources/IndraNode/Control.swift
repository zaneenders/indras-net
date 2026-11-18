import Foundation
import NIOCore
import Networking
import SystemPackage
import TCPActorSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

public enum ControlRequest: Codable, Sendable {
  case status
  case monitor
  case services
  case queryService(String)
  case ping(String)
  case join(PeerAddress)
  case reload
}

public enum ControlReply: Codable, Sendable {
  case status(NodeStatus)
  case monitor(MonitorSnapshot)
  case services([DiscoveredService])
  case pong(PingResult)
  case failure(String)
}

private let controlTarget: UInt32 = 0x494E_4452

private enum ControlOpcode: UInt32 {
  case request = 1
  case reply = 2
  case serviceReport = 3
}

public enum NodeControl: Sendable {
  public static func request(_ request: ControlRequest, socket: String, timeout: Duration) async throws -> ControlReply
  {
    try await exchange(request, socket: socket, timeout: timeout, decodeReply: decodeControlReply)
  }

  public static func query<Value: Codable & Sendable>(
    _ service: String, returning _: Value.Type, socket: String, timeout: Duration
  ) async throws -> [ServiceReport<Value>] {
    try await exchange(.queryService(service), socket: socket, timeout: timeout) { frame in
      if frame.opcode == ControlOpcode.reply.rawValue {
        _ = try decodeControlReply(frame)
        throw IndraError("Unexpected daemon response")
      }
      return try decode(frame, opcode: .serviceReport, as: [ServiceReport<Value>].self)
    }
  }

  private static func exchange<Value: Sendable>(
    _ request: ControlRequest, socket: String, timeout: Duration,
    decodeReply: @escaping @Sendable (Frame) throws -> Value
  ) async throws -> Value {
    guard timeout > .zero else { throw IndraError("Control request timeout must be positive") }
    try validateSocketPath(socket)
    return try await withDeadline(
      deadline: .now.advanced(by: timeout), timeoutError: IndraError("Control request timed out")
    ) {
      let connection: Connection
      do {
        connection = try await TCP.connect(unixDomainSocketPath: socket, connectTimeout: .seconds(2))
      } catch {
        throw IndraError("Cannot connect to daemon at \(socket). Start 'indra daemon' with the same --socket. \(error)")
      }
      return try await withTaskCancellationHandler {
        try await connection.executeThenClose { input, output in
          try await output.write(encode(request, opcode: .request))
          output.finish()
          var iterator = input.makeAsyncIterator()
          guard let frame = try await iterator.next() else { throw IndraError("Daemon disconnected") }
          return try decodeReply(frame)
        }
      } onCancel: {
        connection.channel.close(promise: nil)
      }
    }
  }
}

public func runNode(
  configurationPath: String, socket: String, services: [ServiceInstaller], onReady: @Sendable (NodeStatus) -> Void
) async throws {
  let configuration = try NodeConfiguration.load(path: configurationPath)
  let lease = try SocketLease(path: socket)
  defer { withExtendedLifetime(lease) {} }
  let system = try await TCPActorSystem(
    host: configuration.listenHost, advertisedHost: configuration.advertisedHost, port: configuration.port,
    callTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)),
    connectTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)))
  let runtime = NodeRuntime(
    configuration: configuration, configurationPath: configurationPath,
    peerHistoryPath: configurationPath + ".peers.json", system: system)
  do {
    try await runtime.start(services: services)
    let listener = try await TCP.listen(unixDomainSocketPath: socket)
    defer { try? FileManager.default.removeItem(atPath: socket) }
    onReady(await runtime.status())
    try await withThrowingTaskGroup(of: Void.self) { tasks in
      tasks.addTask { try await runtime.runMembership() }
      tasks.addTask {
        try await withTaskCancellationHandler {
          try await listener.executeThenClose { incoming in
            try await withThrowingDiscardingTaskGroup { clients in
              for try await connection in incoming {
                clients.addTask {
                  try? await serveControlConnection(connection, runtime: runtime)
                }
              }
            }
          }
        } onCancel: {
          listener.channel.close(promise: nil)
        }
      }
      defer { tasks.cancelAll() }
      _ = try await tasks.next()
    }
  } catch {
    await runtime.shutdown()
    throw error
  }
  await runtime.shutdown()
}

func serveControlConnection(_ connection: Connection, runtime: NodeRuntime) async throws {
  try await withDeadline(
    deadline: .now.advanced(by: .seconds(65)), timeoutError: IndraError("Control request timed out")
  ) {
    try await withTaskCancellationHandler {
      try await connection.executeThenClose { input, _ in
        var iterator = input.makeAsyncIterator()
        guard let frame = try await iterator.next() else { return }
        let reply: Frame
        do {
          let request = try decode(frame, opcode: .request, as: ControlRequest.self)
          reply = try await handle(request, runtime: runtime)
        } catch {
          reply = try encode(ControlReply.failure(String(describing: error)), opcode: .reply)
        }
        // Async writer completion only enqueues data; await the socket write before closing.
        try await connection.channel.writeAndFlush(reply).get()
      }
    } onCancel: {
      connection.channel.close(promise: nil)
    }
  }
}

private func handle(_ request: ControlRequest, runtime: NodeRuntime) async throws -> Frame {
  let reply: ControlReply
  switch request {
  case .status: reply = .status(await runtime.status())
  case .monitor: reply = .monitor(await runtime.monitor())
  case .services: reply = .services(await runtime.receptionist.services())
  case .queryService(let name):
    let status = await runtime.status()
    let payload = try await runtime.receptionist.query(name, status: status)
    return controlFrame(opcode: .serviceReport, payload: payload)
  case .ping(let target): reply = .pong(try await runtime.ping(target))
  case .join(let address): reply = .pong(try await runtime.join(address))
  case .reload:
    try await runtime.reload()
    reply = .status(await runtime.status())
  }
  return try encode(reply, opcode: .reply)
}

private func controlFrame(opcode: ControlOpcode, payload: Data) -> Frame {
  Frame(target: controlTarget, opcode: opcode.rawValue, payload: ByteBuffer(bytes: payload))
}

private func encode<Value: Encodable>(_ value: Value, opcode: ControlOpcode) throws -> Frame {
  controlFrame(opcode: opcode, payload: try JSONEncoder().encode(value))
}

private func decode<Value: Decodable>(_ frame: Frame, opcode: ControlOpcode, as type: Value.Type) throws -> Value {
  guard frame.target == controlTarget, frame.opcode == opcode.rawValue else {
    throw IndraError("Invalid control message")
  }
  return try JSONDecoder().decode(type, from: Data(frame.payload.readableBytesView))
}

private func decodeControlReply(_ frame: Frame) throws -> ControlReply {
  let reply = try decode(frame, opcode: .reply, as: ControlReply.self)
  if case .failure(let message) = reply { throw IndraError(message) }
  return reply
}

func withDeadline<Value: Sendable>(
  deadline: ContinuousClock.Instant, timeoutError: any Error, operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { tasks in
    tasks.addTask { try await operation() }
    tasks.addTask {
      try await Task.sleep(until: deadline, clock: .continuous)
      throw timeoutError
    }
    defer { tasks.cancelAll() }
    guard let value = try await tasks.next() else { throw CancellationError() }
    return value
  }
}

private final class SocketLease: Sendable {
  private let descriptor: SystemPackage.FileDescriptor

  init(path: String) throws {
    try validateSocketPath(path)
    let parent = URL(fileURLWithPath: path).deletingLastPathComponent().path
    try FileManager.default.createDirectory(
      atPath: parent, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    let attributes = try FileManager.default.attributesOfItem(atPath: parent)
    guard attributes[.type] as? FileAttributeType == .typeDirectory,
      let permissions = attributes[.posixPermissions] as? Int, permissions & 0o077 == 0,
      attributes[.ownerAccountID] as? UInt32 == geteuid()
    else { throw IndraError("Control socket directory must belong to this user and have mode 0700") }
    let descriptor = try SystemPackage.FileDescriptor.open(
      FilePath(path + ".lock"), .readWrite, options: [.create, .noFollow, .closeOnExec],
      permissions: .ownerReadWrite, retryOnInterrupt: true)
    do {
      guard flock(descriptor.rawValue, LOCK_EX | LOCK_NB) == 0 else {
        throw IndraError("Another daemon owns this control socket")
      }
      if let existing = try? FileManager.default.attributesOfItem(atPath: path) {
        guard existing[.type] as? FileAttributeType == .typeSocket else {
          throw IndraError("Control path exists and is not a socket")
        }
        try FileManager.default.removeItem(atPath: path)
      }
      self.descriptor = descriptor
    } catch {
      try? descriptor.close()
      throw error
    }
  }

  deinit { try? descriptor.close() }
}

private func validateSocketPath(_ path: String) throws {
  guard path.hasPrefix("/"), !path.utf8.contains(0), path.utf8.count < 104 else {
    throw IndraError("Control socket requires an absolute path shorter than 104 bytes")
  }
}
