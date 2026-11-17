import Foundation
import NIOCore
import Networking
import Synchronization
import SystemPackage
import TCPActorSystem
import UnixSignals

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

public enum ControlRequest: Codable, Sendable {
  case status
  case ping(String)
  case join(PeerAddress)
  case reload
}

public enum ControlReply: Codable, Sendable {
  case status(NodeStatus)
  case pong(PingResult)
  case failure(String)
}

public enum NodeControl: Sendable {
  public static func request(_ request: ControlRequest, socket: String) async throws -> ControlReply {
    try validateSocketPath(socket)
    return try await withDeadline(timeout: .seconds(65)) {
      let connection: Connection
      do {
        connection = try await TCP.connect(unixDomainSocketPath: socket, connectTimeout: .seconds(2))
      } catch {
        throw IndraError("Cannot connect to daemon at \(socket). Start 'indra daemon' with the same --socket. \(error)")
      }
      return try await connection.executeThenClose { input, output in
        try await output.write(encode(request, opcode: 1))
        output.finish()
        var iterator = input.makeAsyncIterator()
        guard let frame = try await iterator.next() else { throw IndraError("Daemon disconnected") }
        let reply = try decode(frame, opcode: 2, as: ControlReply.self)
        if case .failure(let message) = reply { throw IndraError(message) }
        return reply
      }
    }
  }
}

public func runNode(configurationPath: String, socket: String, onReady: @Sendable (NodeStatus) -> Void) async throws {
  let configuration = try NodeConfiguration.load(path: configurationPath)
  let lease = try SocketLease(path: socket)
  defer { withExtendedLifetime(lease) {} }
  let system = try await TCPActorSystem(
    host: configuration.listenHost, advertisedHost: configuration.advertisedHost, port: configuration.port,
    callTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)),
    connectTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)))
  let runtime = NodeRuntime(configuration: configuration, configurationPath: configurationPath, system: system)
  do {
    try await runtime.start()
    let listener = try await TCP.listen(unixDomainSocketPath: socket)
    defer { try? FileManager.default.removeItem(atPath: socket) }
    let connections = ControlConnections()
    let reloadSignals = await UnixSignalsSequence(trapping: [.sighup])
    onReady(await runtime.status())
    do {
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        tasks.addTask { try await runtime.runMembership() }
        tasks.addTask {
          while !Task.isCancelled {
            try? await runtime.reload()
            try await Task.sleep(for: .seconds(1))
          }
        }
        tasks.addTask {
          for await _ in reloadSignals { try? await runtime.reload() }
        }
        tasks.addTask {
          try await withTaskCancellationHandler {
            try await listener.executeThenClose { incoming in
              try await withThrowingDiscardingTaskGroup { clients in
                for try await connection in incoming {
                  let token = connections.insert(connection.channel)
                  clients.addTask {
                    defer { connections.remove(token) }
                    try? await withDeadline(timeout: .seconds(65)) {
                      try await connection.executeThenClose { input, output in
                        var iterator = input.makeAsyncIterator()
                        guard let frame = try await iterator.next() else { return }
                        let reply: ControlReply
                        do {
                          let request = try decode(frame, opcode: 1, as: ControlRequest.self)
                          reply = try await handle(request, runtime: runtime)
                        } catch { reply = .failure(String(describing: error)) }
                        try await output.write(encode(reply, opcode: 2))
                      }
                    }
                  }
                }
              }
            }
          } onCancel: {
            listener.channel.close(promise: nil)
            connections.close()
          }
        }
        defer { tasks.cancelAll() }
        _ = try await tasks.next()
      }
      await runtime.shutdown()
    } catch {
      await runtime.shutdown()
      throw error
    }
  } catch {
    await system.shutdown()
    throw error
  }
}

private func handle(_ request: ControlRequest, runtime: NodeRuntime) async throws -> ControlReply {
  switch request {
  case .status: return .status(await runtime.status())
  case .ping(let target): return .pong(try await runtime.ping(target))
  case .join(let address): return .pong(try await runtime.join(address))
  case .reload:
    try await runtime.reload()
    return .status(await runtime.status())
  }
}

private func encode<Value: Encodable>(_ value: Value, opcode: UInt32) throws -> Frame {
  Frame(target: 0x494E_4452, opcode: opcode, payload: ByteBuffer(bytes: try JSONEncoder().encode(value)))
}

private func decode<Value: Decodable>(_ frame: Frame, opcode: UInt32, as type: Value.Type) throws -> Value {
  guard frame.target == 0x494E_4452, frame.opcode == opcode else { throw IndraError("Invalid control message") }
  return try JSONDecoder().decode(type, from: Data(frame.payload.readableBytesView))
}

private func withDeadline<Value: Sendable>(
  timeout: Duration, operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { tasks in
    tasks.addTask { try await operation() }
    tasks.addTask {
      try await Task.sleep(for: timeout)
      throw IndraError("Control request timed out")
    }
    defer { tasks.cancelAll() }
    guard let value = try await tasks.next() else { throw CancellationError() }
    return value
  }
}

private final class ControlConnections: Sendable {
  private let channels = Mutex<[UUID: any Channel]>([:])

  func insert(_ channel: any Channel) -> UUID {
    let token = UUID()
    channels.withLock { $0[token] = channel }
    return token
  }

  func remove(_ token: UUID) { _ = channels.withLock { $0.removeValue(forKey: token) } }

  func close() {
    channels.withLock { channels in
      for channel in channels.values { channel.close(promise: nil) }
    }
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
