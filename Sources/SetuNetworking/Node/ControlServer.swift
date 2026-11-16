import Foundation
import Logging
import NIOCore
import Networking
import Setu
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

package func runNode(
  host: String, port: Int, directory: String, socket: String, temporaryDirectory: String,
  maxActiveUploads: Int, uploadIdleTimeout: Duration,
  downloadRecoveryBytes: Int64,
  onRestart: (@Sendable () -> Void)?
)
  async throws(any Error)
{
  guard downloadRecoveryBytes >= 0 else { throw TransferError("Download recovery budget must not be negative") }
  let socketPath = FilePath(socket)
  let parent = socketPath.removingLastComponent()
  try await FileSystem.shared.createDirectory(
    at: parent, withIntermediateDirectories: true,
    permissions: .ownerReadWriteExecute)
  guard let info = try await FileSystem.shared.info(forFileAt: parent), info.type == .directory,
    info.permissions.rawValue & 0o077 == 0, info.userID.rawValue == geteuid()
  else {
    throw TransferError("Control socket directory must be owned by this user and private (0700)")
  }
  try await FileSystem.shared.withFileHandle(
    forWritingAt: FilePath(socket + ".lock"),
    options: .modifyFile(createIfNecessary: true, permissions: .ownerReadWrite)
  ) { lock in
    try await lock.withUnsafeDescriptor { descriptor in
      guard flock(descriptor.rawValue, LOCK_EX | LOCK_NB) == 0 else {
        throw IOError(errnoCode: errno, reason: "Another node owns this control socket")
      }
    }
    if let existing = try await FileSystem.shared.info(forFileAt: socketPath, infoAboutSymbolicLink: true) {
      guard existing.type == .socket else {
        throw TransferError("Control path exists and is not a socket")
      }
      _ = try await FileSystem.shared.removeItem(at: socketPath)
    }
    let server = try await TransferServer.start(
      host: host, port: port, directory: directory, temporaryDirectory: temporaryDirectory,
      maxActiveUploads: maxActiveUploads, uploadIdleTimeout: uploadIdleTimeout, onRestart: onRestart)
    let root = URL(fileURLWithPath: directory).path
    let node = NodeRuntime(server: server, directory: root, downloadRecoveryBytes: downloadRecoveryBytes)
    let connections = ServerConnections()
    do {
      let listener = try await TCP.listen(unixDomainSocketPath: socket)
      let channel = listener.channel
      let logger = Logger(label: "setu.node")
      logger.notice(
        "Peers: \(host):\(server.port) over framed TCP; control: \(socket); shared: \(root); temporary: \(server.temporary.directory)"
      )
      do {
        try await withTaskCancellationHandler {
          try await listener.executeThenClose { inbound in
            await withDiscardingTaskGroup { tasks in
              do {
                for try await connection in inbound {
                  connections.add(connection.channel)
                  tasks.addTask {
                    try? await withDeadline(timeout: .seconds(60)) {
                      try await connection.executeThenClose { input, output in
                        var iterator = input.makeAsyncIterator()
                        guard let frame = try await iterator.next() else { return }
                        let reply: NodeReply
                        do {
                          let request = try SetuWire.decode(
                            frame, target: SetuWire.control,
                            opcode: SetuWire.request, as: NodeRequest.self)
                          reply = try await node.handle(request)
                        } catch { reply = .failure(describeError(error)) }
                        try await output.write(
                          SetuWire.encode(
                            reply, target: SetuWire.control,
                            opcode: SetuWire.response))
                        output.finish()
                        guard try await iterator.next() == nil else {
                          throw TransferError("Unexpected control request continuation")
                        }
                      }
                    }
                  }
                }
              } catch {}
              tasks.cancelAll()
            }
          }
        } onCancel: {
          channel.close(promise: nil)
        }
      } catch {
        await connections.close()
        throw error
      }
      await connections.close()
      await node.close()
      await server.shutdown()
    } catch {
      await node.close()
      await server.shutdown()
      _ = await Task.detached { try? await FileSystem.shared.removeItem(at: socketPath) }.value
      throw error
    }
    // Remove the socket even when shutdown was requested by task cancellation.
    _ = await Task.detached { try? await FileSystem.shared.removeItem(at: socketPath) }.value
  }
}
