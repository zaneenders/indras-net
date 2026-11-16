package import Foundation
import NIOCore
import Networking
package import Setu

public actor TransferServer {
  package nonisolated let catalog: FileCatalog
  package nonisolated let node: UUID
  public nonisolated let port: Int
  package nonisolated let temporary: TemporaryFiles
  private let listener: Listener
  private let connections: ServerConnections
  private let store: UploadStore
  private let task: Task<Void, Never>
  private var shutdownTask: Task<Void, Never>?

  private init(listener: Listener, service: TransferService, temporary: TemporaryFiles) {
    self.listener = listener
    self.catalog = service.catalog
    self.node = service.catalog.node
    self.port = listener.channel.localAddress!.port!
    self.temporary = temporary
    self.store = service.store
    let connections = ServerConnections()
    self.connections = connections
    self.task = Task {
      try? await listener.executeThenClose { inbound in
        await withDiscardingTaskGroup { tasks in
          do {
            for try await connection in inbound {
              connections.add(connection.channel)
              tasks.addTask {
                try? await service.handle(connection, idleTimeout: .seconds(120))
              }
            }
          } catch {}
          tasks.cancelAll()
        }
      }
    }
  }

  public static func start(
    host: String, port: Int, directory: String, temporaryDirectory: String,
    maxActiveUploads: Int, uploadIdleTimeout: Duration, onRestart: (@Sendable () -> Void)?
  ) async throws -> TransferServer {
    guard !host.isEmpty, (0...65535).contains(port) else { throw TransferError("Invalid bind host or port") }
    let temporary = try await TemporaryFiles(directory: temporaryDirectory, protecting: [directory])
    do {
      let store = try await UploadStore(
        root: directory, quota: 1_099_511_627_776, temporary: temporary,
        maxActiveUploads: maxActiveUploads, idleTimeout: uploadIdleTimeout)
      do {
        let catalog = FileCatalog(root: store.root, node: UUID())
        let listener = try await TCP.listen(host: host, port: port)
        await store.startCleanup()
        return TransferServer(
          listener: listener, service: TransferService(store: store, catalog: catalog, onRestart: onRestart),
          temporary: temporary)
      } catch {
        await store.close()
        throw error
      }
    } catch {
      await temporary.close()
      throw error
    }
  }

  public func shutdown() async {
    if let shutdownTask {
      await shutdownTask.value
      return
    }
    let task = Task { [listener, connections, task, store, temporary] in
      try? await listener.channel.close()
      await connections.close()
      await task.value
      await store.close()
      await temporary.close()
    }
    shutdownTask = task
    await task.value
  }

  deinit {
    if shutdownTask == nil {
      task.cancel()
      listener.channel.close(promise: nil)
      let connections = connections
      let task = task
      let store = store
      let temporary = temporary
      Task {
        await connections.close()
        await task.value
        await store.close()
        await temporary.close()
      }
    }
  }
}
