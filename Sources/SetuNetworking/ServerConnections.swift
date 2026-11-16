import NIOCore
import Setu
import Synchronization

final class ServerConnections: Sendable {
  private struct State: Sendable {
    var closed = false
    var channels: [ObjectIdentifier: any Channel] = [:]
  }
  private let state = Mutex(State())

  var isClosed: Bool { state.withLock { $0.closed } }

  func add(_ channel: any Channel) {
    let id = ObjectIdentifier(channel)
    let accepted = state.withLock { state in
      guard !state.closed else {
        return false
      }
      state.channels[id] = channel
      return true
    }
    guard accepted else {
      channel.close(promise: nil)
      return
    }
    channel.closeFuture.whenComplete { [weak self] _ in
      _ = self?.state.withLock { $0.channels.removeValue(forKey: id) }
    }
  }

  func close() async {
    let channels = state.withLock { state in
      state.closed = true
      let channels = Array(state.channels.values)
      state.channels.removeAll()
      return channels
    }
    await withTaskGroup { group in
      for channel in channels {
        group.addTask { try? await channel.close() }
      }
    }
  }
}
