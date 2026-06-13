import Foundation
import IndrasNet

/// Shared in-memory routing layer for `SimulatedTransport` instances.
extension SimulatedTransport {
  package actor Mesh {
    private struct ConnectionWaiter {
      let id: UUID
      let continuation: CheckedContinuation<Bool, Never>
    }

    private var handlers: [PeerId: IndrasNetInboundHandler] = [:]
    private var listenPorts: [PeerId: Int] = [:]
    private var disconnectedLinks: Set<Link> = []
    private var connectionWaiters: [PeerId: [PeerId: [ConnectionWaiter]]] = [:]
    package init() {}

    func register(peer: PeerId, listenPort: Int) {
      listenPorts[peer] = listenPort
      resumeConnectionWaiters(involving: peer)
    }

    func unregister(peer: PeerId) {
      handlers.removeValue(forKey: peer)
      listenPorts.removeValue(forKey: peer)
      connectionWaiters.removeValue(forKey: peer)
      for peerID in connectionWaiters.keys {
        connectionWaiters[peerID]?.removeValue(forKey: peer)
      }
      resumeConnectionWaiters(involving: peer)
    }

    func setHandler(peer: PeerId, handler: IndrasNetInboundHandler?) {
      if let handler {
        handlers[peer] = handler
      } else {
        handlers.removeValue(forKey: peer)
      }
    }

    func connectedPeers(for peer: PeerId) -> Set<PeerId> {
      Set(listenPorts.keys.filter { $0 != peer && canDeliver(from: peer, to: $0) })
    }

    func canDeliver(from sender: PeerId, to recipient: PeerId) -> Bool {
      sender == recipient
        || (listenPorts[sender] != nil && listenPorts[recipient] != nil
          && !disconnectedLinks.contains(Link(sender, recipient)))
    }

    func deliver(from sender: PeerId, to recipient: PeerId, message: RaftMessage) async throws {
      guard canDeliver(from: sender, to: recipient) else {
        throw IndrasNetTransportError.peerNotConnected(recipient)
      }
      guard let handler = handlers[recipient] else {
        throw IndrasNetTransportError.peerNotConnected(recipient)
      }
      await handler(message, sender)
    }

    func waitForConnection(from waiter: PeerId, to peer: PeerId, timeout: Duration) async -> Bool {
      if canDeliver(from: waiter, to: peer) {
        return true
      }
      if Task.isCancelled {
        return false
      }

      let waiterID = UUID()

      return await withTaskGroup(of: Bool.self) { group in
        group.addTask {
          await self.awaitConnection(from: waiter, to: peer, waiterID: waiterID)
        }
        group.addTask {
          try? await Task.sleep(for: timeout)
          await self.removeConnectionWaiter(from: waiter, to: peer, id: waiterID)
          return false
        }

        defer { group.cancelAll() }
        return await group.next() ?? false
      }
    }

    package func disconnect(from sender: PeerId, to recipient: PeerId) {
      disconnectedLinks.insert(Link(sender, recipient))
      resumeConnectionWaiters(involving: sender)
      resumeConnectionWaiters(involving: recipient)
    }

    package func reconnect(from sender: PeerId, to recipient: PeerId) {
      disconnectedLinks.remove(Link(sender, recipient))
      resumeConnectionWaiters(involving: sender)
      resumeConnectionWaiters(involving: recipient)
    }

    package func disconnect(_ peer: PeerId) {
      for other in listenPorts.keys where other != peer {
        disconnect(from: peer, to: other)
      }
    }

    package func reconnect(_ peer: PeerId) {
      disconnectedLinks = disconnectedLinks.filter { !$0.involves(peer) }
      resumeConnectionWaiters(involving: peer)
    }

    package func reconnectAll() {
      disconnectedLinks.removeAll()
      for peer in listenPorts.keys {
        resumeConnectionWaiters(involving: peer)
      }
    }

    private func awaitConnection(from waiter: PeerId, to peer: PeerId, waiterID: UUID) async -> Bool {
      if canDeliver(from: waiter, to: peer) {
        return true
      }

      return await withTaskCancellationHandler {
        await withCheckedContinuation { continuation in
          registerConnectionWaiter(from: waiter, to: peer, id: waiterID, continuation: continuation)
        }
      } onCancel: {
        Task { await self.removeConnectionWaiter(from: waiter, to: peer, id: waiterID) }
      }
    }

    private func registerConnectionWaiter(
      from waiter: PeerId,
      to peer: PeerId,
      id: UUID,
      continuation: CheckedContinuation<Bool, Never>
    ) {
      if canDeliver(from: waiter, to: peer) {
        continuation.resume(returning: true)
        return
      }
      connectionWaiters[waiter, default: [:]][peer, default: []].append(
        ConnectionWaiter(id: id, continuation: continuation)
      )
    }

    private func removeConnectionWaiter(from waiter: PeerId, to peer: PeerId, id: UUID) {
      guard var waitersForPeer = connectionWaiters[waiter]?[peer] else { return }
      waitersForPeer.removeAll { $0.id == id }
      if waitersForPeer.isEmpty {
        connectionWaiters[waiter]?.removeValue(forKey: peer)
        if connectionWaiters[waiter]?.isEmpty == true {
          connectionWaiters.removeValue(forKey: waiter)
        }
      } else {
        connectionWaiters[waiter]?[peer] = waitersForPeer
      }
    }

    private func resumeConnectionWaiters(involving peer: PeerId) {
      for waiter in connectionWaiters.keys {
        guard let remotes = connectionWaiters[waiter] else { continue }
        for remote in remotes.keys {
          guard remote == peer || waiter == peer else { continue }
          guard canDeliver(from: waiter, to: remote) else { continue }
          guard let waiters = connectionWaiters[waiter]?.removeValue(forKey: remote) else { continue }
          if connectionWaiters[waiter]?.isEmpty == true {
            connectionWaiters.removeValue(forKey: waiter)
          }
          for connectionWaiter in waiters {
            connectionWaiter.continuation.resume(returning: true)
          }
        }
      }
    }
  }
}
