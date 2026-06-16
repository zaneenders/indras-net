@testable import IndrasNet

struct PartitionMap: Sendable {
  private var disconnectedLinks: Set<Link> = []

  init() {}

  func canDeliver(from sender: PeerId, to recipient: PeerId) -> Bool {
    sender == recipient || !disconnectedLinks.contains(Link(sender, recipient))
  }

  mutating func disconnect(from sender: PeerId, to recipient: PeerId) {
    disconnectedLinks.insert(Link(sender, recipient))
  }

  mutating func reconnect(from sender: PeerId, to recipient: PeerId) {
    disconnectedLinks.remove(Link(sender, recipient))
  }

  mutating func disconnect(_ peer: PeerId, from peers: some Sequence<PeerId>) {
    for other in peers where other != peer {
      disconnect(from: peer, to: other)
    }
  }

  mutating func reconnect(_ peer: PeerId) {
    disconnectedLinks = disconnectedLinks.filter { !$0.involves(peer) }
  }

  mutating func reconnectAll() {
    disconnectedLinks.removeAll()
  }
}
