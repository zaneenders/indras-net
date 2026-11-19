import Foundation
import _NIOFileSystem

struct RememberedPeer: Codable, Sendable {
  let id: UUID
  let address: PeerAddress
  let lastSeen: Date
}

struct PeerHistory: Codable, Sendable {
  let version: Int
  let nodeID: UUID
  let cluster: String
  private(set) var peers: [RememberedPeer]

  init(nodeID: UUID, cluster: String) {
    self.version = 1
    self.nodeID = nodeID
    self.cluster = cluster
    self.peers = []
  }

  var addresses: [PeerAddress] { peers.map(\.address) }

  mutating func record(_ node: NodeIdentity, at date: Date) {
    guard node.id != nodeID else { return }
    peers.removeAll { $0.id == node.id || $0.address == node.address }
    peers.append(RememberedPeer(id: node.id, address: node.address, lastSeen: date))
    peers.sort { $0.lastSeen > $1.lastSeen }
    if peers.count > 256 { peers.removeLast(peers.count - 256) }
  }

  static func load(path: String, nodeID: UUID, cluster: String) async throws -> Self {
    let history: Self
    do {
      history = try await readNodeJSON(Self.self, from: path)
    } catch let error as FileSystemError where error.code == .notFound {
      return Self(nodeID: nodeID, cluster: cluster)
    }
    guard history.version == 1 else { throw IndraError("Unsupported peer history version") }
    guard history.nodeID == nodeID, history.cluster == cluster else {
      return Self(nodeID: nodeID, cluster: cluster)
    }
    guard history.peers.count <= 256,
      Set(history.peers.map(\.id)).count == history.peers.count,
      Set(history.addresses).count == history.peers.count,
      !history.peers.contains(where: { $0.id == nodeID })
    else { throw IndraError("Invalid peer history") }
    for peer in history.peers { try peer.address.validate() }
    var sorted = history
    sorted.peers.sort { $0.lastSeen > $1.lastSeen }
    return sorted
  }

  func save(path: String) async throws {
    try await writeNodeJSON(self, to: path, overwrite: true)
  }
}
