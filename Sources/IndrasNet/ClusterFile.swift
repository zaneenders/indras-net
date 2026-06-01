import Foundation

public struct ClusterFile: Sendable, Codable, Equatable {
  var peers: [ClusterEndpoint]

  init(peers: [ClusterEndpoint] = []) {
    self.peers = peers
  }

  public static func load(fromPath path: String) throws -> ClusterFile {
    try load(from: URL(fileURLWithPath: path))
  }

  private static func load(from url: URL) throws -> ClusterFile {
    let data = try Data(contentsOf: url)
    return try JSONDecoder().decode(ClusterFile.self, from: data)
  }

  func peerEndpoints(
    listenHost: String,
    listenPort: Int
  ) -> [ClusterEndpoint] {
    peers.filter { $0.host != listenHost || $0.port != listenPort }
  }
}
