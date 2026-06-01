public struct ClusterEndpoint: Sendable, Codable, Equatable {
  var host: String
  var port: Int

  public init(host: String, port: Int) {
    self.host = host
    self.port = port
  }

  var addressKey: String {
    Self.addressKey(host: host, port: port)
  }

  var peerID: PeerID {
    PeerID(addressKey)
  }

  static func addressKey(host: String, port: Int) -> String {
    "\(host):\(port)"
  }

  func tcpConfiguration(peers: [ClusterEndpoint]) -> IndrasNetTCPConfiguration {
    IndrasNetTCPConfiguration(
      localPeerID: peerID,
      host: host,
      port: port,
      peers: peers
    )
  }
}
