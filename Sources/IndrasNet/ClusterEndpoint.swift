public struct ClusterEndpoint: Sendable, Codable, Equatable {
  public var host: String
  public var port: Int

  public init(host: String, port: Int) {
    self.host = host
    self.port = port
  }

  public var addressKey: String {
    Self.addressKey(host: host, port: port)
  }

  public var peerID: PeerID {
    PeerID(addressKey)
  }

  public static func addressKey(host: String, port: Int) -> String {
    "\(host):\(port)"
  }

  public static func parseAddressKey(_ key: String) -> (host: String, port: Int)? {
    let parts = key.split(separator: ":", maxSplits: 1).map(String.init)
    guard parts.count == 2, let port = Int(parts[1]) else { return nil }
    return (parts[0], port)
  }

  public func tcpConfiguration(peers: [ClusterEndpoint]) -> IndrasNetTCPConfiguration {
    IndrasNetTCPConfiguration(
      localPeerID: peerID,
      host: host,
      port: port,
      peers: peers
    )
  }
}
