public struct NodeAddress: Sendable, Codable, Equatable {
  public let host: String
  public let port: Int

  public init(host: String, port: Int) {
    self.host = host
    self.port = port
  }

  public var addressKey: String {
    Self.addressKey(host: host, port: port)
  }

  static func addressKey(host: String, port: Int) -> String {
    "\(host):\(port)"
  }

  public func tcpConfiguration() -> TransportConfiguration {
    TransportConfiguration(
      localPeerID: addressKey,
      host: host,
      port: port
    )
  }
}
