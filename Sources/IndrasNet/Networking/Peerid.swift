public typealias PeerId = String

extension PeerId {
  var nodeAddress: NodeAddress? {
    let parts = self.split(separator: ":", maxSplits: 1).map(String.init)
    guard parts.count == 2, let port = Int(parts[1]) else { return nil }
    return NodeAddress(host: parts[0], port: port)
  }
}
