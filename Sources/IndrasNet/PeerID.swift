struct PeerID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
  var rawValue: String

  init(_ rawValue: String) {
    self.rawValue = rawValue
  }

  var description: String { self.rawValue }

  static func < (lhs: PeerID, rhs: PeerID) -> Bool {
    lhs.rawValue < rhs.rawValue
  }
}
