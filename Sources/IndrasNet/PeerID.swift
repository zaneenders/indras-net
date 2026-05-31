public struct PeerID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
  public var rawValue: String

  public init(_ rawValue: String) {
    self.rawValue = rawValue
  }

  public var description: String { self.rawValue }

  public static func < (lhs: PeerID, rhs: PeerID) -> Bool {
    lhs.rawValue < rhs.rawValue
  }
}
