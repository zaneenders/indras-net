public struct ShellActionEvent: Equatable, Sendable {
  public let selfNode: String
  public let kind: String
  public let direction: String
  public let peer: String

  public init(selfNode: String, kind: String, direction: String, peer: String) {
    self.selfNode = selfNode
    self.kind = kind
    self.direction = direction
    self.peer = peer
  }
}
