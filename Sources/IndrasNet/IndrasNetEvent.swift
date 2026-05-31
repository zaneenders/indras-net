public enum IndrasNetEvent: Sendable, Equatable, Codable {
  public enum Direction: String, Sendable, Equatable, Codable {
    case sent
    case received
  }

  case listening(host: String, port: Int)
  case message(direction: Direction, type: String, payload: String)
  case sessionComplete
  case failed(error: String)
}
