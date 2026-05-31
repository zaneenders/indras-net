import Foundation

public enum IndrasNetEvent: Sendable, Equatable, Codable {
  case listening(node: String, host: String, port: Int)
  case running
  case pingSent(from: String, to: String)
  case pingReceived(node: String, from: String)
  case pongSent(from: String, to: String)
  case pongReceived(node: String, from: String)
  case failedToPing(node: String, peer: String, error: String)
  case failedToPong(node: String, peer: String, error: String)
  case message(direction: IndrasNetEventDirection, type: String, payload: String)
  case failed(error: String)
}
