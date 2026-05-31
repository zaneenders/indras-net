import Foundation
import NIOCore

public struct ClientScriptStep: Sendable, Codable, Equatable {
  public enum Action: String, Sendable, Codable {
    case send
    case expect
  }

  public var action: Action
  public var type: String
  public var payload: String?

  public init(action: Action, type: String, payload: String? = nil) {
    self.action = action
    self.type = type
    self.payload = payload
  }

  public static let defaultPingHello: [ClientScriptStep] = [
    ClientScriptStep(action: .send, type: "ping"),
    ClientScriptStep(action: .expect, type: "pong"),
    ClientScriptStep(action: .send, type: "hello", payload: "ok"),
    ClientScriptStep(action: .expect, type: "hello", payload: "ok"),
  ]

  public static func load(from url: URL) throws -> [ClientScriptStep] {
    let data = try Data(contentsOf: url)
    return try JSONDecoder().decode([ClientScriptStep].self, from: data)
  }

  func messageType() throws -> MessageType {
    guard let type = MessageType(name: type) else {
      throw ScriptError.unknownType(type)
    }
    return type
  }

  func payloadBuffer() -> ByteBuffer {
    var buffer = ByteBuffer()
    if let payload {
      buffer.writeString(payload)
    }
    return buffer
  }

  enum ScriptError: Error, CustomStringConvertible {
    case unknownType(String)
    case unexpectedMessage(expected: String, got: Message)
    case payloadMismatch(expected: String, got: String)
    case inboundEndedBeforeExpectation(expected: String)

    var description: String {
      switch self {
      case .unknownType(let type):
        return "unknown message type in script: \(type)"
      case .unexpectedMessage(let expected, let got):
        return "expected \(expected), got \(got.type)"
      case .payloadMismatch(let expected, let got):
        return "expected payload \(expected), got \(got)"
      case .inboundEndedBeforeExpectation(let expected):
        return "connection closed before receiving \(expected)"
      }
    }
  }
}
