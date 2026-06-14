import Foundation

struct RaftClient: Sendable {
  static let defaultClientID = "raft"

  let id: PeerId
  private(set) var nextRequestID: UInt128

  init(id: PeerId = RaftClient.defaultClientID) {
    self.id = id
    self.nextRequestID = 1
  }

  mutating func makeRequest(command: Data) -> ClientSubmit.Args {
    defer { nextRequestID += 1 }
    return ClientSubmit.Args(requestId: nextRequestID, command: command)
  }
}
