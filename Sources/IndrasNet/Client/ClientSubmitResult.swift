import Foundation
import NIO
import NIOCore
import NIOPosix

public struct ClientSubmitResult: Sendable, Equatable {
  public enum Status: Sendable, Equatable {
    case ok
    case notLeader(leader: String?)
  }

  public let requestID: UInt128
  public let status: Status
  public let logIndex: UInt128

  init(_ reply: ClientSubmit.Reply) {
    self.requestID = reply.requestId
    self.logIndex = reply.logIndex
    switch reply.status {
    case .ok:
      self.status = .ok
    case .notLeader:
      self.status = .notLeader(leader: reply.leaderId)
    }
  }
}
