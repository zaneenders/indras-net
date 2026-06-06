import NIOCore

enum AppMessage: Equatable, Sendable {
  case requestVote(RequestVote.Args)
  case requestVoteReply(RequestVote.Reply)
  case appendEntries(AppendEntries.Args)
}

extension AppMessage {
  init?(_ message: Message) {
    switch message.type {
    case .requestVote:
      guard let args = RequestVote.Args(from: message) else { return nil }
      self = .requestVote(args)
    case .requestVoteResponse:
      guard let reply = RequestVote.Reply(from: message) else { return nil }
      self = .requestVoteReply(reply)
    case .appendEntries:
      guard let args = AppendEntries.Args(from: message) else { return nil }
      self = .appendEntries(args)
    default:
      return nil
    }
  }

  var message: Message {
    switch self {
    case .requestVote(let args):
      return args.toMessage()
    case .requestVoteReply(let reply):
      return reply.toMessage()
    case .appendEntries(let args):
      return args.toMessage()
    }
  }
}

extension MessageType {
  static let requestVote = MessageType(rawValue: 0x0003)
  static let requestVoteResponse = MessageType(rawValue: 0x0004)
  static let appendEntries = MessageType(rawValue: 0x0005)
}
