import NIOCore

enum RaftMessage: Equatable, Sendable {
  case requestVote(RequestVote.Args)
  case requestVoteReply(RequestVote.Reply)
  case appendEntries(AppendEntries.Args)
  case appendEntriesReply(AppendEntries.Reply)
  case clientSubmit(ClientSubmit.Args)
  case clientSubmitReply(ClientSubmit.Reply)
}

extension RaftMessage {
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
    case .appendEntriesResponse:
      guard let reply = AppendEntries.Reply(from: message) else { return nil }
      self = .appendEntriesReply(reply)
    case .clientSubmit:
      guard let args = ClientSubmit.Args(from: message) else { return nil }
      self = .clientSubmit(args)
    case .clientSubmitResponse:
      guard let reply = ClientSubmit.Reply(from: message) else { return nil }
      self = .clientSubmitReply(reply)
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
    case .appendEntriesReply(let reply):
      return reply.toMessage()
    case .clientSubmit(let args):
      return args.toMessage()
    case .clientSubmitReply(let reply):
      return reply.toMessage()
    }
  }
}

extension MessageType {
  static let requestVote = MessageType(rawValue: 0x0003)
  static let requestVoteResponse = MessageType(rawValue: 0x0004)
  static let appendEntries = MessageType(rawValue: 0x0005)
  static let appendEntriesResponse = MessageType(rawValue: 0x0006)
  static let clientSubmit = MessageType(rawValue: 0x0007)
  static let clientSubmitResponse = MessageType(rawValue: 0x0008)
}
