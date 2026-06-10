import Foundation

struct LogEntry: Equatable, Sendable {
  let term: Term
  let command: Data

  init(term: Term, command: Data) {
    self.term = term
    self.command = command
  }
}
