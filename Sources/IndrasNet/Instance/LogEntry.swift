import Foundation

package struct LogEntry: Equatable, Sendable, Codable {
  let term: Term
  let command: Data

  init(term: Term, command: Data) {
    self.term = term
    self.command = command
  }
}
