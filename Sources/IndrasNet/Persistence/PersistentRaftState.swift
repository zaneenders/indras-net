import Foundation

/// Durable Raft state from Figure 2: `currentTerm`, `votedFor`, and the log.
package struct PersistentRaftState: Codable, Equatable, Sendable {
  var currentTerm: Term
  var votedFor: PeerId?
  var log: [LogEntry]

  package init(currentTerm: Term = 0, votedFor: PeerId? = nil, log: [LogEntry] = .sentinel) {
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.log = log
  }
}
