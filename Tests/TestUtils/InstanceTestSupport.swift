import Foundation
import OrderedCollections

@testable import IndrasNet

public enum InstanceTestSupport {
  public static let defaultTestSeed: UInt64 = 1

  public static func electionTimeout(
    seed: UInt64 = defaultTestSeed,
    timing: NodeTiming = .default
  ) -> Duration {
    var rng = SeededRandomNumberGenerator(seed: seed)
    return .milliseconds(Int64.random(in: timing.electionTimeoutRange, using: &rng))
  }
}

extension Instance {
  public static func forTests(
    id: PeerId,
    seed: UInt64 = InstanceTestSupport.defaultTestSeed,
    peers: OrderedSet<PeerId> = [],
    role: Role = .follower,
    currentTerm: Term = 0,
    votedFor: PeerId? = nil,
    votes: OrderedDictionary<PeerId, Bool> = [:],
    commitIndex: LogIndex = 0,
    lastApplied: LogIndex = 0,
    log: [LogEntry] = .sentinel,
    timing: NodeTiming = .default
  ) -> Instance {
    Instance(
      id: id,
      peers: peers,
      role: role,
      currentTerm: currentTerm,
      votedFor: votedFor,
      votes: votes,
      commitIndex: commitIndex,
      lastApplied: lastApplied,
      log: log,
      timing: timing,
      rng: SeededRandomNumberGenerator(seed: seed)
    )
  }
}

public protocol RaftSchedulesNext {
  var scheduleNextDelay: Duration? { get }
}

extension TimerDirective: RaftSchedulesNext {
  public var scheduleNextDelay: Duration? {
    if case .scheduleNext(let delay) = self { delay } else { nil }
  }
}

extension RequestVote.Args.Action: RaftSchedulesNext {
  public var scheduleNextDelay: Duration? {
    if case .scheduleNext(let delay) = self { delay } else { nil }
  }
}

extension AppendEntries.Args.Action: RaftSchedulesNext {
  public var scheduleNextDelay: Duration? {
    if case .scheduleNext(let delay) = self { delay } else { nil }
  }
}

extension RequestVote.Reply.Action: RaftSchedulesNext {
  public var scheduleNextDelay: Duration? {
    if case .scheduleNext(let delay) = self { delay } else { nil }
  }
}

extension AppendEntries.Reply.Action: RaftSchedulesNext {
  public var scheduleNextDelay: Duration? {
    if case .scheduleNext(let delay) = self { delay } else { nil }
  }
}

extension Array where Element: RaftSchedulesNext {
  public var scheduledDelay: Duration? {
    compactMap(\.scheduleNextDelay).last
  }
}
