import Foundation

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
    peers: Set<PeerId> = [],
    role: Role = .follower,
    currentTerm: Term = 0,
    votedFor: PeerId? = nil,
    votes: [PeerId: Bool] = [:],
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

extension Array where Element == TimerDirective {
  public var scheduledDelay: Duration? {
    compactMap { directive in
      if case .scheduleNext(let delay) = directive { delay } else { nil }
    }.last
  }
}

extension Array where Element == RequestVote.Args.Action {
  public var scheduledDelay: Duration? {
    compactMap { action in
      if case .scheduleNext(let delay) = action { delay } else { nil }
    }.last
  }
}

extension Array where Element == AppendEntries.Args.Action {
  public var scheduledDelay: Duration? {
    compactMap { action in
      if case .scheduleNext(let delay) = action { delay } else { nil }
    }.last
  }
}

extension Array where Element == RequestVote.Reply.Action {
  public var scheduledDelay: Duration? {
    compactMap { action in
      if case .scheduleNext(let delay) = action { delay } else { nil }
    }.last
  }
}

extension Array where Element == AppendEntries.Reply.Action {
  public var scheduledDelay: Duration? {
    compactMap { action in
      if case .scheduleNext(let delay) = action { delay } else { nil }
    }.last
  }
}
