public enum TimerDirective: Equatable, Sendable {
  case apply(entry: LogEntry, atIndex: LogIndex)
  case persist
  case scheduleNext(delay: Duration)
  case requestVote(to: PeerId, args: RequestVote.Args)
  case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
}
