enum TimerDirective: Equatable {
  case scheduleNext(delay: Duration)
  case requestVote(to: PeerId, args: RequestVote.Args)
  case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
}
