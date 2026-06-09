enum TimerAction: Equatable {
  case requestVote(to: PeerId, args: RequestVote.Args)
  case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
}
