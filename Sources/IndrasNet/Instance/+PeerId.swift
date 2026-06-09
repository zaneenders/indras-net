extension [PeerId: Bool] {
  func isLeader(_ peers: Int) -> Bool {
    let votes = self.values.reduce(
      into: 0,
      { count, votedFor in
        if votedFor {
          count += 1
        }
      })
    return votes > peers / 2
  }
}
