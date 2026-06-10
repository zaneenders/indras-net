extension [PeerId: Bool] {
  func isLeader(_ peerCount: Int) -> Bool {
    let granted = self.values.reduce(into: 0) { count, vote in
      if vote { count += 1 }
    }
    let clusterSize = peerCount + 1
    return granted * 2 > clusterSize
  }
}
