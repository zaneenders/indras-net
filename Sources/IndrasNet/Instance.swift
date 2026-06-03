struct Instance {
  var members: Set<PeerID> = []

  let id: PeerID

  init(_ peerID: PeerID) {
    self.id = peerID
  }

  func ping(_ peer: PeerID) -> [PingAction] {
    [.callPong(peer)]
  }

  func update(_ timeStamp: ContinuousClock.Instant, connected: Set<PeerID>) -> [UpdateAction] {
    var result: [UpdateAction] = [.next(timeStamp.advanced(by: .seconds(1)))]
    let missing = members.subtracting(connected)
    if !missing.isEmpty {
      result.append(.dialsToStart(Array(missing)))
    }
    if !connected.isEmpty {
      result.append(.pingsToSend(Array(connected)))
    }
    return result
  }
}

enum UpdateAction {
  case next(ContinuousClock.Instant)
  case dialsToStart([PeerID])
  case pingsToSend([PeerID])
}

enum PingAction {
  case callPong(PeerID)
}
