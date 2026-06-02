struct Instance {
  var members: Set<PeerID> = []
  var heardFrom: [PeerID: ContinuousClock.Instant] = [:]

  let id: PeerID

  init(_ peerID: PeerID) {
    self.id = peerID
  }

  mutating func ping(_ peer: PeerID, _ snapShot: ContinuousClock.Instant) -> [PingAction] {
    heardFrom[peer] = snapShot
    return [.callPong(peer)]
  }

  mutating func pong(_ peer: PeerID, _ snapShot: ContinuousClock.Instant) {
    heardFrom[peer] = snapShot
  }

  mutating func update(_ timeStamp: ContinuousClock.Instant, connected: Set<PeerID>) -> [UpdateAction] {
    for (peer, snap) in heardFrom where snap < timeStamp.advanced(by: .seconds(-3)) {
      heardFrom.removeValue(forKey: peer)
    }
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
