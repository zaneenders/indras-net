typealias PeerID = String
struct Instance {
  var peers: Set<PeerID> = []
  var heardFrom: [PeerID: ContinuousClock.Instant] = [:]

  let id: PeerID

  init(_ peerID: PeerID) {
    self.id = peerID
  }

  mutating func ping(_ peer: PeerID, _ snapShot: ContinuousClock.Instant) -> [PingAction] {
    heardFrom[peer] = snapShot
    return [.callPong(peer)]
  }

  mutating func pong(_ peer: PeerID, _ snapShot: ContinuousClock.Instant) -> [PongAction] {
    heardFrom[peer] = snapShot
    return [.callPing(peer)]
  }

  mutating func hello(_ peer: PeerID, _ snapShot: ContinuousClock.Instant) -> [HelloAction] {
    heardFrom[peer] = snapShot
    return [.callPing(peer)]
  }

  mutating func update(_ timeSamp: ContinuousClock.Instant) -> [UpdateAction] {
    var peersToPing = peers
    for (peer, snap) in heardFrom {
      if snap < timeSamp.advanced(by: .seconds(-3)) {
        heardFrom.removeValue(forKey: peer)
      } else {
        peersToPing.remove(peer)
      }
    }
    var result: [UpdateAction] = [
      .next(timeSamp.advanced(by: .seconds(1)))
    ]
    if !peersToPing.isEmpty {
      result.append(.hellosToSend(peersToPing.map { .callPing($0) }))
    }
    return result
  }
}

enum UpdateAction {
  case next(ContinuousClock.Instant)
  case hellosToSend([HelloAction])
}

enum HelloAction {
  case callPing(PeerID)
}

enum PingAction {
  case callPong(PeerID)
}

enum PongAction {
  case callPing(PeerID)
}
