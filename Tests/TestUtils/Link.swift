@testable import IndrasNet

struct Link: Hashable, Equatable {
  let lower: PeerId
  let upper: PeerId

  init(_ a: PeerId, _ b: PeerId) {
    // Respect Node ordering
    if a <= b {
      lower = a
      upper = b
    } else {
      lower = b
      upper = a
    }
  }

  func involves(_ peer: PeerId) -> Bool {
    peer == lower || peer == upper
  }
}
