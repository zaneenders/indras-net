import Foundation
import OrderedCollections

/// Tracks in-flight and completed client writes for idempotent submit handling.
struct ClientRequestLog {
  struct Pending: Equatable {
    let requestId: UInt128
    let client: PeerId
  }

  private var byIndex: [LogIndex: Pending] = [:]
  private var inFlight: OrderedDictionary<PeerId, OrderedDictionary<UInt128, LogIndex>> = [:]
  private var completed: OrderedDictionary<PeerId, OrderedDictionary<UInt128, LogIndex>> = [:]
  private var waiters: OrderedDictionary<UInt128, OrderedSet<PeerId>> = [:]

  func completedIndex(client: PeerId, requestId: UInt128) -> LogIndex? {
    completed[client]?[requestId]
  }

  func inFlightIndex(client: PeerId, requestId: UInt128) -> LogIndex? {
    inFlight[client]?[requestId]
  }

  func pending(at index: LogIndex) -> Pending? {
    byIndex[index]
  }

  mutating func append(requestId: UInt128, client: PeerId, atIndex index: LogIndex) {
    byIndex[index] = Pending(requestId: requestId, client: client)
    inFlight[client, default: [:]][requestId] = index
  }

  mutating func addWaiter(_ client: PeerId, forRequestId requestId: UInt128) {
    waiters[requestId, default: OrderedSet()].append(client)
  }

  /// Moves the request at `index` to completed and returns who to notify.
  mutating func complete(atIndex index: LogIndex) -> (pending: Pending, waiters: OrderedSet<PeerId>)? {
    guard let pending = byIndex.removeValue(forKey: index) else { return nil }
    inFlight[pending.client]?.removeValue(forKey: pending.requestId)
    if inFlight[pending.client]?.isEmpty == true {
      inFlight.removeValue(forKey: pending.client)
    }
    completed[pending.client, default: [:]][pending.requestId] = index
    let waiterSet = waiters.removeValue(forKey: pending.requestId) ?? OrderedSet()
    return (pending, waiterSet)
  }

  mutating func resetSessions() {
    inFlight.removeAll()
    completed.removeAll()
  }

  mutating func drainForAbort() -> [(LogIndex, Pending)] {
    let pending = byIndex.map { ($0.key, $0.value) }
    byIndex.removeAll()
    waiters.removeAll()
    resetSessions()
    return pending
  }
}
