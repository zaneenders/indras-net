import Distributed
import TCPActorSystem

distributed actor Counter {
  typealias ActorSystem = TCPActorSystem

  private var count = 0

  init(actorSystem: ActorSystem) {
    self.actorSystem = actorSystem
  }

  distributed func add(_ amount: Int) throws -> Int {
    guard amount >= 0 else { throw CounterError.negativeAmount }
    count += amount
    return count
  }

  distributed func value() -> Int { count }

  distributed func pause(milliseconds: Int) async throws {
    try await Task.sleep(for: .milliseconds(milliseconds))
  }

  distributed func addThenPause(_ amount: Int, milliseconds: Int) async throws -> Int {
    let value = try add(amount)
    try await Task.sleep(for: .milliseconds(milliseconds))
    return value
  }

  distributed func echo(_ value: Int, milliseconds: Int) async throws -> Int {
    try await Task.sleep(for: .milliseconds(milliseconds))
    return value
  }

  distributed func callBack(_ counter: Counter, amount: Int) async throws -> Int {
    try await counter.add(amount)
  }
}

enum CounterError: Error, Codable, Sendable {
  case negativeAmount
}
