import Distributed
import NIOCore
import TCPActorSystem

enum CounterDemo {
  static func serve(
    host: String, port: Int, callTimeout: Duration, connectTimeout: TimeAmount, lifetime: Duration
  ) async throws {
    let system = try await TCPActorSystem(
      host: host, port: port, callTimeout: callTimeout, connectTimeout: connectTimeout)
    let counter = Counter(actorSystem: system)
    defer { withExtendedLifetime(counter) {} }
    do {
      try system.publish(counter, as: "counter")
      print("Counter ready at \(system.endpoint.host):\(system.endpoint.port)")
      try await Task.sleep(for: lifetime)
      await system.shutdown()
    } catch {
      await system.shutdown()
      throw error
    }
  }

  static func check(
    host: String, peerHost: String, peerPort: Int, callTimeout: Duration, connectTimeout: TimeAmount,
    delayMilliseconds: Int, concurrentCalls: Int
  ) async throws {
    let system = try await TCPActorSystem(host: host, port: 0, callTimeout: callTimeout, connectTimeout: connectTimeout)
    system.registerError(CounterError.self)
    let callback = Counter(actorSystem: system)
    defer { withExtendedLifetime(callback) {} }
    do {
      let counter = try await system.service(named: "counter", host: peerHost, port: peerPort, as: Counter.self)
      let initial = try await counter.value()
      guard try await counter.add(1) == initial + 1, try await counter.add(40) == initial + 41 else {
        throw DemoError.wrongValue
      }
      do {
        _ = try await counter.add(-1)
        throw DemoError.missingError
      } catch CounterError.negativeAmount {}
      try await counter.pause(milliseconds: 0)
      guard try await counter.callBack(callback, amount: 7) == 7, try await callback.value() == 7 else {
        throw DemoError.wrongCallback
      }
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        for value in 0..<concurrentCalls {
          tasks.addTask {
            guard try await counter.echo(value, milliseconds: (concurrentCalls - value) % 32) == value else {
              throw DemoError.wrongCorrelation
            }
          }
        }
        try await tasks.waitForAll()
      }
      do {
        _ = try await counter.addThenPause(1, milliseconds: delayMilliseconds)
        throw DemoError.missingTimeout
      } catch RPCError.timedOut {}
      let final = try await counter.value()
      guard final == initial + 42 else { throw DemoError.wrongValue }
      print(
        "PASS client=\(system.endpoint.host):\(system.endpoint.port) server=\(counter.id.endpoint.host):\(counter.id.endpoint.port) "
          + "count=\(final) concurrent=\(concurrentCalls) callback=7 typed-error timeout no-replay")
      await system.shutdown()
    } catch {
      await system.shutdown()
      throw error
    }
  }
}

private enum DemoError: Error {
  case wrongValue
  case wrongCallback
  case wrongCorrelation
  case missingError
  case missingTimeout
}
