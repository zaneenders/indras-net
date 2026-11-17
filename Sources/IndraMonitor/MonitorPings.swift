import Foundation
import IndraNode
import Synchronization
import TCPActorSystem

struct ManualPing: Sendable {
  enum Outcome: Sendable {
    case pending
    case success(PingResult)
    case failure(String)
  }

  let target: NodeIdentity
  let outcome: Outcome

  var isPending: Bool {
    if case .pending = outcome { return true }
    return false
  }

  var summary: String {
    let address = PeerAddress(host: target.endpoint.host, port: target.endpoint.port)
    switch outcome {
    case .pending: return "PING \(target.name) \(address): waiting…"
    case .success(let pong):
      let latency = pong.milliseconds.formatted(.number.precision(.fractionLength(2)))
      let process = pong.node.endpoint.node == target.endpoint.node ? "" : " (different process replied)"
      return "PONG \(target.name) \(address): \(latency) ms\(process)"
    case .failure(let error): return "PING \(target.name) \(address): \(error)"
    }
  }
}

final class MonitorPings: Sendable {
  private let requests: AsyncStream<NodeIdentity>
  private let pending: AsyncStream<NodeIdentity>.Continuation
  private let latest = Mutex<ManualPing?>(nil)

  init() {
    (requests, pending) = AsyncStream.makeStream(bufferingPolicy: .bufferingOldest(1))
  }

  func read() -> ManualPing? { latest.withLock { $0 } }

  func ping(_ node: NodeIdentity) {
    let accepted = latest.withLock { result in
      guard result?.isPending != true else { return false }
      result = ManualPing(target: node, outcome: .pending)
      return true
    }
    if accepted { pending.yield(node) }
  }

  func finish() { pending.finish() }

  func run(socket: String) async {
    for await node in requests {
      guard !Task.isCancelled else { return }
      let outcome: ManualPing.Outcome
      do {
        let address = PeerAddress(host: node.endpoint.host, port: node.endpoint.port)
        guard case .pong(let pong) = try await NodeControl.request(.ping(address.description), socket: socket) else {
          throw IndraError("Unexpected ping response")
        }
        outcome = .success(pong)
      } catch {
        guard !Task.isCancelled else { return }
        outcome = .failure(String(String(describing: error).prefix(512)))
      }
      latest.withLock { $0 = ManualPing(target: node, outcome: outcome) }
    }
  }
}
