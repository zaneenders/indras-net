import Foundation
import IndraNode
import Slate
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

@MainActor final class MonitorPings {
  private let requests = ScreenSource<NodeIdentity>(policy: .ordered(capacity: 1))
  let updates = ScreenSource<ManualPing>(policy: .latest)
  private var latest: ManualPing?

  func read() -> ManualPing? { latest }

  func ping(_ node: NodeIdentity) {
    guard latest?.isPending != true else { return }
    guard requests.publish(node) == .accepted else { return }
    latest = ManualPing(target: node, outcome: .pending)
  }

  func finish() {
    requests.finish()
    updates.finish()
  }

  func run(socket: String) async {
    while let node = await requests.next() {
      guard !Task.isCancelled else { return }
      let outcome: ManualPing.Outcome
      do {
        let address = PeerAddress(host: node.endpoint.host, port: node.endpoint.port)
        guard
          case .pong(let pong) = try await NodeControl.request(
            .ping(address.description), socket: socket, timeout: .seconds(3))
        else { throw IndraError("Unexpected ping response") }
        outcome = .success(pong)
      } catch {
        guard !Task.isCancelled else { return }
        outcome = .failure(String(String(describing: error).prefix(512)))
      }
      let result = ManualPing(target: node, outcome: outcome)
      latest = result
      updates.publish(result)
    }
  }
}
