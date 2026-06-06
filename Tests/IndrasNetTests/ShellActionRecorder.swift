import Logging
import Synchronization
import Testing

@testable import IndrasNet

struct ShellActionEvent: Equatable, Sendable {
  let selfNode: String
  let kind: String
  let direction: String
  let peer: String
}

final class ShellActionRecorder: Sendable {
  private let events = Mutex<[ShellActionEvent]>([])

  func record(selfNode: String, kind: String, direction: String, peer: String) {
    events.withLock {
      $0.append(
        ShellActionEvent(selfNode: selfNode, kind: kind, direction: direction, peer: peer)
      )
    }
  }

  func count(
    selfNode: String,
    kind: String,
    direction: String,
    peer: String
  ) -> Int {
    events.withLock {
      $0.count { event in
        event.selfNode == selfNode
          && event.kind == kind
          && event.direction == direction
          && event.peer == peer
      }
    }
  }
}

struct ShellActionLogHandler: LogHandler {
  let selfNode: String
  let recorder: ShellActionRecorder

  var logLevel: Logger.Level = .info
  var metadata: Logger.Metadata = [:]

  subscript(metadataKey key: String) -> Logger.Metadata.Value? {
    get { metadata[key] }
    set { metadata[key] = newValue }
  }

  func log(event: LogEvent) {
    var probe = self.metadata
    if let eventMetadata = event.metadata {
      probe.merge(eventMetadata) { _, new in new }
    }
    guard case .string(let kind)? = probe[ShellLogKey.kind],
      case .string(let direction)? = probe[ShellLogKey.direction],
      case .string(let peer)? = probe[ShellLogKey.peer]
    else {
      return
    }
    recorder.record(selfNode: selfNode, kind: kind, direction: direction, peer: peer)
  }
}

extension TestHelpers {
  static func shellLogger(node: NodeAddress, recorder: ShellActionRecorder) -> Logger {
    let handler = ShellActionLogHandler(selfNode: node.addressKey, recorder: recorder)
    return Logger(label: "test.shell.\(node.addressKey)") { _ in handler }
  }

  static func electionTrafficMet(
    recorder: ShellActionRecorder,
    nodes: [NodeAddress],
    minimum: Int
  ) -> Bool {
    for local in nodes {
      for remote in nodes where remote.addressKey != local.addressKey {
        if recorder.count(
          selfNode: local.addressKey, kind: "requestVote", direction: "out", peer: remote.addressKey
        ) < minimum {
          return false
        }
        if recorder.count(
          selfNode: remote.addressKey, kind: "requestVote", direction: "in", peer: local.addressKey
        ) < minimum {
          return false
        }
      }
    }
    return true
  }
}
