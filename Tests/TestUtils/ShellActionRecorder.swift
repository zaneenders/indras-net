import Logging
import Synchronization
import Testing

@testable import IndrasNet

public final class ShellActionRecorder: Sendable {
  private let events = Mutex<[ShellActionEvent]>([])

  public init() {}

  public func record(selfNode: String, kind: String, direction: String, peer: String) {
    events.withLock {
      $0.append(
        ShellActionEvent(selfNode: selfNode, kind: kind, direction: direction, peer: peer)
      )
    }
  }

  public func count(
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

  public func totalCount(kind: String, direction: String) -> Int {
    events.withLock {
      $0.count { event in
        event.kind == kind && event.direction == direction
      }
    }
  }
}

struct ShellActionLogHandler: LogHandler {
  let selfNode: String
  let recorder: ShellActionRecorder

  var logLevel: Logger.Level = .trace
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
  public static func shellLogger(node: NodeAddress, recorder: ShellActionRecorder) -> Logger {
    let handler = ShellActionLogHandler(selfNode: node.addressKey, recorder: recorder)
    var logger = Logger(label: "test.shell.\(node.addressKey)") { _ in handler }
    logger.logLevel = .trace
    return logger
  }

  public static func electionOccurred(recorder: ShellActionRecorder, minimumOutbound: Int) -> Bool {
    recorder.totalCount(kind: "requestVote", direction: "out") >= minimumOutbound
  }

  public static func leaderHeartbeatsStarted(recorder: ShellActionRecorder, minimumOutbound: Int) -> Bool {
    recorder.totalCount(kind: "appendEntries", direction: "out") >= minimumOutbound
  }
}
