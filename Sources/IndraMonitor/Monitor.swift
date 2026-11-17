import Foundation
import IndraNode
import Slate
import Synchronization
import TCPActorSystem

public func runMonitor(socket: String) async throws {
  let inbox = MonitorInbox()
  let pings = MonitorPings()
  let poller = Task.detached(priority: .utility) { await poll(socket: socket, inbox: inbox) }
  let pinger = Task.detached(priority: .utility) { await pings.run(socket: socket) }
  do {
    try await MainActor.run {
      var screen = MonitorScreen()
      screen.inbox = inbox
      screen.pings = pings
      try screen.run(inputFileDescriptor: 0, outputFileDescriptor: 1, signalHandling: .managed)
    }
    poller.cancel()
    pings.finish()
    pinger.cancel()
    await poller.value
    await pinger.value
  } catch {
    poller.cancel()
    pings.finish()
    pinger.cancel()
    await poller.value
    await pinger.value
    throw error
  }
}

struct MonitorReading: Sendable {
  var generation: UInt64 = 0
  var status: NodeStatus?
  var snapshot: MonitorSnapshot?
  var error: String?
  var receivedAt: ContinuousClock.Instant?
}

final class MonitorInbox: Sendable {
  private let latest = Mutex(MonitorReading())

  func read() -> MonitorReading { latest.withLock { $0 } }

  func publish(status: NodeStatus?, snapshot: MonitorSnapshot?, error: String?) {
    latest.withLock { reading in
      reading.generation &+= 1
      reading.status = status
      reading.snapshot = snapshot
      reading.error = error
      reading.receivedAt = error == nil ? .now : reading.receivedAt
    }
  }
}

private func poll(socket: String, inbox: MonitorInbox) async {
  var legacy = false
  var iteration = 0
  while !Task.isCancelled {
    do {
      if !legacy || iteration % 5 == 0 {
        do {
          guard case .monitor(let snapshot) = try await request(.monitor, socket: socket) else {
            throw IndraError("Unexpected monitor response")
          }
          inbox.publish(status: snapshot.status, snapshot: snapshot, error: nil)
          legacy = false
        } catch {
          try Task.checkCancellation()
          guard case .status(let status) = try await request(.status, socket: socket) else { throw error }
          inbox.publish(status: status, snapshot: nil, error: nil)
          legacy = true
        }
      } else {
        guard case .status(let status) = try await request(.status, socket: socket) else {
          throw IndraError("Unexpected status response")
        }
        inbox.publish(status: status, snapshot: nil, error: nil)
      }
    } catch {
      if !Task.isCancelled { inbox.publish(status: nil, snapshot: nil, error: String(describing: error)) }
    }
    iteration &+= 1
    do { try await Task.sleep(for: .seconds(1)) } catch { return }
  }
}

private func request(_ request: ControlRequest, socket: String) async throws -> ControlReply {
  try await withThrowingTaskGroup(of: ControlReply.self) { tasks in
    tasks.addTask { try await NodeControl.request(request, socket: socket) }
    tasks.addTask {
      try await Task.sleep(for: .seconds(3))
      throw IndraError("Daemon did not respond within 3 seconds")
    }
    defer { tasks.cancelAll() }
    guard let reply = try await tasks.next() else { throw CancellationError() }
    return reply
  }
}

struct MonitorModel: Sendable {
  var generation: UInt64 = 0
  var status: NodeStatus?
  var snapshot: MonitorSnapshot?
  var error: String?
  var receivedAt: ContinuousClock.Instant?
  var selectedProcess: UUID?
  var inboundRate: Double?
  var outboundRate: Double?
  var history: [Double] = []
  var paused = false
  var showDetails = false
  var manualPing: ManualPing?

  mutating func apply(_ reading: MonitorReading) {
    guard !paused, reading.generation != generation else { return }
    generation = reading.generation
    error = reading.error
    guard let next = reading.status else { return }
    let sameProcess = status?.node.endpoint.node == next.node.endpoint.node
    inboundRate = nil
    outboundRate = nil
    if sameProcess, let before = snapshot, let after = reading.snapshot {
      let elapsed = after.uptimeSeconds - before.uptimeSeconds
      if elapsed > 0, after.inbound.messages >= before.inbound.messages,
        after.outbound.messages >= before.outbound.messages
      {
        inboundRate = Double(after.inbound.messages - before.inbound.messages) / elapsed
        outboundRate = Double(after.outbound.messages - before.outbound.messages) / elapsed
      }
    }
    if !sameProcess || reading.snapshot == nil { history.removeAll(keepingCapacity: true) }
    if let inboundRate, let outboundRate {
      if history.count == 60 { history.removeFirst() }
      history.append(inboundRate + outboundRate)
    }
    status = next
    snapshot = reading.snapshot
    receivedAt = reading.receivedAt
    if !next.members.contains(where: { $0.node.endpoint.node == selectedProcess }) {
      selectedProcess = next.members.first?.node.endpoint.node
    }
  }

  var selectedIndex: Int? { status?.members.firstIndex { $0.node.endpoint.node == selectedProcess } }
  var selected: Member? {
    guard let selectedIndex else { return nil }
    return status?.members[selectedIndex]
  }

  mutating func move(_ offset: Int) {
    guard let members = status?.members, !members.isEmpty else { return }
    selectedProcess = members[max(0, min(members.count - 1, (selectedIndex ?? 0) + offset))].node.endpoint.node
  }
}

struct MonitorScreen: ScreenApplication, ~Sendable {
  var inbox: MonitorInbox?
  var pings: MonitorPings?
  let drawing = MonitorDrawing()

  init() {}

  static var configuration: ScreenApplicationConfiguration {
    ScreenApplicationConfiguration(
      maximumSize: TerminalSize(columns: 320, rows: 120), viewportOverflow: .clip,
      mouseTracking: false, enhancedKeyboard: true, kittyGraphicsQueryID: nil,
      capabilities: TerminalCapabilities(color: .trueColor, unicode: .grapheme), maximumPasteBytes: 1024,
      layoutBudget: LayoutBudget(nodeCapacity: 32, maximumBlockVisits: 128))
  }

  var tickInterval: Duration? { .milliseconds(200) }

  mutating func event(_ event: borrowing ScreenEvent, copyBuffer: inout CopyBuffer) -> ScreenUpdate {
    switch event {
    case .tick:
      if let inbox { drawing.model.apply(inbox.read()) }
      drawing.model.manualPing = pings?.read()
      return .redraw
    case .key(let key, let modifiers):
      if key == .control(.c) || (key == .character("c") && modifiers.contains(.control)) { return .quit }
      switch key {
      case .character("q"): return .quit
      case .escape:
        if drawing.model.showDetails { drawing.model.showDetails = false } else { return .quit }
      case .up, .character("k"): drawing.model.move(-1)
      case .down, .character("j"): drawing.model.move(1)
      case .enter: drawing.model.showDetails.toggle()
      case .character("p"), .character("P"):
        guard let node = drawing.model.selected?.node, let pings else { return .none }
        pings.ping(node)
        drawing.model.manualPing = pings.read()
      case .character(" "):
        drawing.model.paused.toggle()
        if let inbox { drawing.model.apply(inbox.read()) }
      default: return .none
      }
      return .redraw
    default: return .none
    }
  }

  borrowing func render(in size: TerminalSize) -> some Block {
    Canvas(drawing).frame(width: .fill, height: .fill, alignment: .topLeading)
  }
}
