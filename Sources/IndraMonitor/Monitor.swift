import Foundation
import IndraNode
import Observation
import Slate
import TCPActorSystem

@MainActor public func runMonitor(socket: String) async throws {
  var screen = MonitorScreen(socket: socket)
  let session = ScreenSession(maximumOutputBytes: 4 * 1_048_576, taskCapacity: 4)
  do {
    try await session.run(
      application: &screen, inputFileDescriptor: 0, outputFileDescriptor: 1, signalHandling: .managed)
  } catch ScreenApplicationError.cancelled { return }
}

struct MonitorReading: Sendable {
  enum Result: Sendable {
    case snapshot(MonitorSnapshot, receivedAt: ContinuousClock.Instant)
    case failure(String)
  }

  let generation: UInt64
  let result: Result
}

private func poll(socket: String, readings: ScreenSource<MonitorReading>) async {
  var generation: UInt64 = 0
  while !Task.isCancelled {
    generation &+= 1
    let result: MonitorReading.Result
    do {
      guard case .monitor(let snapshot) = try await NodeControl.request(.monitor, socket: socket, timeout: .seconds(3))
      else { throw IndraError("Expected a monitor snapshot. Check that the daemon supports monitor.") }
      result = .snapshot(snapshot, receivedAt: .now)
    } catch {
      guard !Task.isCancelled else { return }
      result = .failure(String(describing: error))
    }
    readings.publish(MonitorReading(generation: generation, result: result))
    do { try await Task.sleep(for: .seconds(1)) } catch { return }
  }
}

struct MonitorModel: Sendable {
  var generation: UInt64 = 0
  var status: NodeStatus? { snapshot?.status }
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
    switch reading.result {
    case .failure(let message): error = message
    case .snapshot(let next, let receivedAt): apply(next, receivedAt: receivedAt)
    }
  }

  private mutating func apply(_ nextSnapshot: MonitorSnapshot, receivedAt: ContinuousClock.Instant) {
    error = nil
    let next = nextSnapshot.status
    let sameProcess = status?.node.endpoint.node == next.node.endpoint.node
    inboundRate = nil
    outboundRate = nil
    if sameProcess, let before = snapshot {
      let elapsed = nextSnapshot.uptimeSeconds - before.uptimeSeconds
      if elapsed > 0, nextSnapshot.inbound.messages >= before.inbound.messages,
        nextSnapshot.outbound.messages >= before.outbound.messages
      {
        inboundRate = Double(nextSnapshot.inbound.messages - before.inbound.messages) / elapsed
        outboundRate = Double(nextSnapshot.outbound.messages - before.outbound.messages) / elapsed
      }
    }
    if !sameProcess { history.removeAll(keepingCapacity: true) }
    if let inboundRate, let outboundRate {
      if history.count == 60 { history.removeFirst() }
      history.append(inboundRate + outboundRate)
    }
    snapshot = nextSnapshot
    self.receivedAt = receivedAt
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

@MainActor @Observable final class MonitorScreen: ScreenApplication {
  private let socket: String?
  var pings: MonitorPings?
  var model = MonitorModel()
  private var latest: MonitorReading?

  init(socket: String?) { self.socket = socket }

  func start(in session: ScreenSession) {
    guard let socket else { return }
    let readings = ScreenSource<MonitorReading>(policy: .latest)
    let pings = MonitorPings()
    self.pings = pings
    precondition(
      session.consume(readings, at: 0) { [self] reading in
        latest = reading
        model.apply(reading)
      })
    precondition(session.startTask(at: 1) { await poll(socket: socket, readings: readings) })
    precondition(session.startTask(at: 2) { await pings.run(socket: socket) })
    precondition(session.consume(pings.updates, at: 3) { [self] result in model.manualPing = result })
  }

  func stop() async { pings?.finish() }

  static var configuration: ScreenApplicationConfiguration {
    ScreenApplicationConfiguration(
      maximumSize: TerminalSize(columns: 320, rows: 120), viewportOverflow: .clip,
      mouseTracking: false, enhancedKeyboard: true, kittyGraphicsQueryID: nil,
      capabilities: TerminalCapabilities(color: .trueColor, unicode: .grapheme), maximumPasteBytes: 1024,
      layoutBudget: MonitorView.layoutBudget)
  }

  var tickInterval: Duration? { .seconds(1) }

  func event(_ event: borrowing ScreenEvent, copyBuffer: inout CopyBuffer) -> ScreenUpdate {
    switch event {
    case .tick:
      return .redraw
    case .key(let key, let modifiers):
      if key == .control(.c) || (key == .character("c") && modifiers.contains(.control)) { return .quit }
      switch key {
      case .character("q"): return .quit
      case .escape:
        if model.showDetails { model.showDetails = false } else { return .quit }
      case .up, .character("k"): model.move(-1)
      case .down, .character("j"): model.move(1)
      case .enter: model.showDetails.toggle()
      case .character("p"), .character("P"):
        guard let node = model.selected?.node, let pings else { return .none }
        pings.ping(node)
        model.manualPing = pings.read()
      case .character(" "):
        model.paused.toggle()
        if let latest { model.apply(latest) }
      default: return .none
      }
      return .redraw
    default: return .none
    }
  }

  borrowing func render(in size: TerminalSize) -> some Block {
    MonitorView(model: model, size: size)
  }
}
