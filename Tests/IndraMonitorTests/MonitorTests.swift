import Foundation
import Slate
import TCPActorSystem
import Testing

@testable import IndraMonitor
@testable import IndraNode

struct MonitorTests {
  @Test func pingKeyUsesSelectedAddressWhilePausedAndDoesNotQueueRepeatedPresses() async throws {
    var screen = MonitorScreen()
    let pings = MonitorPings()
    screen.pings = pings
    screen.drawing.model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 10, inbound: 0, outbound: 0, generation: 1))
    screen.drawing.model.paused = true
    var copyBuffer = CopyBuffer()
    #expect(screen.event(.key(.character("p"), modifiers: []), copyBuffer: &copyBuffer) == .redraw)
    let original = try #require(pings.read())
    #expect(original.isPending)
    #expect(original.target.endpoint.port == 7332)
    screen.drawing.model.move(1)
    _ = screen.event(.key(.character("P"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(pings.read()?.target == original.target)
    let socket = "/tmp/indra-missing-\(UUID().uuidString).sock"
    let worker = Task { await pings.run(socket: socket) }
    do {
      try await waitForPing(pings)
      _ = screen.event(.tick(elapsed: .milliseconds(200)), copyBuffer: &copyBuffer)
      if case .failure = screen.drawing.model.manualPing?.outcome {
      } else {
        Issue.record("Expected the daemon connection failure to be shown")
      }
      #expect(screen.drawing.model.paused)
      #expect(screen.drawing.model.generation == 1)
      #expect(pings.read()?.target == original.target)
      _ = screen.event(.key(.character("p"), modifiers: []), copyBuffer: &copyBuffer)
      try await waitForPing(pings)
      #expect(pings.read()?.target.name == "king-dog")
      #expect(screen.drawing.model.status?.members.first?.state == .alive)
      pings.finish()
      await worker.value
    } catch {
      pings.finish()
      worker.cancel()
      await worker.value
      throw error
    }
  }

  @Test func pingResultIsVisibleWithOlderDaemonsAndDistinguishesProcessReplacement() throws {
    let drawing = MonitorDrawing()
    let status = fixtureStatus(process: UUID())
    drawing.model.apply(MonitorReading(generation: 1, status: status, snapshot: nil, error: nil, receivedAt: .now))
    let selected = try #require(drawing.model.selected?.node)
    let replacement = NodeIdentity(
      id: selected.id, name: selected.name,
      endpoint: ActorEndpoint(host: selected.endpoint.host, port: selected.endpoint.port, node: UUID()), build: nil)
    drawing.model.manualPing = ManualPing(
      target: selected, outcome: .success(PingResult(node: replacement, milliseconds: 12.34)))
    let text = try render(drawing, size: TerminalSize(columns: 140, rows: 42))
    #expect(text.contains("P ping"))
    #expect(text.contains("PONG studio 127.0.0.1:7332: 12.34 ms (different process replied)"))
    #expect(text.contains("Update daemon to enable probe telemetry."))
    #expect(drawing.model.status?.members.first?.node.endpoint == selected.endpoint)
  }

  @Test func ratesUseDaemonTimeAndResetAfterRestart() {
    let status = fixtureStatus(process: UUID())
    var model = MonitorModel()
    model.apply(reading(status: status, uptime: 10, inbound: 2, outbound: 3, generation: 1))
    #expect(model.inboundRate == nil)
    model.apply(reading(status: status, uptime: 12, inbound: 6, outbound: 9, generation: 2))
    #expect(model.inboundRate == 2)
    #expect(model.outboundRate == 3)
    #expect(model.history == [5])
    model.apply(reading(status: fixtureStatus(process: UUID()), uptime: 1, inbound: 0, outbound: 0, generation: 3))
    #expect(model.inboundRate == nil)
    #expect(model.history.isEmpty)
  }

  @Test func pauseAndDisconnectPreserveTheObservedView() {
    let status = fixtureStatus(process: UUID())
    let initial = reading(status: status, uptime: 10, inbound: 2, outbound: 3, generation: 1)
    var model = MonitorModel()
    model.apply(initial)
    model.move(2)
    #expect(model.selected?.node.name == "pi")
    model.paused = true
    model.apply(reading(status: status, uptime: 12, inbound: 6, outbound: 9, generation: 2))
    #expect(model.generation == 1)
    model.paused = false
    model.apply(MonitorReading(generation: 3, status: nil, snapshot: nil, error: "Disconnected", receivedAt: .now))
    #expect(model.status?.node.name == "tower")
    #expect(model.error == "Disconnected")
    #expect(model.snapshot?.uptimeSeconds == 10)
    model.apply(MonitorReading(generation: 4, status: status, snapshot: nil, error: nil, receivedAt: .now))
    #expect(model.error == nil)
    #expect(model.snapshot == nil)
    #expect(model.selected?.node.name == "pi")
  }

  @Test(arguments: [
    TerminalSize(columns: 60, rows: 22), TerminalSize(columns: 80, rows: 24), TerminalSize(columns: 140, rows: 42)
  ])
  func rendersMembershipAndBuildsAtSupportedSizes(size: TerminalSize) throws {
    let drawing = MonitorDrawing()
    drawing.model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 12, inbound: 6, outbound: 9, generation: 1))
    drawing.model.move(2)
    let text = try render(drawing, size: size)
    #expect(text.contains("INDRA / CLUSTER MONITOR"))
    #expect(text.contains("LIVE"))
    #expect(text.contains("tower"))
    #expect(text.contains("pi"))
    #expect(text.contains("dirty"))
    #expect(text.contains("RECENT ACTIVITY"))
    #expect(text.contains("Q quit"))
    drawing.model.showDetails = true
    let details = try render(drawing, size: size)
    #expect(details.contains("SELECTED PEER"))
    #expect(details.contains("Build:"))
  }

  @Test func smallTerminalShowsResizeInstructions() throws {
    let text = try render(MonitorDrawing(), size: TerminalSize(columns: 40, rows: 12))
    #expect(text.contains("Resize to at least 60"))
    #expect(text.contains("Q quit"))
  }
}

private func waitForPing(_ pings: MonitorPings) async throws {
  let deadline = ContinuousClock.now.advanced(by: .seconds(3))
  while pings.read()?.isPending == true {
    guard ContinuousClock.now < deadline else { throw IndraError("Timed out waiting for ping result") }
    try await Task.sleep(for: .milliseconds(10))
  }
}

private func render(_ drawing: MonitorDrawing, size: TerminalSize) throws -> String {
  var surface = try Surface(maximumSize: size, layoutBudget: LayoutBudget(nodeCapacity: 32, maximumBlockVisits: 128))
  try surface.render(size: size, render: { frame in drawing.paint(in: &frame) })
  var text = ""
  for y in 0..<size.rows {
    for x in 0..<size.columns {
      if let cell = surface.cell(x: x, y: y), !cell.isContinuation { text.unicodeScalars.append(cell.glyph.scalar) }
    }
    text += "\n"
  }
  return text
}

private func fixtureStatus(process: UUID) -> NodeStatus {
  let build = BuildIdentity(commit: String(repeating: "a", count: 40), isDirty: true)
  let node = NodeIdentity(
    id: UUID(), name: "tower", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7331, node: process), build: build)
  let peers = ["studio", "king-dog", "pi"].enumerated().map { index, name in
    Member(
      node: NodeIdentity(
        id: UUID(), name: name,
        endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332 + index, node: UUID()), build: build),
      state: index == 2 ? .suspect : .alive)
  }
  return NodeStatus(
    node: node, cluster: "test", members: peers, seeds: [], configurationError: nil, discoveryErrors: [:])
}

private func reading(status: NodeStatus, uptime: Double, inbound: Int, outbound: Int, generation: UInt64)
  -> MonitorReading
{
  var incoming = ProbeTotals()
  var outgoing = ProbeTotals()
  for _ in 0..<inbound { incoming.record(.ack) }
  for _ in 0..<outbound { outgoing.record(.ping) }
  let snapshot = MonitorSnapshot(
    status: status, uptimeSeconds: uptime, protocolPeriod: 1, localHealthMultiplier: 0,
    probeIntervalMilliseconds: 3000, requestTimeoutMilliseconds: 2000, outbound: outgoing, inbound: incoming,
    inFlight: 0, events: [], changes: [])
  return MonitorReading(generation: generation, status: status, snapshot: snapshot, error: nil, receivedAt: .now)
}
