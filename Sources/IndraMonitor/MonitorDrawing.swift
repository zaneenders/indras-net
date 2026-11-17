import Foundation
import IndraNode
import Slate
import TCPActorSystem

final class MonitorDrawing: CanvasDrawing, ~Sendable {
  var model = MonitorModel()
  private let background = Color.rgb(15, 20, 27)

  private func style(_ color: Color, bold: Bool) -> Style {
    Style(foreground: color, background: background, bold: bold, dim: false, underlined: false, inverse: false)
  }
  private var normal: Style { style(.rgb(211, 221, 230), bold: false) }
  private var muted: Style { style(.rgb(125, 145, 161), bold: false) }
  private var border: Style { style(.rgb(62, 84, 104), bold: false) }
  private var accent: Style { style(.rgb(98, 213, 229), bold: true) }
  private var good: Style { style(.rgb(124, 211, 155), bold: false) }
  private var warning: Style { style(.rgb(240, 191, 105), bold: false) }
  private var bad: Style { style(.rgb(240, 128, 128), bold: false) }

  func paint(in frame: inout Frame) {
    let width = frame.area.width
    let height = frame.area.height
    frame.fill(frame.area, with: Cell(.space, style: normal))
    guard width >= 60, height >= 22 else {
      line("INDRA / CLUSTER MONITOR", x: 1, y: 1, width: width - 2, style: accent, frame: &frame)
      line("Resize to at least 60 columns x 22 rows", x: 1, y: 3, width: width - 2, style: normal, frame: &frame)
      line("Q quit", x: 1, y: 5, width: width - 2, style: muted, frame: &frame)
      return
    }
    line("INDRA / CLUSTER MONITOR", x: 2, y: 1, width: width - 28, style: accent, frame: &frame)
    let mode =
      model.paused ? "PAUSED" : model.error != nil ? "DISCONNECTED" : model.status == nil ? "CONNECTING" : "LIVE"
    let age = model.receivedAt.map { "  \(integer($0.duration(to: .now)))s ago" } ?? ""
    line(
      mode + age, x: width - 26, y: 1, width: 24,
      style: model.error != nil ? bad : model.paused ? warning : good, frame: &frame)
    if let status = model.status {
      line(
        "\(status.cluster) / \(status.node.name) / \(address(status.node)) / local SWIM view",
        x: 2, y: 2, width: width - 4, style: muted, frame: &frame)
      line(
        "Daemon \(status.node.build?.summary ?? "unknown build")   Client \(BuildIdentity.current.summary)",
        x: 2, y: 3, width: width - 4, style: muted, frame: &frame)
    }
    summary(width: width, frame: &frame)
    let activityHeight = height >= 32 ? 10 : 6
    let body = Rect(x: 2, y: 9, width: width - 4, height: height - activityHeight - 11)
    if width >= 110, !model.showDetails {
      let left = (body.width * 3) / 5
      members(in: Rect(x: body.x, y: body.y, width: left - 1, height: body.height), frame: &frame)
      details(in: Rect(x: body.x + left, y: body.y, width: body.width - left, height: body.height), frame: &frame)
    } else if model.showDetails {
      details(in: body, frame: &frame)
    } else {
      members(in: body, frame: &frame)
    }
    activity(in: Rect(x: 2, y: height - activityHeight - 1, width: width - 4, height: activityHeight), frame: &frame)
    line(
      "↑/↓ select  P ping  Enter details  Space pause  Q quit", x: 2, y: height - 1,
      width: width - 4, style: muted, frame: &frame)
  }

  private func summary(width: Int, frame: inout Frame) {
    let gap = 1
    let cardWidth = (width - 6) / 3
    let cards = (0..<3).map { Rect(x: 2 + $0 * (cardWidth + gap), y: 5, width: cardWidth, height: 4) }
    let peers = model.status?.members ?? []
    card(
      "MEMBERSHIP",
      lines: [
        "\(peers.filter { $0.state == .alive }.count) alive / \(peers.count) peers",
        "\(peers.filter { $0.state == .suspect }.count) suspect / \(peers.filter { $0.state == .unreachable || $0.state == .dead }.count) down"
      ],
      in: cards[0], frame: &frame)
    let rates: String
    if model.error != nil {
      rates = "stale / no live rates"
    } else {
      rates = "↓ \(rate(model.inboundRate))  ↑ \(rate(model.outboundRate)) /s"
    }
    card(
      "PROBE TRAFFIC",
      lines: [
        rates,
        "\(model.snapshot?.inFlight ?? 0) in flight / \(model.snapshot?.outbound.failures ?? 0) failed"
      ],
      in: cards[1], frame: &frame)
    let engine =
      model.snapshot.map { "LHM \($0.localHealthMultiplier) / period \($0.protocolPeriod)" } ?? "Awaiting telemetry"
    let trend = sparkline(model.history, width: max(0, cardWidth - 4))
    card(
      "SWIM / TRAFFIC TREND", lines: [engine, trend.isEmpty ? "collecting samples…" : trend], in: cards[2],
      frame: &frame)
  }

  private func members(in area: Rect, frame: inout Frame) {
    frame.border(area, title: " MEMBERS ", style: border)
    let peers = model.status?.members ?? []
    guard !peers.isEmpty else {
      line(
        model.status == nil ? "Connecting to daemon…" : "No peers discovered", x: area.x + 2,
        y: area.y + 2, width: area.width - 4, style: muted, frame: &frame)
      return
    }
    let rows = max(0, area.height - 3)
    let first = max(0, (model.selectedIndex ?? 0) - rows + 1)
    let wide = area.width >= 68
    line(
      wide ? "NAME          STATE        ADDRESS               BUILD" : "NAME          STATE        BUILD",
      x: area.x + 2, y: area.y + 1, width: area.width - 4, style: muted, frame: &frame)
    for (row, member) in peers.dropFirst(first).prefix(rows).enumerated() {
      let selected = member.node.endpoint.node == model.selectedProcess
      let rowStyle =
        selected
        ? Style(
          foreground: .rgb(226, 245, 249), background: .rgb(28, 57, 73),
          bold: true, dim: false, underlined: false, inverse: false) : stateStyle(member.state)
      let y = area.y + 2 + row
      frame.fill(Rect(x: area.x + 1, y: y, width: area.width - 2, height: 1), with: Cell(.space, style: rowStyle))
      var text = "\(selected ? "›" : " ")\(pad(member.node.name, to: 12)) \(pad(member.state.rawValue, to: 12)) "
      if wide { text += pad(address(member.node), to: 21) + " " }
      text += member.node.build?.summary ?? "unknown"
      line(text, x: area.x + 1, y: y, width: area.width - 2, style: rowStyle, frame: &frame)
    }
  }

  private func details(in area: Rect, frame: inout Frame) {
    frame.border(area, title: " SELECTED PEER ", style: border)
    guard let member = model.selected else {
      line("Select a peer to inspect", x: area.x + 2, y: area.y + 2, width: area.width - 4, style: muted, frame: &frame)
      return
    }
    let node = member.node
    var lines: [String] = [
      "\(node.name) / \(member.state.rawValue)", address(node),
      "Build: \(node.build?.summary ?? "unknown (older daemon)")",
      "Node: \(node.id.uuidString)", "Process: \(node.endpoint.node.uuidString)"
    ]
    if let build = node.build { lines.append("Commit: \(build.commit)") }
    if let status = model.status, status.seeds.contains(PeerAddress(host: node.endpoint.host, port: node.endpoint.port))
    {
      lines.append("Configured seed")
    }
    if let snapshot = model.snapshot {
      lines.append("Observer up \(Int(snapshot.uptimeSeconds))s / LHM \(snapshot.localHealthMultiplier)")
      lines.append(
        "Probe \(snapshot.probeIntervalMilliseconds)ms / timeout \(snapshot.requestTimeoutMilliseconds)ms (base)")
      lines.append("Observer OUT ping \(snapshot.outbound.ping) / indirect \(snapshot.outbound.pingRequest)")
      lines.append("Observer IN ack \(snapshot.inbound.ack) / nack \(snapshot.inbound.nack)")
    }
    for (index, text) in lines.prefix(max(0, area.height - 2)).enumerated() {
      line(
        text, x: area.x + 2, y: area.y + 1 + index, width: area.width - 4,
        style: index == 0 ? stateStyle(member.state) : normal, frame: &frame)
    }
  }

  private func activity(in area: Rect, frame: inout Frame) {
    frame.border(area, title: " RECENT ACTIVITY / newest first ", style: border)
    var row = area.y + 1
    if let ping = model.manualPing {
      let style: Style
      switch ping.outcome {
      case .pending: style = warning
      case .success: style = good
      case .failure: style = bad
      }
      line(ping.summary, x: area.x + 2, y: row, width: area.width - 4, style: style, frame: &frame)
      row += 1
    }
    if let error = model.error {
      line("STALE VIEW: \(error)", x: area.x + 2, y: row, width: area.width - 4, style: bad, frame: &frame)
      return
    }
    guard let snapshot = model.snapshot else {
      line(
        model.status == nil ? "Waiting for daemon…" : "Update daemon to enable probe telemetry.",
        x: area.x + 2, y: row, width: area.width - 4, style: warning, frame: &frame)
      return
    }
    if let error = snapshot.status.configurationError {
      line("CONFIG: \(error)", x: area.x + 2, y: row, width: area.width - 4, style: bad, frame: &frame)
      row += 1
    }
    for change in snapshot.changes.suffix(2).reversed() {
      guard row < area.y + area.height - 1 else { break }
      line(
        "+\(Int(change.uptimeSeconds))s SWIM \(change.node.name): \(change.previous?.rawValue ?? "new") → \(change.current.rawValue)",
        x: area.x + 2, y: row, width: area.width - 4, style: stateStyle(change.current), frame: &frame)
      row += 1
    }
    for event in snapshot.events.reversed().prefix(max(0, area.y + area.height - 1 - row)) {
      let kind = event.kind == .pingRequest ? "PING_REQ" : event.kind.rawValue.uppercased()
      var text =
        "+\(Int(event.uptimeSeconds))s \(event.direction == .inbound ? "IN " : "OUT") \(pad(kind, to: 8)) \(event.peer) #\(event.sequenceNumber)"
      if let target = event.target, target != event.peer { text += " → \(target)" }
      if let milliseconds = event.roundTripMilliseconds { text += " \(rate(milliseconds))ms" }
      text += event.error.map { " \($0)" } ?? " gossip=\(event.gossipMembers)"
      line(
        text, x: area.x + 2, y: row, width: area.width - 4,
        style: event.kind == .failure || event.kind == .nack ? warning : muted, frame: &frame)
      row += 1
    }
  }

  private func line(_ text: String, x: Int, y: Int, width: Int, style: Style, frame: inout Frame) {
    frame.text(text, in: Rect(x: x, y: y, width: max(0, width), height: 1), style: style)
  }

  private func card(_ title: String, lines: [String], in area: Rect, frame: inout Frame) {
    frame.border(area, title: " \(title) ", style: border)
    for (index, text) in lines.enumerated() {
      line(text, x: area.x + 2, y: area.y + 1 + index, width: area.width - 4, style: normal, frame: &frame)
    }
  }

  private func address(_ node: NodeIdentity) -> String {
    PeerAddress(host: node.endpoint.host, port: node.endpoint.port).description
  }
  private func rate(_ value: Double?) -> String {
    value.map { $0.formatted(.number.precision(.fractionLength(1))) } ?? "—"
  }
  private func integer(_ duration: Duration) -> Int { max(0, Int(duration.components.seconds)) }
  private func pad(_ text: String, to width: Int) -> String {
    let clipped = String(text.prefix(width))
    return clipped + String(repeating: " ", count: max(0, width - clipped.count))
  }
  private func stateStyle(_ state: Member.State) -> Style {
    switch state {
    case .alive: good
    case .suspect: warning
    case .unreachable, .dead: bad
    }
  }
  private func sparkline(_ values: [Double], width: Int) -> String {
    let levels = Array("▁▂▃▄▅▆▇█")
    let peak = max(1, values.max() ?? 1)
    return String(values.suffix(width).map { levels[min(7, max(0, Int($0 / peak * 7)))] })
  }
}
