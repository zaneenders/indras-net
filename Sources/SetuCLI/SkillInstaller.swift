import ArgumentParser
import Foundation

enum SkillAgent: String, CaseIterable, ExpressibleByArgument {
  case claude, codex
}

struct SkillTarget {
  let name: String
  let skill: URL
  let detected: Bool
}

enum SkillState: String {
  case current, outdated, missing, conflict
}

struct SkillInstaller {
  let home: URL
  let environment: [String: String]
  private let files = FileManager.default

  init(
    home: URL,
    environment: [String: String]
  ) {
    self.home = home
    self.environment = environment
  }

  func targets(agents: [SkillAgent]) -> [SkillTarget] {
    // Filter through allCases so repeated --agent flags never produce duplicate writes.
    SkillAgent.allCases.filter { agents.isEmpty || agents.contains($0) }.map { agent in
      let config = configuredDirectory(
        agent == .claude ? "CLAUDE_CONFIG_DIR" : "CODEX_HOME", fallback: agent == .claude ? ".claude" : ".codex")
      let skillDirectory = agent == .claude ? config : home.appendingPathComponent(".agents")
      return SkillTarget(
        name: agent == .claude ? "Claude Code" : "Codex",
        skill: skillDirectory.appendingPathComponent("skills/setu/SKILL.md"),
        detected: files.fileExists(atPath: config.path) || executableExists(agent.rawValue))
    }
  }

  func setupTargets(agents: [SkillAgent]) -> [SkillTarget] {
    let candidates = targets(agents: agents)
    if !agents.isEmpty { return candidates }
    let detected = candidates.filter(\.detected)
    if !detected.isEmpty { return detected }
    return [
      SkillTarget(
        name: "Shared agents (no supported agent found)",
        skill: home.appendingPathComponent(".agents/skills/setu/SKILL.md"), detected: false)
    ]
  }

  func skillState(_ target: SkillTarget) throws(any Error) -> SkillState {
    guard files.fileExists(atPath: target.skill.path) else {
      let directory = target.skill.deletingLastPathComponent()
      if files.fileExists(atPath: directory.path), !((try files.contentsOfDirectory(atPath: directory.path)).isEmpty) {
        return .conflict
      }
      return .missing
    }
    let data = try Data(contentsOf: target.skill)
    if data == Data(SkillGuide.stub.utf8) { return .current }
    return String(data: data, encoding: .utf8)?.contains(SkillGuide.marker) == true ? .outdated : .conflict
  }

  func setup(_ targets: [SkillTarget], force: Bool, dryRun: Bool) throws(any Error) -> [String] {
    var changes: [Change] = []
    var messages: [String] = []
    // Validate every target before writing so a conflict cannot leave another agent partially installed.
    for target in targets {
      let state = try skillState(target)
      if state == .conflict && !force {
        throw ValidationError("A skill Setu did not install exists at \(target.skill.path). Use --force to replace it.")
      }
      if state != .current {
        changes.append(.write(target.skill, Data(SkillGuide.stub.utf8)))
        messages.append("\(dryRun ? "Would install" : "Installed") \(target.name) skill: \(target.skill.path)")
      } else {
        messages.append("\(target.name) skill is current: \(target.skill.path)")
      }
    }
    if !dryRun { try apply(changes) }
    return messages
  }

  func remove(_ targets: [SkillTarget], dryRun: Bool) throws(any Error) -> [String] {
    var changes: [Change] = []
    var messages: [String] = []
    for target in targets {
      let state = try skillState(target)
      if state == .current || state == .outdated {
        changes.append(.remove(target.skill))
        messages.append("\(dryRun ? "Would remove" : "Removed") \(target.name) skill: \(target.skill.path)")
      } else if state == .conflict {
        messages.append("Left unrelated skill in place: \(target.skill.path)")
      }
    }
    if !dryRun { try apply(changes) }
    return messages.isEmpty ? ["No Setu integration installed."] : messages
  }

  private func configuredDirectory(_ key: String, fallback: String) -> URL {
    if let value = environment[key], !value.isEmpty { return URL(fileURLWithPath: value) }
    return home.appendingPathComponent(fallback)
  }

  private func executableExists(_ name: String) -> Bool {
    (environment["PATH"] ?? "").split(separator: ":", omittingEmptySubsequences: false).contains { directory in
      files.isExecutableFile(
        atPath: URL(fileURLWithPath: directory.isEmpty ? "." : String(directory))
          .appendingPathComponent(name).path)
    }
  }

  private enum Change {
    case write(URL, Data)
    case remove(URL)
  }

  private func apply(_ changes: [Change]) throws(any Error) {
    for change in changes {
      switch change {
      case .write(let url, let data):
        try files.createDirectory(
          at: url.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700]
        )
        let permissions =
          files.fileExists(atPath: url.path)
          ? try files.attributesOfItem(atPath: url.path)[.posixPermissions] : nil
        try data.write(to: url, options: .atomic)
        try files.setAttributes([.posixPermissions: permissions ?? 0o600], ofItemAtPath: url.path)
      case .remove(let url):
        try files.removeItem(at: url)
        let directory = url.deletingLastPathComponent()
        // Preserve supporting files the user may have added to the skill directory.
        if try files.contentsOfDirectory(atPath: directory.path).isEmpty { try files.removeItem(at: directory) }
      }
    }
  }
}
