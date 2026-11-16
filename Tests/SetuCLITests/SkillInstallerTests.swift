import ArgumentParser
import Foundation
import Testing

@testable import SetuCLI

struct SkillInstallerTests {
  @Test func setupIsIdempotentAndNeverTouchesAgentSettings() throws(any Error) {
    let root = try temporaryHome()
    defer { try? FileManager.default.removeItem(at: root) }
    let installer = SkillInstaller(home: root, environment: ["PATH": "/nonexistent"])
    let targets = installer.targets(agents: [])
    // Even malformed settings and legacy hooks are outside the installer's scope.
    let settings = [
      root.appendingPathComponent(".claude/settings.json"), root.appendingPathComponent(".codex/hooks.json")
    ]
    for url in settings { try write(Data("{user settings".utf8), to: url) }
    _ = try installer.setup(targets, force: false, dryRun: false)
    let skills = try targets.map { try Data(contentsOf: $0.skill) }
    _ = try installer.setup(targets, force: false, dryRun: false)
    for (index, target) in targets.enumerated() {
      #expect(try installer.skillState(target) == .current)
      #expect(try Data(contentsOf: target.skill) == skills[index])
      #expect(try String(contentsOf: target.skill, encoding: .utf8).contains("setu agent"))
      #expect(try FileManager.default.attributesOfItem(atPath: target.skill.path)[.posixPermissions] as? Int == 0o600)
    }
    let extra = targets[0].skill.deletingLastPathComponent().appendingPathComponent("notes.txt")
    try Data("user notes".utf8).write(to: extra)
    _ = try installer.remove(targets, dryRun: false)
    for target in targets { #expect(!FileManager.default.fileExists(atPath: target.skill.path)) }
    for url in settings { #expect(try String(contentsOf: url, encoding: .utf8) == "{user settings") }
    #expect(try String(contentsOf: extra, encoding: .utf8) == "user notes")
    #expect(try installer.remove(targets, dryRun: false).allSatisfy { !$0.contains("Removed") })
  }

  @Test func dryRunsNeverCreateOrChangeFiles() throws(any Error) {
    let root = try temporaryHome()
    defer { try? FileManager.default.removeItem(at: root) }
    let installer = SkillInstaller(home: root, environment: ["PATH": "/nonexistent"])
    let targets = installer.targets(agents: [])
    _ = try installer.setup(targets, force: false, dryRun: true)
    #expect(try FileManager.default.contentsOfDirectory(atPath: root.path).isEmpty)
    _ = try installer.setup(targets, force: false, dryRun: false)
    let skills = try targets.map { try Data(contentsOf: $0.skill) }
    _ = try installer.remove(targets, dryRun: true)
    for (index, target) in targets.enumerated() {
      #expect(try Data(contentsOf: target.skill) == skills[index])
    }
    #expect(!FileManager.default.fileExists(atPath: root.appendingPathComponent(".codex").path))
    #expect(!FileManager.default.fileExists(atPath: root.appendingPathComponent(".claude/settings.json").path))
  }

  @Test func conflictingSkillRequiresForceAndIsNeverRemoved() throws(any Error) {
    let root = try temporaryHome()
    defer { try? FileManager.default.removeItem(at: root) }
    let installer = SkillInstaller(home: root, environment: [:])
    let targets = installer.targets(agents: [])
    let conflict = targets[1].skill
    try write(Data("My own Setu skill".utf8), to: conflict)
    #expect(throws: ValidationError.self) { try installer.setup(targets, force: false, dryRun: false) }
    #expect(!FileManager.default.fileExists(atPath: targets[0].skill.path))
    _ = try installer.remove(targets, dryRun: false)
    #expect(try String(contentsOf: conflict, encoding: .utf8) == "My own Setu skill")
    _ = try installer.setup(targets, force: true, dryRun: false)
    #expect(try installer.skillState(targets[1]) == .current)
  }

  @Test func updatesOwnedSkills() throws(any Error) {
    let root = try temporaryHome()
    defer { try? FileManager.default.removeItem(at: root) }
    let installer = SkillInstaller(home: root, environment: [:])
    let target = try #require(installer.targets(agents: [.claude]).first)
    try write(Data((SkillGuide.marker + "\nOld guide\n").utf8), to: target.skill)
    #expect(try installer.skillState(target) == .outdated)
    _ = try installer.setup([target], force: false, dryRun: false)
    #expect(try installer.skillState(target) == .current)
  }

  @Test func detectsAgentsAndHonorsCustomConfigDirectories() throws(any Error) {
    let root = try temporaryHome()
    defer { try? FileManager.default.removeItem(at: root) }
    let empty = SkillInstaller(home: root, environment: ["PATH": "/nonexistent"])
    let shared = try #require(empty.setupTargets(agents: []).first)
    #expect(shared.skill == root.appendingPathComponent(".agents/skills/setu/SKILL.md"))
    let claudeConfig = root.appendingPathComponent("custom claude")
    try FileManager.default.createDirectory(at: claudeConfig, withIntermediateDirectories: true)
    let configured = SkillInstaller(
      home: root, environment: ["PATH": "/nonexistent", "CLAUDE_CONFIG_DIR": claudeConfig.path])
    #expect(configured.setupTargets(agents: []).count == 1)
    let claude = try #require(configured.setupTargets(agents: []).first)
    #expect(claude.skill == claudeConfig.appendingPathComponent("skills/setu/SKILL.md"))
    let codex = try #require(configured.setupTargets(agents: [.codex, .codex]).first)
    #expect(codex.skill == shared.skill)
    #expect(configured.targets(agents: [.codex, .codex]).count == 1)
    let bin = root.appendingPathComponent("bin")
    let executable = bin.appendingPathComponent("codex")
    try write(Data("#!/bin/sh\n".utf8), to: executable)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
    let onPath = SkillInstaller(home: root, environment: ["PATH": bin.path])
    #expect(onPath.setupTargets(agents: []).map(\.name) == ["Codex"])
  }

  @Test func nodeIsACommandOfTheMainExecutable() throws(any Error) {
    #expect(throws: (any Error).self) { try SetuCommand.parseAsRoot(["node"]) }
    for hostOption in ["--host", "-H"] {
      let command = try SetuCommand.parseAsRoot([
        "node", hostOption, "127.0.0.1", "--port", "9000", "--temporary-directory", "/tmp/setu-test"
      ])
      let node = try #require(command as? SetuNode)
      #expect(node.host == "127.0.0.1" && node.port == 9000)
      #expect(node.temporaryDirectory == "/tmp/setu-test")
    }
  }
}

private func temporaryHome() throws(any Error) -> URL {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-skills-\(UUID())")
  try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
  return root
}

private func write(_ data: Data, to url: URL) throws(any Error) {
  try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
  try data.write(to: url)
}
