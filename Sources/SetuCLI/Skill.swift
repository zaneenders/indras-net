import ArgumentParser
import Foundation
import Setu
import SetuNetworking

struct Skill: ParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Install and manage Setu's agent skill.",
    discussion: """
      Setu installs a skill stub that reads its usage guide from the installed CLI, so upgrading Setu also \
      updates what agents are told.

      Run 'setu setup skill' once per account, and 'setu setup skill status' to see what is installed. \
      These commands only install instruction files; they do not install the executable or configure \
      or start a node. Run 'setu agent' for node installation, configuration, and transfer instructions.
      """,
    subcommands: [SkillInstall.self, SkillStatus.self, SkillRemove.self],
    defaultSubcommand: SkillInstall.self)
}

private struct AgentOptions: ParsableArguments {
  @Option(help: "Use only this agent: claude or codex (repeatable).") var agent: [SkillAgent] = []
}

private struct SkillStatus: ParsableCommand {
  static let configuration = CommandConfiguration(commandName: "status", abstract: "Report installed skills.")
  @OptionGroup var agents: AgentOptions

  func run() throws(any Error) {
    let installer = SkillInstaller(
      home: URL(fileURLWithPath: FileManager.default.homeDirectoryForCurrentUser.path),
      environment: ProcessInfo.processInfo.environment)
    for target in installer.targets(agents: agents.agent) {
      print("\(target.name)\(target.detected ? " (found)" : "")")
      print("  skill  \(try installer.skillState(target).rawValue)  \(target.skill.path)")
    }
  }
}

private struct SkillInstall: ParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "install", abstract: "Install Setu's skill into detected or selected agents.",
    discussion: """
      With no --agent, install into detected agents. If neither is found, install only the shared skill in \
      ~/.agents/skills. Existing settings are preserved. A skill Setu did not install is left alone unless \
      --force is passed. Running setup again safely updates an earlier installation.
      """)
  @OptionGroup var agents: AgentOptions
  @Flag(help: "Report changes without writing files.") var dryRun = false
  @Flag(help: "Replace an existing skill named setu that Setu did not install.") var force = false

  func run() throws(any Error) {
    let installer = SkillInstaller(
      home: URL(fileURLWithPath: FileManager.default.homeDirectoryForCurrentUser.path),
      environment: ProcessInfo.processInfo.environment)
    let targets = installer.setupTargets(agents: agents.agent)
    let changes = try installer.setup(targets, force: force, dryRun: dryRun)
    for change in changes { print(change) }
    if !dryRun { print("Restart your agent to load the skill.") }
  }
}

private struct SkillRemove: ParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "remove", abstract: "Remove only skills installed by Setu.")
  @OptionGroup var agents: AgentOptions
  @Flag(help: "Report changes without writing files.") var dryRun = false

  func run() throws(any Error) {
    let installer = SkillInstaller(
      home: URL(fileURLWithPath: FileManager.default.homeDirectoryForCurrentUser.path),
      environment: ProcessInfo.processInfo.environment)
    for change in try installer.remove(installer.targets(agents: agents.agent), dryRun: dryRun) {
      print(change)
    }
  }
}
