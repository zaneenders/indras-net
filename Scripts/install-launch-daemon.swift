#!/usr/bin/env swift
import Foundation
import Synchronization

#if os(macOS)
import Darwin
#else
import Glibc
#endif

private let files = FileManager.default
private let label = "com.indras-net.indra"
private let sudo = "/usr/bin/sudo"
private let launchctl = "/bin/launchctl"
private let interrupted = Atomic<Bool>(false)

private struct InstallError: Error, CustomStringConvertible {
  let description: String
}

private struct LaunchDaemon: Decodable {
  let label: String
  let userName: String
  let programArguments: [String]

  enum CodingKeys: String, CodingKey {
    case label = "Label"
    case userName = "UserName"
    case programArguments = "ProgramArguments"
  }
}

private enum CommandMode {
  case required
  case probe
}

@discardableResult
private func run(_ executable: String, _ arguments: [String], mode: CommandMode) throws -> Bool {
  let process = Process()
  process.executableURL = URL(filePath: executable, directoryHint: .notDirectory, relativeTo: nil)
  process.arguments = arguments
  if mode == .probe {
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
  }
  try process.run()
  process.waitUntilExit()
  let status = process.terminationStatus
  let succeeded = process.terminationReason == .exit && status == 0
  if mode == .required, !succeeded {
    let result = process.terminationReason == .exit ? "exit status \(status)" : "signal \(status)"
    throw InstallError(description: "\(([executable] + arguments).joined(separator: " ")) failed (\(result)).")
  }
  return succeeded
}

private func report(_ message: String) {
  try? FileHandle.standardError.write(contentsOf: Data("\(message)\n".utf8))
}

private func checkInterruption() throws {
  if interrupted.load(ordering: .relaxed) {
    throw InstallError(description: "Installation interrupted.")
  }
}

private func temporaryDirectory(in parent: URL, prefix: String) throws -> URL {
  let directory = parent.appending(path: "\(prefix).\(UUID())", directoryHint: .isDirectory)
  try files.createDirectory(at: directory, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
  return directory
}

private func validate(_ data: Data, account: String) throws {
  let plist = try PropertyListDecoder().decode(LaunchDaemon.self, from: data)
  guard plist.label == label, plist.userName == account, plist.programArguments.count >= 2,
    plist.programArguments[0].hasPrefix("/"), files.isExecutableFile(atPath: plist.programArguments[0]),
    plist.programArguments[1] == "daemon"
  else {
    throw InstallError(
      description: """
        Expected \(label) with UserName set to \(account) \
        and an absolute Indra executable followed by daemon.
        """)
  }
}

private func install(from path: String, userID: uid_t) throws {
  let account = ProcessInfo.processInfo.userName
  let home = ProcessInfo.processInfo.environment["HOME"] ?? files.homeDirectoryForCurrentUser.path
  let input = URL(filePath: path, directoryHint: .notDirectory, relativeTo: nil)
  let state = input.deletingLastPathComponent().resolvingSymlinksInPath()
  let source = state.appending(path: input.lastPathComponent, directoryHint: .notDirectory)
  let daemon = URL(filePath: "/Library/LaunchDaemons/\(label).plist", directoryHint: .notDirectory, relativeTo: nil)
  let agent = URL(
    filePath: "\(home)/Library/LaunchAgents/\(label).plist", directoryHint: .notDirectory, relativeTo: nil)
  let lock = state.appending(path: ".\(label).lock", directoryHint: .notDirectory)
  let work = try temporaryDirectory(in: files.temporaryDirectory, prefix: label)
  let previous = work.appending(path: "previous.plist", directoryHint: .notDirectory)
  let installArguments = ["/usr/bin/install", "-o", "root", "-g", "wheel", "-m", "644"]
  var locked = false
  var archivedAgent: URL?
  var stoppedAgentDomains: [String] = []
  var stoppedDaemon = false
  var attemptedDaemonInstall = false
  var savedDaemon = false
  var committed = false

  defer {
    var rollbackFailed = false
    func restore(_ operation: () throws -> Void) {
      do {
        try operation()
      } catch {
        rollbackFailed = true
        report("Rollback: \(error)")
      }
    }

    if !committed {
      if attemptedDaemonInstall {
        restore {
          if savedDaemon {
            try run(sudo, installArguments + [previous.path, daemon.path], mode: .required)
          } else {
            try run(sudo, ["/bin/rm", "-f", daemon.path], mode: .required)
          }
        }
      }
      if stoppedDaemon {
        restore { try run(sudo, [launchctl, "bootstrap", "system", daemon.path], mode: .required) }
      }
      if let archivedAgent {
        restore { try files.moveItem(at: archivedAgent, to: agent) }
      }
      if files.fileExists(atPath: agent.path) {
        for domain in stoppedAgentDomains {
          restore { try run(sudo, [launchctl, "bootstrap", domain, agent.path], mode: .required) }
        }
      }
    }
    if rollbackFailed {
      report("Rollback failed. Installation files are in \(work.path).")
    } else {
      try? files.removeItem(at: work)
    }
    if locked {
      try? files.removeItem(at: lock)
    }
  }

  guard try run("/usr/bin/shlock", ["-f", lock.path, "-p", String(getpid())], mode: .probe) else {
    throw InstallError(description: "Could not lock \(state.path)")
  }
  locked = true
  try checkInterruption()

  let candidate = work.appending(path: "\(label).plist", directoryHint: .notDirectory)
  let data = try Data(contentsOf: source, options: [])
  try validate(data, account: account)
  try data.write(to: candidate, options: [])

  var replaceDaemon = true
  if files.fileExists(atPath: daemon.path) {
    let existing = daemon.resolvingSymlinksInPath()
    let attributes = try files.attributesOfItem(atPath: existing.path)
    if attributes[.type] as? FileAttributeType == .typeRegular {
      let previousData = try Data(contentsOf: existing, options: [])
      try previousData.write(to: previous, options: [])
      savedDaemon = true
      let permissionsMatch =
        attributes[.ownerAccountName] as? String == "root"
        && attributes[.groupOwnerAccountName] as? String == "wheel"
        && attributes[.posixPermissions] as? Int == 0o644
      replaceDaemon = data != previousData || !permissionsMatch
    }
  }

  try checkInterruption()
  try run(sudo, ["-v"], mode: .required)
  try checkInterruption()
  if (try? files.attributesOfItem(atPath: agent.path)) != nil {
    var archive = state.appending(path: "\(label).agent.plist", directoryHint: .notDirectory)
    if (try? files.attributesOfItem(atPath: archive.path)) != nil {
      let backup = try temporaryDirectory(in: state, prefix: "agent-backup")
      archive = backup.appending(path: "\(label).plist", directoryHint: .notDirectory)
    }
    try files.moveItem(at: agent, to: archive)
    archivedAgent = archive
  }

  for domain in ["gui/\(userID)", "user/\(userID)"] {
    try checkInterruption()
    let service = "\(domain)/\(label)"
    if try run(launchctl, ["print", service], mode: .probe) {
      try run(sudo, [launchctl, "bootout", service], mode: .required)
      stoppedAgentDomains.append(domain)
    }
  }

  let service = "system/\(label)"
  try checkInterruption()
  if replaceDaemon {
    if try run(launchctl, ["print", service], mode: .probe) {
      try run(sudo, [launchctl, "bootout", service], mode: .required)
      stoppedDaemon = true
    }
    try checkInterruption()
    attemptedDaemonInstall = true
    try run(sudo, installArguments + [candidate.path, daemon.path], mode: .required)
  }
  try checkInterruption()
  if try !run(launchctl, ["print", service], mode: .probe) {
    try run(sudo, [launchctl, "bootstrap", "system", daemon.path], mode: .required)
  }
  committed = true
  print("Indra is registered with launchd at \(service).")
}

do {
  guard CommandLine.arguments.count == 2 else {
    throw InstallError(description: "Usage: \(CommandLine.arguments[0]) /path/to/\(label).plist")
  }
  let userID = geteuid()
  #if os(macOS)
  let isMacOS = true
  #else
  let isMacOS = false
  #endif
  guard isMacOS, userID != 0 else {
    throw InstallError(description: "Run this script on macOS from the account that runs Indra.")
  }

  for number in [SIGHUP, SIGINT, SIGTERM] {
    signal(number, { _ in interrupted.store(true, ordering: .relaxed) })
  }
  try install(from: CommandLine.arguments[1], userID: userID)
} catch {
  report(String(describing: error))
  exit(EXIT_FAILURE)
}
