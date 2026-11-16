import ArgumentParser
import Foundation
import Setu
import SetuNetworking

struct Setup: ParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Install agent skills and print node service definitions.",
    discussion: "Run 'setu agent' for installation and service startup instructions.",
    subcommands: [Skill.self, PrintSystemd.self, PrintLaunchd.self])
}

struct ServiceOptions: ParsableArguments {
  @Option(help: "Absolute installed Setu executable path; defaults to this executable.")
  var executable = Bundle.main.executableURL?.resolvingSymlinksInPath().path ?? CommandLine.arguments[0]
  @Option(name: [.long, .customShort("H")], help: "Node listen address; bind to an interface reachable by peers.")
  var host: String
  @Option(help: "TCP port, 1 through 65535.") var port = 8443
  @Option(help: "Absolute private shared-file directory.") var directory = NodePaths.state + "/shared"
  @Option(help: "Absolute private Unix control socket path.") var socket = NodePaths.socket
  @Option(help: "Absolute private temporary directory; abandoned Setu payloads are cleared on startup.")
  var temporaryDirectory: String
  @OptionGroup var uploads: UploadOptions
  @OptionGroup var recovery: RecoveryOptions

  func validate() throws(any Error) {
    guard !host.isEmpty, !host.hasPrefix("-"), !host.contains(where: \.isWhitespace),
      !host.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }),
      (1...65535).contains(port)
    else {
      throw ValidationError("Specify a nonempty host and a port from 1 through 65535")
    }
    for (name, value) in [
      ("executable", executable), ("directory", directory), ("socket", socket),
      ("temporary-directory", temporaryDirectory)
    ] {
      try Self.validatePath(value, option: name)
    }
  }

  static func validatePath(_ value: String, option: String) throws(any Error) {
    guard value.hasPrefix("/"),
      !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) })
    else {
      throw ValidationError("Specify an absolute path without control characters for --\(option)")
    }
  }

  var arguments: [String] {
    [
      executable, "node", "--host", host, "--port", String(port),
      "--directory", directory, "--socket", socket, "--temporary-directory", temporaryDirectory
    ] + uploads.arguments + recovery.arguments
  }
}

struct PrintSystemd: ParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "print-systemd", abstract: "Print a systemd user service to stdout.",
    discussion: """
      Redirect stdout to ~/.config/systemd/user/setu.service after creating its parent directory. \
      Use --executable to select a stable installed binary if running from a build directory. \
      systemd executable paths cannot contain quotes or backslashes. \
      This command does not write files or start a service. Run 'setu agent' for startup instructions.
      """)
  @OptionGroup var service: ServiceOptions

  func validate() throws(any Error) {
    guard !service.executable.contains(where: { "\"'\\".contains($0) }) else {
      throw ValidationError("systemd requires an --executable path without quotes or backslashes")
    }
  }

  func run() { print(ServiceDefinition.systemd(arguments: service.arguments)) }
}

struct PrintLaunchd: ParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "print-launchd", abstract: "Print a macOS launchd user agent plist to stdout.",
    discussion: """
      Redirect stdout to ~/Library/LaunchAgents/com.zaneenders.setu.plist after creating its parent directory. \
      Create --log-directory before loading the agent. Use --executable to select a stable installed binary \
      if running from a build directory. This command does not write files or load an agent. \
      Run 'setu agent' for startup instructions.
      """)
  @OptionGroup var service: ServiceOptions
  @Option(help: "Absolute log directory; create it before loading the agent.")
  var logDirectory = FileManager.default.homeDirectoryForCurrentUser.path + "/Library/Logs/setu"

  func validate() throws(any Error) {
    try ServiceOptions.validatePath(logDirectory, option: "log-directory")
  }

  func run() throws(any Error) {
    print(try ServiceDefinition.launchd(arguments: service.arguments, logDirectory: logDirectory))
  }
}

enum ServiceDefinition {
  static func systemd(arguments: [String]) -> String {
    let command = arguments.enumerated().map {
      systemdArgument($0.element, isExecutable: $0.offset == 0)
    }.joined(separator: " ")
    return """
      [Unit]
      Description=Setu file transfer node

      [Service]
      Type=exec
      ExecStart=\(command)
      Restart=on-failure
      RestartSec=3
      UMask=0077
      TimeoutStopSec=30
      NoNewPrivileges=yes

      [Install]
      WantedBy=default.target
      """
  }

  private static func systemdArgument(_ value: String, isExecutable: Bool) -> String {
    // Specifiers expand in every word, but environment variables expand only in arguments.
    var escaped = value.replacingOccurrences(of: "\\", with: "\\\\")
      .replacingOccurrences(of: "\"", with: "\\\"")
      .replacingOccurrences(of: "%", with: "%%")
    if !isExecutable { escaped = escaped.replacingOccurrences(of: "$", with: "$$") }
    return "\"\(escaped)\""
  }

  static func launchd(arguments: [String], logDirectory: String) throws(any Error) -> String {
    let directory = URL(fileURLWithPath: logDirectory)
    let plist: [String: Any] = [
      "Label": "com.zaneenders.setu",
      "ProgramArguments": arguments,
      "RunAtLoad": true,
      "KeepAlive": true,
      "ThrottleInterval": 10,
      "Umask": 0o077,
      "StandardOutPath": directory.appendingPathComponent("stdout.log").path,
      "StandardErrorPath": directory.appendingPathComponent("stderr.log").path
    ]
    let data = try PropertyListSerialization.data(fromPropertyList: plist, format: .xml, options: 0)
    return String(decoding: data, as: UTF8.self)
  }
}
