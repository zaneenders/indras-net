import ArgumentParser
import Foundation
import Testing

@testable import SetuCLI

struct SetupTests {
  @Test func skillInstallationUsesSetupCommands() throws(any Error) {
    for arguments in [
      ["setup", "skill", "--agent", "codex", "--dry-run"],
      ["setup", "skill", "install", "--agent", "claude"],
      ["setup", "skill", "--force"]
    ] {
      let command = try SetuCommand.parseAsRoot(arguments)
      #expect(type(of: command).configuration.commandName == "install")
    }
    for action in ["status", "remove"] {
      let command = try SetuCommand.parseAsRoot(["setup", "skill", action])
      #expect(type(of: command).configuration.commandName == action)
    }
    #expect(try SetuCommand.parseAsRoot(["agent"]) is Agent)
  }

  @Test func serviceGeneratorsRequireExplicitHostAndTemporaryDirectory() throws(any Error) {
    for command in ["print-systemd", "print-launchd"] {
      for flags in [[], ["--host", "100.64.0.1"], ["--temporary-directory", "/tmp/setu"]] {
        #expect(throws: (any Error).self) {
          try SetuCommand.parseAsRoot(["setup", command] + flags)
        }
      }
    }
  }

  @Test func serviceGeneratorsRejectInvalidValues() throws(any Error) {
    for command in ["print-systemd", "print-launchd"] {
      for flags in [
        ["--host", ""], ["--host", "127.0.0.1\n[Service]"], ["--port", "0"], ["--port", "65536"],
        ["--executable", "setu"], ["--directory", "~/shared"],
        ["--socket", "control.sock"], ["--temporary-directory", "relative"],
        ["--directory", "/tmp/shared\nExecStart=/bin/false"]
      ] {
        var configured = serviceFlags
        for index in stride(from: 0, to: flags.count, by: 2) {
          let existing = try #require(configured.firstIndex(of: flags[index]))
          configured[existing + 1] = flags[index + 1]
        }
        #expect(throws: (any Error).self) {
          try SetuCommand.parseAsRoot(["setup", command] + configured)
        }
      }
    }
    #expect(throws: (any Error).self) {
      try SetuCommand.parseAsRoot(["setup", "print-launchd"] + serviceFlags + ["--log-directory", "logs"])
    }
  }

  @Test func generatorsPreserveExplicitNodeConfiguration() throws(any Error) {
    let systemd = try SetuCommand.parseAsRoot(["setup", "print-systemd"] + serviceFlags)
    #expect(try #require(systemd as? PrintSystemd).service.arguments == expectedArguments)
    let launchd = try SetuCommand.parseAsRoot(["setup", "print-launchd"] + serviceFlags)
    #expect(try #require(launchd as? PrintLaunchd).service.arguments == expectedArguments)
  }

  @Test func systemdRejectsUnsupportedExecutablePathsWhileLaunchdPreservesThem() throws(any Error) {
    for executable in ["/opt/Setu 'App'/setu", "/opt/Setu \"App\"/setu", "/opt/Setu\\App/setu"] {
      var configured = serviceFlags
      configured[1] = executable
      #expect(throws: (any Error).self) {
        try SetuCommand.parseAsRoot(["setup", "print-systemd"] + configured)
      }
      let parsed = try SetuCommand.parseAsRoot(["setup", "print-launchd"] + configured)
      let command = try #require(parsed as? PrintLaunchd)
      #expect(command.service.arguments.first == executable)
    }
  }

  @Test func systemdEscapesLiteralPathsAndExpansionCharacters() {
    let unit = ServiceDefinition.systemd(arguments: [
      "/opt/Setu App/$HOME %h/setu", "/tmp/100% $HOME ${USER} \\\"quoted\\\""
    ])
    let exec = unit.split(separator: "\n").first { $0.hasPrefix("ExecStart=") }
    #expect(exec == #"ExecStart="/opt/Setu App/$HOME %%h/setu" "/tmp/100%% $$HOME $${USER} \\\"quoted\\\"""#)
  }

  @Test func launchdPlistRoundTripsArgumentsAndPrivateServiceSettings() throws(any Error) {
    let arguments = expectedArguments + ["/tmp/a & b <c> \"quoted\" $HOME %h \\path"]
    let xml = try ServiceDefinition.launchd(arguments: arguments, logDirectory: "/Users/Test User/Logs & Output")
    // The optional output-format pointer is nil; no pointer is read or written by the caller.
    let decoded = unsafe try PropertyListSerialization.propertyList(from: Data(xml.utf8), options: [], format: nil)
    let plist = try #require(decoded as? [String: Any])
    #expect(plist["ProgramArguments"] as? [String] == arguments)
    #expect(plist["Label"] as? String == "com.zaneenders.setu")
    #expect(plist["RunAtLoad"] as? Bool == true)
    #expect(plist["KeepAlive"] as? Bool == true)
    #expect(plist["Umask"] as? Int == 0o077)
    #expect(plist["ThrottleInterval"] as? Int == 10)
    #expect(plist["StandardOutPath"] as? String == "/Users/Test User/Logs & Output/stdout.log")
    #expect(plist["StandardErrorPath"] as? String == "/Users/Test User/Logs & Output/stderr.log")
  }

  private var serviceFlags: [String] {
    [
      "--executable", "/opt/Setu App/setu", "--host", "100.64.0.1", "--port", "9000",
      "--directory", "/srv/shared files",
      "--socket", "/run/setu.sock", "--temporary-directory", "/srv/setu temporary"
    ]
  }

  private var expectedArguments: [String] {
    [
      "/opt/Setu App/setu", "node", "--host", "100.64.0.1", "--port", "9000",
      "--directory", "/srv/shared files",
      "--socket", "/run/setu.sock", "--temporary-directory", "/srv/setu temporary"
    ]
  }
}
