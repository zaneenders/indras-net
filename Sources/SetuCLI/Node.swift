import ArgumentParser
import Foundation
import Setu
import SetuNetworking

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

struct SetuNode: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "node", abstract: "Run a foreground Setu node; suitable for systemd or launchd.")

  @Option(name: [.long, .customShort("H")], help: "TCP listen address; bind to an interface reachable by peers.")
  var host = "0.0.0.0"
  @Option(help: "TCP port; 0 chooses an available port.") var port = 8443
  @Option(help: "Private directory for shared files and incoming uploads.") var directory = NodePaths.state + "/shared"
  @Option(help: "Private Unix control socket for the CLI.") var socket = NodePaths.socket
  @Option(help: "Private temporary directory; abandoned Setu payloads are cleared on startup.")
  var temporaryDirectory: String
  @OptionGroup var uploads: UploadOptions
  @OptionGroup var recovery: RecoveryOptions

  func validate() throws(any Error) {
    guard !host.isEmpty, (0...65535).contains(port) else {
      throw ValidationError("Specify a nonempty host and a port from 0 through 65535")
    }
    guard temporaryDirectory.hasPrefix("/") else {
      throw ValidationError("Specify an absolute path for --temporary-directory")
    }
  }

  func run() async throws(any Error) {
    let socket = URL(fileURLWithPath: socket).path
    let signals = ShutdownSignals()
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        try await runNode(
          host: host, port: port, directory: directory, socket: socket,
          temporaryDirectory: temporaryDirectory, maxActiveUploads: uploads.maxActiveUploads,
          uploadIdleTimeout: .seconds(uploads.uploadIdleTimeoutSeconds),
          downloadRecoveryBytes: recovery.bytes,
          onRestart: { _exit(EXIT_FAILURE) })
      }
      group.addTask { await signals.wait() }
      defer { group.cancelAll() }
      _ = try await group.next()
    }
  }
}
