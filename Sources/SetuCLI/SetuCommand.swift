import ArgumentParser
import Foundation
import Setu
import SetuNetworking

@main
struct SetuCommand: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "setu",
    abstract: "Transfer files through the local Setu node.",
    subcommands: [
      Status.self, List.self, Get.self, Send.self, Finish.self, Jobs.self, Job.self, Wait.self, Cancel.self,
      SetuNode.self,
      Restart.self, Peers.self, Setup.self, Agent.self
    ])

  static func main() async {
    let arguments = Array(CommandLine.arguments.dropFirst())
    do {
      var command = try await asyncParseAsRoot(arguments)
      // Concrete dispatch keeps the package's EmbeddedRestrictions checks enabled.
      switch command {
      case let command as Status: try await command.run()
      case let command as List: try await command.run()
      case let command as Send: try await command.run()
      case let command as Get: try await command.run()
      case let command as Finish: try await command.run()
      case let command as Jobs: try await command.run()
      case let command as Job: try await command.run()
      case let command as Wait: try await command.run()
      case let command as Cancel: try await command.run()
      case let command as Restart: try await command.run()
      case let command as Peers: try await command.run()
      case let command as SetuNode: try await command.run()
      default: try command.run()
      }
    } catch {
      let code = exitCode(for: error)
      if arguments.prefix(while: { $0 != "--" }).contains(where: { $0 == "--json" || $0 == "--json-stream" }),
        code != .success
      {
        // A failed transfer has already emitted its full job, including the error and progress.
        if !(error is ExitCode) {
          let failure = JSONOutput.Failure(
            code: code == .validationFailure ? "invalid_arguments" : "command_failed", message: message(for: error))
          try? JSONOutput(
            status: nil, files: nil, job: nil, jobs: nil, error: failure, restartRequested: nil, event: nil,
            transfer: nil, transfers: nil, peers: nil
          ).write()
        }
        exit(withError: code)
      }
      exit(withError: error)
    }
  }
}

private struct Restart: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Ask a peer to exit with status 1 so its supervisor can restart it.",
    discussion: """
      Sends directly to the peer port; no local node or SSH is required. The peer acknowledges,
      then immediately exits. Active transfers are interrupted; startup clears abandoned payloads.
      Requires a supervisor configured to restart the peer. This command does not wait for recovery.
      """)
  @Option(name: [.long, .customShort("H")], help: "Remote node IP address or DNS name.") var host: String
  @Option(help: "Remote node TCP port.") var port = 8443
  @OptionGroup var format: OutputOptions

  func validate() throws(any Error) {
    guard !host.isEmpty, (1...65535).contains(port) else { throw ValidationError("Specify a valid host and port") }
  }

  func run() async throws(any Error) {
    try await TransferClient.requestRestart(host: host, port: port)
    if format.structured {
      try JSONOutput(
        status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: true, event: nil, transfer: nil,
        transfers: nil, peers: nil
      ).write()
    } else {
      print("Restart requested for \(host):\(port)")
    }
  }
}

private struct ControlOptions: ParsableArguments {
  @Option(help: "Local node's private Unix control socket.") var socket = NodePaths.socket

  func withNode(_ body: @Sendable (NodeControlClient) async throws(any Error) -> Void) async throws(any Error) {
    let client = try NodeControlClient(socket: URL(fileURLWithPath: socket).path)
    do {
      try await body(client)
      await client.close()
    } catch {
      await client.close()
      throw error
    }
  }
}

private struct PeerOptions: ParsableArguments {
  @Option(name: [.long, .customShort("H")], help: "Remote node IP address or DNS name; omit to list local files.")
  var host: String?
  @OptionGroup var transport: TransportOptions

  func target(required: Bool) throws(any Error) -> PeerTarget? {
    guard let host else {
      if required { throw ValidationError("Specify --host for the remote node") }
      return nil
    }
    return try transport.target(host: host)
  }
}

struct TransportOptions: ParsableArguments {
  @Option(help: "Remote node TCP port.") var port = 8443
  @Option(help: "Concurrent peer connections, 1 through 8.") var connections = 4
  // Peer chunks always use the transport's gzip coding.
  @Option(name: .customLong("chunk-size-mib"), help: "Chunk size in MiB, 1 through 64.") var chunkSizeMiB = 8

  func target(host: String) throws(any Error) -> PeerTarget {
    guard !host.isEmpty, (1...65535).contains(port), (1...8).contains(connections),
      (1...64).contains(chunkSizeMiB)
    else {
      throw ValidationError("Specify a valid host/port, 1–8 connections, and a 1–64 MiB chunk size")
    }
    return PeerTarget(
      host: host, port: port, connections: connections,
      chunkSize: chunkSizeMiB * 1_048_576)
  }
}

private struct Status: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Check the local node.")
  @OptionGroup var control: ControlOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    try await control.withNode { client in
      guard case .status(let node, let port, let directory) = try await client.request(.status) else {
        throw ValidationError("Unexpected node reply")
      }
      if format.structured {
        try JSONOutput(
          status: .init(node: node, port: port, directory: directory), files: nil, job: nil, jobs: nil, error: nil,
          restartRequested: nil, event: nil, transfer: nil, transfers: nil, peers: nil
        ).write()
      } else {
        print("Node \(node) · peer port \(port) · shared directory \(directory)")
      }
    }
  }
}

private struct List: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "List local files or ask the node to list a peer's files.")
  @OptionGroup var control: ControlOptions
  @OptionGroup var peer: PeerOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    let target = try peer.target(required: false)
    try await control.withNode { client in
      guard case .files(let files) = try await client.request(.list(target)) else {
        throw ValidationError("Unexpected node reply")
      }
      if format.structured {
        try JSONOutput(
          status: nil, files: files, job: nil, jobs: nil, error: nil, restartRequested: nil, event: nil, transfer: nil,
          transfers: nil, peers: nil
        ).write()
      } else {
        for file in files { print("\(file.id)  \(file.size) bytes  \(file.name)") }
      }
    }
  }
}

private struct Send: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Upload a file to one or more peers.")
  @Argument var file: String
  @Option(name: [.customLong("host"), .customShort("H")], help: "Remote node; repeat to send to several computers.")
  var hosts: [String] = []
  @Option(
    name: [.long, .customShort("D")], help: "Absolute directory on each receiver; defaults to its shared directory.")
  var destination: String?
  @Flag(
    name: [.long, .customShort("C")],
    help: "Create the destination directory and missing parents; requires --destination.")
  var create = false
  @Option(help: "Maximum simultaneous recipient transfers, 1 through 8.") var parallel = 3
  @Flag(inversion: .prefixedNo, help: "Wait for completion; the node keeps running if the CLI exits.") var wait = true
  @OptionGroup var control: ControlOptions
  @OptionGroup var transport: TransportOptions
  @OptionGroup var format: OutputOptions

  func validate() throws(any Error) {
    guard !hosts.isEmpty, Set(hosts).count == hosts.count, (1...8).contains(parallel) else {
      throw ValidationError("Specify distinct --host recipients and --parallel from 1 through 8")
    }
    if let destination, !validDestinationPath(destination) {
      throw ValidationError("--destination must be an absolute directory on the receiver")
    }
    if create, destination == nil { throw ValidationError("--create requires --destination") }
    if !wait, hosts.count > parallel {
      throw ValidationError("For --no-wait, increase --parallel to cover all recipients (at most 8)")
    }
    for host in hosts { _ = try transport.target(host: host) }
  }

  func run() async throws(any Error) {
    let targets = try hosts.map { try transport.target(host: $0) }
    let file = URL(fileURLWithPath: file).path
    try await control.withNode { client in
      guard targets.count == 1 else {
        try await sendBatch(
          file: file, targets: targets, destination: destination, createDestination: create,
          parallel: parallel, wait: wait, request: { try await client.request($0) }, format: format)
        return
      }
      guard
        case .job(let job) = try await client.request(
          .upload(peer: targets[0], file: file, destination: destination, createDestination: create))
      else {
        throw ValidationError("Unexpected node reply")
      }
      try await monitor(job, client: client, wait: wait, format: format)
    }
  }
}

private struct Get: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Ask the node to download a peer's file.")
  @Argument(help: "File ID returned by list.") var fileID: String
  @Option(help: "Destination; defaults to the node's shared directory.") var output: String?
  @Flag(inversion: .prefixedNo, help: "Wait for completion; the node keeps running if the CLI exits.") var wait = true
  @OptionGroup var control: ControlOptions
  @OptionGroup var peer: PeerOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    guard let target = try peer.target(required: true) else {
      return
    }
    let output = output.map { URL(fileURLWithPath: $0).path }
    try await control.withNode { client in
      guard case .job(let job) = try await client.request(.get(peer: target, fileID: fileID, output: output)) else {
        throw ValidationError("Unexpected node reply")
      }
      try await monitor(job, client: client, wait: wait, format: format)
    }
  }
}

private struct Finish: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Place a retained, verified download without transferring it again.")
  @Argument var id: String
  @Option(help: "New local destination; defaults to the original output path.") var output: String?
  @Flag(inversion: .prefixedNo, help: "Wait for publication to finish.") var wait = true
  @OptionGroup var control: ControlOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    let output = output.map { URL(fileURLWithPath: $0).path }
    try await control.withNode { client in
      guard case .job(let job) = try await client.request(.finish(id: id, output: output)) else {
        throw ValidationError("Unexpected node reply")
      }
      try await monitor(job, client: client, wait: wait, format: format)
    }
  }
}

private struct Jobs: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Show the node's recent transfer jobs.")
  @OptionGroup var control: ControlOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    try await control.withNode { client in
      guard case .jobs(let jobs) = try await client.request(.jobs) else {
        throw ValidationError("Unexpected node reply")
      }
      if format.structured {
        try JSONOutput(
          status: nil, files: nil, job: nil, jobs: jobs, error: nil, restartRequested: nil, event: nil, transfer: nil,
          transfers: nil, peers: nil
        ).write()
      } else {
        for job in jobs { printJob(job) }
      }
    }
  }
}

private struct Job: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Inspect one transfer job without waiting.")
  @Argument var id: String
  @OptionGroup var control: ControlOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    try await control.withNode { client in
      guard case .job(let job) = try await client.request(.job(id)) else {
        throw ValidationError("Unexpected node reply")
      }
      if format.structured {
        try JSONOutput(
          status: nil, files: nil, job: job, jobs: nil, error: nil, restartRequested: nil, event: nil, transfer: nil,
          transfers: nil, peers: nil
        ).write()
      } else {
        printJob(job)
      }
    }
  }
}

private struct Wait: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Wait for an existing transfer job.")
  @Argument var id: String
  @OptionGroup var control: ControlOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    try await control.withNode { client in
      guard case .job(let job) = try await client.request(.job(id)) else {
        throw ValidationError("Unexpected node reply")
      }
      try await monitor(job, client: client, wait: true, format: format)
    }
  }
}

private struct Cancel: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Cancel a node's transfer job.")
  @Argument var id: String
  @OptionGroup var control: ControlOptions
  @OptionGroup var format: OutputOptions

  func run() async throws(any Error) {
    try await control.withNode { client in
      guard case .job(let job) = try await client.request(.cancel(id)) else {
        throw ValidationError("Unexpected node reply")
      }
      if format.structured {
        try JSONOutput(
          status: nil, files: nil, job: job, jobs: nil, error: nil, restartRequested: nil, event: nil, transfer: nil,
          transfers: nil, peers: nil
        ).write()
      } else {
        print("Cancellation requested for \(id)")
      }
    }
  }
}

private func printJob(_ job: NodeJob) {
  print("\(job.id)  \(job.state.rawValue)  \(job.operation)" + (job.error.map { " · \($0)" } ?? ""))
  if job.state == .running, let progress = job.progress { print("  " + ProgressDisplay.line(progress)) }
}

private func monitor(
  _ submitted: NodeJob, client: NodeControlClient, wait: Bool, format: OutputOptions
) async throws(any Error) {
  if !format.structured { print("Job \(submitted.id)") }
  guard wait else {
    if format.structured {
      try JSONOutput(
        status: nil, files: nil, job: submitted, jobs: nil, error: nil, restartRequested: nil,
        event: format.jsonStream ? "submitted" : nil, transfer: nil, transfers: nil, peers: nil
      ).write()
    }
    return
  }
  var job = submitted
  var display = ProgressDisplay()
  var lastEvent = ContinuousClock.now - .seconds(1)
  var lastPhase: TransferProgress.Phase?
  defer { display.finish() }
  while job.state == .running {
    if !format.structured { display.render(job.progress) }
    if format.jsonStream, lastPhase != job.progress?.phase || lastEvent.duration(to: .now) >= .seconds(1) {
      try JSONOutput(
        status: nil, files: nil, job: job, jobs: nil, error: nil, restartRequested: nil, event: "progress",
        transfer: nil, transfers: nil, peers: nil
      ).write()
      lastPhase = job.progress?.phase
      lastEvent = .now
    }
    try await Task.sleep(for: .milliseconds(250))
    guard case .job(let update) = try await client.request(.job(job.id)) else {
      throw ValidationError("Unexpected node reply")
    }
    job = update
  }
  display.finish()
  if format.structured {
    try JSONOutput(
      status: nil, files: nil, job: job, jobs: nil, error: nil, restartRequested: nil,
      event: format.jsonStream ? job.state.rawValue : nil, transfer: nil, transfers: nil, peers: nil
    ).write()
    guard job.state == .completed else { throw ExitCode.failure }
    return
  }
  guard let result = job.result else {
    let recovery =
      job.recovery == nil ? "" : "\nRun 'setu finish \(job.id) --output /absolute/destination' within 15 minutes."
    throw ValidationError((job.error ?? "Transfer \(job.state.rawValue)") + recovery)
  }
  print(
    "Verified: \(formatDecimal(Double(result.bytesTransferred) / 1_048_576, places: 2)) MiB "
      + "in \(formatDecimal(result.elapsedSeconds, places: 3)) s · "
      + "\(formatDecimal(result.mebibytesPerSecond, places: 2)) MiB/s "
      + "(\(formatDecimal(result.megabitsPerSecond, places: 2)) Mbit/s) · \(result.resumedChunks) chunks reused")
  if let receipt = result.receipt {
    print("Destination: \(receipt.destination ?? "receiver shared directory") · SHA-256: \(receipt.sha256)")
  }
}
