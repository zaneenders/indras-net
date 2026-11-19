package import ArgumentParser
import Foundation
package import IndraNode
import TCPActorSystem
import _NIOFileSystem

package struct SetuOptions: ParsableArguments {
  @Option(help: "Absolute directory for files shared by this node.") var setuDirectory: String?

  package init() {}

  package func installer() throws -> ServiceInstaller? {
    guard let setuDirectory else { return nil }
    guard setuDirectory.hasPrefix("/") else { throw ValidationError("Specify an absolute --setu-directory") }
    return SetuService.installer(directory: setuDirectory)
  }
}

package struct SetuCommand: AsyncParsableCommand {
  package static let configuration = CommandConfiguration(
    commandName: "setu", abstract: "Transfer files between Indra nodes.",
    subcommands: [List.self, Send.self, Get.self, Jobs.self, Wait.self, Cancel.self, Receipt.self])
  package init() {}
}

private struct Control: ParsableArguments {
  @Option(help: "Local daemon control socket.") var socket = NodePaths.socket
  @Flag(help: "Print JSON.") var json = false

  func request(_ command: FileCommand) async throws -> FileReply {
    try await NodeControl.command(
      SetuService.key.name, input: command, returning: FileReply.self,
      socket: socket, timeout: NodeControl.commandTimeout)
  }

  func display(_ reply: FileReply) throws {
    switch reply {
    case .files(let files):
      if json { try printJSON(files) } else { for file in files { print("\(file.size)  \(file.name)") } }
    case .job(let job):
      if json { try printJSON(job) } else { printJob(job) }
    case .jobs(let jobs):
      if json { try printJSON(jobs) } else { for job in jobs { printJob(job) } }
    case .receipt(let receipt):
      guard let receipt else { throw ValidationError("No recent commit receipt for this transfer") }
      if json { try printJSON(receipt) } else { print("\(receipt.id)  \(receipt.byteCount) bytes  \(receipt.sha256)") }
    }
  }

  private func printJob(_ job: FileJob) {
    print("\(job.id)  \(job.state.rawValue)  \(job.peer)  \(job.file)")
    if let id = job.transferID, job.receipt == nil { print("Transfer \(id)") }
    if let receipt = job.receipt { print("\(receipt.id)  \(receipt.byteCount) bytes  \(receipt.sha256)") }
    if let error = job.error { print(error) }
  }

  private func printJSON(_ value: some Encodable) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    print(String(decoding: try encoder.encode(value), as: UTF8.self))
  }

  func monitor(_ command: FileCommand, wait: Bool) async throws {
    guard case .job(var job) = try await request(command) else { throw ValidationError("Expected a transfer job") }
    while wait && job.state == .running {
      try await Task.sleep(for: .milliseconds(250))
      guard case .job(let update) = try await request(.job(job.id)) else {
        throw ValidationError("Expected a transfer job")
      }
      job = update
    }
    try display(.job(job))
    if job.state == .failed || job.state == .cancelled || job.state == .commitUnconfirmed { throw ExitCode.failure }
  }
}

private struct List: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "List shared files by name.")
  @OptionGroup var control: Control
  @Option(help: "Peer name or UUID. Omit for the local node.") var peer: String?
  func run() async throws { try await control.display(control.request(.list(peer: peer))) }
}

private struct Send: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Send a file to a peer's shared directory.")
  @OptionGroup var control: Control
  @Argument var file: String
  @Option var peer: String
  @Flag(inversion: .prefixedNo) var wait = true
  func run() async throws {
    try await control.monitor(.send(path: absolutePath(FilePath(file)).string, peer: peer), wait: wait)
  }
}

private struct Get: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Fetch a shared file by name.")
  @OptionGroup var control: Control
  @Argument var name: String
  @Option var peer: String
  @Option(help: "Local output path. Defaults to the local shared directory.") var output: String?
  @Flag(inversion: .prefixedNo) var wait = true
  func run() async throws {
    let destination: String? = if let output { try await absolutePath(FilePath(output)).string } else { nil }
    try await control.monitor(.get(name: name, peer: peer, output: destination), wait: wait)
  }
}

private struct Jobs: AsyncParsableCommand {
  @OptionGroup var control: Control
  func run() async throws { try await control.display(control.request(.jobs)) }
}

private struct Wait: AsyncParsableCommand {
  @OptionGroup var control: Control
  @Argument var job: String
  func run() async throws {
    guard let id = UUID(uuidString: job) else { throw ValidationError("Expected a job UUID") }
    try await control.monitor(.job(id), wait: true)
  }
}

private struct Cancel: AsyncParsableCommand {
  @OptionGroup var control: Control
  @Argument var job: String
  func run() async throws {
    guard let id = UUID(uuidString: job) else { throw ValidationError("Expected a job UUID") }
    try await control.display(control.request(.cancel(id)))
  }
}

private struct Receipt: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Look up a recent commit on the receiving node.")
  @OptionGroup var control: Control
  @Argument var transfer: String
  @Option var peer: String?
  func run() async throws {
    guard let id = UUID(uuidString: transfer) else { throw ValidationError("Expected a transfer UUID") }
    try await control.display(control.request(.receipt(id: id, peer: peer)))
  }
}
