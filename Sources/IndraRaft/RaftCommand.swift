package import ArgumentParser
import Foundation
package import IndraNode

package struct RaftOptions: ParsableArguments {
  @Option(help: "Existing absolute directory for the local Raft replica.") var raftDirectory: String?
  @Option(help: "Discover a Raft group with 'auto', or specify its bootstrap node UUID.") var raftJoin: String?

  package init() {}

  package func installer() throws -> ServiceInstaller {
    if let raftDirectory, !raftDirectory.hasPrefix("/") {
      throw ValidationError("Specify an absolute --raft-directory")
    }
    let mode: RaftStartMode
    if let raftJoin {
      guard raftDirectory != nil else { throw ValidationError("Specify --raft-directory with --raft-join") }
      if raftJoin == "auto" {
        mode = .discover
      } else if let id = UUID(uuidString: raftJoin) {
        mode = .join(id.uuidString)
      } else {
        throw ValidationError("Specify 'auto' or a bootstrap node UUID with --raft-join")
      }
    } else {
      mode = .bootstrap
    }
    return RaftService.installer(directory: raftDirectory, mode: mode)
  }
}

package struct RaftCommand: AsyncParsableCommand {
  package static let configuration = CommandConfiguration(
    commandName: "raft", abstract: "Read and write a Raft replica.",
    subcommands: [Status.self, Get.self, Put.self, Delete.self])

  package init() {}
}

private struct Control: ParsableArguments {
  @Option(help: "Private daemon control socket (or INDRA_SOCKET).") var socket: String?
  @Option(help: "Name or UUID of the Indra node hosting Raft.") var peer: String?

  func request(_ request: RaftRequest) async throws -> RaftReply {
    let socket = try await NodePaths.socket(override: socket)
    if case .status = request, peer == nil {
      return try await NodeControl.command(
        RaftService.key.name, input: request, returning: RaftReply.self, socket: socket,
        timeout: NodeControl.commandTimeout)
    }
    return try await NodeControl.command(
      RaftForward.commandName, input: RaftForward(peer: peer, group: nil, request: request), returning: RaftReply.self,
      socket: socket, timeout: NodeControl.commandTimeout)
  }
}

private struct WriteOptions: ParsableArguments {
  @OptionGroup var control: Control
  @Option(name: .customLong("request-id"), help: "UUID to reuse when retrying this write.") var requestID: String?

  func id() throws -> UUID {
    guard let requestID else { return UUID() }
    guard let id = UUID(uuidString: requestID) else { throw ValidationError("Specify a UUID with --request-id") }
    return id
  }

  func submit(_ request: RaftRequest) async throws {
    guard case .applied(let result) = try await control.request(request) else {
      throw IndraError("Unexpected daemon response")
    }
    let removed = result.removed.map { " removed=\($0)" } ?? ""
    print("index=\(result.index) request_id=\(result.requestID)\(removed)")
  }
}

private struct Status: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Show the local Raft role or a replica’s progress.")
  @OptionGroup var control: Control

  func run() async throws {
    switch try await control.request(.status) {
    case .client(let node):
      print("node=\(node) role=client")
    case .status(let status):
      print(
        "node=\(status.node) group=\(status.group) role=\(status.role) leader=\(status.leader ?? "-") voters=\(status.voters) readers=\(status.readers) term=\(status.term) commit=\(status.commitIndex) applied=\(status.appliedIndex) snapshot=\(status.snapshotIndex)"
      )
    default:
      throw IndraError("Unexpected daemon response")
    }
  }
}

private struct Get: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Read an applied value.")
  @OptionGroup var control: Control
  @Flag(help: "Print the value as JSON.") var json = false
  @Argument var key: String

  func run() async throws {
    guard case .value(let value) = try await control.request(.get(key: key)) else {
      throw IndraError("Unexpected daemon response")
    }
    guard let value else { throw IndraError("Key not found") }
    print(try value.formatted(asJSON: json))
  }
}

private struct Put: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Commit and apply a value.")
  @OptionGroup var options: WriteOptions
  @Flag(help: "Store a JSON object as a dictionary.") var json = false
  @Argument var key: String
  @Argument var value: String

  func run() async throws {
    let stored: RaftValue
    if json {
      do { stored = try RaftValue(documentJSON: value) }
      catch { throw ValidationError("Specify a valid JSON object with --json") }
    } else {
      stored = .string(value)
    }
    try await options.submit(.put(key: key, value: stored, requestID: options.id()))
  }
}

private struct Delete: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Commit and apply a key removal.")
  @OptionGroup var options: WriteOptions
  @Argument var key: String

  func run() async throws {
    try await options.submit(.delete(key: key, requestID: options.id()))
  }
}
