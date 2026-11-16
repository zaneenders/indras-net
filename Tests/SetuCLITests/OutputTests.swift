import ArgumentParser
import Foundation
import SetuNetworking
import Testing

@testable import Setu
@testable import SetuCLI

struct OutputTests {
  @Test func queryAndTransferCommandsAcceptJSON() throws(any Error) {
    for arguments in [
      ["status"], ["list"], ["jobs"], ["job", "123"], ["wait", "123"], ["cancel", "123"],
      ["restart", "--host", "localhost"],
      ["peers", "--host", "tower", "--host", "pi"],
      ["finish", "123", "--output", "/tmp/finished"],
      ["send", "/tmp/source", "--host", "localhost", "--no-wait"],
      ["get", String(repeating: "a", count: 64), "--host", "localhost"]
    ] {
      _ = try SetuCommand.parseAsRoot(arguments + ["--json"])
      if arguments.contains("--host") {
        _ = try SetuCommand.parseAsRoot(arguments.map { $0 == "--host" ? "-H" : $0 } + ["--json"])
      }
    }
  }

  @Test func batchAndStreamingOptionsAreValidated() throws(any Error) {
    _ = try SetuCommand.parseAsRoot([
      "send", "/tmp/source", "--host", "tower", "--host", "pi",
      "--destination", "/Users/Test User/Downloads", "--create", "--json-stream"
    ])
    _ = try SetuCommand.parseAsRoot([
      "send", "/tmp/source", "-H", "tower", "--host", "pi",
      "-D", "/Users/Test User/Downloads", "-C", "--json-stream"
    ])
    for arguments in [
      ["send", "/tmp/source", "--host", "tower", "--host", "tower"],
      ["send", "/tmp/source", "--host", "tower", "--parallel", "9"],
      ["send", "/tmp/source", "--host", "tower", "--create"],
      ["send", "/tmp/source", "-H", "tower", "-C"],
      ["send", "/tmp/source", "--host", "tower", "--host", "pi", "--parallel", "1", "--no-wait"],
      ["status", "--json", "--json-stream"]
    ] {
      #expect(throws: (any Error).self) { try SetuCommand.parseAsRoot(arguments) }
    }
  }

  @Test func destinationPathsReplaceReceiverAliases() throws(any Error) {
    for arguments in [
      ["send", "/tmp/source", "--host", "tower", "--destination", "downloads"],
      ["send", "/tmp/source", "--host", "tower", "--destination", "downloads=/tmp/downloads"],
      ["send", "/tmp/source", "--host", "tower", "--destination", "relative/path"],
      ["send", "/tmp/source", "--host", "tower", "--destination", "/tmp/a\nb"],
      ["node", "--temporary-directory", "/tmp/private", "--destination", "downloads=/tmp/downloads"],
      [
        "setup", "print-launchd", "--host", "localhost", "--temporary-directory", "/tmp/private",
        "--destination", "downloads=/tmp/downloads"
      ],
      [
        "setup", "print-systemd", "--host", "localhost", "--temporary-directory", "/tmp/private",
        "--destination", "downloads=/tmp/downloads"
      ]
    ] {
      #expect(throws: (any Error).self) {
        try SetuCommand.parseAsRoot(arguments)
      }
    }
  }

  @Test func oldControlMessagesAndResultsRemainDecodable() throws(any Error) {
    let request = Data(
      #"{"send":{"peer":{"host":"localhost","port":8443,"connections":4,"chunkSize":8388608},"file":"/tmp/source"}}"#
        .utf8)
    guard case .send(_, let file) = try JSONDecoder().decode(NodeRequest.self, from: request) else {
      Issue.record("Expected an older send request to decode")
      return
    }
    #expect(file == "/tmp/source")
    let targeted = try NodeRequest.upload(
      peer: PeerTarget(host: "localhost", port: 8443, connections: 4, chunkSize: defaultChunkSize), file: file,
      destination: "/Users/Test User/Downloads", createDestination: false)
    let encoded = String(decoding: try JSONEncoder().encode(targeted), as: UTF8.self)
    #expect(encoded.contains("sendTo") && !encoded.contains("\"send\""))
    let creating = try NodeRequest.upload(
      peer: PeerTarget(host: "localhost", port: 8443, connections: 4, chunkSize: defaultChunkSize), file: file,
      destination: "/Users/Test User/Downloads", createDestination: true)
    let creatingJSON = String(decoding: try JSONEncoder().encode(creating), as: UTF8.self)
    #expect(creatingJSON.contains("sendToCreatingDirectory") && !creatingJSON.contains("\"sendTo\""))
    #expect(throws: (any Error).self) {
      try NodeRequest.upload(
        peer: PeerTarget(host: "localhost", port: 8443, connections: 4, chunkSize: defaultChunkSize), file: file,
        destination: nil, createDestination: true)
    }
    let result = try JSONDecoder().decode(
      TransferResult.self, from: Data(#"{"bytesTransferred":1,"elapsedSeconds":1,"resumedChunks":0}"#.utf8))
    #expect(result.receipt == nil && result.bytesTransferred == 1)
  }

  @Test func jsonHasAnExplicitSchemaAndIncludesJobFailuresAndProgress() throws(any Error) {
    let progress = try JSONDecoder().decode(
      TransferProgress.self,
      from: Data(#"{"phase":"transferring","completedBytes":1024,"totalBytes":2048,"bytesPerSecond":512}"#.utf8))
    let job = NodeJob(
      id: "id", operation: "send /tmp/source", createdAt: 1, state: .failed,
      result: nil, error: "Disk is full", progress: progress, peer: nil, destination: nil, recovery: nil)
    let encoded = try JSONOutput(
      status: nil, files: nil, job: job, jobs: nil, error: nil, restartRequested: nil, event: nil, transfer: nil,
      transfers: nil, peers: nil
    ).encoded()
    let decoded = try JSONDecoder().decode(Envelope.self, from: Data(encoded.utf8))
    #expect(decoded.schemaVersion == 1)
    #expect(decoded.job?.id == "id")
    #expect(decoded.job?.error == "Disk is full")
    #expect(decoded.job?.progress == progress)
    #expect(!encoded.contains("_0"))
    let error = try JSONOutput(
      status: nil, files: nil, job: nil, jobs: nil, error: .init(code: "invalid_arguments", message: "Missing --host"),
      restartRequested: nil, event: nil, transfer: nil, transfers: nil, peers: nil
    ).encoded()
    #expect(error == #"{"error":{"code":"invalid_arguments","message":"Missing --host"},"schemaVersion":1}"#)
    #expect(
      try JSONOutput(
        status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: true, event: nil, transfer: nil,
        transfers: nil, peers: nil
      ).encoded() == #"{"restartRequested":true,"schemaVersion":1}"#)
  }

  @Test func servicePrintersPreserveUploadLimits() throws(any Error) {
    for name in ["print-systemd", "print-launchd"] {
      let arguments = [
        "setup", name, "--host", "localhost", "--temporary-directory", "/tmp/private",
        "--max-active-uploads", "8", "--upload-idle-timeout-seconds", "60"
      ]
      let parsed = try SetuCommand.parseAsRoot(arguments)
      let options = (parsed as? PrintSystemd)?.service ?? (parsed as? PrintLaunchd)?.service
      let shortParsed = try SetuCommand.parseAsRoot(arguments.map { $0 == "--host" ? "-H" : $0 })
      let shortOptions = (shortParsed as? PrintSystemd)?.service ?? (shortParsed as? PrintLaunchd)?.service
      #expect(try #require(shortOptions).arguments == #require(options).arguments)
      #expect(
        try #require(options).arguments.suffix(4) == [
          "--max-active-uploads", "8", "--upload-idle-timeout-seconds", "60"
        ])
      for flag in ["--max-active-uploads", "--upload-idle-timeout-seconds"] {
        #expect(throws: (any Error).self) { try SetuCommand.parseAsRoot(arguments + [flag, "0"]) }
      }
    }
  }

  @Test func recoveryBudgetIsValidatedAndPreservedInServiceDefinitions() throws(any Error) {
    for prefix in [["node"], ["setup", "print-systemd"], ["setup", "print-launchd"]] {
      let arguments = prefix + ["--host", "localhost", "--temporary-directory", "/tmp/private"]
      for value in ["0", "2048"] {
        let parsed = try SetuCommand.parseAsRoot(arguments + ["--download-recovery-mib", value])
        if let node = parsed as? SetuNode {
          #expect(node.recovery.bytes == Int64(value)! * 1_048_576)
        } else {
          let options = (parsed as? PrintSystemd)?.service ?? (parsed as? PrintLaunchd)?.service
          #expect(try #require(options).arguments.suffix(2) == ["--download-recovery-mib", value])
        }
      }
      for value in ["-1", String(Int64.max)] {
        #expect(throws: (any Error).self) {
          try SetuCommand.parseAsRoot(arguments + ["--download-recovery-mib", value])
        }
      }
    }
  }

  @Test func progressFormattingHandlesEmptyAndIndeterminatePhases() throws(any Error) {
    for (json, expected) in [
      (#"{"phase":"hashing","completedBytes":0,"totalBytes":0,"bytesPerSecond":0}"#, "Hashing: 100.0%"),
      (#"{"phase":"verifying","completedBytes":0,"bytesPerSecond":0}"#, "Verifying")
    ] {
      let progress = try JSONDecoder().decode(TransferProgress.self, from: Data(json.utf8))
      #expect(ProgressDisplay.line(progress).hasPrefix(expected))
    }
  }

  private struct Envelope: Decodable {
    let schemaVersion: Int
    let job: NodeJob?
  }
}
