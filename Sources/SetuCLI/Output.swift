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

struct OutputOptions: ParsableArguments {
  @Flag(help: "Print one JSON document (schema version 1); suppress live terminal progress.") var json = false
  @Flag(help: "Stream progress and final results as newline-delimited JSON.") var jsonStream = false
  var structured: Bool { json || jsonStream }

  func validate() throws(any Error) {
    if json && jsonStream { throw ValidationError("Choose either --json or --json-stream") }
  }
}

struct TransferOutcome: Encodable, Sendable {
  let host: String
  var job: NodeJob?
  var error: String?
}

// Keep the CLI schema independent of Swift's synthesized associated-value enum encoding.
struct JSONOutput: Encodable, Sendable {
  struct Status: Encodable, Sendable {
    let node: String
    let port: Int
    let directory: String
  }
  struct Failure: Encodable, Sendable {
    let code: String
    let message: String
  }

  let schemaVersion = 1
  var status: Status? = nil
  var files: [FileEntry]? = nil
  var job: NodeJob? = nil
  var jobs: [NodeJob]? = nil
  var error: Failure? = nil
  var restartRequested: Bool? = nil
  var event: String? = nil
  var transfer: TransferOutcome? = nil
  var transfers: [TransferOutcome]? = nil
  var peers: [DiscoveredPeer]? = nil

  func encoded() throws(any Error) -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    return String(decoding: try encoder.encode(self), as: UTF8.self)
  }

  func write() throws(any Error) {
    // Bypass stdio buffering so progress is visible immediately when piped to an agent.
    let line = try encoded() + "\n"
    try FileHandle.standardOutput.write(contentsOf: Data(line.utf8))
  }
}

struct ProgressDisplay {
  private let interactive = isatty(STDERR_FILENO) == 1
  private var lastPhase: TransferProgress.Phase?

  mutating func render(_ progress: TransferProgress?) {
    guard let progress else { return }
    if interactive {
      write("\r\u{1B}[2K" + Self.line(progress))
    } else if progress.phase != lastPhase {
      write(Self.line(progress) + "\n")
    }
    lastPhase = progress.phase
  }

  mutating func finish() {
    if interactive, lastPhase != nil { write("\r\u{1B}[2K") }
    lastPhase = nil
  }

  static func line(_ progress: TransferProgress) -> String {
    var text = progress.phase.rawValue.capitalized
    if let total = progress.totalBytes {
      let percentage = total == 0 ? 100 : min(100, Double(progress.completedBytes) * 100 / Double(total))
      text += ": \(formatDecimal(percentage, places: 1))% · "
      text += "\(formatDecimal(Double(progress.completedBytes) / 1_048_576, places: 2)) / "
      text += "\(formatDecimal(Double(total) / 1_048_576, places: 2)) MiB"
      if progress.phase != .completed {
        text += " · \(formatDecimal(progress.bytesPerSecond / 1_048_576, places: 2)) MiB/s"
      }
    }
    return text
  }

  private func write(_ text: String) {
    try? FileHandle.standardError.write(contentsOf: Data(text.utf8))
  }
}

func formatDecimal(_ value: Double, places: Int) -> String {
  value.formatted(
    .number.locale(Locale(identifier: "en_US_POSIX")).grouping(.never).precision(.fractionLength(places)))
}
