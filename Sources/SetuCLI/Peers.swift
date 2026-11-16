import ArgumentParser
import Foundation
import Setu
import SetuNetworking
import Subprocess

#if canImport(System)
import System
#else
import SystemPackage
#endif

struct DiscoveredPeer: Encodable, Sendable {
  let host: String
  var operatingSystem: String?
  var online: Bool?
  var available = false
  var node: String?
  var error: String?
}

struct TailnetStatus: Decodable {
  struct Peer: Decodable {
    let hostName: String
    let dnsName: String
    let operatingSystem: String
    let online: Bool

    enum CodingKeys: String, CodingKey {
      case hostName = "HostName"
      case dnsName = "DNSName"
      case operatingSystem = "OS"
      case online = "Online"
    }
  }
  let peers: [String: Peer]?
  enum CodingKeys: String, CodingKey { case peers = "Peer" }

  var candidates: [DiscoveredPeer] {
    (peers ?? [:]).values.map { peer in
      let dns = peer.dnsName.hasSuffix(".") ? String(peer.dnsName.dropLast()) : peer.dnsName
      let host = dns.isEmpty ? peer.hostName : dns
      return DiscoveredPeer(host: host, operatingSystem: peer.operatingSystem, online: peer.online)
    }.sorted { $0.host < $1.host }
  }
}

struct Peers: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Find Setu peers using explicit hosts or the local Tailscale device list.")
  @Flag(help: "Read candidate devices from the signed-in local Tailscale client.") var tailscale = false
  @Option(name: [.customLong("host"), .customShort("H")], help: "Check a specific peer; repeat for several peers.")
  var hosts: [String] = []
  @Option(help: "Absolute Tailscale CLI path; otherwise searches PATH and the macOS app bundle.")
  var tailscaleExecutable: String?
  @Option(help: "Peer TCP port.") var port = 8443
  @OptionGroup var format: OutputOptions

  func validate() throws(any Error) {
    guard tailscale || !hosts.isEmpty, (1...65535).contains(port), hosts.allSatisfy({ !$0.isEmpty }) else {
      throw ValidationError("Specify --tailscale or at least one --host, and a valid port")
    }
    if let tailscaleExecutable, !tailscaleExecutable.hasPrefix("/") {
      throw ValidationError("--tailscale-executable must be an absolute path")
    }
  }

  func run() async throws(any Error) {
    var candidates = tailscale ? try await readTailnet().candidates : []
    for host in hosts where !candidates.contains(where: { $0.host == host }) {
      candidates.append(DiscoveredPeer(host: host))
    }
    var results: [DiscoveredPeer] = []
    // Probe in small batches so large tailnets do not open unbounded connections.
    for start in stride(from: 0, to: candidates.count, by: 8) {
      let batch = Array(candidates[start..<min(start + 8, candidates.count)])
      let inspected = await withTaskGroup(of: DiscoveredPeer.self, returning: [DiscoveredPeer].self) { group in
        for candidate in batch {
          group.addTask {
            var result = candidate
            guard candidate.online != false else { return result }
            do {
              let peer = try await TransferClient.inspect(host: candidate.host, port: port)
              result.available = true
              result.node = peer.node
            } catch { result.error = SetuCommand.message(for: error) }
            return result
          }
        }
        var results: [DiscoveredPeer] = []
        for await result in group { results.append(result) }
        return results
      }
      results += inspected
    }
    results.sort { $0.host < $1.host }
    if format.structured {
      try JSONOutput(
        status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil, event: nil, transfer: nil,
        transfers: nil, peers: results
      ).write()
    } else {
      for peer in results {
        let state = peer.available ? "ready" : (peer.online == false ? "offline" : "unavailable")
        print("\(peer.host)  \(state)")
      }
    }
  }

  private func readTailnet() async throws(any Error) -> TailnetStatus {
    let paths = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":").map {
      String($0) + "/tailscale"
    }
    let candidates =
      tailscaleExecutable.map { [$0] } ?? (paths + ["/Applications/Tailscale.app/Contents/MacOS/Tailscale"])
    guard let executable = candidates.first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else {
      throw ValidationError("Tailscale CLI not found; use --tailscale-executable /absolute/path")
    }
    let result = try await Subprocess.run(
      .path(.init(executable)), arguments: ["status", "--json"],
      output: .data(limit: 16_777_216), error: .string(limit: 65_536))
    guard result.terminationStatus.isSuccess else {
      let message =
        result.standardError.isEmpty ? String(decoding: result.standardOutput, as: UTF8.self) : result.standardError
      throw ValidationError("Tailscale status failed: " + message)
    }
    return try JSONDecoder().decode(TailnetStatus.self, from: result.standardOutput)
  }
}
