import ArgumentParser
import Foundation
import Setu
import SetuNetworking

func sendBatch(
  file: String, targets: [PeerTarget], destination: String?, createDestination: Bool, parallel: Int, wait: Bool,
  request: @Sendable (NodeRequest) async throws(any Error) -> NodeReply, format: OutputOptions
) async throws(any Error) {
  var outcomes = targets.map { TransferOutcome(host: $0.host, job: nil, error: nil) }
  var active: [Int: NodeJob] = [:]
  var next = 0
  var controlFailed = false
  var lastEvent = ContinuousClock.now - .seconds(5)
  while !controlFailed && (next < targets.count || !active.isEmpty) {
    while next < targets.count, active.count < parallel {
      let index = next
      next += 1
      do {
        guard
          case .job(let job) = try await request(
            .upload(peer: targets[index], file: file, destination: destination, createDestination: createDestination))
        else { throw ValidationError("Unexpected node reply") }
        outcomes[index].job = job
        if wait, job.state == .running { active[index] = job }
        if format.jsonStream {
          try JSONOutput(
            status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil, event: "submitted",
            transfer: outcomes[index], transfers: nil, peers: nil
          ).write()
        } else if !format.structured {
          print("\(targets[index].host): job \(job.id)")
        }
      } catch {
        try Task.checkCancellation()
        outcomes[index].error = SetuCommand.message(for: error)
        // A lost submission reply may still have started a node-owned transfer.
        controlFailed = true
        if format.jsonStream {
          try JSONOutput(
            status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil, event: "submission_failed",
            transfer: outcomes[index], transfers: nil, peers: nil
          ).write()
        }
        break
      }
    }
    if controlFailed { break }
    let report = lastEvent.duration(to: .now) >= .seconds(format.jsonStream ? 1 : 5)
    for index in active.keys.sorted() {
      let id = active[index]!.id
      let job: NodeJob
      do {
        job = try await pollBatchJob(id, request: request)
      } catch {
        try Task.checkCancellation()
        outcomes[index].error =
          "Status unavailable: \(SetuCommand.message(for: error)). Use 'setu wait \(id)' to check the transfer."
        // Keep every running job's slot: losing contact does not stop its transfer.
        controlFailed = true
        if format.jsonStream {
          try JSONOutput(
            status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil, event: "monitoring_failed",
            transfer: outcomes[index], transfers: nil, peers: nil
          ).write()
        }
        break
      }
      outcomes[index].job = job
      if job.state != .running {
        active.removeValue(forKey: index)
        if format.jsonStream {
          try JSONOutput(
            status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil, event: job.state.rawValue,
            transfer: outcomes[index], transfers: nil, peers: nil
          ).write()
        } else if !format.structured {
          print("\(targets[index].host): \(job.state.rawValue)" + (job.error.map { " · \($0)" } ?? ""))
        }
      } else if report {
        if format.jsonStream {
          try JSONOutput(
            status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil, event: "progress",
            transfer: outcomes[index], transfers: nil, peers: nil
          ).write()
        } else if !format.structured, let progress = job.progress {
          print("\(targets[index].host): \(ProgressDisplay.line(progress))")
        }
      }
    }
    if report { lastEvent = .now }
    if !controlFailed && !active.isEmpty { try await Task.sleep(for: .milliseconds(250)) }
  }
  if controlFailed {
    for index in outcomes.indices where outcomes[index].error == nil {
      if let job = outcomes[index].job, job.state == .running {
        outcomes[index].error = "Monitoring stopped; use 'setu wait \(job.id)' to check the transfer."
      } else if outcomes[index].job == nil {
        outcomes[index].error = "Not submitted because contact with the local node was lost."
      }
    }
  }
  if format.structured {
    try JSONOutput(
      status: nil, files: nil, job: nil, jobs: nil, error: nil, restartRequested: nil,
      event: format.jsonStream ? "finished" : nil, transfer: nil, transfers: outcomes, peers: nil
    ).write()
  } else {
    for outcome in outcomes {
      if let error = outcome.error { print("\(outcome.host): failed · \(error)") }
      if let receipt = outcome.job?.result?.receipt {
        print("\(outcome.host): \(receipt.destination ?? "receiver shared directory") · SHA-256: \(receipt.sha256)")
      }
    }
  }
  if outcomes.contains(where: { $0.error != nil || (wait && $0.job?.state != .completed) }) {
    throw ExitCode.failure
  }
}

private func pollBatchJob(
  _ id: String, request: @Sendable (NodeRequest) async throws(any Error) -> NodeReply
) async throws(any Error) -> NodeJob {
  // Reading status is safe to retry; submissions must never be replayed after a lost reply.
  for attempt in 0..<3 {
    try Task.checkCancellation()
    do {
      guard case .job(let job) = try await request(.job(id)) else {
        throw ValidationError("Unexpected node reply")
      }
      return job
    } catch {
      try Task.checkCancellation()
      guard attempt < 2 else { throw error }
      try await Task.sleep(for: .milliseconds(250 * (1 << attempt)))
    }
  }
  throw ValidationError("Job status retry limit reached")
}
