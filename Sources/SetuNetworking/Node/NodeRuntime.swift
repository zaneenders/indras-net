import Foundation
import Setu

actor NodeRuntime {
  private struct Work: Sendable {
    var job: NodeJob
    let task: Task<Void, Never>
    let progress: TransferProgressTracker
    let peer: PeerTarget
    var recoveryClient: TransferClient?
  }
  let server: TransferServer
  let directory: String
  private var peers: [PeerTarget: TransferClient] = [:]
  private var work: [String: Work] = [:]
  private let recoveries: DownloadRecoveries
  private var stopping = false

  init(server: TransferServer, directory: String, downloadRecoveryBytes: Int64) {
    self.server = server
    self.directory = directory
    recoveries = DownloadRecoveries(budgetBytes: downloadRecoveryBytes)
  }

  func handle(_ request: NodeRequest) async throws(any Error) -> NodeReply {
    guard !stopping else {
      throw TransferError("Node is stopping")
    }
    switch request {
    case .status:
      return .status(node: server.node.uuidString, port: server.port, directory: directory)
    case .list(let peer):
      if let peer {
        do { return .files(try await client(peer).list()) } catch {
          peers.removeValue(forKey: peer)
          throw error
        }
      }
      return .files(try await server.catalog.list())
    case .send(let peer, let file):
      return try send(peer: peer, file: file, destination: nil, createDestination: false)
    case .sendTo(let peer, let file, let destination):
      return try send(peer: peer, file: file, destination: destination, createDestination: false)
    case .sendToCreatingDirectory(let peer, let file, let destination):
      return try send(peer: peer, file: file, destination: destination, createDestination: true)
    case .get(let peer, let fileID, let output):
      let directory = self.directory
      return .job(
        try submit(peer: peer, operation: "get \(fileID)", destination: output, connected: nil) { client, progress in
          await progress.begin(.hashing, totalBytes: nil, completedBytes: 0)
          let manifest = try await client.request(fileID: fileID)
          return try await client.download(manifest, to: output ?? directory + "/" + manifest.name, progress: progress)
        })
    case .finish(let id, let output):
      guard let original = work[id], let recovery = original.job.recovery,
        let retained = original.recoveryClient, let destination = output ?? original.job.destination
      else { throw TransferError("Job has no recoverable download; specify --output if needed") }
      let next = try submit(
        peer: original.peer, operation: "finish \(id)", destination: destination, connected: retained
      ) { client, progress in
        try await client.finishDownload(recoveryID: recovery.id, to: destination, progress: progress)
      }
      work[id]?.job.recovery = nil
      work[id]?.recoveryClient = nil
      return .job(next)
    case .jobs:
      var jobs: [NodeJob] = []
      for id in Array(work.keys) {
        if let job = await snapshot(id) { jobs.append(job) }
      }
      return .jobs(jobs.sorted { $0.createdAt < $1.createdAt })
    case .job(let id):
      guard let job = await snapshot(id) else {
        throw TransferError("Unknown job")
      }
      return .job(job)
    case .cancel(let id):
      guard let item = work[id] else {
        throw TransferError("Unknown job")
      }
      item.task.cancel()
      if let recovery = item.job.recovery, let client = item.recoveryClient {
        try await client.discardRecovery(recovery.id)
        work[id]?.job.recovery = nil
        work[id]?.recoveryClient = nil
        work[id]?.job.state = .cancelled
      }
      guard let job = await snapshot(id) else { throw TransferError("Unknown job") }
      return .job(job)
    }
  }

  private func send(
    peer: PeerTarget, file: String, destination: String?, createDestination: Bool
  ) throws(TransferError) -> NodeReply {
    .job(
      try submit(peer: peer, operation: "send \(file)", destination: destination, connected: nil) { client, progress in
        try await client.upload(
          file: file, destination: destination, createDestination: createDestination, progress: progress)
      })
  }

  private func client(_ peer: PeerTarget) async throws(any Error) -> TransferClient {
    if let client = peers[peer] {
      return client
    }
    guard peers.count < 32 else {
      throw TransferError("Node is limited to 32 peer connections")
    }
    let client = try await TransferClient.connect(
      host: peer.host, port: peer.port,
      connections: peer.connections, chunkSize: peer.chunkSize, temporary: try await server.temporary.scope(),
      recoveries: recoveries)
    guard !stopping else {
      await client.close()
      throw CancellationError()
    }
    if let existing = peers[peer] {
      await client.close()
      return existing
    }
    guard peers.count < 32 else {
      await client.close()
      throw TransferError("Node is limited to 32 peer connections")
    }
    peers[peer] = client
    return client
  }

  private func submit(
    peer: PeerTarget, operation: String, destination: String?, connected: TransferClient?,
    transfer: @escaping @Sendable (TransferClient, TransferProgressTracker) async throws(any Error) -> TransferResult
  ) throws(TransferError) -> NodeJob {
    if work.count >= 128,
      let oldest = work.values.filter({ $0.job.state != .running })
        .min(by: { $0.job.createdAt < $1.job.createdAt })
    {
      work.removeValue(forKey: oldest.job.id)
    }
    guard work.count < 128 else {
      throw TransferError("Node job limit reached")
    }
    guard work.values.filter({ $0.job.state == .running }).count < 8 else {
      throw TransferError("Node supports at most 8 active transfers")
    }
    let job = NodeJob(
      id: UUID().uuidString, operation: operation, createdAt: Date().timeIntervalSince1970,
      state: .running, result: nil, error: nil,
      progress: TransferProgress(phase: .connecting, completedBytes: 0, totalBytes: nil, bytesPerSecond: 0),
      peer: peer.host, destination: destination, recovery: nil)
    let progress = TransferProgressTracker(onUpdate: nil)
    let task = Task {
      var activeClient = connected
      do {
        let started = ContinuousClock.now
        try Task.checkCancellation()
        let client: TransferClient
        if let activeClient { client = activeClient } else { client = try await self.client(peer) }
        activeClient = client
        let result = try await transfer(client, progress)
        // Include peer connection and download preparation in the user's throughput.
        let total = TransferResult(
          bytesTransferred: result.bytesTransferred, resumedChunks: result.resumedChunks, started: started,
          receipt: result.receipt)
        finished(job.id, result: total, error: nil, cancelled: false, progress: await progress.snapshot())
      } catch {
        if let publication = error as? DownloadPublicationError {
          work[job.id]?.job.recovery = publication.recovery
          work[job.id]?.job.destination = publication.destination
          work[job.id]?.recoveryClient = activeClient
        } else {
          peers.removeValue(forKey: peer)
        }
        finished(
          job.id, result: nil, error: describeError(error), cancelled: Task.isCancelled,
          progress: await progress.snapshot())
      }
    }
    work[job.id] = Work(job: job, task: task, progress: progress, peer: peer, recoveryClient: nil)
    return job
  }

  private func snapshot(_ id: String) async -> NodeJob? {
    guard let item = work[id] else { return nil }
    if item.job.state != .running {
      if let recovery = item.job.recovery, let client = item.recoveryClient,
        !(await client.hasRecovery(recovery.id))
      {
        work[id]?.job.recovery = nil
        work[id]?.recoveryClient = nil
      }
      return work[id]?.job
    }
    let value = await item.progress.snapshot()
    guard var job = work[id]?.job else { return nil }
    // A job can finish while the actor awaits its tracker. Preserve its final snapshot.
    if job.state == .running { job.progress = value }
    return job
  }

  private func finished(
    _ id: String, result: TransferResult?, error: String?, cancelled: Bool, progress: TransferProgress
  ) {
    work[id]?.job.progress = progress
    work[id]?.job.result = result
    work[id]?.job.error = cancelled ? "Transfer cancelled" : error
    work[id]?.job.state = cancelled ? .cancelled : (error == nil ? .completed : .failed)
  }

  func close() async {
    stopping = true
    let tasks = work.values.map(\.task)
    for task in tasks { task.cancel() }
    for task in tasks { await task.value }
    for item in work.values {
      if let recovery = item.job.recovery, let client = item.recoveryClient {
        try? await client.discardRecovery(recovery.id)
      }
    }
    for client in peers.values { await client.close() }
    peers.removeAll()
  }
}
