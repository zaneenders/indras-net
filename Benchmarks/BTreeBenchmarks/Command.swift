import ArgumentParser
import BTree
import Foundation
import NIOCore
import ProfileRecorder

@main
struct BTreeBenchmark: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "btree-bench",
    abstract: "Measure and profile the disk B+ tree.",
    subcommands: [Measure.self, Profile.self, Stress.self],
    defaultSubcommand: Measure.self)
}

extension Workload: ExpressibleByArgument {}

struct BenchmarkOptions: ParsableArguments {
  @Option(help: "Number of records.") var entries = 2_000
  @Option(help: "Number of partitions.") var partitions = 16
  @Option(help: "Maximum records requested by a scan.") var scanLimit = 32
  @Option(help: "Existing directory in which to create temporary stores.") var directory = "."

  func validate() throws {
    guard partitions > 0, entries >= partitions else {
      throw ValidationError("Use at least one partition and at least one record per partition.")
    }
    guard scanLimit > 0 else { throw ValidationError("Use a positive scan limit.") }
    guard !directory.isEmpty else { throw ValidationError("Provide a directory path.") }
  }
}

private struct Measure: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Report timings for each workload.",
    discussion: "Each write workload commits one transaction. Each scan operation reads one cursor page.")
  @OptionGroup var options: BenchmarkOptions
  @Option(help: "Measure one workload.") var workload: Workload?
  @Option(help: "Number of timed rounds after warm-up.") var rounds = 7

  func validate() throws {
    guard rounds > 0 else { throw ValidationError("Use a positive round count.") }
  }

  func run() async throws {
    let benchmark = Benchmark(options: options)
    let workloads = workload.map { [$0] } ?? Workload.allCases
    let latencyFormat = FloatingPointFormatStyle<Double>.number
      .locale(Locale(identifier: "en_US_POSIX")).grouping(.never).precision(.fractionLength(1))
    print(
      "entries=\(options.entries) partitions=\(options.partitions) scan_limit=\(options.scanLimit) "
        + "directory=\(options.directory) rounds=\(rounds)")
    print("workload\tops_per_round\tmedian_ns/op\tmin_ns/op")
    for workload in workloads {
      _ = try await measure(workload, benchmark: benchmark)
      var latencies: [Double] = []
      var operations = 0
      for _ in 0..<rounds {
        let sample = try await measure(workload, benchmark: benchmark)
        operations = sample.operations
        latencies.append(sample.latency)
      }
      latencies.sort()
      let median = (latencies[(rounds - 1) / 2] + latencies[rounds / 2]) / 2
      let medianText = median.formatted(latencyFormat)
      let minimumText = latencies[0].formatted(latencyFormat)
      print("\(workload.rawValue)\t\(operations)\t\(medianText)\t\(minimumText)")
    }
  }

  private func measure(_ workload: Workload, benchmark: Benchmark) async throws -> (operations: Int, latency: Double) {
    try await benchmark.withPreparedStore(for: workload) { tree in
      let start = ContinuousClock.now
      let result = try await benchmark.run(workload, in: tree)
      let elapsed = start.duration(to: .now)
      try await benchmark.check(result, for: workload, in: tree)
      return (result.operations, elapsed / .nanoseconds(1) / Double(result.operations))
    }
  }
}

private struct Profile: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Capture CPU samples from a repeated disk workload.")
  @OptionGroup var options: BenchmarkOptions
  @Argument(help: "Workload to profile.") var workload: Workload
  @Option(help: "Path for a new raw .swipr file.") var output: String
  @Option(help: "Number of samples, 10 ms apart.") var samples = 200

  func validate() throws {
    guard samples > 0, samples <= Int(Int32.max) else {
      throw ValidationError("Use a sample count between 1 and \(Int32.max).")
    }
    guard !output.isEmpty, output != "-" else { throw ValidationError("Provide an output file path.") }
  }

  func run() async throws {
    let benchmark = Benchmark(options: options)
    let workload = workload
    let output = output
    let samples = samples
    let runWorkload: @Sendable (Benchmark.Tree) async throws -> Void = { tree in
      let result = try await benchmark.run(workload, in: tree)
      try await benchmark.check(result, for: workload, in: tree)
    }

    try await benchmark.withPreparedStore(for: workload) { tree in
      try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
          do {
            try await runWorkload(tree)
            while !Task.isCancelled {
              if workload.changesTree {
                try await benchmark.withPreparedStore(for: workload, execute: runWorkload)
              } else {
                try await runWorkload(tree)
              }
            }
          } catch is CancellationError where Task.isCancelled {
          } catch DiskBTreeError.cancelled where Task.isCancelled {}
        }
        group.addTask {
          try await ProfileRecorderSampler.sharedInstance.requestSamples(
            outputFilePath: output, failIfFileExists: true,
            count: samples, timeBetweenSamples: .milliseconds(10), mode: .cpuTime)
        }
        _ = try await group.next()
        group.cancelAll()
        try await group.waitForAll()
      }
    }
    print("Wrote the \(workload.rawValue) CPU profile to \(output).")
  }
}
