#!/usr/bin/env swift
import Foundation

#if os(Linux)
import Glibc
#else
import Darwin
#endif

let root = URL(filePath: #filePath, directoryHint: .notDirectory, relativeTo: nil)
  .resolvingSymlinksInPath().deletingLastPathComponent().deletingLastPathComponent()
let scratch = root.appending(path: ".build/coverage", directoryHint: .isDirectory)

@discardableResult
func run(_ arguments: [String], captureOutput: Bool) throws -> Data {
  let process = Process()
  process.executableURL = URL(filePath: "/usr/bin/env", directoryHint: .notDirectory, relativeTo: nil)
  process.arguments = arguments
  process.currentDirectoryURL = root
  let pipe = captureOutput ? Pipe() : nil
  process.standardOutput = pipe ?? FileHandle.standardOutput
  process.standardError = FileHandle.standardError
  try process.run()
  let output = try pipe?.fileHandleForReading.readToEnd() ?? Data()
  process.waitUntilExit()
  let status = process.terminationStatus
  guard status == 0 else {
    exit(process.terminationReason == .exit ? status : 128 + status)
  }
  return output
}

struct PackageManifest: Decodable {
  struct Target: Decodable {
    let name: String
    let type: String
  }

  let name: String
  let targets: [Target]
}

func testBinary(named name: String, in products: URL) -> String? {
  #if os(macOS)
  let paths = ["\(name).xctest/Contents/MacOS/\(name)"]
  #else
  let paths = ["\(name).so", "\(name).xctest"]
  #endif
  return paths.map { products.appending(path: $0, directoryHint: .notDirectory).path }
    .first { FileManager.default.fileExists(atPath: $0) }
}

func fail(_ message: String) -> Never {
  try? FileHandle.standardError.write(contentsOf: Data("test-coverage: \(message)\n".utf8))
  exit(EXIT_FAILURE)
}

do {
  let test = ["swift", "test", "--scratch-path", scratch.path]
  try run(test + ["--enable-code-coverage"], captureOutput: false)
  let reportPath = try String(decoding: run(test + ["--show-codecov-path"], captureOutput: true), as: UTF8.self)
    .trimmingCharacters(in: .newlines)
  let coverage = URL(filePath: reportPath, directoryHint: .notDirectory, relativeTo: nil).deletingLastPathComponent()
  let products = coverage.deletingLastPathComponent()

  let manifest = try JSONDecoder().decode(
    PackageManifest.self, from: run(["swift", "package", "dump-package"], captureOutput: true))
  var binaries = manifest.targets.filter { $0.type == "test" }.map(\.name).sorted()
    .compactMap { testBinary(named: $0, in: products) }
  if binaries.isEmpty, let binary = testBinary(named: "\(manifest.name)PackageTests", in: products) {
    binaries = [binary]
  }
  guard !binaries.isEmpty else { fail("No test binaries found in \(products.path)") }

  let indra = products.appending(path: "indra", directoryHint: .notDirectory)
  guard FileManager.default.isExecutableFile(atPath: indra.path) else { fail("Missing executable at \(indra.path)") }
  let profile = coverage.appending(path: "default.profdata", directoryHint: .notDirectory)
  let sources = root.appending(path: "Sources", directoryHint: .isDirectory)
  #if os(macOS)
  let llvmCov = ["xcrun", "llvm-cov"]
  #else
  let llvmCov = ["llvm-cov"]
  #endif
  try run(
    llvmCov + ["report", indra.path, "--instr-profile=\(profile.path)"]
      + binaries.flatMap { ["--object", $0] } + ["--sources", sources.path],
    captureOutput: false)
} catch {
  fail(String(describing: error))
}
