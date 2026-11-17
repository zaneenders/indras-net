import Foundation

#if os(Linux)
import Glibc
#else
import Darwin
#endif

func fail(_ message: String) -> Never {
  try? FileHandle.standardError.write(contentsOf: Data("embed-git-version: \(message)\n".utf8))
  exit(EXIT_FAILURE)
}

func git(_ arguments: [String], in package: String) -> String? {
  let process = Process()
  let output = Pipe()
  process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
  process.arguments = ["git", "-C", package] + arguments
  process.standardOutput = output
  process.standardError = FileHandle.nullDevice
  do {
    try process.run()
  } catch {
    return nil
  }
  let data = output.fileHandleForReading.readDataToEndOfFile()
  process.waitUntilExit()
  guard process.terminationReason == .exit, process.terminationStatus == 0 else { return nil }
  return String(decoding: data, as: UTF8.self).trimmingCharacters(in: .newlines)
}

func validatedHash(_ value: String?) -> String {
  guard let value, !value.isEmpty,
    value.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) })
  else { return "unknown" }
  return value
}

guard CommandLine.arguments.count == 3 else {
  fail("Usage: swift embed-git-version.swift <package-directory> <output-file>")
}
let package = CommandLine.arguments[1]
let output = URL(fileURLWithPath: CommandLine.arguments[2])
let fullHash = validatedHash(git(["rev-parse", "HEAD"], in: package))
let isDirty: String
if fullHash != "unknown", let status = git(["status", "--porcelain", "--untracked-files=normal"], in: package) {
  isDirty = status.isEmpty ? "false" : "true"
} else {
  isDirty = "nil"
}
let source = """
  enum EmbeddedBuildIdentity {
    static let fullHash = "\(fullHash)"
    static let isDirty: Bool? = \(isDirty)
  }

  """
let data = Data(source.utf8)
do {
  try FileManager.default.createDirectory(
    at: output.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: nil)
  // Avoid recompiling when the build metadata has not changed.
  if (try? Data(contentsOf: output)) != data {
    try data.write(to: output, options: .atomic)
  }
} catch {
  fail(error.localizedDescription)
}
