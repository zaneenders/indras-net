import Foundation
import PackagePlugin

@main struct GitBuildPlugin: BuildToolPlugin {
  func createBuildCommands(context: PluginContext, target: Target) throws -> [Command] {
    let outputDirectory = context.pluginWorkDirectoryURL.appendingPathComponent("Generated")
    let outputFile = outputDirectory.appendingPathComponent("GitVersion.swift")
    let script = context.package.directoryURL.appendingPathComponent("Scripts/embed-git-version.swift")
    // Prebuild commands cannot use executables built from source.
    let swift = try context.tool(named: "swift")
    #if os(macOS)
    // SwiftPM's prebuild environment omits SDKROOT; xcrun selects the macOS SDK.
    let executable = URL(fileURLWithPath: "/usr/bin/xcrun")
    let arguments = [swift.url.path]
    #else
    let executable = swift.url
    let arguments: [String] = []
    #endif

    // Check every build: Git's index does not change for unstaged edits or untracked files.
    return [
      .prebuildCommand(
        displayName: "Embedding git version hash and working tree state",
        executable: executable,
        arguments: arguments + [script.path, context.package.directoryURL.path, outputFile.path],
        outputFilesDirectory: outputDirectory
      )
    ]
  }
}
