// swift-tools-version: 6.3

import PackageDescription

let package = Package(
  name: "indras-net",
  platforms: [
    .macOS(.v26)
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", branch: "2.100.0"),
    .package(url: "https://github.com/swiftlang/swift-subprocess.git", branch: "0.5"),
  ],
  targets: [
    .executableTarget(
      name: "indras-net",
      dependencies: ["IndrasNet"]
    ),
    .target(
      name: "IndrasNet",
      dependencies: [
        .product(name: "NIO", package: "swift-nio")
      ]
    ),
    .testTarget(
      name: "IndrasNetTests",
      dependencies: [
        "IndrasNet",
        .product(name: "NIOEmbedded", package: "swift-nio"),
      ]
    ),
    .testTarget(
      name: "IndrasNetIntegrationTests",
      dependencies: [
        "IndrasNet",
        .product(name: "Subprocess", package: "swift-subprocess"),
      ]
    ),
  ],
  swiftLanguageModes: [.v6]
)
