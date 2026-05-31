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
      dependencies: ["IndrasNet"],
      swiftSettings: [
        .treatAllWarnings(as: .error)
      ]
    ),
    .target(
      name: "IndrasNet",
      dependencies: [
        .product(name: "NIO", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
      ],
      swiftSettings: [
        .treatAllWarnings(as: .error)
      ]
    ),
    .testTarget(
      name: "IndrasNetTests",
      dependencies: [
        "IndrasNet",
        .product(name: "NIOEmbedded", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
      ]
    ),
    .testTarget(
      name: "IndrasNetE2ETests",
      dependencies: [
        "IndrasNet",
        .product(name: "Subprocess", package: "swift-subprocess"),
      ]
    ),
  ],
  swiftLanguageModes: [.v6]
)
