// swift-tools-version: 6.3

import PackageDescription

let package = Package(
  name: "indras-net",
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", branch: "2.100.0")
  ],
  targets: [
    .target(
      name: "IndrasNet",
      dependencies: [
        .product(name: "NIO", package: "swift-nio")
      ]
    ),
    .testTarget(
      name: "IndrasNetTests",
      dependencies: ["IndrasNet"]
    ),
  ],
  swiftLanguageModes: [.v6]
)
