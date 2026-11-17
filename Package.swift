// swift-tools-version: 6.4
import PackageDescription

let sharedSwiftSettings: [SwiftSetting] = [
  .strictMemorySafety(),
  .treatAllWarnings(as: .error),
  .enableExperimentalFeature("Lifetimes"),
  .enableUpcomingFeature("MemberImportVisibility"),
  .enableUpcomingFeature("InternalImportsByDefault"),
  .enableUpcomingFeature("ExistentialAny"),
  .enableUpcomingFeature("ApproachableConcurrency"),
  .treatWarning("ExplicitSendable", as: .error)
]

let networkingSwiftSettings = sharedSwiftSettings + [.treatWarning("EmbeddedRestrictions", as: .error)]

let package = Package(
  name: "IndrasNet",
  platforms: [.macOS(.v27)],
  products: [
    .library(name: "Networking", targets: ["Networking"]),
    .library(name: "TCPActorSystem", targets: ["TCPActorSystem"]),
    .library(name: "IndraNode", targets: ["IndraNode"]),
    .executable(name: "indra", targets: ["IndraCLI"])
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.105.0"),
    .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.6.0"),
    .package(url: "https://github.com/swift-server/swift-service-lifecycle", from: "2.13.0"),
    .package(url: "https://github.com/apple/swift-system.git", from: "1.5.0"),
    .package(url: "https://github.com/apple/swift-cluster-membership.git", from: "0.4.0")
  ],
  targets: [
    .target(
      name: "IndraNode",
      dependencies: [
        "Networking", "TCPActorSystem",
        .product(name: "SWIM", package: "swift-cluster-membership"),
        .product(name: "ClusterMembership", package: "swift-cluster-membership"),
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "SystemPackage", package: "swift-system"),
        .product(name: "UnixSignals", package: "swift-service-lifecycle")
      ],
      swiftSettings: sharedSwiftSettings
    ),
    .executableTarget(
      name: "IndraCLI",
      dependencies: [
        "IndraNode", "TCPActorSystem",
        .product(name: "ArgumentParser", package: "swift-argument-parser"),
        .product(name: "UnixSignals", package: "swift-service-lifecycle")
      ],
      swiftSettings: sharedSwiftSettings
    ),
    .testTarget(
      name: "IndraNodeTests",
      dependencies: ["IndraNode", "TCPActorSystem", "Networking", .product(name: "NIOCore", package: "swift-nio")],
      swiftSettings: sharedSwiftSettings
    ),
    .target(
      name: "Networking",
      dependencies: [
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio")
      ],
      swiftSettings: networkingSwiftSettings
    ),
    .target(
      name: "TCPActorSystem",
      dependencies: [
        "Networking",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOConcurrencyHelpers", package: "swift-nio")
      ],
      swiftSettings: sharedSwiftSettings
    ),
    .testTarget(
      name: "TCPActorSystemTests",
      dependencies: ["TCPActorSystem", "Networking", .product(name: "NIOCore", package: "swift-nio")],
      swiftSettings: sharedSwiftSettings
    ),
    .testTarget(
      name: "NetworkingTests",
      dependencies: [
        "Networking",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOEmbedded", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio")
      ],
      swiftSettings: networkingSwiftSettings
    )
  ]
)
