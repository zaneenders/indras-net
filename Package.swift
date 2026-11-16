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
    .executable(name: "indras-counter", targets: ["IndrasCounter"]),
    .executable(name: "setu", targets: ["SetuCLI"])
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.105.0"),
    .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.6.0"),
    .package(url: "https://github.com/apple/swift-configuration.git", from: "1.5.0"),
    .package(url: "https://github.com/apple/swift-system.git", from: "1.8.1"),
    .package(url: "https://github.com/apple/swift-crypto.git", from: "5.0.0"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.12.1"),
    .package(url: "https://github.com/swiftlang/swift-subprocess.git", from: "1.0.2"),
    .package(url: "https://github.com/zaneenders/swift-zlib.git", revision: "ad3d04ec7b80c19beb959fa7cfbcaf2ee72efe0a")
  ],
  targets: [
    .target(
      name: "Setu",
      dependencies: [
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOFoundationEssentialsCompat", package: "swift-nio"),
        .product(name: "_NIOFileSystem", package: "swift-nio"),
        .product(name: "Crypto", package: "swift-crypto"),
        .product(name: "Logging", package: "swift-log"),
        .product(name: "Zlib", package: "swift-zlib")
      ],
      swiftSettings: networkingSwiftSettings
    ),
    .target(
      name: "SetuNetworking",
      dependencies: [
        "Setu", "Networking", .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
        .product(name: "_NIOFileSystem", package: "swift-nio"),
        .product(name: "Logging", package: "swift-log")
      ],
      swiftSettings: networkingSwiftSettings
    ),
    .executableTarget(
      name: "SetuCLI",
      dependencies: [
        "Setu", "SetuNetworking",
        .product(name: "ArgumentParser", package: "swift-argument-parser"),
        .product(name: "Subprocess", package: "swift-subprocess"),
        .product(name: "SystemPackage", package: "swift-system")
      ],
      swiftSettings: networkingSwiftSettings
    ),
    .testTarget(
      name: "SetuTests",
      dependencies: [
        "Setu", .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "_NIOFileSystem", package: "swift-nio")
      ],
      swiftSettings: networkingSwiftSettings
    ),
    .testTarget(
      name: "SetuNetworkingTests",
      dependencies: [
        "Setu", "SetuNetworking", "Networking",
        .product(name: "NIOCore", package: "swift-nio")
      ],
      swiftSettings: networkingSwiftSettings
    ),
    .testTarget(
      name: "SetuCLITests",
      dependencies: [
        "SetuCLI", "Setu", "SetuNetworking",
        .product(name: "ArgumentParser", package: "swift-argument-parser")
      ],
      swiftSettings: networkingSwiftSettings
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
    .executableTarget(
      name: "IndrasCounter",
      dependencies: [
        "TCPActorSystem",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "ArgumentParser", package: "swift-argument-parser"),
        .product(name: "Configuration", package: "swift-configuration"),
        .product(name: "SystemPackage", package: "swift-system")
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
