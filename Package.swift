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
  .treatWarning("EmbeddedRestrictions", as: .error),
  .treatWarning("ExplicitSendable", as: .error)
]

let package = Package(
  name: "IndrasNet",
  platforms: [.macOS(.v13)],
  products: [
    .executable(name: "indras-echo", targets: ["IndrasEcho"])
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.105.0")
  ],
  targets: [
    .target(
      name: "IndrasNet",
      dependencies: [
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio")
      ],
      swiftSettings: sharedSwiftSettings
    ),
    .executableTarget(
      name: "IndrasEcho",
      dependencies: ["IndrasNet", .product(name: "NIOCore", package: "swift-nio")],
      swiftSettings: sharedSwiftSettings
    ),
    .testTarget(
      name: "IndrasNetTests",
      dependencies: [
        "IndrasNet",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOEmbedded", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio")
      ],
      swiftSettings: sharedSwiftSettings
    )
  ]
)
