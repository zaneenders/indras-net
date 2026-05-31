// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "indras-net",
    targets: [
        .target(
            name: "IndrasNet"
        ),
        .testTarget(
            name: "IndrasNetTests",
            dependencies: ["IndrasNet"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
