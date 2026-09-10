// swift-tools-version: 6.3.3
import PackageDescription

let settings: [SwiftSetting] = [
    .enableUpcomingFeature("ApproachableConcurrency"),
    .swiftLanguageMode(.v6),
    .strictMemorySafety(),
]

let package = Package(
    name: "VMBridge",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "VMBridge", targets: ["VMBridge"]),
        .library(name: "VMBridgeVirtualization", targets: ["VMBridgeVirtualization"]),
    ],
    targets: [
        .target(name: "VMBridge", swiftSettings: settings),
        .target(
            name: "VMBridgeVirtualization", dependencies: ["VMBridge"], swiftSettings: settings),
        .testTarget(name: "VMBridgeTests", dependencies: ["VMBridge"], swiftSettings: settings),
        .testTarget(
            name: "VMBridgeVirtualizationTests",
            dependencies: ["VMBridge", "VMBridgeVirtualization"], swiftSettings: settings),
    ],
    swiftLanguageModes: [.v6]
)
