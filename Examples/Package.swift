// swift-tools-version: 6.3.3
import PackageDescription

let settings: [SwiftSetting] = [.swiftLanguageMode(.v6), .strictMemorySafety()]
let package = Package(
    name: "VMBridgeExamples",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "VMBridgeGuestExample", targets: ["VMBridgeGuestExample"]),
        .library(name: "VMBridgeHostExample", targets: ["VMBridgeHostExample"]),
    ],
    dependencies: [.package(path: "..")],
    targets: [
        .target(
            name: "VMBridgeExampleMessages",
            dependencies: [.product(name: "VMBridge", package: "VMBridge")], swiftSettings: settings
        ),
        .executableTarget(
            name: "VMBridgeGuestExample",
            dependencies: [
                "VMBridgeExampleMessages", .product(name: "VMBridge", package: "VMBridge"),
            ], swiftSettings: settings),
        .target(
            name: "VMBridgeHostExample",
            dependencies: [
                "VMBridgeExampleMessages", .product(name: "VMBridge", package: "VMBridge"),
                .product(name: "VMBridgeVirtualization", package: "VMBridge"),
            ], swiftSettings: settings),
    ]
)
