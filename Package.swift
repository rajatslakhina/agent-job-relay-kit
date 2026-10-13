// swift-tools-version: 6.0
import PackageDescription

// Only the platforms CI actually builds are declared: Linux (implicit, `swift test`)
// and iOS / macOS on the macos-15 runner. There is deliberately no executable or
// app target here — the runnable demo lives in its own repository and consumes this
// package as a version-pinned remote dependency.
let package = Package(
    name: "AgentJobRelay",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
    ],
    products: [
        .library(name: "AgentJobRelay", targets: ["AgentJobRelay"]),
    ],
    targets: [
        .target(name: "AgentJobRelay"),
        .testTarget(name: "AgentJobRelayTests", dependencies: ["AgentJobRelay"]),
    ]
)
