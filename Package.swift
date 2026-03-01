// swift-tools-version: 6.1

import PackageDescription

let package = Package(
    name: "apple-loc",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-argument-parser", from: "1.7.0"),
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.10.0"),
    ],
    targets: [
        .target(
            name: "Csqlitevec",
            path: "Sources/Csqlitevec",
            cSettings: [
                .headerSearchPath("include"),
                .define("SQLITE_CORE"),
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3"),
            ]
        ),
        .executableTarget(
            name: "apple-loc",
            dependencies: [
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "GRDB", package: "GRDB.swift"),
                "Csqlitevec",
            ],
            path: "Sources/apple-loc"
        ),
    ]
)
