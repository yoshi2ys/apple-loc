import Foundation

/// Bundle priority tiers for source-text deduplication.
/// Lower raw value = higher priority. When multiple bundles contribute the same
/// source text on a platform, the highest-priority bundle wins.
enum BundlePriority: Int, Comparable, Sendable {
    case coreFramework  = 1   // Foundation, UIKit, AppKit, SwiftUI, CoreFoundation
    case framework      = 2   // Other .framework bundles
    case app            = 3   // .app bundles
    case plugin         = 4   // .appex, .bundle, .pluginkit
    case other          = 5   // Everything else

    static func < (lhs: BundlePriority, rhs: BundlePriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    private static let coreFrameworks: Set<String> = [
        "foundation", "uikit", "appkit", "swiftui", "corefoundation",
        "foundation.framework", "uikit.framework", "appkit.framework",
        "swiftui.framework", "corefoundation.framework",
    ]

    /// Derive priority from a bundle_name string.
    static func from(bundleName: String) -> BundlePriority {
        let name = bundleName.lowercased()

        // Tier 1: core frameworks
        if coreFrameworks.contains(name) { return .coreFramework }

        // Tier 2: any .framework
        if name.hasSuffix(".framework") { return .framework }

        // Tier 3: .app
        if name.hasSuffix(".app") { return .app }

        // Tier 4: plugin-like bundles
        if name.hasSuffix(".appex") || name.hasSuffix(".bundle")
            || name.hasSuffix(".pluginkit") { return .plugin }

        // Tier 5: everything else
        return .other
    }
}
