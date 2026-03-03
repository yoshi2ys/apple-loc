import Foundation

/// Bundle priority tiers for source-text deduplication and display ordering.
/// Lower raw value = higher priority. When multiple bundles contribute the same
/// source text on a platform, the highest-priority bundle wins.
enum BundlePriority: Int, Comparable, Sendable {
    case tier1     = 1  // EmbedTier T1 (Foundation, AppKit, UIKitCore, SwiftUI, etc.)
    case tier2     = 2  // EmbedTier T2 (Photos, Calendar, Safari, etc.)
    case tier3     = 3  // EmbedTier T3 (Terminal, GameCenter, etc.)
    case framework = 4  // Unknown .framework
    case app       = 5  // Unknown .app
    case plugin    = 6  // .appex, .bundle, .pluginkit
    case other     = 7  // Everything else
    case excluded  = 8  // EmbedTier excluded (ImageIO, etc.)

    static func < (lhs: BundlePriority, rhs: BundlePriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    /// Derive priority from a bundle_name string.
    /// Delegates to EmbedTierClassifier for curated bundles, then falls back
    /// to extension-based classification for unknowns.
    static func from(bundleName: String) -> BundlePriority {
        if let tier = EmbedTierClassifier.classify(bundleName) {
            switch tier {
            case 1: return .tier1
            case 2: return .tier2
            case 3: return .tier3
            default: break
            }
        } else if EmbedTierClassifier.isExcluded(bundleName) {
            return .excluded
        }

        // Extension-based fallback for unknown bundles
        let name = bundleName.lowercased()
        if name.hasSuffix(".framework") { return .framework }
        if name.hasSuffix(".app") { return .app }
        if name.hasSuffix(".appex") || name.hasSuffix(".bundle")
            || name.hasSuffix(".pluginkit") { return .plugin }
        return .other
    }
}
