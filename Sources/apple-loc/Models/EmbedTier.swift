import ArgumentParser

/// Controls which bundles are eligible for embedding, based on tier classification.
enum EmbedTier: ExpressibleByArgument, Sendable {
    case upTo(Int)  // 1, 2, or 3
    case all        // everything except exclusions

    init?(argument: String) {
        switch argument.lowercased() {
        case "1": self = .upTo(1)
        case "2": self = .upTo(2)
        case "3": self = .upTo(3)
        case "all": self = .all
        default: return nil
        }
    }

    var defaultValueDescription: String { "2" }

    var stringValue: String {
        switch self {
        case .upTo(let n): return "\(n)"
        case .all: return "all"
        }
    }
}

/// Classifies bundles into embedding tiers and decides whether to embed them.
enum EmbedTierClassifier {

    /// Returns the tier (1, 2, or 3) for a known bundle, or nil if excluded/unknown.
    static func classify(_ bundleName: String) -> Int? {
        let name = bundleName.lowercased()
        if excluded.contains(name) { return nil }
        if tier1.contains(name) { return 1 }
        if tier2.contains(name) { return 2 }
        if tier3.contains(name) { return 3 }
        return nil  // unknown
    }

    /// Whether a bundle should be embedded under the given tier setting.
    static func shouldEmbed(_ bundleName: String, tier: EmbedTier) -> Bool {
        let name = bundleName.lowercased()
        if excluded.contains(name) { return false }
        switch tier {
        case .upTo(let maxTier):
            guard let t = classifyLowercased(name) else { return false }
            return t <= maxTier
        case .all:
            return true
        }
    }

    /// Whether a bundle is in the exclusion list.
    static func isExcluded(_ bundleName: String) -> Bool {
        excluded.contains(bundleName.lowercased())
    }

    /// Classify a pre-lowercased bundle name (avoids redundant lowercasing).
    private static func classifyLowercased(_ name: String) -> Int? {
        if excluded.contains(name) { return nil }
        if tier1.contains(name) { return 1 }
        if tier2.contains(name) { return 2 }
        if tier3.contains(name) { return 3 }
        return nil
    }

    // MARK: - Bundle Lists

    private static let tier1: Set<String> = [
        // Core Frameworks
        "foundation.framework",
        "coretext.framework",
        "corelocation.framework",
        // UI Frameworks
        "appkit.framework",
        "uikitcore.framework",
        "swiftui.framework",
        // Sharing
        "sharing.framework",
        "sharesheet.framework",
        // Security
        "security.framework",
        "securityfoundation.framework",
        "authenticationservices.framework",
        // Cloud & Store
        "cloudkit.framework",
        "storekit.framework",
        // System Apps
        "finder.app",
        "finderkit.framework",
        "desktopservicesui.framework",
        "springboard.app",
        "controlcenter.app",
        "system settings.app",
        "loginwindow.app",
        "setup assistant.app",
    ]

    private static let tier2: Set<String> = [
        // Communication
        "chatkit.framework",
        "conversationkit.framework",
        "mail.app",
        "messageui.framework",
        "facetime.app",
        // Photos & Media
        "photos.app",
        "photosuicore.framework",
        "music.app",
        "tv.app",
        "videosui.framework",
        "avkit.framework",
        "avfcore.framework",
        "quicktime player.app",
        "books.app",
        "podcastskit.framework",
        "podcastsfoundation.framework",
        // Productivity
        "calendar.app",
        "calendaruikit.framework",
        "eventkitui.framework",
        "eventkit.framework",
        "notes.app",
        "notesshared.framework",
        "reminders.app",
        "remindersuicore.framework",
        "freeform.app",
        "journal.app",
        "calculator.app",
        "calculate.framework",
        // Maps & Location
        "maps.app",
        "mapkit.framework",
        "findmy.app",
        // Contacts
        "contacts.app",
        "contacts.framework",
        "contactsui.framework",
        // Health & Fitness
        "healthkit.framework",
        "healthui.framework",
        // Home & IoT
        "homekit.framework",
        "home.framework",
        "homeui.framework",
        // Weather
        "weather.app",
        "weatherkit.framework",
        "weatherui.framework",
        // Safari & Web
        "safari.app",
        "safari.framework",
        // Shortcuts & Automation
        "workflowkit.framework",
        "actionkit.framework",
        "intents.framework",
        "voiceshortcutclient.framework",
        // Store & Payments
        "passkit.framework",
        "appstorekit.framework",
        "app store.app",
        // Documents & Files
        "documentmanager.framework",
        "quicklook.framework",
        "preview.app",
        "icloud.app",
        "icloudsettings.framework",
        // Accessibility
        "accessibility.framework",
        "accessibilityutilities.framework",
        "screenreader.framework",
        "voiceoverservices.framework",
        "voiceovertouch.app",
        // Accounts & Identity
        "appleaccount.framework",
        "accountsui.framework",
        "familycircleui.framework",
        // Print
        "printkit.framework",
        "printcore.framework",
    ]

    private static let tier3: Set<String> = [
        // Gaming & AR
        "gamecenterui.framework",
        "arkitcore.framework",
        // Settings Panels
        "accessibilitysettings.bundle",
        "internationalsettings.bundle",
        "notificationssettings.bundle",
        "camerasettings.bundle",
        // Utilities (macOS)
        "terminal.app",
        "disk utility.app",
        "activity monitor.app",
        "keychain access.app",
        "font book.app",
        "textedit.app",
        "automator.framework",
        "applescript.framework",
        // Other Notable
        "managedconfiguration.framework",
        "focussettingsui.framework",
        "contentkit.framework",
        "networkextension.framework",
        "corespotlight.framework",
        "corebluetooth.framework",
        "avatarkit.framework",
        "coreemoji.framework",
        "magnifiersupport.framework",
    ]

    private static let excluded: Set<String> = [
        "imageio.framework",
        "photosformats.framework",
        "iworkimport.framework",
        "moments.framework",
        "ampdevices.framework",
        "riograph.framework",
        "photoanalysis.framework",
        "photosgraph.framework",
        "blissreader.framework",
        "airportassistant.framework",
        "homedatamodel.framework",
    ]

}
