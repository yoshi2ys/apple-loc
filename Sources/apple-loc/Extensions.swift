import Foundation

// MARK: - Logging & Formatting

func logStderr(_ message: String, terminator: String = "\n") {
    FileHandle.standardError.write(Data((message + terminator).utf8))
}

func formatTime(_ seconds: Double) -> String {
    let m = Int(seconds) / 60
    let s = Int(seconds) % 60
    return m > 0 ? "\(m)m\(s)s" : "\(s)s"
}

func formatRate(_ count: Int, _ seconds: Double) -> String {
    guard seconds > 0 else { return "—" }
    return String(format: "%.0f", Double(count) / seconds)
}

extension Array where Element == Float {
    /// Convert a float vector to raw bytes for sqlite-vec insertion/queries.
    var asData: Data {
        withUnsafeBufferPointer { Data(buffer: $0) }
    }
}

extension DatabaseManager {
    /// Default database path shared across all commands.
    static let defaultDBPath = "~/.apple-loc/apple-loc.db"
}

extension String {
    /// Split a comma-separated string into trimmed components.
    /// e.g. "ja,en" → ["ja", "en"]
    var commaSeparated: [String] {
        split(separator: ",").map(String.init)
    }

    /// Both underscore and hyphen variants of a language code.
    /// "zh_Hans" → ["zh_Hans", "zh-Hans"], "ja" → ["ja"]
    var languageCodeVariants: [String] {
        if contains("_") {
            return [self, replacingOccurrences(of: "_", with: "-")]
        } else if contains("-") {
            return [self, replacingOccurrences(of: "-", with: "_")]
        }
        return [self]
    }

    /// Comma-separated language list → Set with both _ and - variants.
    var normalizedLanguageSet: Set<String> {
        Set(commaSeparated.flatMap(\.languageCodeVariants))
    }
}

extension Duration {
    var seconds: Double {
        Double(components.seconds) + Double(components.attoseconds) / 1e18
    }
}

extension Set where Element == String {
    /// Check if a language code matches this filter set.
    /// Exact match first, then base-prefix match:
    /// filter {"zh"} matches "zh_CN", "zh-Hans", etc.
    func matchesLanguage(_ language: String) -> Bool {
        if contains(language) { return true }
        if let idx = language.firstIndex(where: { $0 == "_" || $0 == "-" }) {
            return contains(String(language[..<idx]))
        }
        return false
    }
}
