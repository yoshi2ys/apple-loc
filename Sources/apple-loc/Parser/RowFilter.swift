import Foundation

/// Shared filter logic for excluding low-value localization rows.
/// Used by both SQLDumpParser and JSONDataParser.
struct RowFilter: Sendable {
    var filterIBKeys: Bool = true

    /// Characters allowed in format-placeholder-only strings (e.g. "%@", "%d %@", "%1$@ %2$@")
    private static let formatOnlyChars: Set<Character> = Set(" \t%@dlufegc0123456789$.*-+#")

    /// Files to exclude (metadata / low-value)
    private static let excludedFilePatterns: Set<String> = [
        "infoplist.loctable", "infoplist.strings", "infoplist.xcstrings",
    ]
    private static let excludedFilePrefixes = ["appintents", "appshortcuts"]

    /// Plist metadata keys to exclude
    private static let plistMetadataKeys: Set<String> = [
        "CFBundleName", "CFBundleDisplayName", "CFBundleGetInfoString",
        "CFBundleShortVersionString", "NSHumanReadableCopyright",
    ]

    /// Combined filter: IB keys, excluded files, plist metadata, format-only strings.
    /// Language filtering is NOT included here — it's handled at the parser level.
    func shouldInclude(source: String, fileName: String) -> Bool {
        !isIBKey(source)
            && !isExcludedFile(fileName)
            && !isPlistMetadataKey(source)
            && !isFormatOnlyString(source)
    }

    /// Check if a file should be excluded (plist metadata, AppIntents, etc.).
    private func isExcludedFile(_ fileName: String) -> Bool {
        let lower = fileName.lowercased()
        if Self.excludedFilePatterns.contains(lower) { return true }
        return Self.excludedFilePrefixes.contains(where: { lower.hasPrefix($0) })
    }

    /// Check if the source key is a plist metadata key.
    private func isPlistMetadataKey(_ source: String) -> Bool {
        Self.plistMetadataKeys.contains(source)
    }

    /// Check if the source text is only format placeholders (e.g. "%@", "%d %@").
    private func isFormatOnlyString(_ source: String) -> Bool {
        guard !source.isEmpty else { return true }
        return source.allSatisfy { Self.formatOnlyChars.contains($0) }
    }

    /// Check if a source key is an Interface Builder Object ID (e.g. "D1K-K5-gc3.title").
    /// Pattern: exactly `[A-Za-z0-9]{3}-[A-Za-z0-9]{2}-[A-Za-z0-9]{3}.` at the start.
    private func isIBKey(_ source: String) -> Bool {
        guard filterIBKeys else { return false }
        let s = source.utf8
        // Minimum length: 3 + 1 + 2 + 1 + 3 + 1 = 11 characters
        guard s.count >= 11 else { return false }
        var i = s.startIndex
        @inline(__always) func isAlnum(_ b: UInt8) -> Bool {
            (b >= 0x30 && b <= 0x39)    // 0-9
            || (b >= 0x41 && b <= 0x5A) // A-Z
            || (b >= 0x61 && b <= 0x7A) // a-z
        }
        // [A-Za-z0-9]{3}
        for _ in 0..<3 { guard isAlnum(s[i]) else { return false }; i = s.index(after: i) }
        guard s[i] == 0x2D else { return false }; i = s.index(after: i) // '-'
        // [A-Za-z0-9]{2}
        for _ in 0..<2 { guard isAlnum(s[i]) else { return false }; i = s.index(after: i) }
        guard s[i] == 0x2D else { return false }; i = s.index(after: i) // '-'
        // [A-Za-z0-9]{3}
        for _ in 0..<3 { guard isAlnum(s[i]) else { return false }; i = s.index(after: i) }
        guard s[i] == 0x2E else { return false } // '.'
        return true
    }
}
