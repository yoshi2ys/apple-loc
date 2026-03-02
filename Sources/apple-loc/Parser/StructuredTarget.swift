import Foundation

/// Utilities for handling structured (stringsdict-derived) target strings.
///
/// About 1% of Apple translations are JSON objects rather than plain text,
/// representing device-specific rules, plural forms, or variable-width text.
enum StructuredTarget {

    /// Resolve a structured target to plain text suitable for embedding.
    ///
    /// - `NSStringDeviceSpecificRuleType` → "other" value (or first available)
    /// - `NSStringLocalizedFormatKey` (simple) → the format key string itself
    /// - `NSStringLocalizedFormatKey` (complex with `%#@var@`) → variables replaced with "other" forms
    /// - `NSStringVariableWidthRuleType` → value for the largest numeric width key
    /// - Unrecognized JSON / plain text → returned as-is
    static func resolveForEmbedding(_ target: String) -> String {
        guard target.first == "{" else { return target }
        guard let json = parseJSON(target) else { return target }

        // NSStringDeviceSpecificRuleType — device-specific text (iphone/ipad/mac/etc.)
        if let rules = json["NSStringDeviceSpecificRuleType"] as? [String: String] {
            return rules["other"] ?? rules.values.first ?? target
        }

        // NSStringLocalizedFormatKey — plural/format rules
        if let formatKey = json["NSStringLocalizedFormatKey"] as? String {
            if !formatKey.contains("%#@") {
                // Simple: the format key itself is the text
                return formatKey
            }
            // Complex: replace %#@varName@ references with their "other" form
            return resolveFormatKey(formatKey, variables: json)
        }

        // NSStringVariableWidthRuleType — width-based variants
        if let rules = json["NSStringVariableWidthRuleType"] as? [String: String] {
            // Pick the largest numeric key (most detailed text)
            let best = rules.max(by: { (Int($0.key) ?? 0) < (Int($1.key) ?? 0) })
            return best?.value ?? rules.values.first ?? target
        }

        return target
    }

    /// Parse a target string as a JSON dictionary, returning nil for plain text.
    static func parseAsJSON(_ target: String) -> [String: Any]? {
        guard target.first == "{" else { return nil }
        return parseJSON(target)
    }

    // MARK: - Private

    private static func parseJSON(_ target: String) -> [String: Any]? {
        guard let data = target.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data),
              let dict = obj as? [String: Any] else {
            return nil
        }
        return dict
    }

    /// Replace `%#@varName@` patterns with the "other" value from the corresponding variable dict.
    private static func resolveFormatKey(_ formatKey: String, variables: [String: Any]) -> String {
        var result = formatKey
        var searchStart = result.startIndex

        while let range = result.range(of: "%#@", range: searchStart..<result.endIndex) {
            let afterPrefix = range.upperBound
            guard let endAt = result.range(of: "@", range: afterPrefix..<result.endIndex) else { break }
            let varName = String(result[afterPrefix..<endAt.lowerBound])
            let fullRange = range.lowerBound..<endAt.upperBound

            if let varDict = variables[varName] as? [String: Any],
               let otherValue = varDict["other"] as? String {
                result.replaceSubrange(fullRange, with: otherValue)
                searchStart = result.index(fullRange.lowerBound, offsetBy: otherValue.count)
            } else {
                searchStart = endAt.upperBound
            }
        }

        return result
    }
}
