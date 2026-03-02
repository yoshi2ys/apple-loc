import Foundation

/// A grouped search/lookup result with translations across languages.
struct SearchResult: Codable, Sendable {
    var source: String
    var bundleName: String
    var fileName: String?
    var platform: String
    var distance: Double?
    var translations: [String: String]  // language -> target
    var bundles: [String]?  // all originating bundles (nil when source_bundles table absent)

    enum CodingKeys: String, CodingKey {
        case source, platform, distance, translations, bundles
        case bundleName = "bundle_name"
        case fileName = "file_name"
    }
}

/// Wrapper for JSON output.
struct ResultsOutput: Codable, Sendable {
    var results: [SearchResult]
}

extension ResultsOutput {
    /// Serialize to JSON Data, expanding structured targets (stringsdict JSON) as nested objects.
    func buildJSONData() throws -> Data {
        let resultsArray: [[String: Any]] = results.map { r in
            var dict: [String: Any] = [
                "source": r.source,
                "bundle_name": r.bundleName,
                "platform": r.platform,
            ]
            if let f = r.fileName { dict["file_name"] = f }
            if let d = r.distance { dict["distance"] = d }
            if let b = r.bundles { dict["bundles"] = b }

            var trans: [String: Any] = [:]
            for (lang, target) in r.translations {
                if let parsed = StructuredTarget.parseAsJSON(target) {
                    trans[lang] = parsed
                } else {
                    trans[lang] = target
                }
            }
            dict["translations"] = trans
            return dict
        }

        let output: [String: Any] = ["results": resultsArray]
        return try JSONSerialization.data(withJSONObject: output, options: [.prettyPrinted, .sortedKeys])
    }

    func printJSON() throws {
        let data = try buildJSONData()
        print(String(data: data, encoding: .utf8)!)
    }
}
