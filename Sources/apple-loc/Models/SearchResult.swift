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
    func printJSON() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(self)
        print(String(data: data, encoding: .utf8)!)
    }
}
