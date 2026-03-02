import Foundation

/// Parses JSON localization files from the applelocalization-tools data directory.
/// Directory structure: data/{ios,macos}/{version}/*.json
struct JSONDataParser {
    let dataDir: String
    let allowedLanguages: Set<String>?
    let allowedPlatforms: Set<String>?
    var filterIBKeys: Bool = true

    private static let osDirectories: Set<String> = ["ios", "macos"]

    /// Resolve the JSON data root directory (may be dir itself or dir/data).
    static func resolveDataDir(_ dir: String) -> String {
        let contents = (try? FileManager.default.contentsOfDirectory(atPath: dir)) ?? []
        if !osDirectories.isDisjoint(with: contents) { return dir }
        return (dir as NSString).appendingPathComponent("data")
    }

    // MARK: - Codable Models

    private struct JSONLocFile: Decodable {
        let framework: String
        let localizations: [String: [JSONTranslation]]
    }

    private struct JSONTranslation: Decodable {
        let language: String
        let target: String
        let filename: String
    }

    // MARK: - Discovery

    /// Discover all JSON files grouped by platform.
    /// Returns [(platform: "ios26", files: [URL])], sorted by platform.
    func discoverFiles() throws -> [(platform: String, files: [URL])] {
        let fm = FileManager.default
        let baseURL = URL(fileURLWithPath: dataDir)

        var result: [(platform: String, files: [URL])] = []

        // Iterate over os directories (ios, macos)
        let osDirs = try fm.contentsOfDirectory(atPath: dataDir)
            .filter { Self.osDirectories.contains($0) }
            .sorted()

        for osName in osDirs {
            let osURL = baseURL.appendingPathComponent(osName)
            let versions = try fm.contentsOfDirectory(atPath: osURL.path).sorted()

            for version in versions {
                let platform = Self.platformName(os: osName, version: version)

                if let allowed = allowedPlatforms, !allowed.contains(platform) {
                    continue
                }

                let versionURL = osURL.appendingPathComponent(version)
                let files = try fm.contentsOfDirectory(atPath: versionURL.path)
                    .filter { $0.hasSuffix(".json") }
                    .sorted()
                    .map { versionURL.appendingPathComponent($0) }

                if !files.isEmpty {
                    result.append((platform: platform, files: files))
                }
            }
        }

        guard !result.isEmpty else {
            throw ParserError.noDataFiles(dataDir)
        }
        return result
    }

    // MARK: - Parsing

    /// Parse all discovered files and yield batches via the handler.
    /// Each batch corresponds to one JSON file. Returns total rows parsed.
    func parse(batchHandler: ([ParsedRow]) throws -> Void) throws -> Int {
        let groups = try discoverFiles()
        let filter = RowFilter(filterIBKeys: filterIBKeys)
        let decoder = JSONDecoder()
        var totalParsed = 0

        for group in groups {
            for fileURL in group.files {
                let rows = try parseFile(fileURL, platform: group.platform, filter: filter, decoder: decoder)
                if !rows.isEmpty {
                    try batchHandler(rows)
                    totalParsed += rows.count
                }
            }
        }

        return totalParsed
    }

    /// Parse a single JSON file into ParsedRows.
    private func parseFile(_ url: URL, platform: String, filter: RowFilter, decoder: JSONDecoder) throws -> [ParsedRow] {
        let data = try Data(contentsOf: url)
        let locFile = try decoder.decode(JSONLocFile.self, from: data)

        var rows: [ParsedRow] = []

        for (sourceKey, translations) in locFile.localizations {
            for trans in translations {
                if let allowed = allowedLanguages, !allowed.matchesLanguage(trans.language) {
                    continue
                }
                guard filter.shouldInclude(source: sourceKey, fileName: trans.filename) else {
                    continue
                }

                rows.append(ParsedRow(
                    id: nil,
                    groupId: nil,
                    source: sourceKey,
                    target: trans.target,
                    language: trans.language,
                    fileName: trans.filename,
                    bundleName: locFile.framework,
                    platform: platform
                ))
            }
        }

        return rows
    }

    // MARK: - Platform Name Conversion

    /// Convert os name + version to platform identifier.
    /// "ios" + "26.1" → "ios26", "macos" + "15.6" → "macos15"
    static func platformName(os: String, version: String) -> String {
        let major = version.split(separator: ".").first.map(String.init) ?? version
        return "\(os)\(major)"
    }
}
