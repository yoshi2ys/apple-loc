import Foundation
import GRDB

/// Batch-fetches SearchResult objects using IN queries, avoiding N+1 patterns.
enum ResultFetcher {

    struct Candidate {
        let sourceId: Int64
        let distance: Double?
    }

    /// Fetch results for a batch of candidates.
    /// Uses two IN queries instead of per-row lookups.
    static func fetch(
        candidates: [Candidate],
        in db: Database,
        langFilter: Set<String>?,
        frameworkFilter: String?,
        platformFilter: String?,
        limit: Int,
        deduplicateByTranslation: Bool = false
    ) throws -> [SearchResult] {
        guard !candidates.isEmpty else { return [] }

        let ids = candidates.map(\.sourceId)
        let placeholders = Array(repeating: "?", count: ids.count).joined(separator: ",")

        // Check if source_bundles table exists (absent in compact mode)
        let hasBundlesTable = try DatabaseManager.tableExists("source_bundles", in: db)

        // Batch fetch source_strings
        let ssRows = try Row.fetchAll(db, sql: """
            SELECT id, source, bundle_name, file_name, platform
            FROM source_strings WHERE id IN (\(placeholders))
        """, arguments: StatementArguments(ids))

        var sourceMap: [Int64: Row] = [:]
        for row in ssRows {
            sourceMap[row["id"] as Int64] = row
        }

        // Batch fetch all bundle names per source (when source_bundles exists)
        var bundlesMap: [Int64: [String]] = [:]
        if hasBundlesTable {
            let sbRows = try Row.fetchAll(db, sql: """
                SELECT source_id, bundle_name FROM source_bundles
                WHERE source_id IN (\(placeholders))
            """, arguments: StatementArguments(ids))
            for row in sbRows {
                let sourceId: Int64 = row["source_id"]
                bundlesMap[sourceId, default: []].append(row["bundle_name"])
            }
        }

        // Batch fetch translations
        let transRows = try Row.fetchAll(db, sql: """
            SELECT source_id, language, target
            FROM translations WHERE source_id IN (\(placeholders))
        """, arguments: StatementArguments(ids))

        var transMap: [Int64: [(language: String, target: String)]] = [:]
        for row in transRows {
            let sourceId: Int64 = row["source_id"]
            transMap[sourceId, default: []].append((row["language"], row["target"]))
        }

        // Assemble results in candidate order (preserves ranking)
        var results: [SearchResult] = []
        var seenTranslations: Set<String> = []

        for candidate in candidates {
            guard let ss = sourceMap[candidate.sourceId] else { continue }

            let bundleName: String = ss["bundle_name"]
            let rowPlatform: String = ss["platform"]
            let allBundles = bundlesMap[candidate.sourceId]

            // Framework filter: match against all bundles when available, else primary only
            if let f = frameworkFilter {
                if let allBundles {
                    guard allBundles.contains(where: { $0.localizedCaseInsensitiveContains(f) }) else { continue }
                } else {
                    guard bundleName.localizedCaseInsensitiveContains(f) else { continue }
                }
            }
            if let p = platformFilter, rowPlatform != p { continue }

            var translations: [String: String] = [:]
            for (lang, target) in transMap[candidate.sourceId] ?? [] {
                if let filter = langFilter, !filter.matchesLanguage(lang) { continue }
                translations[lang] = target
            }

            if translations.isEmpty { continue }

            if deduplicateByTranslation {
                let fingerprint = translations.sorted(by: { $0.key < $1.key })
                    .map { "\($0.key):\($0.value)" }.joined(separator: "|")
                guard seenTranslations.insert(fingerprint).inserted else { continue }
            }

            if results.count >= limit { break }

            let sortedBundles = allBundles?.sorted()
            results.append(SearchResult(
                source: ss["source"],
                bundleName: bundleName,
                fileName: ss["file_name"],
                platform: rowPlatform,
                distance: candidate.distance,
                translations: translations,
                bundles: sortedBundles
            ))
        }

        return results
    }
}
