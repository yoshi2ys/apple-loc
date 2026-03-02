import ArgumentParser
import Foundation
import GRDB
import NaturalLanguage

struct SearchCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "search",
        abstract: "Semantic search for related official translations."
    )

    @Argument(help: "Natural language search query.")
    var query: String

    @Option(name: .long, help: "Output language filter (comma-separated). Omit to show all languages.")
    var lang: String?

    @Option(name: .long, help: "Filter by bundle/framework name.")
    var framework: String?

    @Option(name: .long, help: "Filter by platform (e.g. \"ios26\").")
    var platform: String?

    @Option(name: .long, help: "Query language for text search (e.g. \"ja\"). Omit to auto-detect or search all languages.")
    var queryLang: String?

    @Option(name: .long, help: "Maximum number of results.")
    var limit: Int = 5

    @Option(name: .long, help: "SQLite database path.")
    var db: String = DatabaseManager.defaultDBPath

    func run() async throws {
        let dbPath = DatabaseManager.resolvePath(db)
        let dbQueue = try DatabaseManager.openDatabase(at: dbPath)
        let langFilter = lang.map { $0.normalizedLanguageSet }
        let fw = framework
        let plat = platform
        let maxResults = limit
        let overFetchLimit = (fw != nil || plat != nil) ? maxResults * 5 : maxResults

        // Detect query language
        let recognizer = NLLanguageRecognizer()
        recognizer.processString(query)
        let detectedLang = recognizer.dominantLanguage

        let rawQueryLang = queryLang ?? detectedLang?.rawValue ?? "en"
        let queryLangVariants = rawQueryLang.languageCodeVariants
        let isEnglish = queryLangVariants.contains("en")

        // --- Semantic search (language-aware) ---
        // Resolve language code to DB form (handles zh_Hans ↔ zh-Hans) and check embeddings
        var semanticIds: [(sourceId: Int64, distance: Double)] = []

        let (queryLangCode, hasEmbeddings) = try await dbQueue.read { db -> (String, Bool) in
            for variant in queryLangVariants {
                if try Int.fetchOne(db, sql: """
                    SELECT 1 FROM vec_mapping WHERE language = ? LIMIT 1
                """, arguments: [variant]) != nil {
                    return (variant, true)
                }
            }
            for variant in queryLangVariants {
                if try Int.fetchOne(db, sql: """
                    SELECT 1 FROM translations WHERE language = ? LIMIT 1
                """, arguments: [variant]) != nil {
                    return (variant, false)
                }
            }
            return (rawQueryLang, false)
        }

        if hasEmbeddings {
            if let resolvedLang = EmbeddingService.resolveLanguage(for: queryLangCode),
               let svc = EmbeddingService(language: resolvedLang) {
                try svc.load()
                let queryVec = try svc.embed(query.lowercased())

                semanticIds = try await dbQueue.read { db in
                    let vecData = queryVec.asData
                    // KNN returns vec_mapping IDs as rowids
                    let vecRows = try Row.fetchAll(db, sql: """
                        SELECT v.rowid, v.distance
                        FROM vec_source_strings v
                        WHERE v.embedding MATCH ? AND v.k = ? AND v.language = ?
                    """, arguments: [vecData, overFetchLimit, queryLangCode])

                    // Resolve mapping IDs back to source_ids
                    return try vecRows.compactMap { row -> (sourceId: Int64, distance: Double)? in
                        let mappingId: Int64 = row["rowid"]
                        let distance: Double = row["distance"]
                        guard let mapRow = try Row.fetchOne(db, sql: """
                            SELECT source_id FROM vec_mapping WHERE id = ?
                        """, arguments: [mappingId]) else { return nil }
                        return (sourceId: mapRow["source_id"] as Int64, distance: distance)
                    }
                }
            }
        }

        // --- Text search ---
        // When --query-lang is specified, search that language only.
        // For English queries, search English translations.
        // For non-English auto-detected queries, search ALL languages to avoid
        // misdetection (e.g. NLLanguageRecognizer confuses ja/zh for short CJK text).
        let textLang: String? = queryLang != nil ? queryLangCode : (isEnglish ? "en" : nil)
        let escapedQuery = query
            .replacingOccurrences(of: "%", with: "\\%")
            .replacingOccurrences(of: "_", with: "\\_")
        let likePattern = "%\(escapedQuery)%"

        // Phase 1: exact match (distance = -1)
        let exactIds: [Int64] = try await dbQueue.read { db in
            let sql: String
            let args: StatementArguments
            if let textLang {
                sql = "SELECT DISTINCT t.source_id FROM translations t WHERE t.language = ? AND t.target = ? LIMIT ?"
                args = [textLang, query, overFetchLimit]
            } else {
                sql = "SELECT DISTINCT t.source_id FROM translations t WHERE t.target = ? LIMIT ?"
                args = [query, overFetchLimit]
            }
            return try Row.fetchAll(db, sql: sql, arguments: args).map { $0["source_id"] as Int64 }
        }

        // Phase 2: partial match (distance = 0), excluding exact matches
        let partialIds: [Int64] = try await dbQueue.read { db in
            let sql: String
            let args: StatementArguments
            if let textLang {
                sql = """
                    SELECT DISTINCT t.source_id FROM translations t
                    WHERE t.language = ? AND t.target LIKE ? ESCAPE '\\' AND t.target != ?
                    LIMIT ?
                """
                args = [textLang, likePattern, query, overFetchLimit]
            } else {
                sql = """
                    SELECT DISTINCT t.source_id FROM translations t
                    WHERE t.target LIKE ? ESCAPE '\\' AND t.target != ?
                    LIMIT ?
                """
                args = [likePattern, query, overFetchLimit]
            }
            return try Row.fetchAll(db, sql: sql, arguments: args).map { $0["source_id"] as Int64 }
        }

        // --- Merge: deduplicate by source_id, keep lowest distance ---
        var bestDistance: [Int64: Double] = [:]
        for item in semanticIds {
            bestDistance[item.sourceId] = item.distance
        }
        for id in exactIds {
            bestDistance[id] = min(bestDistance[id] ?? 0, -1.0)
        }
        for id in partialIds {
            if bestDistance[id] == nil {
                bestDistance[id] = 0.0
            }
        }

        let sortedIds = bestDistance.sorted { $0.value < $1.value }.map { ($0.key, $0.value) }

        // --- Fetch full results ---
        let candidates = sortedIds.map { ResultFetcher.Candidate(sourceId: $0.0, distance: $0.1) }
        let results: [SearchResult] = try await dbQueue.read { db in
            try ResultFetcher.fetch(
                candidates: candidates,
                in: db,
                langFilter: langFilter,
                frameworkFilter: fw,
                platformFilter: plat,
                limit: maxResults,
                deduplicateByTranslation: true
            )
        }

        try ResultsOutput(results: results).printJSON()
    }

}
