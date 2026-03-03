import ArgumentParser
import Foundation
import GRDB

struct EmbedCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "embed",
        abstract: "Generate embeddings for existing translations in the database."
    )

    @Option(name: .long, help: "Comma-separated language codes to embed (e.g. \"ja,ko\").")
    var langs: String

    @Option(name: .long, help: "SQLite database path.")
    var db: String = DatabaseManager.defaultDBPath

    @Option(name: .long, help: "Number of parallel embedding workers per language (default: CPU cores / 2).")
    var concurrency: Int?

    @Option(name: .long, help: "Batch size for embedding.")
    var batchSize: Int = 1000

    @Option(name: .long, help: "Embedding tier: 1 (core UI), 2 (+ primary apps, default), 3 (+ extended), or \"all\".")
    var embedTier: EmbedTier = .upTo(2)

    @Flag(name: .long, help: "Delete existing embeddings for the specified languages and regenerate.")
    var force: Bool = false

    func run() async throws {
        let dbPath = DatabaseManager.resolvePath(db)
        guard FileManager.default.fileExists(atPath: dbPath) else {
            throw ValidationError("Database not found at '\(dbPath)'. Run 'ingest' first.")
        }

        let targetLangs = langs.commaSeparated
        guard !targetLangs.isEmpty else {
            throw ValidationError("--langs requires at least one language code.")
        }

        let dbQueue = try DatabaseManager.openDatabase(at: dbPath)

        // Force-delete existing embeddings if requested
        if force {
            try await dbQueue.write { db in
                try DatabaseManager.deleteEmbeddings(for: targetLangs, in: db)
            }
            logStderr("Deleted existing embeddings for: \(targetLangs.joined(separator: ", "))")
        }

        // Create worker pool
        let pool = try EmbedWorkerPool(languages: targetLangs, concurrency: concurrency, log: { logStderr($0) })

        let startTime = ContinuousClock.now
        var totalVectors = 0

        // Process each language
        for lang in targetLangs {
            guard pool.supportedLanguages.contains(lang) else {
                logStderr("Skipping '\(lang)' (no workers loaded).")
                continue
            }

            // Find the DB-stored language variant
            let dbLang: String = try await dbQueue.read { db in
                for variant in lang.languageCodeVariants {
                    if try Int.fetchOne(db, sql:
                        "SELECT 1 FROM translations WHERE language = ? LIMIT 1",
                        arguments: [variant]) != nil {
                        return variant
                    }
                }
                return lang
            }

            let translationCount: Int = try await dbQueue.read { db in
                try Int.fetchOne(db, sql:
                    "SELECT COUNT(*) FROM translations WHERE language = ?",
                    arguments: [dbLang])!
            }

            if translationCount == 0 {
                logStderr("0 translations found for '\(lang)', nothing to embed.")
                continue
            }

            logStderr("Embedding '\(lang)' (\(translationCount) translations)...")

            var cursor: Int64 = 0
            var langVectors = 0

            while true {
                // Cursor-based query: fetch translations without embeddings
                let cursorSnapshot = cursor
                let rawBatch: [(sourceId: Int64, text: String, bundleName: String)] = try await dbQueue.read { db in
                    try Row.fetchAll(db, sql: """
                        SELECT t.source_id, t.target, ss.bundle_name FROM translations t
                        INNER JOIN source_strings ss ON ss.id = t.source_id
                        LEFT JOIN vec_mapping vm ON vm.source_id = t.source_id AND vm.language = ?
                        WHERE t.language = ? AND vm.id IS NULL AND t.source_id > ?
                        ORDER BY t.source_id LIMIT ?
                    """, arguments: [dbLang, dbLang, cursorSnapshot, batchSize]).map { row in
                        (sourceId: row["source_id"] as Int64,
                         text: (row["target"] as String).lowercased(),
                         bundleName: row["bundle_name"] as String)
                    }
                }

                if rawBatch.isEmpty { break }
                cursor = rawBatch.last!.sourceId

                // Filter by tier
                let batch = rawBatch.filter { EmbedTierClassifier.shouldEmbed($0.bundleName, tier: embedTier) }
                if batch.isEmpty { continue }

                // Generate embeddings
                let targets = batch.map { (language: lang, text: $0.text) }
                let embeddings = pool.embed(targets: targets)

                // Insert into DB
                let batchInserted: Int = try await dbQueue.write { db in
                    var count = 0
                    for (i, item) in batch.enumerated() {
                        guard let vec = embeddings[i] else { continue }
                        try DatabaseManager.upsertVector(in: db, sourceId: item.sourceId, language: dbLang, embedding: vec)
                        count += 1
                    }
                    return count
                }

                langVectors += batchInserted
                let elapsed = (ContinuousClock.now - startTime).seconds
                logStderr("\r  \(lang): \(langVectors) vectors | \(formatRate(langVectors, elapsed)) vec/s", terminator: "")
            }

            totalVectors += langVectors
            logStderr("")  // newline after progress
            logStderr("  \(lang): \(langVectors) vectors generated.")
        }

        let elapsed = (ContinuousClock.now - startTime).seconds
        logStderr("Done: \(totalVectors) vectors in \(formatTime(elapsed))")

        // JSON summary to stdout
        let summary: [String: Any] = [
            "total_vectors": totalVectors,
            "languages": targetLangs,
            "elapsed_seconds": round(elapsed * 10) / 10,
            "db_path": dbPath,
            "embed_tier": embedTier.stringValue,
        ]
        let jsonData = try JSONSerialization.data(withJSONObject: summary, options: [.sortedKeys])
        print(String(data: jsonData, encoding: .utf8)!)
    }

}
