import ArgumentParser
import Foundation
import GRDB
import NaturalLanguage

struct IngestCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "ingest",
        abstract: "Ingest Apple localization data from JSON files."
    )

    @Option(name: .long, help: "Directory containing JSON data files.")
    var dataDir: String

    @Option(name: .long, help: "Comma-separated language codes to import. Omit to import all languages.")
    var langs: String?

    @Option(name: .long, help: "Comma-separated platform filter (e.g. \"ios26,macos26\"). Omit to import all.")
    var platform: String?

    @Option(name: .long, help: "SQLite database output path.")
    var db: String = DatabaseManager.defaultDBPath

    @Flag(name: .long, help: "Overwrite existing database.")
    var force: Bool = false

    @Flag(name: .long, help: "Append to existing database instead of creating a new one.")
    var append: Bool = false

    @Option(name: .long, help: "Batch size for inserts.")
    var batchSize: Int = 5000

    @Option(name: .long, help: "Number of parallel embedding workers per language (default: CPU cores / 2).")
    var concurrency: Int?

    @Option(name: .long, help: "Embedding mode: \"none\" (skip), \"en\" (English only, default), or comma-separated codes (e.g. \"ja,en\").")
    var embed: EmbedMode = .en

    @Flag(name: .long, help: "Skip source_bundles table (saves space, but --framework filter only matches the primary bundle).")
    var compact: Bool = false

    func run() async throws {
        // Validate flags
        if append && force {
            throw ValidationError("--append and --force are mutually exclusive.")
        }

        let dbPath = DatabaseManager.resolvePath(db)
        let fm = FileManager.default
        let dbExists = fm.fileExists(atPath: dbPath)

        // Check existing DB
        if append {
            guard dbExists else {
                throw ValidationError("Database not found at '\(dbPath)'. --append requires an existing database.")
            }
        } else if dbExists {
            if force {
                try fm.removeItem(atPath: dbPath)
            } else {
                throw ValidationError("Database already exists at '\(dbPath)'. Use --force to overwrite or --append to add data.")
            }
        }

        // Determine which languages embeddings will cover
        let embedLangs = embed.languages()

        // Parse language filter
        let langCodes: [String]?
        if let langsArg = langs {
            var codes = langsArg.commaSeparated
            // Auto-add embed languages to ensure they're imported
            for el in embedLangs where !codes.contains(el) {
                logStderr("Auto-adding '\(el)' (required for embedding).")
                codes.append(el)
            }
            langCodes = codes
            logStderr("Importing \(codes.count) languages: \(codes.joined(separator: ", "))")
        } else {
            langCodes = nil
            logStderr("Importing all languages.")
        }

        // Load embedding workers via pool
        let pool: EmbedWorkerPool?
        switch embed {
        case .none:
            logStderr("Embedding: disabled (--embed none)")
            pool = nil

        case .en, .langs:
            pool = try EmbedWorkerPool(languages: embed.languages(), concurrency: concurrency, log: { logStderr($0) })
        }

        // Open DB and create schema
        let dbQueue = try DatabaseManager.openDatabase(at: dbPath, create: true)

        // Auto-detect compact mode on append
        let effectiveCompact: Bool
        if append {
            effectiveCompact = try await dbQueue.read { db in
                !(try DatabaseManager.tableExists("source_bundles", in: db))
            }
            if effectiveCompact != compact && compact {
                logStderr("Warning: ignoring --compact (existing DB already has source_bundles).")
            }
        } else {
            effectiveCompact = compact
        }

        try DatabaseManager.createSchema(in: dbQueue, compact: effectiveCompact)
        logStderr(append
            ? "Appending to database at \(dbPath)"
            : "Database created at \(dbPath)\(effectiveCompact ? " (compact mode)" : "")")

        let allowedLangSet = langCodes.map { Set($0.flatMap(\.languageCodeVariants)) }
        let allowedPlatformSet = platform.map { Set($0.commaSeparated) }

        let startTime = ContinuousClock.now
        var batch: [ParsedRow] = []
        batch.reserveCapacity(batchSize)
        var stats = IngestStats()
        var dedupCache = DedupCache()

        let jsonParser = JSONDataParser(
            dataDir: JSONDataParser.resolveDataDir(dataDir),
            allowedLanguages: allowedLangSet,
            allowedPlatforms: allowedPlatformSet
        )
        _ = try jsonParser.parse { fileBatch in
            batch.append(contentsOf: fileBatch)
            if batch.count >= batchSize {
                try processBatch(batch, into: dbQueue, pool: pool, stats: &stats, dedupCache: &dedupCache, compact: effectiveCompact)
                batch.removeAll(keepingCapacity: true)
                printProgress(stats: stats, since: startTime)
            }
        }

        // Flush remaining
        if !batch.isEmpty {
            try processBatch(batch, into: dbQueue, pool: pool, stats: &stats, dedupCache: &dedupCache, compact: effectiveCompact)
        }

        let elapsed = (ContinuousClock.now - startTime).seconds
        logStderr("")  // newline after progress
        logStderr("Done: \(stats.totalRows) rows, \(stats.sources) sources, \(stats.vectors) vectors in \(formatTime(elapsed))")
        logStderr("Dedup cache: \(dedupCache.entries.count) unique (source, platform) pairs")

        // Output summary as JSON to stdout
        let embedModeStr: String
        switch embed {
        case .none: embedModeStr = "none"
        case .en: embedModeStr = "en"
        case .langs(_): embedModeStr = "langs"
        }

        let summary: [String: Any] = [
            "total_rows": stats.totalRows,
            "source_strings": stats.sources,
            "total_vectors": stats.vectors,
            "dedup_entries": dedupCache.entries.count,
            "languages": langCodes as Any? ?? "all",
            "platform": platform ?? "all",
            "elapsed_seconds": round(elapsed * 10) / 10,
            "db_path": dbPath,
            "embed_mode": embedModeStr,
            "embed_langs": pool.map { Array($0.supportedLanguages.sorted()) } ?? [],
        ]
        let jsonData = try JSONSerialization.data(withJSONObject: summary, options: [.sortedKeys])
        print(String(data: jsonData, encoding: .utf8)!)
    }

    // MARK: - Embed Mode

    enum EmbedMode: ExpressibleByArgument, Sendable {
        case none
        case en
        case langs([String])

        init?(argument: String) {
            switch argument.lowercased() {
            case "none": self = .none
            case "en": self = .en
            default: self = .langs(argument.commaSeparated)
            }
        }

        var defaultValueDescription: String { "en" }

        /// Languages this mode embeds.
        func languages() -> [String] {
            switch self {
            case .none: return []
            case .en: return ["en"]
            case .langs(let codes): return codes
            }
        }
    }

    // MARK: - Dedup Types

    private struct DedupKey: Hashable, Sendable {
        let source: String
        let platform: String
    }

    private struct DedupEntry {
        var sourceId: Int64
        var bundlePriority: Int
    }

    private struct VecKey: Hashable {
        let sourceId: Int64
        let language: String
    }

    private struct DedupCache {
        var entries: [DedupKey: DedupEntry] = [:]
        var vectorized: Set<VecKey> = []  // (sourceId, language) pairs that have vectors
    }

    private struct IngestStats {
        var totalRows = 0
        var sources = 0
        var vectors = 0
    }

    // MARK: - Progress

    private func printProgress(stats: IngestStats, since startTime: ContinuousClock.Instant) {
        let elapsed = ContinuousClock.now - startTime
        let secs = elapsed.seconds
        logStderr("\r  \(stats.totalRows) rows, \(stats.sources) sources, \(stats.vectors) vectors | \(formatRate(stats.totalRows, secs)) rows/s | \(formatTime(secs))", terminator: "")
    }

    // MARK: - Batch Processing

    private func processBatch(
        _ rows: [ParsedRow],
        into dbQueue: DatabaseQueue,
        pool: EmbedWorkerPool?,
        stats: inout IngestStats,
        dedupCache: inout DedupCache,
        compact: Bool = false
    ) throws {
        let embedLangs = pool?.supportedLanguages ?? []
        // Step 1: In-batch dedup — for each (source, platform), keep the best bundle
        struct BatchEntry {
            var row: ParsedRow
            var bundlePriority: Int
            var translations: [(language: String, target: String)]  // all languages for this source
            var bundleNames: Set<String>  // all originating bundles for this (source, platform)
        }

        var batchBest: [DedupKey: BatchEntry] = [:]

        for row in rows {
            let key = DedupKey(source: row.source, platform: row.platform)
            let priority = BundlePriority.from(bundleName: row.bundleName).rawValue

            if var existing = batchBest[key] {
                // Always collect translations
                existing.translations.append((row.language, row.target))
                existing.bundleNames.insert(row.bundleName)
                // Upgrade bundle if this row has better priority
                if priority < existing.bundlePriority {
                    existing.row = row
                    existing.bundlePriority = priority
                }
                batchBest[key] = existing
            } else {
                batchBest[key] = BatchEntry(
                    row: row,
                    bundlePriority: priority,
                    translations: [(row.language, row.target)],
                    bundleNames: [row.bundleName]
                )
            }
        }

        // Step 2: Filter against cross-batch cache — skip if cache already has better priority
        struct PendingSource {
            let key: DedupKey
            let entry: BatchEntry
            let needsUpsert: Bool  // false if cache hit with equal/better priority
        }

        var pending: [PendingSource] = []
        for (key, entry) in batchBest {
            if let cached = dedupCache.entries[key] {
                if entry.bundlePriority < cached.bundlePriority {
                    // This batch has better priority → needs DB UPSERT
                    pending.append(PendingSource(key: key, entry: entry, needsUpsert: true))
                } else {
                    // Cache already has equal/better priority → only insert translations
                    pending.append(PendingSource(key: key, entry: entry, needsUpsert: false))
                }
            } else {
                // New source → needs INSERT
                pending.append(PendingSource(key: key, entry: entry, needsUpsert: true))
            }
        }

        // Step 3: Compute embeddings (multi-language, parallel)
        struct EmbedTarget {
            let key: DedupKey
            let language: String
            let text: String
        }
        var embeddingTargets: [EmbedTarget] = []

        for p in pending {
            guard p.needsUpsert else { continue }
            let cachedSourceId = dedupCache.entries[p.key]?.sourceId

            for lang in embedLangs {
                // Skip if already vectorized for this (sourceId, language)
                if let sid = cachedSourceId,
                   dedupCache.vectorized.contains(VecKey(sourceId: sid, language: lang)) {
                    continue
                }
                // Find translation in this language within the batch entry
                if let trans = p.entry.translations.first(where: { $0.language == lang }) {
                    let resolved = StructuredTarget.resolveForEmbedding(trans.target)
                    embeddingTargets.append(EmbedTarget(key: p.key, language: lang, text: resolved.lowercased()))
                }
            }
        }

        struct EmbedResultKey: Hashable {
            let key: DedupKey
            let language: String
        }
        var embeddings: [EmbedResultKey: [Float]] = [:]

        if !embeddingTargets.isEmpty, let pool {
            let poolTargets = embeddingTargets.map { (language: $0.language, text: $0.text) }
            let poolResults = pool.embed(targets: poolTargets)

            for (i, target) in embeddingTargets.enumerated() {
                if let vec = poolResults[i] {
                    embeddings[EmbedResultKey(key: target.key, language: target.language)] = vec
                }
            }
        }

        // Step 4: DB transaction — UPSERT sources, insert translations, insert vectors
        var newSourcesInBatch = 0
        var newVectors = 0

        try dbQueue.inTransaction(.immediate) { db in
            for p in pending {
                let sourceId: Int64

                if p.needsUpsert {
                    // Priority-conditional UPSERT: only overwrite if new priority is better
                    try db.execute(sql: """
                        INSERT INTO source_strings(group_id, source, bundle_name, bundle_priority, file_name, platform)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source, platform) DO UPDATE SET
                            group_id = excluded.group_id,
                            bundle_name = excluded.bundle_name,
                            bundle_priority = excluded.bundle_priority,
                            file_name = excluded.file_name
                        WHERE excluded.bundle_priority < source_strings.bundle_priority
                    """, arguments: [
                        p.entry.row.groupId, p.entry.row.source,
                        p.entry.row.bundleName, p.entry.bundlePriority,
                        p.entry.row.fileName, p.entry.row.platform,
                    ])

                    let idRow = try Row.fetchOne(db, sql: """
                        SELECT id FROM source_strings WHERE source = ? AND platform = ?
                    """, arguments: [p.entry.row.source, p.entry.row.platform])!
                    sourceId = idRow["id"]
                    newSourcesInBatch += 1
                } else {
                    // Use cached sourceId
                    sourceId = dedupCache.entries[p.key]!.sourceId
                }

                // UPSERT translations
                for trans in p.entry.translations {
                    try db.execute(sql: """
                        INSERT INTO translations(source_id, language, target)
                        VALUES (?, ?, ?)
                        ON CONFLICT(source_id, language) DO UPDATE SET target = excluded.target
                    """, arguments: [sourceId, trans.language, trans.target])
                }

                // Update dedup cache
                dedupCache.entries[p.key] = DedupEntry(
                    sourceId: sourceId,
                    bundlePriority: min(
                        p.entry.bundlePriority,
                        dedupCache.entries[p.key]?.bundlePriority ?? .max
                    )
                )

                // Record all originating bundles (INSERT OR IGNORE handles cross-batch dedup)
                if !compact {
                    for bundleName in p.entry.bundleNames {
                        try db.execute(sql: """
                            INSERT INTO source_bundles(source_id, bundle_name)
                            VALUES (?, ?)
                            ON CONFLICT(source_id, bundle_name) DO NOTHING
                        """, arguments: [sourceId, bundleName])
                    }
                }

                // Insert vectors via vec_mapping for each language that has an embedding
                for lang in embedLangs {
                    let vecKey = VecKey(sourceId: sourceId, language: lang)
                    let embedKey = EmbedResultKey(key: p.key, language: lang)
                    if let vec = embeddings[embedKey], !dedupCache.vectorized.contains(vecKey) {
                        try DatabaseManager.upsertVector(in: db, sourceId: sourceId, language: lang, embedding: vec)
                        dedupCache.vectorized.insert(vecKey)
                        newVectors += 1
                    }
                }
            }

            return .commit
        }

        stats.totalRows += rows.count
        stats.sources += newSourcesInBatch
        stats.vectors += newVectors
    }

}
