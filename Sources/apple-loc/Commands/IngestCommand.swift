import ArgumentParser
import Foundation
import GRDB
import NaturalLanguage

struct IngestCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "ingest",
        abstract: "Ingest Apple localization data from PostgreSQL dump files."
    )

    @Option(name: .long, help: "Directory containing data.sql.* files.")
    var dataDir: String

    @Option(name: .long, help: "Comma-separated language codes to import. Omit to import all languages.")
    var langs: String?

    @Option(name: .long, help: "Comma-separated platform filter (e.g. \"ios26,macos26\"). Omit to import all.")
    var platform: String?

    @Option(name: .long, help: "SQLite database output path.")
    var db: String = DatabaseManager.defaultDBPath

    @Flag(name: .long, help: "Overwrite existing database.")
    var force: Bool = false

    @Option(name: .long, help: "Batch size for inserts.")
    var batchSize: Int = 5000

    @Option(name: .long, help: "Number of parallel embedding workers per language (default: CPU cores / 2).")
    var concurrency: Int?

    @Option(name: .long, help: "Embedding mode: \"none\" (skip), \"en\" (English only, default), or comma-separated codes (e.g. \"ja,en\").")
    var embed: EmbedMode = .en

    @Flag(name: .long, help: "Skip source_bundles table (saves space, but --framework filter only matches the primary bundle).")
    var compact: Bool = false

    func run() async throws {
        let dbPath = DatabaseManager.resolvePath(db)
        let fm = FileManager.default

        // Check existing DB
        if fm.fileExists(atPath: dbPath) {
            if force {
                try fm.removeItem(atPath: dbPath)
            } else {
                throw ValidationError("Database already exists at '\(dbPath)'. Use --force to overwrite.")
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
                log("Auto-adding '\(el)' (required for embedding).")
                codes.append(el)
            }
            langCodes = codes
            log("Importing \(codes.count) languages: \(codes.joined(separator: ", "))")
        } else {
            langCodes = nil
            log("Importing all languages.")
        }

        // Load embedding workers
        var embedWorkers: [EmbedWorker] = []
        switch embed {
        case .none:
            log("Embedding: disabled (--embed none)")

        case .en, .langs:
            let targetLangs = embed.languages()
            let workerCount = concurrency ?? max(1, ProcessInfo.processInfo.activeProcessorCount / 2)

            let maxWorkers = ProcessInfo.processInfo.activeProcessorCount
            let totalWorkers = workerCount * targetLangs.count
            if totalWorkers > maxWorkers {
                throw ValidationError(
                    "Too many embedding workers: \(workerCount) × \(targetLangs.count) languages = \(totalWorkers) (max: \(maxWorkers)). Reduce --concurrency or the number of languages."
                )
            }

            var loadedLangCount = 0
            for code in targetLangs {
                guard let resolvedLang = EmbeddingService.resolveLanguage(for: code) else {
                    log("Warning: no embedding model for '\(code)', skipping.")
                    continue
                }
                let usedFallback = resolvedLang.rawValue != code
                for i in 0..<workerCount {
                    guard let svc = EmbeddingService(language: resolvedLang) else { break }
                    try svc.load()
                    embedWorkers.append(EmbedWorker(
                        language: code, service: svc,
                        queue: DispatchQueue(label: "embed-\(code)-\(i)")
                    ))
                }
                if embedWorkers.last?.language == code {
                    loadedLangCount += 1
                    if usedFallback { log("  '\(code)' → using '\(resolvedLang.rawValue)' embedding model") }
                }
            }
            log("Embedding workers: \(workerCount) × \(loadedLangCount) languages = \(embedWorkers.count) total")
        }

        // Open DB and create schema
        let dbQueue = try DatabaseManager.openDatabase(at: dbPath, create: true)
        try DatabaseManager.createSchema(in: dbQueue, compact: compact)
        log("Database created at \(dbPath)\(compact ? " (compact mode)" : "")")

        // Parse and ingest
        var parser = SQLDumpParser(
            dataDir: dataDir,
            allowedLanguages: langCodes.map { Set($0.flatMap(\.languageCodeVariants)) },
            allowedPlatforms: platform.map { Set($0.commaSeparated) }
        )
        parser.onTableFound = { table, processing in
            log("  \(processing ? "▶" : "⏭") \(table)")
        }

        let startTime = ContinuousClock.now
        var batch: [ParsedRow] = []
        batch.reserveCapacity(batchSize)
        var stats = IngestStats()
        var dedupCache = DedupCache()
        let workersByLang = Dictionary(grouping: embedWorkers, by: \.language)

        _ = try parser.parse { row in
            batch.append(row)
            if batch.count >= batchSize {
                try processBatch(batch, into: dbQueue, workersByLang: workersByLang, stats: &stats, dedupCache: &dedupCache, compact: compact)
                batch.removeAll(keepingCapacity: true)

                let elapsed = ContinuousClock.now - startTime
                let secs = elapsed.seconds
                log("\r  \(stats.totalRows) rows, \(stats.sources) sources, \(stats.vectors) vectors | \(formatRate(stats.totalRows, secs)) rows/s | \(formatTime(secs))", terminator: "")
            }
        }

        // Flush remaining
        if !batch.isEmpty {
            try processBatch(batch, into: dbQueue, workersByLang: workersByLang, stats: &stats, dedupCache: &dedupCache, compact: compact)
        }

        let elapsed = (ContinuousClock.now - startTime).seconds
        log("")  // newline after progress
        log("Done: \(stats.totalRows) rows, \(stats.sources) sources, \(stats.vectors) vectors in \(formatTime(elapsed))")
        log("Dedup cache: \(dedupCache.entries.count) unique (source, platform) pairs")

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
            "embed_langs": embedWorkers.map(\.language),
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

    private struct EmbedWorker {
        let language: String
        let service: EmbeddingService
        let queue: DispatchQueue
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

    // MARK: - Batch Processing

    private func processBatch(
        _ rows: [ParsedRow],
        into dbQueue: DatabaseQueue,
        workersByLang: [String: [EmbedWorker]],
        stats: inout IngestStats,
        dedupCache: inout DedupCache,
        compact: Bool = false
    ) throws {
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

            for lang in workersByLang.keys {
                // Skip if already vectorized for this (sourceId, language)
                if let sid = cachedSourceId,
                   dedupCache.vectorized.contains(VecKey(sourceId: sid, language: lang)) {
                    continue
                }
                // Find translation in this language within the batch entry
                if let trans = p.entry.translations.first(where: { $0.language == lang }) {
                    embeddingTargets.append(EmbedTarget(key: p.key, language: lang, text: trans.target.lowercased()))
                }
            }
        }

        struct EmbedResultKey: Hashable {
            let key: DedupKey
            let language: String
        }
        var embeddings: [EmbedResultKey: [Float]] = [:]

        if !embeddingTargets.isEmpty {
            // Group targets by language, then distribute across that language's workers
            let targetsByLang = Dictionary(grouping: embeddingTargets.indices, by: { embeddingTargets[$0].language })

            for (lang, indices) in targetsByLang {
                let workers = workersByLang[lang]!
                if workers.count == 1 {
                    // Single-worker path (no async overhead)
                    for i in indices {
                        let t = embeddingTargets[i]
                        if let vec = try? workers[0].service.embed(t.text) {
                            embeddings[EmbedResultKey(key: t.key, language: lang)] = vec
                        }
                    }
                } else {
                    // Parallel embedding — one serial queue per worker
                    let count = indices.count
                    nonisolated(unsafe) let results = UnsafeMutableBufferPointer<[Float]?>.allocate(capacity: count)
                    results.initialize(repeating: nil)
                    defer { results.deallocate() }

                    let targets = embeddingTargets  // local let copy for Sendable closure
                    let group = DispatchGroup()
                    for (j, idx) in indices.enumerated() {
                        let workerIdx = j % workers.count
                        group.enter()
                        workers[workerIdx].queue.async {
                            results[j] = try? workers[workerIdx].service.embed(targets[idx].text)
                            group.leave()
                        }
                    }
                    group.wait()

                    for (j, idx) in indices.enumerated() {
                        if let vec = results[j] {
                            let t = embeddingTargets[idx]
                            embeddings[EmbedResultKey(key: t.key, language: lang)] = vec
                        }
                    }
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
                for lang in workersByLang.keys {
                    let vecKey = VecKey(sourceId: sourceId, language: lang)
                    let embedKey = EmbedResultKey(key: p.key, language: lang)
                    if let vec = embeddings[embedKey], !dedupCache.vectorized.contains(vecKey) {
                        // Get or create mapping ID (globally unique rowid for vec table)
                        try db.execute(sql: """
                            INSERT INTO vec_mapping(source_id, language) VALUES (?, ?)
                            ON CONFLICT(source_id, language) DO NOTHING
                        """, arguments: [sourceId, lang])
                        let mapRow = try Row.fetchOne(db, sql: """
                            SELECT id FROM vec_mapping WHERE source_id = ? AND language = ?
                        """, arguments: [sourceId, lang])!
                        let mappingId: Int64 = mapRow["id"]

                        try db.execute(
                            sql: "DELETE FROM vec_source_strings WHERE rowid = ?",
                            arguments: [mappingId]
                        )
                        try db.execute(
                            sql: "INSERT INTO vec_source_strings(rowid, language, embedding) VALUES (?, ?, ?)",
                            arguments: [mappingId, lang, vec.asData]
                        )
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

    // MARK: - Helpers

    private func log(_ message: String, terminator: String = "\n") {
        FileHandle.standardError.write(Data((message + terminator).utf8))
    }

    private func formatTime(_ seconds: Double) -> String {
        let m = Int(seconds) / 60
        let s = Int(seconds) % 60
        return m > 0 ? "\(m)m\(s)s" : "\(s)s"
    }

    private func formatRate(_ count: Int, _ seconds: Double) -> String {
        guard seconds > 0 else { return "—" }
        return String(format: "%.0f", Double(count) / seconds)
    }
}

extension Duration {
    var seconds: Double {
        Double(components.seconds) + Double(components.attoseconds) / 1e18
    }
}
