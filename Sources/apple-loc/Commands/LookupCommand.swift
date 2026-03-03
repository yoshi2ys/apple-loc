import ArgumentParser
import Foundation
import GRDB

struct LookupCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "lookup",
        abstract: "Look up translations by source key or target text."
    )

    @Option(name: .long, help: "Source key to search (supports %% wildcards for LIKE). Exclusive with --target.")
    var key: String?

    @Option(name: .long, help: "Target text to reverse-lookup (supports %% wildcards). Exclusive with --key.")
    var target: String?

    @Option(name: .long, help: "Output language filter (comma-separated). Omit to show all languages.")
    var lang: String?

    @Option(name: .long, help: "Filter by bundle/framework name.")
    var framework: String?

    @Option(name: .long, help: "Filter by platform (e.g. \"ios26\").")
    var platform: String?

    @Flag(name: .long, help: "Wrap key/target with %% wildcards for substring matching.")
    var fuzzy: Bool = false

    @Flag(name: .customLong("internal"), help: "Include [Internal] entries (hidden by default).")
    var includeInternal: Bool = false

    @Option(name: .long, help: "Maximum number of results.")
    var limit: Int = 20

    @Option(name: .long, help: "SQLite database path.")
    var db: String = DatabaseManager.defaultDBPath

    func validate() throws {
        if key == nil && target == nil {
            throw ValidationError("Specify either --key or --target.")
        }
        if key != nil && target != nil {
            throw ValidationError("--key and --target are mutually exclusive.")
        }
    }

    func run() async throws {
        let dbPath = DatabaseManager.resolvePath(db)
        let dbQueue = try DatabaseManager.openDatabase(at: dbPath)

        let langFilter = lang.map { $0.normalizedLanguageSet }

        let results: [SearchResult] = try await dbQueue.read { db in
            let lookupResults: [LookupResult]

            if let key = key {
                lookupResults = try lookupByKey(key, in: db)
            } else {
                lookupResults = try lookupByTarget(target!, in: db)
            }

            let candidates = lookupResults.map {
                ResultFetcher.Candidate(sourceId: $0.id, distance: $0.matchRank)
            }
            return try ResultFetcher.fetch(
                candidates: candidates,
                in: db,
                langFilter: langFilter,
                frameworkFilter: framework,
                platformFilter: platform,
                limit: limit,
                includeInternal: includeInternal
            )
        }

        // Sort: match relevance first, then bundle priority, then alphabetical
        let sorted = results.sorted { a, b in
            let da = a.distance ?? Double.greatestFiniteMagnitude
            let db = b.distance ?? Double.greatestFiniteMagnitude
            if da != db { return da < db }
            let pa = BundlePriority.from(bundleName: a.bundleName)
            let pb = BundlePriority.from(bundleName: b.bundleName)
            return pa != pb ? pa < pb : a.source < b.source
        }
        try ResultsOutput(results: sorted).printJSON()
    }

    // MARK: - Private

    private typealias LookupResult = (id: Int64, matchRank: Double?)

    /// Search by source key (exact or LIKE)
    private func lookupByKey(_ key: String, in db: Database) throws -> [LookupResult] {
        if fuzzy {
            // Ranked fuzzy: exact(0) > prefix(1) > substring(2)
            var sql = """
                SELECT id,
                  CASE
                    WHEN source = ? THEN 0.0
                    WHEN source LIKE ? THEN 1.0
                    ELSE 2.0
                  END AS match_rank
                FROM source_strings
                WHERE source LIKE ?
                """
            var args: [any DatabaseValueConvertible] = [key, "\(key)%", "%\(key)%"]

            if !includeInternal {
                sql += " AND source NOT LIKE '[Internal]%'"
            }
            if let p = platform {
                sql += " AND platform = ?"
                args.append(p)
            }
            sql += " ORDER BY match_rank, source LIMIT ?"
            args.append(limit)

            return try Row.fetchAll(db, sql: sql, arguments: StatementArguments(args))
                .map { ($0["id"] as Int64, $0["match_rank"] as Double?) }
        }

        // Non-fuzzy: exact match or user-provided wildcards
        let useLike = key.contains("%")
        var sql = "SELECT id FROM source_strings WHERE "
        sql += useLike ? "source LIKE ?" : "source = ?"
        var args: [any DatabaseValueConvertible] = [key]

        if !includeInternal {
            sql += " AND source NOT LIKE '[Internal]%'"
        }
        if let p = platform {
            sql += " AND platform = ?"
            args.append(p)
        }
        sql += " LIMIT ?"
        args.append(limit)

        return try Row.fetchAll(db, sql: sql, arguments: StatementArguments(args))
            .map { ($0["id"] as Int64, nil as Double?) }
    }

    /// Reverse lookup by target text
    private func lookupByTarget(_ target: String, in db: Database) throws -> [LookupResult] {
        if fuzzy {
            // Ranked fuzzy: exact(0) > prefix(1) > substring(2)
            var sql = """
                SELECT t.source_id AS id,
                  MIN(CASE
                    WHEN t.target = ? THEN 0.0
                    WHEN t.target LIKE ? THEN 1.0
                    ELSE 2.0
                  END) AS match_rank
                FROM translations t
                JOIN source_strings ss ON ss.id = t.source_id
                WHERE t.target LIKE ?
                """
            var args: [any DatabaseValueConvertible] = [target, "\(target)%", "%\(target)%"]

            if !includeInternal {
                sql += " AND ss.source NOT LIKE '[Internal]%'"
            }
            if let p = platform {
                sql += " AND ss.platform = ?"
                args.append(p)
            }
            sql += " GROUP BY t.source_id ORDER BY match_rank LIMIT ?"
            args.append(limit)

            return try Row.fetchAll(db, sql: sql, arguments: StatementArguments(args))
                .map { ($0["id"] as Int64, $0["match_rank"] as Double?) }
        }

        // Non-fuzzy: exact match or user-provided wildcards
        let useLike = target.contains("%")
        var sql = """
            SELECT DISTINCT t.source_id FROM translations t
            JOIN source_strings ss ON ss.id = t.source_id
            WHERE
        """
        sql += useLike ? " t.target LIKE ?" : " t.target = ?"
        var args: [any DatabaseValueConvertible] = [target]

        if !includeInternal {
            sql += " AND ss.source NOT LIKE '[Internal]%'"
        }
        if let p = platform {
            sql += " AND ss.platform = ?"
            args.append(p)
        }
        sql += " LIMIT ?"
        args.append(limit)

        return try Row.fetchAll(db, sql: sql, arguments: StatementArguments(args))
            .map { ($0["source_id"] as Int64, nil as Double?) }
    }
}
