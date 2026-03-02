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
            let sourceIds: [Int64]

            if let key = key {
                sourceIds = try lookupByKey(key, in: db)
            } else {
                sourceIds = try lookupByTarget(target!, in: db)
            }

            let candidates = sourceIds.map { ResultFetcher.Candidate(sourceId: $0, distance: nil) }
            return try ResultFetcher.fetch(
                candidates: candidates,
                in: db,
                langFilter: langFilter,
                frameworkFilter: framework,
                platformFilter: platform,
                limit: limit
            )
        }

        try ResultsOutput(results: results).printJSON()
    }

    // MARK: - Private

    /// Search by source key (exact or LIKE)
    private func lookupByKey(_ key: String, in db: Database) throws -> [Int64] {
        let effectiveKey = fuzzy ? "%\(key)%" : key
        let useLike = effectiveKey.contains("%")
        var sql = "SELECT id FROM source_strings WHERE "
        sql += useLike ? "source LIKE ?" : "source = ?"
        var args: [any DatabaseValueConvertible] = [effectiveKey]

        if let p = platform {
            sql += " AND platform = ?"
            args.append(p)
        }
        sql += " LIMIT ?"
        args.append(limit)

        return try Row.fetchAll(db, sql: sql, arguments: StatementArguments(args)).map { $0["id"] }
    }

    /// Reverse lookup by target text
    private func lookupByTarget(_ target: String, in db: Database) throws -> [Int64] {
        let effectiveTarget = fuzzy ? "%\(target)%" : target
        let useLike = effectiveTarget.contains("%")
        var sql = """
            SELECT DISTINCT t.source_id FROM translations t
            JOIN source_strings ss ON ss.id = t.source_id
            WHERE
        """
        sql += useLike ? " t.target LIKE ?" : " t.target = ?"
        var args: [any DatabaseValueConvertible] = [effectiveTarget]

        if let p = platform {
            sql += " AND ss.platform = ?"
            args.append(p)
        }
        sql += " LIMIT ?"
        args.append(limit)

        return try Row.fetchAll(db, sql: sql, arguments: StatementArguments(args)).map { $0["source_id"] }
    }
}
