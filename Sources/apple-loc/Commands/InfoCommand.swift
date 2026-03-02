import ArgumentParser
import Foundation
import GRDB

struct InfoCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "info",
        abstract: "Show database metadata as JSON (platforms, languages, counts)."
    )

    @Option(name: .long, help: "SQLite database path.")
    var db: String = DatabaseManager.defaultDBPath

    func run() async throws {
        let dbPath = DatabaseManager.resolvePath(db)

        guard FileManager.default.fileExists(atPath: dbPath) else {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let data = try encoder.encode(InfoError(error: "no_database", dbPath: dbPath))
            print(String(data: data, encoding: .utf8)!)
            return
        }

        let dbQueue = try DatabaseManager.openDatabase(at: dbPath)

        let output: InfoOutput = try await dbQueue.read { db in
            let platforms = try String.fetchAll(db, sql:
                "SELECT DISTINCT platform FROM source_strings"
            ).sorted { a, b in
                let am = a.hasPrefix("macos"), bm = b.hasPrefix("macos")
                if am != bm { return am }
                let va = Int(a.drop(while: { !$0.isNumber })) ?? 0
                let vb = Int(b.drop(while: { !$0.isNumber })) ?? 0
                return va > vb
            }
            let languages = try String.fetchAll(db, sql:
                "SELECT DISTINCT language FROM translations ORDER BY language")
            let embeddingLanguages = try String.fetchAll(db, sql:
                "SELECT DISTINCT language FROM vec_mapping ORDER BY language")
            let sourceCount = try Int.fetchOne(db, sql:
                "SELECT COUNT(*) FROM source_strings")!
            let translationCount = try Int.fetchOne(db, sql:
                "SELECT COUNT(*) FROM translations")!
            let vectorCount = try Int.fetchOne(db, sql:
                "SELECT COUNT(*) FROM vec_mapping")!

            return InfoOutput(
                platforms: platforms,
                languages: languages,
                embeddingLanguages: embeddingLanguages,
                counts: InfoCounts(
                    sourceStrings: sourceCount,
                    translations: translationCount,
                    vectors: vectorCount
                )
            )
        }

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(output)
        print(String(data: data, encoding: .utf8)!)
    }
}
