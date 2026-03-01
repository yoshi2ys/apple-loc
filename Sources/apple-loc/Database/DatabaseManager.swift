import Foundation
import GRDB
import Csqlitevec

/// Manages the SQLite database with sqlite-vec extension for vector search.
enum DatabaseManager {

    /// Open (or create) the database at the given path.
    /// sqlite-vec is initialized on each connection via prepareDatabase.
    static func openDatabase(at path: String, create: Bool = false) throws -> DatabaseQueue {
        let url = URL(fileURLWithPath: resolvePath(path))
        let dir = url.deletingLastPathComponent()
        if create {
            try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        }
        var config = Configuration()
        config.prepareDatabase { db in
            // Register sqlite-vec on this connection (compiled with SQLITE_CORE).
            let rc = sqlite3_vec_init(db.sqliteConnection, nil, nil)
            guard rc == SQLITE_OK else {
                throw DatabaseError(message: "sqlite3_vec_init failed (code \(rc))")
            }
            try db.execute(sql: "PRAGMA journal_mode = WAL")
        }
        return try DatabaseQueue(path: url.path, configuration: config)
    }

    /// Create the schema (source_strings + translations + vec_source_strings).
    /// When `compact` is true, the `source_bundles` table is omitted to save space.
    static func createSchema(in db: DatabaseQueue, compact: Bool = false) throws {
        try db.write { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS source_strings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER,
                    source TEXT NOT NULL,
                    bundle_name TEXT NOT NULL,
                    bundle_priority INTEGER NOT NULL DEFAULT 5,
                    file_name TEXT,
                    platform TEXT NOT NULL,
                    UNIQUE(source, platform)
                );
                CREATE INDEX IF NOT EXISTS idx_ss_source ON source_strings(source);
                CREATE INDEX IF NOT EXISTS idx_ss_platform ON source_strings(platform);
                CREATE INDEX IF NOT EXISTS idx_ss_bundle ON source_strings(bundle_name);

                CREATE TABLE IF NOT EXISTS translations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL REFERENCES source_strings(id),
                    language TEXT NOT NULL,
                    target TEXT NOT NULL,
                    UNIQUE(source_id, language)
                );
                CREATE INDEX IF NOT EXISTS idx_trans_source_id ON translations(source_id);
                CREATE INDEX IF NOT EXISTS idx_trans_lang ON translations(language);
                CREATE INDEX IF NOT EXISTS idx_trans_target ON translations(target);

                CREATE TABLE IF NOT EXISTS vec_mapping (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL,
                    language TEXT NOT NULL,
                    UNIQUE(source_id, language)
                );
                CREATE INDEX IF NOT EXISTS idx_vm_source ON vec_mapping(source_id);
                """)
            // sqlite-vec virtual table: 512-dim, partitioned by language
            // rowid = vec_mapping.id (globally unique, partition key for efficient KNN)
            try db.execute(sql: """
                CREATE VIRTUAL TABLE IF NOT EXISTS vec_source_strings USING vec0(
                    language text partition key,
                    embedding float[512]
                );
                """)

            if !compact {
                // Many-to-many: all originating bundles per source string.
                // Without this, only the highest-priority bundle_name survives UPSERT.
                try db.execute(sql: """
                    CREATE TABLE IF NOT EXISTS source_bundles (
                        source_id INTEGER NOT NULL REFERENCES source_strings(id),
                        bundle_name TEXT NOT NULL,
                        UNIQUE(source_id, bundle_name)
                    );
                    CREATE INDEX IF NOT EXISTS idx_sb_source ON source_bundles(source_id);
                    CREATE INDEX IF NOT EXISTS idx_sb_bundle ON source_bundles(bundle_name);
                    """)
            }
        }
    }

    /// Check whether a table exists in the database.
    static func tableExists(_ name: String, in db: Database) throws -> Bool {
        try Int.fetchOne(db, sql: """
            SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?
        """, arguments: [name])! > 0
    }

    /// Resolve ~ to the home directory.
    static func resolvePath(_ path: String) -> String {
        if path.hasPrefix("~/") {
            return FileManager.default.homeDirectoryForCurrentUser
                .appendingPathComponent(String(path.dropFirst(2))).path
        }
        return path
    }
}
