import ArgumentParser
import Foundation
import GRDB
import NaturalLanguage

struct SelftestCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "selftest",
        abstract: "Run internal integration tests.",
        shouldDisplay: false
    )

    func run() async throws {
        try testVecIntegration()
        try testMultiLangVec()
        try testFuzzyLookup()
        try testDedupPriority()
        try testEmbedding()
        try testParallelEmbedding()
        try testSourceBundles()
        try testInfo()
        try testAppendIngest()
        try testEmbedFromDB()
        try testEmbedSkipExisting()
        try testInternalFilter()
        try testJSONParser()
        try testPlatformConversion()
        try testStructuredTargetResolution()
        try testStructuredTargetOutput()
        try testEmbedTierClassifier()
        try testPagination()
        print("All selftests PASSED")
    }

    private func testVecIntegration() throws {
        let path = "/tmp/apple-loc-selftest.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        try dbQueue.writeWithoutTransaction { db in
            // Insert a source_string (source+platform unique, bundle_priority)
            try db.execute(
                sql: "INSERT INTO source_strings(group_id, source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?,?)",
                arguments: [1, "Camera", "UIKit.framework", BundlePriority.tier1.rawValue, "Localizable.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID

            // Insert translations
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "en", "Camera"]
            )
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "ja", "カメラ"]
            )

            // Verify translations
            let count = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM translations WHERE source_id = ?", arguments: [sourceId])!
            assert(count == 2, "FAIL: expected 2 translations, got \(count)")

            // Test UNIQUE(source, platform) constraint via priority-conditional UPSERT
            try db.execute(sql: """
                INSERT INTO source_strings(group_id, source, bundle_name, bundle_priority, file_name, platform)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, platform) DO UPDATE SET
                    bundle_name = excluded.bundle_name,
                    bundle_priority = excluded.bundle_priority
                WHERE excluded.bundle_priority < source_strings.bundle_priority
            """, arguments: [99, "Camera", "SomeApp.app", BundlePriority.app.rawValue, "L.strings", "ios26"])

            // Verify the original (higher priority) bundle was kept
            let row = try Row.fetchOne(db, sql: "SELECT bundle_name, bundle_priority FROM source_strings WHERE id = ?", arguments: [sourceId])!
            let bn: String = row["bundle_name"]
            assert(bn == "UIKit.framework", "FAIL: priority UPSERT should keep UIKit, got \(bn)")

            // Insert vector via vec_mapping (mapping table gives globally unique rowid)
            try db.execute(
                sql: "INSERT INTO vec_mapping(source_id, language) VALUES (?, ?)",
                arguments: [sourceId, "en"]
            )
            let mappingId = db.lastInsertedRowID

            let vec: [Float] = .init(repeating: 0.1, count: 512)
            try db.execute(
                sql: "INSERT INTO vec_source_strings(rowid, language, embedding) VALUES (?, ?, ?)",
                arguments: [mappingId, "en", vec.asData]
            )

            // Vector search with language filter
            let queryVec: [Float] = .init(repeating: 0.1, count: 512)
            let rows = try Row.fetchAll(db, sql: """
                SELECT rowid, distance FROM vec_source_strings
                WHERE embedding MATCH ? AND k = 1 AND language = ?
            """, arguments: [queryVec.asData, "en"])
            guard let result = rows.first else { fatalError("FAIL: vec - no results") }
            // Resolve mapping ID back to source_id
            let vecRowid: Int64 = result["rowid"]
            let mapRow = try Row.fetchOne(db, sql: "SELECT source_id FROM vec_mapping WHERE id = ?", arguments: [vecRowid])!
            let resolvedSourceId: Int64 = mapRow["source_id"]
            assert(resolvedSourceId == sourceId, "FAIL: vec mapping source_id mismatch")
        }
        print("[PASS] sqlite-vec integration (schema with language partition key)")
    }

    private func testMultiLangVec() throws {
        let path = "/tmp/apple-loc-multilang-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        try dbQueue.writeWithoutTransaction { db in
            try db.execute(
                sql: "INSERT INTO source_strings(group_id, source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?,?)",
                arguments: [1, "Home", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID

            // Create mapping entries for en and ja (different mapping IDs for same source)
            let enVec: [Float] = .init(repeating: 0.2, count: 512)
            let jaVec: [Float] = .init(repeating: 0.8, count: 512)

            try db.execute(sql: "INSERT INTO vec_mapping(source_id, language) VALUES (?, ?)", arguments: [sourceId, "en"])
            let enMappingId = db.lastInsertedRowID
            try db.execute(sql: "INSERT INTO vec_mapping(source_id, language) VALUES (?, ?)", arguments: [sourceId, "ja"])
            let jaMappingId = db.lastInsertedRowID

            try db.execute(
                sql: "INSERT INTO vec_source_strings(rowid, language, embedding) VALUES (?, ?, ?)",
                arguments: [enMappingId, "en", enVec.asData]
            )
            try db.execute(
                sql: "INSERT INTO vec_source_strings(rowid, language, embedding) VALUES (?, ?, ?)",
                arguments: [jaMappingId, "ja", jaVec.asData]
            )

            // KNN with language=en should return the English vector
            let enResult = try Row.fetchAll(db, sql: """
                SELECT rowid, distance FROM vec_source_strings
                WHERE embedding MATCH ? AND k = 1 AND language = ?
            """, arguments: [enVec.asData, "en"])
            assert(enResult.count == 1, "FAIL: multi-lang en search returned \(enResult.count) results")
            let enDist: Double = enResult[0]["distance"]
            assert(enDist < 0.001, "FAIL: en vector distance should be ~0, got \(enDist)")

            // Resolve mapping back to source_id
            let enRowid: Int64 = enResult[0]["rowid"]
            let enMap = try Row.fetchOne(db, sql: "SELECT source_id FROM vec_mapping WHERE id = ?", arguments: [enRowid])!
            assert((enMap["source_id"] as Int64) == sourceId, "FAIL: en mapping source_id mismatch")

            // KNN with language=ja should return the Japanese vector
            let jaResult = try Row.fetchAll(db, sql: """
                SELECT rowid, distance FROM vec_source_strings
                WHERE embedding MATCH ? AND k = 1 AND language = ?
            """, arguments: [jaVec.asData, "ja"])
            assert(jaResult.count == 1, "FAIL: multi-lang ja search returned \(jaResult.count) results")
            let jaDist: Double = jaResult[0]["distance"]
            assert(jaDist < 0.001, "FAIL: ja vector distance should be ~0, got \(jaDist)")

            let jaRowid: Int64 = jaResult[0]["rowid"]
            let jaMap = try Row.fetchOne(db, sql: "SELECT source_id FROM vec_mapping WHERE id = ?", arguments: [jaRowid])!
            assert((jaMap["source_id"] as Int64) == sourceId, "FAIL: ja mapping source_id mismatch")
        }
        print("[PASS] Multi-language vec partition key (same rowid, different languages)")
    }

    private func testFuzzyLookup() throws {
        let path = "/tmp/apple-loc-fuzzy-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        try dbQueue.writeWithoutTransaction { db in
            // Insert sources with "Home" in different key formats
            for (source, bundle) in [("Home", "UIKit.framework"), ("TAB_HOME", "TabBar.framework"), ("HomeScreen", "SpringBoard")] {
                try db.execute(
                    sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                    arguments: [source, bundle, 1, "L.strings", "ios26"]
                )
                let sid = db.lastInsertedRowID
                try db.execute(
                    sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                    arguments: [sid, "en", source]
                )
            }

            // Fuzzy search: LIKE '%Home%' should find Home and HomeScreen
            // (TAB_HOME uses uppercase so case-sensitive LIKE won't match "Home")
            let fuzzyPattern = "%Home%"
            let rows = try Row.fetchAll(db, sql: """
                SELECT source FROM source_strings WHERE source LIKE ? LIMIT 10
            """, arguments: [fuzzyPattern])
            let sources = rows.map { $0["source"] as String }
            assert(sources.contains("Home"), "FAIL: fuzzy should find Home")
            assert(sources.contains("HomeScreen"), "FAIL: fuzzy should find HomeScreen")

            // Case-insensitive variant: LIKE '%home%' should also find TAB_HOME
            // (SQLite LIKE is case-insensitive for ASCII by default)
            let ciFuzzy = "%home%"
            let ciRows = try Row.fetchAll(db, sql: """
                SELECT source FROM source_strings WHERE source LIKE ? LIMIT 10
            """, arguments: [ciFuzzy])
            let ciSources = ciRows.map { $0["source"] as String }
            assert(ciSources.contains("TAB_HOME"), "FAIL: case-insensitive fuzzy should find TAB_HOME")
            assert(ciSources.count == 3, "FAIL: case-insensitive fuzzy should find 3 results, got \(ciSources.count)")

            // Ranked fuzzy: exact match should sort first
            let rankedRows = try Row.fetchAll(db, sql: """
                SELECT source,
                  CASE
                    WHEN source = ?1 THEN 0.0
                    WHEN source LIKE ?2 THEN 1.0
                    ELSE 2.0
                  END AS match_rank
                FROM source_strings
                WHERE source LIKE ?3
                ORDER BY match_rank, source
                LIMIT 10
            """, arguments: ["Home", "Home%", "%Home%"])
            let rankedSources = rankedRows.map { $0["source"] as String }
            assert(rankedSources.first == "Home",
                   "FAIL: exact match 'Home' should be first, got \(rankedSources.first ?? "nil")")
            assert(rankedSources[1] == "HomeScreen",
                   "FAIL: prefix match 'HomeScreen' should be second, got \(rankedSources[1])")
        }
        print("[PASS] Fuzzy lookup (substring matching via LIKE, ranked ordering)")
    }

    private func testDedupPriority() throws {
        // Test BundlePriority classification (now delegates to EmbedTierClassifier)
        assert(BundlePriority.from(bundleName: "UIKitCore.framework") == .tier1)
        assert(BundlePriority.from(bundleName: "Foundation.framework") == .tier1)
        assert(BundlePriority.from(bundleName: "SwiftUI.framework") == .tier1)
        assert(BundlePriority.from(bundleName: "Photos.app") == .tier2)
        assert(BundlePriority.from(bundleName: "Safari.app") == .tier2)
        assert(BundlePriority.from(bundleName: "Terminal.app") == .tier3)
        assert(BundlePriority.from(bundleName: "RandomThing.framework") == .framework)
        assert(BundlePriority.from(bundleName: "RandomThing.app") == .app)
        assert(BundlePriority.from(bundleName: "Share.appex") == .plugin)
        assert(BundlePriority.from(bundleName: "SomeThing") == .other)
        assert(BundlePriority.from(bundleName: "ImageIO.framework") == .excluded)

        // Test ordering (lower rawValue = higher priority)
        assert(BundlePriority.tier1 < .tier2)
        assert(BundlePriority.tier2 < .tier3)
        assert(BundlePriority.tier3 < .framework)
        assert(BundlePriority.framework < .app)
        assert(BundlePriority.app < .plugin)
        assert(BundlePriority.plugin < .other)
        assert(BundlePriority.other < .excluded)

        print("[PASS] Bundle priority deduplication")
    }

    private func testEmbedding() throws {
        guard let svc = EmbeddingService(language: .english) else {
            print("[SKIP] embedding - English model not available")
            return
        }
        try svc.load()
        let vec = try svc.embed("Camera access required")
        assert(vec.count == 512, "FAIL: embedding dimension \(vec.count)")
        assert(vec.contains(where: { $0 != 0 }), "FAIL: embedding all zeros")

        let (supported, unsupported) = EmbeddingService.supportedLanguages(from: ["en", "ja", "xx_FAKE"])
        assert(supported.contains("en"), "FAIL: en should be supported")
        assert(supported.contains("ja"), "FAIL: ja should be supported (NLContextualEmbedding)")
        assert(unsupported.contains("xx_FAKE"), "FAIL: xx_FAKE should be unsupported")
        print("[PASS] NLContextualEmbedding (dim=\(vec.count))")
    }

    private func testParallelEmbedding() throws {
        let workerCounts = [1, 2, 4, 8]
        let testStrings = (0..<40).map { "Test sentence number \($0) for embedding benchmark" }

        // Create max workers upfront
        var allWorkers: [EmbeddingService] = []
        for i in 0..<workerCounts.max()! {
            guard let svc = EmbeddingService(language: .english) else {
                if i == 0 {
                    print("[SKIP] parallel embedding - English model not available")
                    return
                }
                break
            }
            try svc.load()
            allWorkers.append(svc)
        }

        var baselineElapsed: Double = 0
        var report: [String] = []

        for n in workerCounts where n <= allWorkers.count {
            let workers = Array(allWorkers.prefix(n))
            let queues = workers.indices.map { DispatchQueue(label: "embed-\(n)-\($0)") }

            nonisolated(unsafe) let results = UnsafeMutableBufferPointer<[Float]?>.allocate(capacity: testStrings.count)
            results.initialize(repeating: nil)

            let start = ContinuousClock.now
            let group = DispatchGroup()
            for i in testStrings.indices {
                let workerIdx = i % workers.count
                group.enter()
                queues[workerIdx].async {
                    results[i] = try? workers[workerIdx].embed(testStrings[i])
                    group.leave()
                }
            }
            group.wait()
            let elapsed = (ContinuousClock.now - start).seconds

            let allValid = (0..<testStrings.count).allSatisfy { results[$0] != nil && results[$0]!.count == 512 }
            results.deallocate()
            assert(allValid, "FAIL: parallel embedding N=\(n) produced invalid results")

            if n == 1 { baselineElapsed = elapsed }
            let speedup = baselineElapsed / elapsed
            let rate = Double(testStrings.count) / elapsed
            report.append("N=\(n): \(String(format: "%.2f", elapsed))s (\(String(format: "%.0f", rate)) emb/s, \(String(format: "%.2f", speedup))x)")
        }

        print("[PASS] Parallel embedding benchmark (\(testStrings.count) strings):")
        for line in report { print("       \(line)") }
    }

    private func testSourceBundles() throws {
        let path = "/tmp/apple-loc-bundles-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        // Non-compact: source_bundles should exist
        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue, compact: false)

        try dbQueue.writeWithoutTransaction { db in
            // Insert a source string with primary bundle UIKit
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Cancel", "UIKit.framework", BundlePriority.tier1.rawValue, "L.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID

            // Add translations
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "en", "Cancel"]
            )
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "ja", "キャンセル"]
            )

            // Record multiple bundles
            for bundle in ["UIKit.framework", "Photos.framework", "MapKit.framework"] {
                try db.execute(
                    sql: "INSERT OR IGNORE INTO source_bundles(source_id, bundle_name) VALUES (?, ?)",
                    arguments: [sourceId, bundle]
                )
            }

            // Verify all bundles are stored
            let bundleCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM source_bundles WHERE source_id = ?", arguments: [sourceId])!
            assert(bundleCount == 3, "FAIL: expected 3 bundles, got \(bundleCount)")

            // ResultFetcher with --framework Photos should find this source
            let photosResults = try ResultFetcher.fetch(
                candidates: [ResultFetcher.Candidate(sourceId: sourceId, distance: nil)],
                in: db,
                langFilter: nil,
                frameworkFilter: "Photos",
                platformFilter: nil,
                limit: 10
            ).results
            assert(photosResults.count == 1, "FAIL: --framework Photos should match via source_bundles")
            assert(photosResults[0].bundles != nil, "FAIL: bundles should be populated")
            assert(photosResults[0].bundles!.contains("Photos.framework"), "FAIL: bundles should include Photos.framework")
            assert(photosResults[0].bundles!.count == 3, "FAIL: bundles should have 3 entries, got \(photosResults[0].bundles!.count)")

            // ResultFetcher with --framework Nonexistent should not match
            let noResults = try ResultFetcher.fetch(
                candidates: [ResultFetcher.Candidate(sourceId: sourceId, distance: nil)],
                in: db,
                langFilter: nil,
                frameworkFilter: "Nonexistent",
                platformFilter: nil,
                limit: 10
            ).results
            assert(noResults.isEmpty, "FAIL: --framework Nonexistent should match nothing")
        }

        // Test compact mode: source_bundles should not exist
        let compactPath = "/tmp/apple-loc-bundles-compact-test.db"
        try? FileManager.default.removeItem(atPath: compactPath)
        defer { try? FileManager.default.removeItem(atPath: compactPath) }

        let compactDB = try DatabaseManager.openDatabase(at: compactPath, create: true)
        try DatabaseManager.createSchema(in: compactDB, compact: true)

        try compactDB.writeWithoutTransaction { db in
            let tableExists = try DatabaseManager.tableExists("source_bundles", in: db)
            assert(!tableExists, "FAIL: compact mode should not create source_bundles table")

            // Insert and verify fallback to bundle_name for framework filter
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Done", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let sid = db.lastInsertedRowID
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sid, "en", "Done"]
            )

            let fallbackResults = try ResultFetcher.fetch(
                candidates: [ResultFetcher.Candidate(sourceId: sid, distance: nil)],
                in: db,
                langFilter: nil,
                frameworkFilter: "UIKit",
                platformFilter: nil,
                limit: 10
            ).results
            assert(fallbackResults.count == 1, "FAIL: compact fallback should match primary bundle_name")
            assert(fallbackResults[0].bundles == nil, "FAIL: compact mode should have nil bundles")
        }

        print("[PASS] Source bundles (multi-bundle tracking + compact fallback)")
    }

    private func testInfo() throws {
        let path = "/tmp/apple-loc-info-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        try dbQueue.writeWithoutTransaction { db in
            // Insert source strings across two platforms
            for (source, platform) in [("Camera", "ios26"), ("Settings", "ios26"), ("Finder", "macos26")] {
                try db.execute(
                    sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                    arguments: [source, "UIKit.framework", 1, "L.strings", platform]
                )
            }

            // Insert translations in two languages
            try db.execute(sql: "INSERT INTO translations(source_id, language, target) VALUES (1,'ja','カメラ')")
            try db.execute(sql: "INSERT INTO translations(source_id, language, target) VALUES (1,'en','Camera')")
            try db.execute(sql: "INSERT INTO translations(source_id, language, target) VALUES (2,'ja','設定')")
            try db.execute(sql: "INSERT INTO translations(source_id, language, target) VALUES (3,'ja','Finder')")

            // Insert vec_mapping for one language
            try db.execute(sql: "INSERT INTO vec_mapping(source_id, language) VALUES (1,'en')")
            try db.execute(sql: "INSERT INTO vec_mapping(source_id, language) VALUES (2,'en')")

            // Verify platforms
            let platforms = try String.fetchAll(db, sql:
                "SELECT DISTINCT platform FROM source_strings ORDER BY platform")
            assert(platforms == ["ios26", "macos26"], "FAIL: info platforms = \(platforms)")

            // Verify languages
            let languages = try String.fetchAll(db, sql:
                "SELECT DISTINCT language FROM translations ORDER BY language")
            assert(languages == ["en", "ja"], "FAIL: info languages = \(languages)")

            // Verify embedding languages
            let embLangs = try String.fetchAll(db, sql:
                "SELECT DISTINCT language FROM vec_mapping ORDER BY language")
            assert(embLangs == ["en"], "FAIL: info embedding_languages = \(embLangs)")

            // Verify counts
            let ssCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM source_strings")!
            let trCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM translations")!
            let vecCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM vec_mapping")!
            assert(ssCount == 3, "FAIL: info source_strings count = \(ssCount)")
            assert(trCount == 4, "FAIL: info translations count = \(trCount)")
            assert(vecCount == 2, "FAIL: info vec_mapping count = \(vecCount)")
        }

        // Verify non-existent DB path
        assert(!FileManager.default.fileExists(atPath: "/tmp/nonexistent-apple-loc.db"),
               "FAIL: test precondition - nonexistent path exists")

        print("[PASS] Info command queries")
    }

    private func testAppendIngest() throws {
        let path = "/tmp/apple-loc-append-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        // Simulate initial ingest: insert a source string with English translation
        try dbQueue.writeWithoutTransaction { db in
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Camera", "UIKit.framework", BundlePriority.tier1.rawValue, "L.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID

            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "en", "Camera"]
            )
        }

        // Simulate append: add Japanese translation for the same source via UPSERT
        try dbQueue.writeWithoutTransaction { db in
            // UPSERT source_strings (same source+platform → no change)
            try db.execute(sql: """
                INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source, platform) DO UPDATE SET
                    bundle_name = excluded.bundle_name,
                    bundle_priority = excluded.bundle_priority
                WHERE excluded.bundle_priority < source_strings.bundle_priority
            """, arguments: ["Camera", "UIKit.framework", BundlePriority.tier1.rawValue, "L.strings", "ios26"])

            let row = try Row.fetchOne(db, sql:
                "SELECT id FROM source_strings WHERE source = ? AND platform = ?",
                arguments: ["Camera", "ios26"])!
            let sourceId: Int64 = row["id"]

            // UPSERT translation (new language → insert)
            try db.execute(sql: """
                INSERT INTO translations(source_id, language, target) VALUES (?, ?, ?)
                ON CONFLICT(source_id, language) DO UPDATE SET target = excluded.target
            """, arguments: [sourceId, "ja", "カメラ"])

            // Verify both translations exist
            let count = try Int.fetchOne(db, sql:
                "SELECT COUNT(*) FROM translations WHERE source_id = ?",
                arguments: [sourceId])!
            assert(count == 2, "FAIL: append should result in 2 translations, got \(count)")

            // Verify source_strings has only 1 row (not duplicated)
            let ssCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM source_strings")!
            assert(ssCount == 1, "FAIL: append should not duplicate source_strings, got \(ssCount)")

            // Verify both languages are present
            let langs = try String.fetchAll(db, sql:
                "SELECT language FROM translations WHERE source_id = ? ORDER BY language",
                arguments: [sourceId])
            assert(langs == ["en", "ja"], "FAIL: expected [en, ja], got \(langs)")
        }

        print("[PASS] Append ingest (UPSERT translations)")
    }

    private func testEmbedFromDB() throws {
        let path = "/tmp/apple-loc-embedfromdb-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        // Insert source + translation
        try dbQueue.writeWithoutTransaction { db in
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Camera", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "en", "Camera"]
            )
        }

        // Generate embedding using EmbeddingService directly (simulating embed command logic)
        guard let svc = EmbeddingService(language: .english) else {
            print("[SKIP] embed-from-db - English model not available")
            return
        }
        try svc.load()

        let vec = try svc.embed("camera")  // lowercased as in embed command

        // Insert via vec_mapping (same pattern as EmbedCommand)
        try dbQueue.writeWithoutTransaction { db in
            try db.execute(sql: """
                INSERT INTO vec_mapping(source_id, language) VALUES (?, ?)
                ON CONFLICT(source_id, language) DO NOTHING
            """, arguments: [1, "en"])
            let mappingId = db.lastInsertedRowID

            try db.execute(sql:
                "INSERT INTO vec_source_strings(rowid, language, embedding) VALUES (?, ?, ?)",
                arguments: [mappingId, "en", vec.asData])

            // Verify vec_mapping exists
            let vmCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM vec_mapping WHERE language = 'en'")!
            assert(vmCount == 1, "FAIL: expected 1 vec_mapping, got \(vmCount)")

            // KNN search should find it
            let results = try Row.fetchAll(db, sql: """
                SELECT rowid, distance FROM vec_source_strings
                WHERE embedding MATCH ? AND k = 1 AND language = ?
            """, arguments: [vec.asData, "en"])
            assert(results.count == 1, "FAIL: KNN should return 1 result, got \(results.count)")

            let resultRowid: Int64 = results[0]["rowid"]
            let mapRow = try Row.fetchOne(db, sql:
                "SELECT source_id FROM vec_mapping WHERE id = ?", arguments: [resultRowid])!
            let resolvedId: Int64 = mapRow["source_id"]
            assert(resolvedId == 1, "FAIL: KNN result should map to source_id 1, got \(resolvedId)")
        }

        print("[PASS] Embed from DB (vec_mapping + KNN)")
    }

    private func testEmbedSkipExisting() throws {
        let path = "/tmp/apple-loc-embedskip-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        // Insert source + translation + existing embedding
        try dbQueue.writeWithoutTransaction { db in
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Done", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID

            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "en", "Done"]
            )

            // Pre-existing embedding
            try db.execute(sql: "INSERT INTO vec_mapping(source_id, language) VALUES (?, ?)",
                           arguments: [sourceId, "en"])
            let mappingId = db.lastInsertedRowID
            let vec: [Float] = .init(repeating: 0.5, count: 512)
            try db.execute(sql:
                "INSERT INTO vec_source_strings(rowid, language, embedding) VALUES (?, ?, ?)",
                arguments: [mappingId, "en", vec.asData])
        }

        // Cursor query should return 0 rows (embedding already exists)
        try dbQueue.read { db in
            let rows = try Row.fetchAll(db, sql: """
                SELECT t.source_id, t.target FROM translations t
                LEFT JOIN vec_mapping vm ON vm.source_id = t.source_id AND vm.language = ?
                WHERE t.language = ? AND vm.id IS NULL AND t.source_id > ?
                ORDER BY t.source_id LIMIT ?
            """, arguments: ["en", "en", 0, 100])
            assert(rows.isEmpty, "FAIL: cursor query should return 0 rows for already-embedded, got \(rows.count)")
        }

        // Now add a second source WITHOUT embedding
        try dbQueue.writeWithoutTransaction { db in
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Cancel", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let sourceId = db.lastInsertedRowID
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sourceId, "en", "Cancel"]
            )
        }

        // Cursor query should return only the new unembedded row
        try dbQueue.read { db in
            let rows = try Row.fetchAll(db, sql: """
                SELECT t.source_id, t.target FROM translations t
                LEFT JOIN vec_mapping vm ON vm.source_id = t.source_id AND vm.language = ?
                WHERE t.language = ? AND vm.id IS NULL AND t.source_id > ?
                ORDER BY t.source_id LIMIT ?
            """, arguments: ["en", "en", 0, 100])
            assert(rows.count == 1, "FAIL: cursor should return 1 unembedded row, got \(rows.count)")
            let target: String = rows[0]["target"]
            assert(target == "Cancel", "FAIL: unembedded row should be Cancel, got \(target)")
        }

        print("[PASS] Embed skip existing (cursor query filters embedded rows)")
    }

    private func testInternalFilter() throws {
        let path = "/tmp/apple-loc-internal-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        try dbQueue.writeWithoutTransaction { db in
            // Insert a normal entry
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Camera", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let normalId = db.lastInsertedRowID
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [normalId, "en", "Camera"]
            )

            // Insert an [Internal] entry
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["[Internal] File Radar: Chapter Feedback", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let internalId = db.lastInsertedRowID
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [internalId, "en", "[Internal] File Radar: Chapter Feedback"]
            )

            let allCandidates = [
                ResultFetcher.Candidate(sourceId: normalId, distance: nil),
                ResultFetcher.Candidate(sourceId: internalId, distance: nil),
            ]

            // includeInternal: false → only normal entry
            let filtered = try ResultFetcher.fetch(
                candidates: allCandidates,
                in: db,
                langFilter: nil,
                frameworkFilter: nil,
                platformFilter: nil,
                limit: 10,
                includeInternal: false
            ).results
            assert(filtered.count == 1, "FAIL: internal filter should return 1, got \(filtered.count)")
            assert(filtered[0].source == "Camera", "FAIL: filtered result should be Camera")

            // includeInternal: true → both entries
            let unfiltered = try ResultFetcher.fetch(
                candidates: allCandidates,
                in: db,
                langFilter: nil,
                frameworkFilter: nil,
                platformFilter: nil,
                limit: 10,
                includeInternal: true
            ).results
            assert(unfiltered.count == 2, "FAIL: unfiltered should return 2, got \(unfiltered.count)")
        }

        print("[PASS] Internal entry filter ([Internal] prefix hidden by default)")
    }

    private func testJSONParser() throws {
        let tmpDir = "/tmp/apple-loc-json-parser-test"
        try? FileManager.default.removeItem(atPath: tmpDir)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        // Create directory structure: data/ios/26.1/
        let versionDir = (tmpDir as NSString)
            .appendingPathComponent("ios")
            .appending("/26.1")
        try FileManager.default.createDirectory(atPath: versionDir, withIntermediateDirectories: true)

        // Create test JSON file
        let jsonContent = """
        {
            "framework": "UIKitCore.framework",
            "localizations": {
                "Camera": [
                    {"language": "ja", "target": "カメラ", "filename": "Localizable.loctable"},
                    {"language": "en", "target": "Camera", "filename": "Localizable.loctable"},
                    {"language": "de", "target": "Kamera", "filename": "Localizable.loctable"}
                ],
                "Done": [
                    {"language": "ja", "target": "完了", "filename": "Localizable.loctable"},
                    {"language": "en", "target": "Done", "filename": "Localizable.loctable"}
                ],
                "D1K-K5-gc3.title": [
                    {"language": "ja", "target": "あ", "filename": "Main.strings"}
                ],
                "CFBundleName": [
                    {"language": "en", "target": "UIKit", "filename": "InfoPlist.strings"}
                ],
                "%@": [
                    {"language": "ja", "target": "%@", "filename": "Localizable.loctable"}
                ],
                "Settings": [
                    {"language": "en", "target": "Settings", "filename": "AppIntents.strings"}
                ]
            }
        }
        """
        let filePath = (versionDir as NSString).appendingPathComponent("UIKitCore.framework_Localizable.loctable_1.json")
        try jsonContent.write(toFile: filePath, atomically: true, encoding: .utf8)

        // Parse with language filter (ja, en)
        let parser = JSONDataParser(
            dataDir: tmpDir,
            allowedLanguages: Set(["ja", "en"]),
            allowedPlatforms: nil
        )

        var allRows: [ParsedRow] = []
        _ = try parser.parse { batch in
            allRows.append(contentsOf: batch)
        }

        // Should include: Camera(ja), Camera(en), Done(ja), Done(en)
        // Should filter: de (language), IB key, CFBundleName (plist), %@ (format-only), AppIntents (excluded file)
        let sources = allRows.map { "\($0.source):\($0.language)" }.sorted()
        assert(allRows.count == 4, "FAIL: JSON parser expected 4 rows, got \(allRows.count). Sources: \(sources)")
        assert(sources.contains("Camera:en"), "FAIL: should contain Camera:en")
        assert(sources.contains("Camera:ja"), "FAIL: should contain Camera:ja")
        assert(sources.contains("Done:en"), "FAIL: should contain Done:en")
        assert(sources.contains("Done:ja"), "FAIL: should contain Done:ja")

        // Verify platform and bundle
        assert(allRows[0].platform == "ios26", "FAIL: platform should be ios26, got \(allRows[0].platform)")
        assert(allRows[0].bundleName == "UIKitCore.framework", "FAIL: bundle should be UIKitCore.framework")

        // Verify no group_id or id
        assert(allRows[0].groupId == nil, "FAIL: JSON rows should have nil groupId")
        assert(allRows[0].id == nil, "FAIL: JSON rows should have nil id")

        print("[PASS] JSON parser (\(allRows.count) rows, filters working)")
    }

    private func testPlatformConversion() throws {
        assert(JSONDataParser.platformName(os: "ios", version: "26.1") == "ios26",
               "FAIL: ios/26.1 → ios26")
        assert(JSONDataParser.platformName(os: "macos", version: "15.6") == "macos15",
               "FAIL: macos/15.6 → macos15")
        assert(JSONDataParser.platformName(os: "ios", version: "15.7") == "ios15",
               "FAIL: ios/15.7 → ios15")
        assert(JSONDataParser.platformName(os: "macos", version: "12.6") == "macos12",
               "FAIL: macos/12.6 → macos12")
        assert(JSONDataParser.platformName(os: "macos", version: "13.5.2") == "macos13",
               "FAIL: macos/13.5.2 → macos13")

        print("[PASS] Platform name conversion")
    }

    private func testStructuredTargetResolution() throws {
        // NSStringDeviceSpecificRuleType → "other" value
        let deviceJSON = #"{"NSStringDeviceSpecificRuleType":{"iphone":"Aceptar","mac":"Aceptar","other":"OK"}}"#
        let deviceResult = StructuredTarget.resolveForEmbedding(deviceJSON)
        assert(deviceResult == "OK", "FAIL: device-specific should resolve to 'other', got '\(deviceResult)'")

        // NSStringDeviceSpecificRuleType without "other" → first value
        let noOther = #"{"NSStringDeviceSpecificRuleType":{"mac":"Aceptar","iphone":"Aceptar"}}"#
        let noOtherResult = StructuredTarget.resolveForEmbedding(noOther)
        assert(!noOtherResult.contains("{"), "FAIL: device-specific without 'other' should resolve to a value, got '\(noOtherResult)'")

        // NSStringLocalizedFormatKey (simple, no %#@ variables)
        let simpleFormat = #"{"NSStringLocalizedFormatKey":"Done"}"#
        let simpleResult = StructuredTarget.resolveForEmbedding(simpleFormat)
        assert(simpleResult == "Done", "FAIL: simple format key should return 'Done', got '\(simpleResult)'")

        // NSStringLocalizedFormatKey (complex, with %#@var@ reference)
        let complexFormat = #"{"NSStringLocalizedFormatKey":"%#@count@ items","count":{"one":"1","other":"many"}}"#
        let complexResult = StructuredTarget.resolveForEmbedding(complexFormat)
        assert(complexResult == "many items", "FAIL: complex format key should resolve variables, got '\(complexResult)'")

        // NSStringVariableWidthRuleType → largest numeric key
        let widthJSON = #"{"NSStringVariableWidthRuleType":{"1":"OK","25":"Accept","50":"Accept Changes"}}"#
        let widthResult = StructuredTarget.resolveForEmbedding(widthJSON)
        assert(widthResult == "Accept Changes", "FAIL: variable-width should pick largest key, got '\(widthResult)'")

        // Plain text → returned as-is
        let plain = "Hello World"
        assert(StructuredTarget.resolveForEmbedding(plain) == plain, "FAIL: plain text should pass through")

        // Invalid JSON starting with { → returned as-is
        let badJSON = "{not valid json"
        assert(StructuredTarget.resolveForEmbedding(badJSON) == badJSON, "FAIL: invalid JSON should pass through")

        // parseAsJSON: structured → dict, plain → nil
        assert(StructuredTarget.parseAsJSON(deviceJSON) != nil, "FAIL: parseAsJSON should return dict for structured target")
        assert(StructuredTarget.parseAsJSON(plain) == nil, "FAIL: parseAsJSON should return nil for plain text")

        print("[PASS] Structured target resolution")
    }

    private func testStructuredTargetOutput() throws {
        let path = "/tmp/apple-loc-structured-output-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        try dbQueue.writeWithoutTransaction { db in
            // Insert a source with structured target for es, plain for ja
            try db.execute(
                sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                arguments: ["Done", "UIKit.framework", 1, "L.strings", "ios26"]
            )
            let sid = db.lastInsertedRowID
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sid, "ja", "完了"]
            )
            let structuredES = #"{"NSStringDeviceSpecificRuleType":{"mac":"Aceptar","other":"OK"}}"#
            try db.execute(
                sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                arguments: [sid, "es", structuredES]
            )
        }

        // Build a SearchResult and verify printJSON output
        let result = SearchResult(
            source: "Done",
            bundleName: "UIKit.framework",
            fileName: "L.strings",
            platform: "ios26",
            translations: [
                "ja": "完了",
                "es": #"{"NSStringDeviceSpecificRuleType":{"mac":"Aceptar","other":"OK"}}"#,
            ]
        )
        let output = ResultsOutput(results: [result], hasMore: false)

        // Test the actual buildJSONData() code path (same logic as printJSON)
        let data = try output.buildJSONData()
        let jsonString = String(data: data, encoding: .utf8)!

        // Verify: ja should be a plain string, es should be a nested object
        assert(jsonString.contains("\"ja\" : \"完了\""), "FAIL: ja should be plain string in output")
        assert(jsonString.contains("\"NSStringDeviceSpecificRuleType\""), "FAIL: es should contain nested structure key")
        assert(!jsonString.contains("\\\"NSStringDeviceSpecificRuleType"), "FAIL: structured target should not be escaped")

        print("[PASS] Structured target output")
    }

    private func testEmbedTierClassifier() throws {
        // classify()
        assert(EmbedTierClassifier.classify("UIKitCore.framework") == 1, "FAIL: UIKitCore should be tier 1")
        assert(EmbedTierClassifier.classify("Photos.app") == 2, "FAIL: Photos.app should be tier 2")
        assert(EmbedTierClassifier.classify("Terminal.app") == 3, "FAIL: Terminal.app should be tier 3")
        assert(EmbedTierClassifier.classify("ImageIO.framework") == nil, "FAIL: ImageIO should be nil (excluded)")
        assert(EmbedTierClassifier.classify("RandomThing.framework") == nil, "FAIL: unknown bundle should be nil")

        // shouldEmbed(.upTo(1))
        assert(EmbedTierClassifier.shouldEmbed("UIKitCore.framework", tier: .upTo(1)) == true, "FAIL: T1 should embed at tier 1")
        assert(EmbedTierClassifier.shouldEmbed("Photos.app", tier: .upTo(1)) == false, "FAIL: T2 should not embed at tier 1")
        assert(EmbedTierClassifier.shouldEmbed("Terminal.app", tier: .upTo(1)) == false, "FAIL: T3 should not embed at tier 1")
        assert(EmbedTierClassifier.shouldEmbed("ImageIO.framework", tier: .upTo(1)) == false, "FAIL: excluded should not embed")
        assert(EmbedTierClassifier.shouldEmbed("RandomThing.framework", tier: .upTo(1)) == false, "FAIL: unknown should not embed at tier 1")

        // shouldEmbed(.upTo(2))
        assert(EmbedTierClassifier.shouldEmbed("UIKitCore.framework", tier: .upTo(2)) == true, "FAIL: T1 should embed at tier 2")
        assert(EmbedTierClassifier.shouldEmbed("Photos.app", tier: .upTo(2)) == true, "FAIL: T2 should embed at tier 2")
        assert(EmbedTierClassifier.shouldEmbed("Terminal.app", tier: .upTo(2)) == false, "FAIL: T3 should not embed at tier 2")

        // shouldEmbed(.upTo(3))
        assert(EmbedTierClassifier.shouldEmbed("UIKitCore.framework", tier: .upTo(3)) == true, "FAIL: T1 should embed at tier 3")
        assert(EmbedTierClassifier.shouldEmbed("Photos.app", tier: .upTo(3)) == true, "FAIL: T2 should embed at tier 3")
        assert(EmbedTierClassifier.shouldEmbed("Terminal.app", tier: .upTo(3)) == true, "FAIL: T3 should embed at tier 3")

        // shouldEmbed(.all)
        assert(EmbedTierClassifier.shouldEmbed("UIKitCore.framework", tier: .all) == true, "FAIL: T1 should embed at all")
        assert(EmbedTierClassifier.shouldEmbed("Photos.app", tier: .all) == true, "FAIL: T2 should embed at all")
        assert(EmbedTierClassifier.shouldEmbed("Terminal.app", tier: .all) == true, "FAIL: T3 should embed at all")
        assert(EmbedTierClassifier.shouldEmbed("ImageIO.framework", tier: .all) == false, "FAIL: excluded should not embed even at all")
        assert(EmbedTierClassifier.shouldEmbed("RandomThing.framework", tier: .all) == true, "FAIL: unknown should embed at all")

        // isExcluded()
        assert(EmbedTierClassifier.isExcluded("ImageIO.framework") == true, "FAIL: ImageIO should be excluded")
        assert(EmbedTierClassifier.isExcluded("UIKitCore.framework") == false, "FAIL: UIKitCore should not be excluded")
        assert(EmbedTierClassifier.isExcluded("RandomThing.framework") == false, "FAIL: unknown should not be excluded")

        print("[PASS] Embed tier classifier")
    }

    private func testPagination() throws {
        let path = "/tmp/apple-loc-pagination-test.db"
        try? FileManager.default.removeItem(atPath: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let dbQueue = try DatabaseManager.openDatabase(at: path, create: true)
        try DatabaseManager.createSchema(in: dbQueue)

        // Insert 5 source strings with translations
        try dbQueue.writeWithoutTransaction { db in
            for i in 1...5 {
                try db.execute(
                    sql: "INSERT INTO source_strings(source, bundle_name, bundle_priority, file_name, platform) VALUES (?,?,?,?,?)",
                    arguments: ["Item\(i)", "Test.framework", 1, "Test.strings", "ios26"]
                )
                let sid = db.lastInsertedRowID
                try db.execute(
                    sql: "INSERT INTO translations(source_id, language, target) VALUES (?,?,?)",
                    arguments: [sid, "en", "Item \(i)"]
                )
            }

            let candidates = (1...5).map { ResultFetcher.Candidate(sourceId: Int64($0), distance: nil) }

            // Page 1: offset=0, limit=2 → 2 results, hasMore=true
            let page1 = try ResultFetcher.fetch(
                candidates: candidates, in: db,
                langFilter: nil, frameworkFilter: nil, platformFilter: nil,
                limit: 2, offset: 0
            )
            assert(page1.results.count == 2, "FAIL: page1 should have 2 results, got \(page1.results.count)")
            assert(page1.hasMore == true, "FAIL: page1 hasMore should be true")

            // Page 2: offset=2, limit=2 → 2 results, hasMore=true
            let page2 = try ResultFetcher.fetch(
                candidates: candidates, in: db,
                langFilter: nil, frameworkFilter: nil, platformFilter: nil,
                limit: 2, offset: 2
            )
            assert(page2.results.count == 2, "FAIL: page2 should have 2 results, got \(page2.results.count)")
            assert(page2.hasMore == true, "FAIL: page2 hasMore should be true")

            // Page 3: offset=4, limit=2 → 1 result, hasMore=false
            let page3 = try ResultFetcher.fetch(
                candidates: candidates, in: db,
                langFilter: nil, frameworkFilter: nil, platformFilter: nil,
                limit: 2, offset: 4
            )
            assert(page3.results.count == 1, "FAIL: page3 should have 1 result, got \(page3.results.count)")
            assert(page3.hasMore == false, "FAIL: page3 hasMore should be false")

            // Beyond data: offset=5, limit=2 → 0 results, hasMore=false
            let beyond = try ResultFetcher.fetch(
                candidates: candidates, in: db,
                langFilter: nil, frameworkFilter: nil, platformFilter: nil,
                limit: 2, offset: 5
            )
            assert(beyond.results.count == 0, "FAIL: beyond should have 0 results, got \(beyond.results.count)")
            assert(beyond.hasMore == false, "FAIL: beyond hasMore should be false")

            // Large limit: offset=0, limit=10 → 5 results, hasMore=false
            let all = try ResultFetcher.fetch(
                candidates: candidates, in: db,
                langFilter: nil, frameworkFilter: nil, platformFilter: nil,
                limit: 10, offset: 0
            )
            assert(all.results.count == 5, "FAIL: all should have 5 results, got \(all.results.count)")
            assert(all.hasMore == false, "FAIL: all hasMore should be false")

            // Verify has_more appears in JSON output
            let output = ResultsOutput(results: page1.results, hasMore: true)
            let data = try output.buildJSONData()
            let json = String(data: data, encoding: .utf8)!
            assert(json.contains("\"has_more\" : true"), "FAIL: JSON should contain has_more")
        }

        print("[PASS] Pagination (offset + has_more)")
    }
}
