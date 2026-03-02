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
        try testParser()
        try testParserIBFilter()
        try testParserExclusion()
        try testDedupPriority()
        try testEmbedding()
        try testParallelEmbedding()
        try testSourceBundles()
        try testInfo()
        try testAppendIngest()
        try testEmbedFromDB()
        try testEmbedSkipExisting()
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
                arguments: [1, "Camera", "UIKit.framework", BundlePriority.coreFramework.rawValue, "Localizable.strings", "ios26"]
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
        }
        print("[PASS] Fuzzy lookup (substring matching via LIKE)")
    }

    private func testParser() throws {
        let tmpDir = "/tmp/apple-loc-parser-test"
        try? FileManager.default.removeItem(atPath: tmpDir)
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let testData = """
        CREATE TABLE ios26 (id integer NOT NULL);
        COPY ios26 (id, group_id, source, target, language, file_name, bundle_name, bundle_path, platform) FROM stdin;
        1\t1\tCamera\tカメラ\tja\tLocalizable.strings\tUIKit.framework\t/path\tiOS
        2\t1\tCamera\tCamera\ten\tLocalizable.strings\tUIKit.framework\t/path\tiOS
        3\t2\tSettings\t設定\tja\tLocalizable.strings\tPreferences.framework\t/path\tiOS
        4\t2\tSettings\tEinstellungen\tde\tLocalizable.strings\tPreferences.framework\t/path\tiOS
        \\.
        """
        let filePath = (tmpDir as NSString).appendingPathComponent("data.sql.aa")
        try testData.write(toFile: filePath, atomically: true, encoding: .utf8)

        var parser = SQLDumpParser(
            dataDir: tmpDir,
            allowedLanguages: ["ja", "en"],
            allowedPlatforms: ["ios26"]
        )
        parser.filterIBKeys = false  // Don't filter IB keys in this basic test

        var rows: [ParsedRow] = []
        let count = try parser.parse { row in rows.append(row) }

        assert(count == 3, "FAIL: parser expected 3 rows, got \(count)")
        assert(rows[0].source == "Camera" && rows[0].target == "カメラ", "FAIL: row 0")
        assert(rows[0].groupId == 1, "FAIL: row 0 groupId")
        assert(rows[1].language == "en", "FAIL: row 1 language")
        assert(rows[2].platform == "ios26", "FAIL: row 2 platform")
        assert(!rows.contains(where: { $0.language == "de" }), "FAIL: de not filtered")
        print("[PASS] SQL dump parser (\(count) rows)")
    }

    private func testParserIBFilter() throws {
        let tmpDir = "/tmp/apple-loc-ib-test"
        try? FileManager.default.removeItem(atPath: tmpDir)
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let testData = """
        COPY ios26 (id, group_id, source, target, language, file_name, bundle_name, bundle_path, platform) FROM stdin;
        1\t1\tCancel\tキャンセル\tja\tL.strings\tUIKit\t/p\tiOS
        2\t2\tD1K-K5-gc3.title\tあ\tja\tMain.strings\tApp\t/p\tiOS
        3\t3\t8PE-KZ-giS.text\tい\tja\tMain.strings\tApp\t/p\tiOS
        4\t4\t22.title\tキャンセル\tja\tL.strings\tUIKit\t/p\tiOS
        \\.
        """
        let filePath = (tmpDir as NSString).appendingPathComponent("data.sql.aa")
        try testData.write(toFile: filePath, atomically: true, encoding: .utf8)

        let parser = SQLDumpParser(
            dataDir: tmpDir,
            allowedLanguages: ["ja"],
            allowedPlatforms: ["ios26"]
        )

        var rows: [ParsedRow] = []
        let count = try parser.parse { row in rows.append(row) }

        // Should keep Cancel and 22.title, filter out IB keys
        assert(count == 2, "FAIL: IB filter expected 2 rows, got \(count)")
        assert(rows[0].source == "Cancel", "FAIL: first row should be Cancel")
        assert(rows[1].source == "22.title", "FAIL: second row should be 22.title (numeric keys kept)")
        print("[PASS] IB key filter (\(count) rows kept, 2 IB keys filtered)")
    }

    private func testParserExclusion() throws {
        let tmpDir = "/tmp/apple-loc-exclusion-test"
        try? FileManager.default.removeItem(atPath: tmpDir)
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let testData = """
        COPY macos26 (id, group_id, source, target, language, file_name, bundle_name, bundle_path, platform) FROM stdin;
        1\t1\tCamera\tCamera\ten\tLocalizable.strings\tUIKit.framework\t/p\tmacOS
        2\t2\tCFBundleName\tFinder\ten\tInfoPlist.strings\tFinder.app\t/p\tmacOS
        3\t3\tNSHumanReadableCopyright\tCopyright\ten\tInfoPlist.strings\tApp.app\t/p\tmacOS
        4\t4\tSettings\tSettings\ten\tInfoPlist.loctable\tSettings.app\t/p\tmacOS
        5\t5\tOpen\tOpen\ten\tAppIntents.strings\tApp.app\t/p\tmacOS
        6\t6\t%@\t%@\ten\tLocalizable.strings\tUIKit.framework\t/p\tmacOS
        7\t7\t%d %@\t%d %@\ten\tLocalizable.strings\tUIKit.framework\t/p\tmacOS
        8\t8\tDone\tDone\ten\tLocalizable.strings\tUIKit.framework\t/p\tmacOS
        9\t9\tCFBundleDisplayName\tMaps\ten\tLocalizable.strings\tMaps.app\t/p\tmacOS
        10\t10\tShortcuts\tShortcuts\ten\tAppShortcuts.strings\tApp.app\t/p\tmacOS
        \\.
        """
        let filePath = (tmpDir as NSString).appendingPathComponent("data.sql.aa")
        try testData.write(toFile: filePath, atomically: true, encoding: .utf8)

        var parser = SQLDumpParser(
            dataDir: tmpDir,
            allowedLanguages: ["en"],
            allowedPlatforms: ["macos26"]
        )
        parser.filterIBKeys = false

        var rows: [ParsedRow] = []
        let count = try parser.parse { row in rows.append(row) }

        // Should keep: Camera, Done (2 rows)
        // Should filter: CFBundleName (plist key), NSHumanReadableCopyright (plist key),
        //   Settings from InfoPlist.loctable (excluded file), Open from AppIntents (excluded file),
        //   %@ and %d %@ (format-only), CFBundleDisplayName (plist key),
        //   Shortcuts from AppShortcuts (excluded file)
        let sources = rows.map(\.source)
        assert(count == 2, "FAIL: exclusion filter expected 2 rows, got \(count). Sources: \(sources)")
        assert(sources.contains("Camera"), "FAIL: Camera should be kept")
        assert(sources.contains("Done"), "FAIL: Done should be kept")
        assert(!sources.contains("CFBundleName"), "FAIL: CFBundleName should be filtered")
        assert(!sources.contains("NSHumanReadableCopyright"), "FAIL: plist copyright should be filtered")
        assert(!sources.contains("Settings"), "FAIL: InfoPlist.loctable should be filtered")
        assert(!sources.contains("%@"), "FAIL: format-only should be filtered")
        print("[PASS] Parser exclusion filters (\(count) rows kept, 8 filtered)")
    }

    private func testDedupPriority() throws {
        // Test BundlePriority classification
        assert(BundlePriority.from(bundleName: "UIKit.framework") == .coreFramework)
        assert(BundlePriority.from(bundleName: "Foundation") == .coreFramework)
        assert(BundlePriority.from(bundleName: "SwiftUI") == .coreFramework)
        assert(BundlePriority.from(bundleName: "Photos.framework") == .framework)
        assert(BundlePriority.from(bundleName: "Safari.app") == .app)
        assert(BundlePriority.from(bundleName: "Share.appex") == .plugin)
        assert(BundlePriority.from(bundleName: "SomeThing") == .other)

        // Test ordering (lower rawValue = higher priority)
        assert(BundlePriority.coreFramework < .framework)
        assert(BundlePriority.framework < .app)
        assert(BundlePriority.app < .plugin)
        assert(BundlePriority.plugin < .other)

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
                arguments: ["Cancel", "UIKit.framework", BundlePriority.coreFramework.rawValue, "L.strings", "ios26"]
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
            )
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
            )
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
            )
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
                arguments: ["Camera", "UIKit.framework", BundlePriority.coreFramework.rawValue, "L.strings", "ios26"]
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
            """, arguments: ["Camera", "UIKit.framework", BundlePriority.coreFramework.rawValue, "L.strings", "ios26"])

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
}
