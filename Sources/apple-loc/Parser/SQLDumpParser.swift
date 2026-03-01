import Foundation

/// A parsed row from the PostgreSQL COPY data.
struct ParsedRow: Sendable {
    let id: Int
    let groupId: Int
    let source: String      // English original / key
    let target: String      // Translated text
    let language: String
    let fileName: String
    let bundleName: String
    let platform: String    // Derived from table name (e.g. "ios26", "macos15")
}

/// Streams through sorted data.sql.* split files, parsing PostgreSQL COPY format.
/// Yields ParsedRow values, filtering by language, platform, and IB keys early.
struct SQLDumpParser {
    let dataDir: String
    let allowedLanguages: Set<String>?  // nil = all languages
    let allowedPlatforms: Set<String>?  // nil = all platforms
    var filterIBKeys: Bool = true       // Skip Interface Builder object ID keys

    /// Characters allowed in format-placeholder-only strings (e.g. "%@", "%d %@", "%1$@ %2$@")
    private static let formatOnlyChars: Set<Character> = Set(" \t%@dlufegc0123456789$.*-+#")

    /// Files to exclude (metadata / low-value)
    private static let excludedFilePatterns: Set<String> = [
        "infoplist.loctable", "infoplist.strings", "infoplist.xcstrings",
    ]
    private static let excludedFilePrefixes = ["appintents", "appshortcuts"]

    /// Plist metadata keys to exclude
    private static let plistMetadataKeys: Set<String> = [
        "CFBundleName", "CFBundleDisplayName", "CFBundleGetInfoString",
        "CFBundleShortVersionString", "NSHumanReadableCopyright",
    ]

    /// Find and sort all data.sql.* files in the directory.
    func splitFiles() throws -> [String] {
        let fm = FileManager.default
        let contents = try fm.contentsOfDirectory(atPath: dataDir)
        let files = contents
            .filter { $0.hasPrefix("data.sql.") && $0 != "data.sql" }
            .sorted()
            .map { (dataDir as NSString).appendingPathComponent($0) }
        guard !files.isEmpty else {
            throw ParserError.noDataFiles(dataDir)
        }
        return files
    }

    /// Called when a COPY table header is found. Reports table name and whether it's being processed.
    var onTableFound: ((_ table: String, _ processing: Bool) -> Void)?

    /// Parse all split files and call the handler for each matching row.
    /// Returns total rows parsed (matching filter).
    func parse(handler: (ParsedRow) throws -> Void) throws -> Int {
        let files = try splitFiles()
        var currentTable: String?   // e.g. "ios26" — acts as platform
        var inCopyData = false
        var totalParsed = 0

        for filePath in files {
            guard let fh = FileHandle(forReadingAtPath: filePath) else {
                throw ParserError.cannotOpenFile(filePath)
            }
            defer { fh.closeFile() }

            let bufferSize = 1024 * 256     // 256KB read chunks
            var leftover = Data()

            while true {
                let chunk = fh.readData(ofLength: bufferSize)
                if chunk.isEmpty && leftover.isEmpty { break }

                var data = leftover + chunk
                leftover = Data()

                // Process lines in the buffer
                while let nlRange = data.range(of: Data([0x0A])) {
                    let lineData = data[data.startIndex..<nlRange.lowerBound]
                    data = data[nlRange.upperBound...]

                    guard let line = String(data: lineData, encoding: .utf8) else { continue }

                    if !inCopyData {
                        // Look for COPY statement
                        if let table = parseCopyHeader(line) {
                            if let allowed = allowedPlatforms, !allowed.contains(table) {
                                // Skip this entire table's data
                                inCopyData = true
                                currentTable = nil
                                onTableFound?(table, false)
                            } else {
                                currentTable = table
                                inCopyData = true
                                onTableFound?(table, true)
                            }
                        }
                    } else {
                        // In COPY data block
                        if line == "\\." {
                            inCopyData = false
                            currentTable = nil
                            continue
                        }
                        guard let table = currentTable else { continue }   // skipping this table

                        if let row = parseDataRow(line, platform: table),
                           shouldInclude(row) {
                            try handler(row)
                            totalParsed += 1
                        }
                    }
                }

                if chunk.isEmpty {
                    // EOF — process remaining data as last line
                    if !data.isEmpty, let line = String(data: data, encoding: .utf8) {
                        if inCopyData {
                            if line == "\\." {
                                inCopyData = false
                            } else if let table = currentTable,
                                      let row = parseDataRow(line, platform: table),
                                      shouldInclude(row) {
                                try handler(row)
                                totalParsed += 1
                            }
                        }
                    }
                    break
                } else {
                    // Save incomplete line for next chunk
                    leftover = Data(data)
                }
            }
        }
        return totalParsed
    }

    // MARK: - Private

    /// Extract table name from: COPY tablename (...) FROM stdin;
    private func parseCopyHeader(_ line: String) -> String? {
        guard line.hasPrefix("COPY ") else { return nil }
        // "COPY ios26 (id, group_id, ...) FROM stdin;"
        let parts = line.split(separator: " ", maxSplits: 2)
        guard parts.count >= 2 else { return nil }
        return String(parts[1])
    }

    /// Parse a tab-delimited COPY data row.
    /// Columns: id, group_id, source, target, language, file_name, bundle_name, bundle_path, platform
    private func parseDataRow(_ line: String, platform: String) -> ParsedRow? {
        let fields = line.split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count >= 9 else { return nil }

        guard let id = Int(fields[0]) else { return nil }
        let groupId = Int(fields[1]) ?? 0
        let source = unescapePgCopy(String(fields[2]))
        let target = unescapePgCopy(String(fields[3]))
        let language = String(fields[4])
        let fileName = String(fields[5])
        let bundleName = String(fields[6])
        // fields[7] = bundle_path (skipped)
        // fields[8] = platform from original data (e.g. "macOS") — we use table name instead

        return ParsedRow(
            id: id, groupId: groupId,
            source: source, target: target,
            language: language, fileName: fileName,
            bundleName: bundleName, platform: platform
        )
    }

    /// Check if a file should be excluded (plist metadata, AppIntents, etc.).
    private func isExcludedFile(_ fileName: String) -> Bool {
        let lower = fileName.lowercased()
        if Self.excludedFilePatterns.contains(lower) { return true }
        return Self.excludedFilePrefixes.contains(where: { lower.hasPrefix($0) })
    }

    /// Check if the source key is a plist metadata key.
    private func isPlistMetadataKey(_ source: String) -> Bool {
        Self.plistMetadataKeys.contains(source)
    }

    /// Combined filter: language, IB keys, excluded files, plist metadata, format-only strings.
    private func shouldInclude(_ row: ParsedRow) -> Bool {
        (allowedLanguages?.contains(row.language) ?? true)
            && !isIBKey(row.source)
            && !isExcludedFile(row.fileName)
            && !isPlistMetadataKey(row.source)
            && !isFormatOnlyString(row.source)
    }

    /// Check if the source text is only format placeholders (e.g. "%@", "%d %@").
    private func isFormatOnlyString(_ source: String) -> Bool {
        guard !source.isEmpty else { return true }
        return source.allSatisfy { Self.formatOnlyChars.contains($0) }
    }

    /// Check if a source key is an Interface Builder Object ID (e.g. "D1K-K5-gc3.title").
    /// Pattern: exactly `[A-Za-z0-9]{3}-[A-Za-z0-9]{2}-[A-Za-z0-9]{3}.` at the start.
    private func isIBKey(_ source: String) -> Bool {
        guard filterIBKeys else { return false }
        let s = source.utf8
        // Minimum length: 3 + 1 + 2 + 1 + 3 + 1 = 11 characters
        guard s.count >= 11 else { return false }
        var i = s.startIndex
        @inline(__always) func isAlnum(_ b: UInt8) -> Bool {
            (b >= 0x30 && b <= 0x39)    // 0-9
            || (b >= 0x41 && b <= 0x5A) // A-Z
            || (b >= 0x61 && b <= 0x7A) // a-z
        }
        // [A-Za-z0-9]{3}
        for _ in 0..<3 { guard isAlnum(s[i]) else { return false }; i = s.index(after: i) }
        guard s[i] == 0x2D else { return false }; i = s.index(after: i) // '-'
        // [A-Za-z0-9]{2}
        for _ in 0..<2 { guard isAlnum(s[i]) else { return false }; i = s.index(after: i) }
        guard s[i] == 0x2D else { return false }; i = s.index(after: i) // '-'
        // [A-Za-z0-9]{3}
        for _ in 0..<3 { guard isAlnum(s[i]) else { return false }; i = s.index(after: i) }
        guard s[i] == 0x2E else { return false } // '.'
        return true
    }

    /// Unescape PostgreSQL COPY text format backslash sequences.
    private func unescapePgCopy(_ s: String) -> String {
        guard s.contains("\\") else { return s }
        var result = ""
        result.reserveCapacity(s.count)
        var iter = s.makeIterator()
        while let ch = iter.next() {
            if ch == "\\" {
                guard let next = iter.next() else {
                    result.append(ch)
                    break
                }
                switch next {
                case "n": result.append("\n")
                case "r": result.append("\r")
                case "t": result.append("\t")
                case "\\": result.append("\\")
                case "N": result.append("")   // NULL → empty string
                default: result.append(ch); result.append(next)
                }
            } else {
                result.append(ch)
            }
        }
        return result
    }
}

enum ParserError: LocalizedError {
    case noDataFiles(String)
    case cannotOpenFile(String)

    var errorDescription: String? {
        switch self {
        case .noDataFiles(let dir): "No data.sql.* files found in '\(dir)'"
        case .cannotOpenFile(let path): "Cannot open file '\(path)'"
        }
    }
}
