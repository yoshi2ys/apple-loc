import Foundation

/// A parsed row from localization data (SQL dump or JSON).
struct ParsedRow: Sendable {
    let id: Int?
    let groupId: Int?
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
        let filter = RowFilter(filterIBKeys: filterIBKeys)
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
                           shouldInclude(row, filter: filter) {
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
                                      shouldInclude(row, filter: filter) {
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

    /// Combined filter: language + shared RowFilter rules.
    private func shouldInclude(_ row: ParsedRow, filter: RowFilter) -> Bool {
        (allowedLanguages?.matchesLanguage(row.language) ?? true)
            && filter.shouldInclude(source: row.source, fileName: row.fileName)
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
