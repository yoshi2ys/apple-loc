import Foundation

/// A parsed row from localization data.
struct ParsedRow: Sendable {
    let id: Int?
    let groupId: Int?
    let source: String      // English original / key
    let target: String      // Translated text
    let language: String
    let fileName: String
    let bundleName: String
    let platform: String    // Derived from directory name (e.g. "ios26", "macos15")
}

enum ParserError: LocalizedError {
    case noDataFiles(String)

    var errorDescription: String? {
        switch self {
        case .noDataFiles(let dir): "No data files found in '\(dir)'"
        }
    }
}
