import Foundation

extension Array where Element == Float {
    /// Convert a float vector to raw bytes for sqlite-vec insertion/queries.
    var asData: Data {
        withUnsafeBufferPointer { Data(buffer: $0) }
    }
}

extension DatabaseManager {
    /// Default database path shared across all commands.
    static let defaultDBPath = "~/.apple-loc/apple-loc.db"
}

extension String {
    /// Split a comma-separated string into trimmed components.
    /// e.g. "ja,en" → ["ja", "en"]
    var commaSeparated: [String] {
        split(separator: ",").map(String.init)
    }
}
