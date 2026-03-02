import Foundation

/// Counts of records in the database.
struct InfoCounts: Codable, Sendable {
    var sourceStrings: Int
    var translations: Int
    var vectors: Int

    enum CodingKeys: String, CodingKey {
        case sourceStrings = "source_strings"
        case translations, vectors
    }
}

/// Successful info output with database metadata.
struct InfoOutput: Codable, Sendable {
    var platforms: [String]
    var languages: [String]
    var embeddingLanguages: [String]
    var counts: InfoCounts

    enum CodingKeys: String, CodingKey {
        case platforms, languages, counts
        case embeddingLanguages = "embedding_languages"
    }
}

/// Error output when database is not found.
struct InfoError: Codable, Sendable {
    var error: String
    var dbPath: String

    enum CodingKeys: String, CodingKey {
        case error
        case dbPath = "db_path"
    }
}
