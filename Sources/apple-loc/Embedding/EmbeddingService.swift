import Foundation
import NaturalLanguage

/// Wraps NLContextualEmbedding to produce 512-dim sentence vectors via mean pooling.
final class EmbeddingService: @unchecked Sendable {
    let language: NLLanguage
    let dimension: Int
    private let model: NLContextualEmbedding

    /// Returns nil if the language has no contextual embedding model available.
    init?(language: NLLanguage) {
        let models = NLContextualEmbedding.contextualEmbeddings(
            forValues: [.languages: [language.rawValue]]
        )
        guard let model = models.first else { return nil }
        self.model = model
        self.language = language
        self.dimension = model.dimension
    }

    /// Load the model assets. Call once before generating embeddings.
    func load() throws {
        if !model.hasAvailableAssets {
            throw EmbeddingError.assetsUnavailable(language.rawValue)
        }
        try model.load()
    }

    /// Generate a sentence embedding by mean-pooling token vectors.
    func embed(_ text: String) throws -> [Float] {
        let result = try model.embeddingResult(for: text, language: language)
        var sum = [Double](repeating: 0, count: dimension)
        var count = 0
        result.enumerateTokenVectors(in: text.startIndex..<text.endIndex) { tokenVector, _ in
            for i in 0..<min(tokenVector.count, self.dimension) {
                sum[i] += Double(tokenVector[i])
            }
            count += 1
            return true
        }
        guard count > 0 else {
            return [Float](repeating: 0, count: dimension)
        }
        return sum.map { Float($0 / Double(count)) }
    }

    /// Resolve a language variant to the NLLanguage supported by NLContextualEmbedding.
    /// e.g. "es_419" → .spanish, "zh_HK" → .traditionalChinese, "fr_CA" → .french
    /// Returns nil if no embedding model is available for the language or its base.
    static func resolveLanguage(for code: String) -> NLLanguage? {
        // Try exact match
        if !NLContextualEmbedding.contextualEmbeddings(forValues: [.languages: [code]]).isEmpty {
            return NLLanguage(rawValue: code)
        }
        // Fallback to base language
        let base = baseLanguageCode(for: code)
        if base != code,
           !NLContextualEmbedding.contextualEmbeddings(forValues: [.languages: [base]]).isEmpty {
            return NLLanguage(rawValue: base)
        }
        return nil
    }

    private static func baseLanguageCode(for code: String) -> String {
        switch code {
        case "zh_HK", "zh_TW", "zh-HK", "zh-TW": return "zh-Hant"
        case "zh_CN", "zh-CN": return "zh-Hans"
        default: break
        }
        // Generic: strip region (es_419 → es, fr_CA → fr, en_AU → en)
        if let idx = code.firstIndex(where: { $0 == "_" || $0 == "-" }) {
            return String(code[..<idx])
        }
        return code
    }

    /// Check which languages from a list support contextual embeddings.
    static func supportedLanguages(from codes: [String]) -> (supported: [String], unsupported: [String]) {
        var supported: [String] = []
        var unsupported: [String] = []
        for code in codes {
            let models = NLContextualEmbedding.contextualEmbeddings(
                forValues: [.languages: [code]]
            )
            if models.isEmpty {
                unsupported.append(code)
            } else {
                supported.append(code)
            }
        }
        return (supported, unsupported)
    }

}

enum EmbeddingError: LocalizedError {
    case assetsUnavailable(String)
    case unsupportedLanguage(String)

    var errorDescription: String? {
        switch self {
        case .assetsUnavailable(let lang):
            "NLContextualEmbedding assets not available for '\(lang)'. Download may be required."
        case .unsupportedLanguage(let lang):
            "Semantic search is not supported for language '\(lang)'. Use 'lookup' instead."
        }
    }
}
