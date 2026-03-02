import Dispatch
import Foundation
import NaturalLanguage

/// Manages a pool of NLContextualEmbedding workers for parallel, multi-language embedding generation.
struct EmbedWorkerPool {

    struct Worker {
        let language: String
        let service: EmbeddingService
        let queue: DispatchQueue
    }

    private let workersByLanguage: [String: [Worker]]

    var supportedLanguages: Set<String> { Set(workersByLanguage.keys) }

    /// Create a worker pool for the given language codes.
    /// - Parameters:
    ///   - languages: Target language codes (e.g. ["en", "ja"]).
    ///   - concurrency: Workers per language. Defaults to CPU cores / 2.
    ///   - log: Logging callback (writes to stderr by default).
    /// - Throws: `ValidationError` if total workers exceed CPU cores.
    init(languages: [String], concurrency: Int?, log: (String) -> Void = { logStderr($0) }) throws {
        let workerCount = concurrency ?? max(1, ProcessInfo.processInfo.activeProcessorCount / 2)
        let maxWorkers = ProcessInfo.processInfo.activeProcessorCount
        let totalWorkers = workerCount * languages.count

        if totalWorkers > maxWorkers {
            throw EmbedWorkerPoolError.tooManyWorkers(
                requested: totalWorkers,
                perLang: workerCount,
                langCount: languages.count,
                max: maxWorkers
            )
        }

        var allWorkers: [Worker] = []
        var loadedLangCount = 0

        for code in languages {
            guard let resolvedLang = EmbeddingService.resolveLanguage(for: code) else {
                log("Warning: no embedding model for '\(code)', skipping.")
                continue
            }
            let usedFallback = resolvedLang.rawValue != code
            for i in 0..<workerCount {
                guard let svc = EmbeddingService(language: resolvedLang) else { break }
                try svc.load()
                allWorkers.append(Worker(
                    language: code,
                    service: svc,
                    queue: DispatchQueue(label: "embed-\(code)-\(i)")
                ))
            }
            if allWorkers.last?.language == code {
                loadedLangCount += 1
                if usedFallback {
                    log("  '\(code)' → using '\(resolvedLang.rawValue)' embedding model")
                }
            }
        }
        log("Embedding workers: \(workerCount) × \(loadedLangCount) languages = \(allWorkers.count) total")

        self.workersByLanguage = Dictionary(grouping: allWorkers, by: \.language)
    }

    /// Embed a batch of texts in parallel, grouped by language.
    /// Returns an array parallel to `targets` with the resulting embedding or nil on failure.
    func embed(targets: [(language: String, text: String)]) -> [[Float]?] {
        guard !targets.isEmpty else { return [] }

        let targetsByLang = Dictionary(grouping: targets.indices, by: { targets[$0].language })
        var results = [[Float]?](repeating: nil, count: targets.count)

        for (lang, indices) in targetsByLang {
            guard let langWorkers = workersByLanguage[lang], !langWorkers.isEmpty else { continue }

            if langWorkers.count == 1 {
                // Single-worker path — no dispatch overhead
                for i in indices {
                    results[i] = try? langWorkers[0].service.embed(targets[i].text)
                }
            } else {
                // Multi-worker parallel path
                nonisolated(unsafe) let buf = UnsafeMutableBufferPointer<[Float]?>.allocate(capacity: indices.count)
                buf.initialize(repeating: nil)
                defer { buf.deallocate() }

                let group = DispatchGroup()
                for (j, idx) in indices.enumerated() {
                    let workerIdx = j % langWorkers.count
                    group.enter()
                    langWorkers[workerIdx].queue.async {
                        buf[j] = try? langWorkers[workerIdx].service.embed(targets[idx].text)
                        group.leave()
                    }
                }
                group.wait()

                for (j, idx) in indices.enumerated() {
                    results[idx] = buf[j]
                }
            }
        }

        return results
    }
}

enum EmbedWorkerPoolError: LocalizedError {
    case tooManyWorkers(requested: Int, perLang: Int, langCount: Int, max: Int)

    var errorDescription: String? {
        switch self {
        case .tooManyWorkers(let requested, let perLang, let langCount, let max):
            "Too many embedding workers: \(perLang) × \(langCount) languages = \(requested) (max: \(max)). Reduce --concurrency or the number of languages."
        }
    }
}
