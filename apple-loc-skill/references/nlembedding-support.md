# NLContextualEmbedding Language Support

## Overview

The `search` command uses Apple's NLContextualEmbedding framework to vectorize translation texts
and find semantically similar results. All supported languages produce 512-dimensional
vectors (token-level embeddings, mean-pooled for sentence representation).

Which languages have embeddings depends on the `--embed` option used during ingest (e.g., `--embed en,ja`).
Semantic search is available for those languages; others fall back to text matching.

Language variants (`es_419`, `fr_CA`, `zh_HK`, etc.) automatically use the base language model
for embedding (`es`, `fr`, `zh-Hant`).

The `lookup` command works with ANY language in the database — it does not require embedding support.

## Supported languages (NLContextualEmbedding available)

| Code | Language |
|---|---|
| `ar` | Arabic |
| `bg` | Bulgarian |
| `cs` | Czech |
| `da` | Danish |
| `de` | German |
| `en` | English |
| `es` | Spanish |
| `fi` | Finnish |
| `fr` | French |
| `hi` | Hindi |
| `hr` | Croatian |
| `hu` | Hungarian |
| `id` | Indonesian |
| `it` | Italian |
| `ja` | Japanese |
| `kk` | Kazakh |
| `ko` | Korean |
| `nl` | Dutch |
| `pl` | Polish |
| `pt` | Portuguese |
| `ro` | Romanian |
| `ru` | Russian |
| `sk` | Slovak |
| `sv` | Swedish |
| `th` | Thai |
| `tr` | Turkish |
| `uk` | Ukrainian |
| `vi` | Vietnamese |
| `zh-Hans` | Simplified Chinese |
| `zh-Hant` | Traditional Chinese |

## Lookup-only languages (no semantic search)

Languages such as `ca` (Catalan), `el` (Greek), `he` (Hebrew), `ms` (Malay), `no` (Norwegian),
`yue_CN` (Cantonese), and others are available in the database for exact/pattern matching
via the `lookup` command, but do not support semantic `search`.

## Search behavior by query language

| Query language | Semantic search | Text search |
|---|---|---|
| Language with embeddings (e.g., en, ja) | Yes (vector similarity) | Yes (exact + partial) |
| Language without embeddings | No | Yes (searches all languages) |
| With `--query-lang` override | Depends on specified lang | Searches specified language only |

## Error behavior

When embedding model assets are not downloaded on the machine:

```
NLContextualEmbedding assets not available for '<lang>'. Download may be required.
```

macOS typically downloads embedding models on-demand when first requested.

## Technical notes

- **Framework:** NLContextualEmbedding (not NLEmbedding.sentenceEmbedding, which supports fewer languages)
- **Dimensions:** 512 for all languages (uniform)
- **Method:** Token-level embeddings are mean-pooled to produce sentence-level vectors
- **Thread safety:** NLContextualEmbedding is not thread-safe; the tool uses isolated instances with serial dispatch queues
- **Minimum macOS version:** macOS 15 (Sequoia)
