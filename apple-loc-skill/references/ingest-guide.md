# Data Import Guide

## Prerequisites

1. Clone the localization data repository:
   ```bash
   git clone https://github.com/kishikawakatsumi/applelocalization-data.git
   ```

2. The repository contains PostgreSQL COPY format files (tab-delimited `.sql.*` files),
   organized by platform (ios15–ios26, macos12–macos26).

## Recommended import

Import only the languages and platforms you need. The full dataset is ~30 GB across 43 languages
and 10 platforms — importing everything takes significant time and disk space.

```bash
# Latest iOS, key languages, Japanese + English semantic search
apple-loc ingest --data-dir ./applelocalization-data \
  --langs en,ja,fr,de,es,ko,zh-Hans,zh-Hant \
  --platform ios26 \
  --embed en,ja
```

This creates the database at `~/.apple-loc/apple-loc.db`.

## Options

| Option | Default | Description |
|---|---|---|
| `--data-dir` | (required) | Path to the cloned data repository |
| `--langs` | all | Comma-separated language codes to import |
| `--platform` | all | Comma-separated platform filter (e.g., `ios26,macos26`) |
| `--db` | `~/.apple-loc/apple-loc.db` | Output database path |
| `--force` | false | Overwrite existing database |
| `--embed` | `en` | Embedding languages: `none`, `en`, or comma-separated codes (e.g. `ja,en`) |
| `--compact` | false | Skip source_bundles table (saves space, `--framework` matches primary bundle only) |
| `--concurrency` | CPU cores / 2 | Parallel embedding workers per language |

## Examples

```bash
# Latest iOS and macOS, all languages
apple-loc ingest --data-dir ./applelocalization-data --platform ios26,macos26

# With language variants (Canadian French, Latin American Spanish, Hong Kong Chinese)
apple-loc ingest --data-dir ./applelocalization-data \
  --langs en,ja,fr,fr_CA,es,es_419,zh-Hans,zh-Hant,zh_HK,ko \
  --platform ios26 --embed en,ja

# Re-ingest with different settings
apple-loc ingest --data-dir ./applelocalization-data --platform ios26 --embed en,ja --force
```

Note: `--force` is required to overwrite an existing database.

## Embedding behavior

The `--embed` option controls which languages get vector embeddings for semantic search:

| Mode | Behavior |
|---|---|
| `en` (default) | English semantic search only |
| `ja,en` etc. | Semantic search in the specified languages |
| `none` | Skip embedding — faster ingest, text matching only |

- `--concurrency` sets the number of parallel workers **per language**. Total workers
  (concurrency × embed languages) must not exceed CPU core count.
- Language variants (`es_419`, `fr_CA`, `zh_HK`) automatically fall back to the base language
  model (`es`, `fr`, `zh-Hant`) for embedding.
- Languages not in `--embed` are still imported for text search and `lookup`.
- See [nlembedding-support.md](nlembedding-support.md) for supported embedding languages.

## Ingestion optimizations

The ingest pipeline applies several filters:

- **File exclusion**: InfoPlist, AppIntents, and AppShortcuts files are skipped
- **IB key exclusion**: Interface Builder Object ID keys (e.g. `D1K-K5-gc3.title`) are skipped
- **Metadata key exclusion**: `CFBundleName`, `CFBundleDisplayName`, `NSHumanReadableCopyright`, etc.
- **Format-only string exclusion**: Strings like `%@`, `%d %@` that are just format placeholders
- **Cross-bundle deduplication**: For the same source text on a given platform, only the entry
  from the highest-priority bundle is kept (Core frameworks > Frameworks > Apps > Plugins > Other)

## Progress output

Progress is printed to stderr during import:

```
  ⏭ macos12
  ⏭ macos13
  ▶ ios26
  50000 rows, 12000 sources, 11000 vectors | 195 rows/s | 4m12s
```

`▶` = processing, `⏭` = skipped (filtered by `--platform`).

## Output

On success, a JSON summary is printed to stdout.
