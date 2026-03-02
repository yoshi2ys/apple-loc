# apple-loc

CLI to locally search translation data extracted from Apple platform frameworks.

When localizing iOS/macOS apps, you often want to know how Apple translates a specific term. This tool searches translations extracted from Apple platform frameworks locally, with semantic search that finds relevant results even when the wording doesn't match exactly.

## Get Started

```bash
git clone https://github.com/yoshi2ys/apple-loc.git && cd apple-loc
swift build -c release
mkdir -p ~/.local/bin
ln -sf "$(swift build -c release --show-bin-path)/apple-loc" ~/.local/bin/apple-loc

# Download localization data (~28 GB)
git clone https://github.com/kishikawakatsumi/applelocalization-tools.git

apple-loc ingest --data-dir ./applelocalization-tools --platform macos26,ios26
```

> **Note:** Embedding computation takes time. Use `--embed none` for a faster ingest without semantic search, or limit languages with `--embed en` to reduce the scope.

## Usage

### ingest

Import translation data and build the search database.

```bash
# Basic: latest iOS, English + Japanese semantic search
apple-loc ingest --data-dir ./applelocalization-tools --platform ios26 --embed en,ja

# Specific languages only
apple-loc ingest --data-dir ./applelocalization-tools --langs en,en_AU,en_GB,es_US,fr,fr_CA,de,it,ja,es,es_419 --platform ios26

# Multiple platforms
apple-loc ingest --data-dir ./applelocalization-tools --platform macos26,ios26

# Re-ingest with different settings
apple-loc ingest --data-dir ./applelocalization-tools --platform ios26 --embed en,ja --force
```

**Options:**

| Option | Description |
|---|---|
| `--data-dir` | Directory containing JSON data files (required) |
| `--platform` | Platform filter, e.g. `ios26`, `macos26` (default: all) |
| `--langs` | Language codes to import, e.g. `en,ja,fr` (default: all) |
| `--embed` | Languages for semantic search: `en` (default), `ja,en`, or `none` |
| `--concurrency` | Parallel embedding workers per language (default: CPU cores / 2) |
| `--compact` | Skip `source_bundles` table (saves space, `--framework` matches primary bundle only) |
| `--force` | Overwrite existing database |

**`--embed`** controls which languages get semantic search. Languages not included still work with `lookup` and text matching in `search`.

| Value | Behavior |
|---|---|
| `en` (default) | English semantic search only |
| `ja,en` etc. | Semantic search in the specified languages |
| `none` | Skip embedding — faster ingest, text matching only |

Language variants like `es_419`, `fr_CA`, `zh_HK` are supported — embedding automatically falls back to the base language model (`es`, `fr`, `zh-Hant`).

**`--concurrency`** sets the number of parallel workers per language. Total workers (concurrency × number of embed languages) must not exceed CPU core count.

```bash
# 8 workers × 2 languages = 16 total (OK on 16-core machine)
apple-loc ingest --data-dir ./applelocalization-tools --embed en,ja --concurrency 8 --force
```

### search

Find translations by meaning. Combines semantic search (when embeddings exist) with text matching.

```bash
apple-loc search "camera permission"

apple-loc search "privacy settings" --lang ja

# Japanese semantic search (requires --embed ja,en during ingest)
apple-loc search "家に帰る" --lang ja

# Explicit query language override
apple-loc search "設定" --query-lang ja --lang ja

# Filter by framework
apple-loc search "camera" --lang ja --framework UIKit
```

**Options:**

| Option | Description |
|---|---|
| `--lang` | Output language filter, e.g. `ja`, `en,ja,fr` (default: all) |
| `--framework` | Filter by framework/bundle name (substring match, case-insensitive) |
| `--platform` | Filter by platform, e.g. `ios26` |
| `--query-lang` | Query language override for text search (default: auto-detect) |
| `--limit` | Maximum results (default: 5) |

### lookup

Find translations by exact key or text. No embedding required.

```bash
apple-loc lookup --key "Cancel" --lang ja

apple-loc lookup --key "%Camera%" --platform ios26

# Reverse lookup: find source keys by translation text
apple-loc lookup --target "許可" --lang ja

# Compare language variants
apple-loc lookup --key "Cancel" --lang fr,fr_CA,es,es_419,zh_HK
```

`--fuzzy` wraps the search term with `%` wildcards automatically:

```bash
# These are equivalent:
apple-loc lookup --key "%Home%" --lang ja
apple-loc lookup --key "Home" --lang ja --fuzzy

# Find all translations of "自宅"
apple-loc lookup --target "自宅" --lang ja --fuzzy
```

**Options:**

| Option | Description |
|---|---|
| `--key` | Source key to search (supports `%` wildcards). Exclusive with `--target` |
| `--target` | Target text to reverse-lookup (supports `%` wildcards). Exclusive with `--key` |
| `--lang` | Output language filter, e.g. `ja`, `en,ja,fr` (default: all) |
| `--framework` | Filter by framework/bundle name (substring match, case-insensitive) |
| `--platform` | Filter by platform, e.g. `ios26` |
| `--fuzzy` | Wrap key/target with `%` wildcards for substring matching |
| `--limit` | Maximum results (default: 20) |

`--framework` searches all originating bundles per source string. For example, "Cancel" exists in AppKit.framework (macOS), UIKit.framework (iOS), Photos.framework etc. — `--framework Photos` finds it. With `--compact` ingest, only the primary (highest-priority) bundle is matched.

See `apple-loc <command> --help` for all options.

## Claude Code Skill

Install the skill to let Claude Code look up Apple translations during conversations.

```bash
cp -r apple-loc-skill ~/.claude/skills/apple-loc
```

## Limitations

- **macOS only** — semantic search uses Apple's NLContextualEmbedding framework.
- **Semantic search requires embedding** — only languages included in `--embed` during ingest support semantic search. Others fall back to text matching.

## Thanks

Built on [kishikawakatsumi/applelocalization-tools](https://github.com/kishikawakatsumi/applelocalization-tools). If you're looking for a web-based search, check out [applelocalization.com](https://applelocalization.com/).
