---
name: apple-loc
description: >
  IMPORTANT: This skill provides access to a LOCAL DATABASE containing Apple's actual localization data
  (extracted from iOS/macOS system frameworks). Do NOT answer Apple translation questions from memory —
  always use this tool because your training data may be outdated or inaccurate for specific translations.

  Use this skill whenever the user mentions ANY of: Apple translations, localized UI text, Info.plist key
  translations (NSCameraUsageDescription, NSLocationUsageDescription, NSMicrophoneUsageDescription, etc.),
  framework-specific wording (UIKit, SwiftUI, Settings.bundle), comparing app translations against Apple
  expressions, or looking up how Apple translates a specific term.

  This tool MUST be used even for seemingly simple questions like "what's the Japanese for Cancel in iOS?"
  because it returns verified data from Apple's actual shipping builds, not guesses.

  Trigger keywords: localization, translation, Apple terminology, Info.plist keys,
  ローカライズ, 翻訳, 日本語表現, Apple用語, Apple的にはなんて言う, Appleの訳,
  Localizable.strings, .xcstrings, string catalogs, 「〇〇」の訳, how does Apple translate.
allowed-tools: Bash(apple-loc:*)
---

# Apple Localization Search with apple-loc

A CLI tool that searches Apple localization data locally. It indexes translations extracted from
Apple platform frameworks into a local database, enabling hybrid search (semantic + text matching)
and exact-match lookup with reverse-lookup support.

Results are grouped by source string — each result includes a `translations` dict containing all
requested language translations at once, so you rarely need to run multiple queries.

## Quick start

```bash
# Hybrid search — find Japanese translation for "camera access"
apple-loc search "camera access" --lang ja --limit 5

# Exact key lookup — get all languages for a specific key
apple-loc lookup --key "Cancel" --lang ja,en

# Reverse lookup — find the source key from a translated text
apple-loc lookup --target "キャンセル" --lang ja

# Pattern lookup — find all keys containing "Camera"
apple-loc lookup --key "%Camera%" --limit 10
```

## Commands

### search — Hybrid search (semantic + text)

Combines semantic vector search with exact/partial text matching. The search automatically runs
up to three phases and merges the results, ranked by relevance.

**How it works:**
1. **Exact match** (`distance = -1`): Finds translations that exactly match the query text
2. **Partial match** (`distance = 0`): Finds translations containing the query via LIKE
3. **Semantic match** (`distance > 0`): Vector similarity via NLContextualEmbedding — available for any language with embeddings (depends on `--embed` setting during ingest)

The query language is auto-detected. If embeddings exist for that language, semantic search is used;
otherwise, it falls back to text matching only. Use `--query-lang` to override auto-detection if needed.

```bash
apple-loc search "<query>" [--lang ja,en] [--framework <name>] [--platform <name>] [--query-lang <lang>] [--limit 5] [--db <path>]
```

| Option | Default | Description |
|---|---|---|
| `<query>` | (required) | Natural language search query |
| `--lang` | all languages | Output language filter (comma-separated, e.g. `ja`, `ja,en`) |
| `--framework` | all | Filter by bundle name (case-insensitive substring, matches all originating bundles) |
| `--platform` | all | Filter by platform (e.g., `ios26`, `macos15`) |
| `--query-lang` | auto-detect | Override query language for text search (e.g. `ja`) |
| `--limit` | `5` | Max results |
| `--db` | `~/.apple-loc/apple-loc.db` | Database path |

**Output format (JSON):**
```json
{
  "results": [
    {
      "bundle_name": "UIKitCore.framework",
      "bundles": ["UIKitCore.framework", "AVFoundation.framework"],
      "distance": -1,
      "file_name": "Localizable.strings",
      "platform": "ios26",
      "source": "Camera Access",
      "translations": {
        "en": "Camera Access",
        "ja": "カメラアクセス"
      }
    }
  ]
}
```

- `source` — English original or localization key
- `translations` — Dict of `{ language_code: translated_text }` for all requested languages
- `distance` — `-1` = exact match, `0` = partial match, `> 0` = semantic distance (lower = more similar)
- `bundles` — All originating bundles for this source string (sorted). `null` in compact mode. `--framework` filter matches against all bundles listed here.

### lookup — Exact / pattern / reverse search

Search by source key (`--key`) or by translated text (`--target`). Supports exact match and `%` wildcards.
`--key` and `--target` are mutually exclusive — specify exactly one.

```bash
# Forward: search by source key
apple-loc lookup --key "<key>" [--lang ja] [--framework <name>] [--platform <name>] [--limit 20] [--db <path>]

# Reverse: search by translated text
apple-loc lookup --target "<text>" [--lang ja] [--framework <name>] [--platform <name>] [--limit 20] [--db <path>]
```

| Option | Default | Description |
|---|---|---|
| `--key` | (exclusive with --target) | Source key to search. Use `%` for LIKE wildcards. |
| `--target` | (exclusive with --key) | Target text to reverse-lookup. Use `%` for LIKE wildcards. |
| `--fuzzy` | off | Wraps the search term with `%` wildcards automatically (substring match) |
| `--lang` | all languages | Output language filter (comma-separated) |
| `--framework` | all | Filter by bundle name (case-insensitive substring, matches all originating bundles) |
| `--platform` | all | Filter by platform |
| `--limit` | `20` | Max results |
| `--db` | `~/.apple-loc/apple-loc.db` | Database path |

**Search behavior:**
- `--key "Cancel"` — exact match
- `--key "%Camera%"` — pattern match (LIKE)
- `--key "Home" --fuzzy` — equivalent to `--key "%Home%"` (finds `TAB_HOME`, `HOME_SCREEN`, etc.)
- `--target "キャンセル"` — reverse lookup: find the source key for this translation
- `--target "許可" --fuzzy` — reverse pattern: find source keys whose translations contain "許可"

**Output format (JSON):**
```json
{
  "results": [
    {
      "bundle_name": "UIKitCore.framework",
      "bundles": ["AppKit.framework", "Photos.framework", "UIKitCore.framework"],
      "distance": null,
      "file_name": "Localizable.strings",
      "platform": "ios26",
      "source": "Cancel",
      "translations": {
        "en": "Cancel",
        "ja": "キャンセル"
      }
    }
  ]
}
```

Note: `distance` is always `null` for lookup results. `bundles` is `null` in compact mode.

### ingest — Import localization data

Import Apple localization data from PostgreSQL dump files into a local database.
This is a one-time setup step. See [references/ingest-guide.md](references/ingest-guide.md) for details.

```bash
apple-loc ingest --data-dir <path> [--langs en,ja,fr] [--platform ios26] [--embed en,ja] [--db <path>] [--force] [--compact] [--concurrency 8]
```

The `--embed` option controls which languages get vector embeddings for semantic search:
- `en` (default) — English only
- `ja,en` etc. — specified languages (parallel per language)
- `none` — skip embedding (lookup-only usage)

Language variants (`es_419`, `fr_CA`, `zh_HK`, etc.) are supported — embedding automatically
falls back to the base language model (`es`, `fr`, `zh-Hant`).

The `--compact` option skips the `source_bundles` table, reducing database size. In compact mode,
`--framework` only matches the primary (highest-priority) bundle, and `bundles` in results is `null`.

## Choosing search vs. lookup

| Scenario | Command | Why |
|---|---|---|
| Know the exact key name | `lookup --key` | Precise match, fastest |
| Know partial key name | `lookup --key --fuzzy` | Substring match without typing `%` wildcards |
| Know the translated text, want the source key | `lookup --target` | Reverse lookup |
| Have a vague description or concept | `search` | Hybrid search finds related translations by meaning |
| Want multilingual results in one call | either with `--lang ja,en,fr` | Both commands return grouped translations |

## Example workflows

### Find Japanese for a UI concept

When you have a vague idea but don't know the exact key:

```bash
apple-loc search "camera access permission" --lang ja --limit 5
```

The hybrid search returns translations whose meaning is close to the query,
even if the exact wording differs. Exact and partial matches are ranked highest.

### Get English and Japanese together

Both commands support comma-separated `--lang`, so a single call returns both:

```bash
apple-loc lookup --key "Cancel" --lang en,ja
```

```json
{
  "results": [
    {
      "source": "Cancel",
      "translations": { "en": "Cancel", "ja": "キャンセル" },
      ...
    }
  ]
}
```

### Reverse lookup — find the key from a translation

When you have the translated text but need the source key or other language versions:

```bash
apple-loc lookup --target "許可" --lang ja,en --limit 10
```

This finds all source strings whose Japanese translation contains "許可" and shows
both the English source and Japanese translation.

### Search within a specific framework

```bash
apple-loc search "privacy" --lang en --framework UIKit --limit 10
```

The `--framework` filter is a case-insensitive substring match against all originating bundles,
so `UIKit` matches `UIKit.framework`, `UIKitCore.framework`, etc. A source string like "Cancel"
appearing in AppKit.framework (macOS), UIKit.framework (iOS), and Photos.framework is found by any of those filters.

### Compare across platforms

Check how a translation differs between iOS and macOS:

```bash
apple-loc search "photo library" --lang ja --platform ios26 --limit 5
apple-loc search "photo library" --lang ja --platform macos26 --limit 5
```

### Lookup in various languages

The `lookup` command works with any language in the database.
The `search` command uses semantic matching for any language that has embeddings
(configured via `--embed` during ingest). For languages without embeddings, it falls back to text matching.

```bash
# Korean lookup
apple-loc lookup --key "Cancel" --lang ko

# French search (text + semantic if French embeddings were ingested)
apple-loc search "accès à la caméra" --lang fr --limit 5

# Japanese semantic search (requires --embed ja,en during ingest)
apple-loc search "設定" --lang ja --limit 5
```

### Compare language variants

Compare translations between regional variants in a single query:

```bash
apple-loc lookup --key "Cancel" --lang fr,fr_CA,es,es_419,zh_HK
```

## String Catalog (.xcstrings) workflow

When helping users localize an iOS/macOS app using apple-loc, follow these conventions.

### Comment convention — always record provenance

Every translated entry in the String Catalog MUST include a `comment` field with the apple-loc source:

```json
"Cancel": {
  "comment": "apple-loc: SwiftUI.framework",
  "localizations": { ... }
}
```

Format: `apple-loc: <bundle_name>`. For context-specific lookups, append the key:
`apple-loc: GameStoreKit.framework (TAB_HOME)`. For pattern-matched sentences:
`apple-loc: MusicFoundation_MusicCore.bundle (pattern)`.

This lets translators and reviewers see which Apple framework each translation was sourced from.

### Context verification — don't trust the first result blindly

Many English terms have multiple Apple translations depending on context:

| English | Wrong context | Correct context |
|---|---|---|
| Home | 自宅 (Maps address) | ホーム (tab bar label) |
| Appearance | 外観 (generic) | 外観モード (Settings) |
| Recent | 最近使った項目 (iWork) | 最近の項目 (general section header) |

**Workflow for ambiguous terms:**

1. `lookup --key` first — check if the result fits your UI context
2. If not, `lookup --target` with the expected translation to find the correct source key
3. Verify the `bundle_name` / `source` key name matches your usage context (e.g., `TAB_HOME` for tab bars)

```bash
# Step 1: "Home" returns 自宅 — wrong for a tab label
apple-loc lookup --key "Home" --lang ja

# Step 2: fuzzy search to find all keys containing "Home"
apple-loc lookup --key "Home" --lang ja --fuzzy
# → TAB_HOME (GameStoreKit), HOME_SCREEN (SpringBoard), etc.

# Step 3: reverse-lookup the expected translation
apple-loc lookup --target "ホーム" --lang ja --limit 5
# → TAB_HOME (GameStoreKit), TITLE_HOME (PodcastsKit) — confirms ホーム for tabs
```

### Sentence-level patterns — use search, not lookup

For alert messages and confirmation dialogs, use `search` to find Apple's established patterns:

```bash
apple-loc search "Are you sure you want to delete" --lang ja --limit 3
```

Key patterns discovered across languages:

| Pattern | ja | de | fr | it | es |
|---|---|---|---|---|---|
| Delete confirmation | 〜を削除してもよろしいですか? | Soll ... gelöscht werden? | Voulez-vous vraiment supprimer ... ? | Confermi di voler eliminare ...? | ¿Seguro que quieres eliminar ...? |
| Cannot undo | この操作は取り消せません。 | Diese Aktion kann nicht widerrufen werden. | Cette action est irréversible. | L'azione è irreversibile. | Esta acción no se puede deshacer. |

### Multi-language batch lookup

Use comma-separated `--lang` for efficient multi-language queries:

```bash
apple-loc lookup --key "Settings" --lang ja,fr,de,it,es
```

This returns all translations in a single `translations` dict — no need for per-language queries.

### Currently available languages

The database contains only languages that have been ingested. Check availability:

```bash
apple-loc lookup --key "Cancel" --limit 1
# → The translations dict shows all available languages
```

## Available platforms

```
ios15  ios16  ios17  ios18  ios26
macos12  macos13  macos14  macos15  macos26
```

## Ingest recommendations

The full dataset (~30 GB, 43 languages, 10 platforms) is too large to import entirely.
Import only what you need:

- **`--platform`**: Limit to the latest version (e.g., `ios26`) unless you need historical comparison
- **`--langs`**: Specify only the languages your project supports
- **`--embed`**: Only embed languages you'll use for semantic search (e.g., `en,ja`)
- Language variants like `es_419`, `fr_CA`, `zh_HK` can be included in `--langs` — embedding
  falls back to the base language model automatically

```bash
# Typical setup for a Japanese iOS app
apple-loc ingest --data-dir ./applelocalization-data \
  --langs en,ja --platform ios26 --embed en,ja

# Multi-language app with regional variants
apple-loc ingest --data-dir ./applelocalization-data \
  --langs en,ja,fr,fr_CA,de,es,es_419,zh-Hans,zh-Hant,zh_HK,ko \
  --platform ios26 --embed en,ja
```

See [references/ingest-guide.md](references/ingest-guide.md) for full details.

## References

- **Data import guide** — [references/ingest-guide.md](references/ingest-guide.md)
- **NLContextualEmbedding language support** — [references/nlembedding-support.md](references/nlembedding-support.md)
