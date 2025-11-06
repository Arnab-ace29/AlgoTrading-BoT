# Stocks Fundamental Scraper

Collect, enrich, and persist fundamental data from [Screener.in](https://www.screener.in) for whole index rosters in a single run.  
The scraper now records Screener's “Top Ratios” directly while it ingests financial tables, keeping your metadata collection in sync without a second pass. A companion utility is retained for backfilling or selective refreshes.

---

## Key Capabilities

- Scrape the full Screener company page (quarters, profit & loss, balance sheet, cash flow, ratios) for every constituent in an index list.
- Persist section data to MongoDB collections or JSON snapshots with stable identifiers for easy merges.
- Enrich the `scrape metadata` collection with the latest top ratios, their display labels, raw values, and units in the same pass as the table scrape.
- Prioritise stale companies by reading the last update timestamp and retry transient HTTP failures with exponential backoff and optional proxy rotation.
- Optional helpers for standalone runs: dry-run mode, rate limiting, proxy pools, and local JSON snapshots for quick inspection.

---

## What Changed Recently

| Area | Update |
|------|--------|
| `scrape_screener.py` | Writes top ratios alongside section data, so Mongo metadata stays fresh after every scrape. |
| `update_scrape_metadata.py` | Reuses the shared parsing helpers; keep using it for one-off backfills or schema migrations. |
| Metadata schema | Ratio fields are flattened into stable keys with maps for labels, raw text, and units (`ratio_fields`, `ratio_field_map`, `ratio_raw_values`, `ratio_units`). |

---

## Prerequisites

1. **Python**: 3.9 or newer.
2. **Dependencies**: Install via pip (Mongo optional).
   ```bash
   python -m pip install -r requirements.txt
   # or
   python -m pip install requests pandas beautifulsoup4 tqdm pymongo
   ```
3. **MongoDB (optional)**: Running instance at `mongodb://localhost:27017` or supply your own URI via CLI flags.
4. **Source data**: Moneycontrol corporate actions/index constituents in Mongo or as JSON/CSV fallbacks (see below).

---

## Data Prerequisites

- **Corporate actions**  
  - Default Mongo collection: `moneycontrol.corporate_actions`  
  - Optional JSON fallback: `scraper/Historic Data/moneycontrol_corporate_actions.json`

- **Index constituents**  
  - Mongo collection `index_constituents` (default)  
  - or pass `--index-file path/to/index_constituents.json`  
  - Accepts simple strings (`500325,RELIANCE`) or dicts with `SC_BSEID`, `SC_NSEID`, `SC_ISINID`.

---

## Quick Start

```bash
cd stocks_fundamental_scraper

# 1) Scrape NIFTY50 constituents, write to Mongo, enrich metadata ratios
python scrape_screener.py --index NIFTY50

# 2) (Optional) Backfill or force-refresh ratios only
python update_scrape_metadata.py --force

# 3) Sample run without Mongo writes, saving JSON snapshots
python scrape_screener.py --index NIFTY50 --results-dir ./snapshots --disable-mongo
```

**Tip:** Use `--standalone` with either script to target the standalone financial view instead of consolidated results.

---

## Command Reference

### `scrape_screener.py`

| Flag | Description |
|------|-------------|
| `--index <name>` | Index alias, local file path, or `all` for every corporate entry. |
| `--index-file <path>` | Fallback JSON/TXT describing constituents when Mongo is unavailable. |
| `--corporate-actions <path>` | Fallback Moneycontrol dump for corporate metadata. |
| `--min-delay / --max-delay / --delay-jitter` | Pace requests to respect Screener throttling. |
| `--retry-limit / --retry-backoff / --retry-cap` | Exponential backoff settings per request. |
| `--proxy-file <path>` | Newline-delimited list of HTTP(S) proxies for rotation. |
| `--disable-mongo` | Skip Mongo upserts (useful for dry runs or snapshot-only mode). |
| `--results-dir <path>` | Persist JSON snapshots for each section. |
| `--standalone` | Fetch the standalone financial view instead of consolidated. |
| `--limit <n>` | Only scrape the first _n_ resolved companies (debugging). |

On success, each company produces:
- Section rows written to Mongo collections (`balance sheet`, `cash flow`, `profit loss`, `quarters`, `ratios`) when enabled.
- Metadata upsert in `scrape metadata` with fresh `ratio_*` fields, original slug, and update timestamp.
- Optional JSON snapshots under `results-dir`.

### `update_scrape_metadata.py`

Still useful for targeted backfills, schema upgrades, or forcing a refresh without re-scraping tables.

| Flag | Description |
|------|-------------|
| `--force` | Update every company regardless of existing ratio schema version. |
| `--limit <n>` | Cap the number of documents processed. |
| `--proxy-file`, `--no-default-proxies` | Control proxy rotation (same as main scraper). |
| `--retry-delay-min/max`, `--max-retries` | Fine-tune retry spacing. |
| `--dry-run` | Print intended changes without writing to Mongo. |
| `--standalone` | Fetch standalone financials. |

The script now imports the same helper functions used by `scrape_screener.py`, so both sources produce identical ratio field names and metadata.

---

## Output Structure

- **MongoDB**  
  - Collections named after Screener sections plus `scrape metadata`.  
  - Upsert keys include `Index`, `Resolved Slug`, `BSEID`, `NSEID`, `ISINID`, `Parent KPI`, `Child KPI`, `Row Type`.
  - Metadata stores `ratio_fields`, `ratio_field_map`, `ratio_units`, `ratio_raw_values`, `ratio_updated_at`, `ratio_slug`, `ratio_view`, and `ratio_schema_version`.

- **Snapshots (when `--results-dir` is set)**  
  - JSON per section `<results-dir>/<section>.json`.  
  - `exceptions.txt` lists `(BSEID,NSEID)` pairs that failed to scrape.  
  - JSON files are rewritten each run to keep them deduplicated; delete the folder to rebuild from scratch.

---

## Operational Tips

- Screener enforces rate limits. Start with the defaults (`min=1.0s`, `max=2.5s`) and adjust slowly; the retry logic will back off automatically on 429 responses.
- Rotate proxies only through endpoints you trust. The bundled fallback list is disabled when `--no-default-proxies` is supplied.
- For quicker metadata runs, reduce `--delay` and rely on the built-in retry delays. (A 1 s delay per company adds minutes to large runs.)
- When re-running frequently, consider `--limit` on staging environments to avoid hammering Screener during smoke tests.

---

## Development Workflow

1. Create and activate a virtual environment.
2. Install dependencies (`-r requirements.txt`).
3. Run basic checks before committing:
   ```bash
   python -m py_compile scrape_screener.py update_scrape_metadata.py
   # add lint/format commands here if your project uses them
   ```
4. Keep sensitive connection strings (Mongo URIs, proxy creds) out of committed files. Use environment variables or `.env`.

---

## Troubleshooting

| Symptom | Likely Cause & Fix |
|---------|--------------------|
| Lots of HTTP 429 errors | Slow down requests (`--min-delay`, `--max-delay`), enable proxies, or run off-peak. |
| Empty sections in output | Screener may not publish that data; check the website manually for the slug. |
| Metadata missing ratios after scrape | Ensure the Mongo user can write to `scrape metadata`; run with `--force` or the dedicated metadata script to rebuild fields. |
| JSON snapshots keep growing | Delete the snapshot directory before a fresh run to avoid legacy records from previous schemas. |

---

## Need a Backfill Only?

Use `update_scrape_metadata.py` when:
- You have legacy metadata without flattened ratio fields.
- Screener changed its top ratio layout and you want to refresh everything without re-scraping tables.
- You need to reprocess a small subset via `--limit` or a custom Mongo query (modify the script accordingly).

---

Happy scraping! Feel free to adapt the scripts to your scheduling and monitoring stack, and extend the output pipeline to suit your analytics workflows.
