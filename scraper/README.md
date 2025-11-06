# Moneycontrol Corporate Actions Scraper

This scraper pulls comprehensive corporate data for every stock listed on Moneycontrol and keeps it synchronised across JSON backups and a MongoDB datastore. The pipeline covers symbol discovery, detailed identifiers, corporate-action history, and Mongo-friendly data normalisation.

## What's Collected
- **Stock Directory**: All instruments discoverable through Moneycontrol's search API using `0-9` and `a-z` seeds.
- **Identifiers**: ISIN, BSE/NSE tickers, share count, and Moneycontrol ticker name from the `scmas-details` endpoint.
- **Corporate Actions**: Announcements, board meetings, dividends, splits, rights, and AGM/EGM events, with pagination handled automatically to fetch historical entries.

## Data Flow
1. Load existing data from MongoDB if available; otherwise fall back to JSON backups in `Historic Data/`.
2. Refresh symbol metadata (`--refresh-metadata`) or identifier details (`--refresh-details`) when required.
3. Incrementally fetch corporate-action sections, stopping once processed items overlap the cached data.
4. Normalise section payloads so each stores a single list (e.g. `d["dividend"]` only) to avoid duplication.
5. Persist outputs to JSON (as a portable backup) and/or MongoDB (canonical store), streaming Mongo upserts in `chunk-size` batches during the scrape to minimise data loss if the run is interrupted.

## CLI Reference
Run `python moneycontrol_dividends.py [flags]` from the `scraper/` directory. All flags are optional.

| Flag | Type | Description |
| --- | --- | --- |
| `--refresh-metadata` | flag | Force a fresh scrape of stock metadata even if cached locally or in MongoDB. |
| `--refresh-details` | flag | Force refresh of extended identifiers (ISIN/BSE/NSE, share count, ticker name). |
| `--only` | comma-separated list | Restrict the run to specific stock IDs (e.g. `SBIN,TCS`) and/or section codes (`an,bm,d,s,r,ae`). |
| `--push-only` | flag | Skip scraping and operate on existing Mongo/JSON data (combine with `--refresh-details` to update identifiers before pushing). |
| `--json-path` | path | Override the default JSON backup (`Historic Data/moneycontrol_corporate_actions.json`). |
| `--push-to-mongo` | flag | Upsert the resulting dataset into MongoDB (both corporate actions and metadata). |
| `--mongo-uri` | string | MongoDB connection string (default `mongodb://localhost:27017`). |
| `--mongo-db` | string | Mongo database name (default `moneycontrol`). |
| `--mongo-collection` | string | Corporate-action collection name (default `corporate_actions`). |
| `--mongo-metadata-collection` | string | Metadata collection name (default `stock_metadata`). |
| `--chunk-size` | int | Batch size for Mongo bulk writes (default `50`; keep between 25–50). |

## Common Workflows

### Full Refresh + Mongo Push
```bash
python moneycontrol_dividends.py --refresh-metadata --refresh-details --push-to-mongo
```
Uploads all corporate actions and metadata to MongoDB while updating the JSON backups. Mongo writes are streamed in chunks as soon as each batch is scraped, so partial runs still save their progress.

### Daily Incremental Run
```bash
python moneycontrol_dividends.py
```
Uses cached Mongo data when available, fetches only new corporate actions, updates JSON backups.

### Refresh Specific Stocks
```bash
python moneycontrol_dividends.py --only SBIN,TCS --refresh-details --push-to-mongo
```
Processes only the requested stocks/sections and pushes updates to Mongo.

### Push Cached Data Only
```bash
python moneycontrol_dividends.py --push-only --refresh-details --push-to-mongo
```
Skips scraping, refreshes identifiers, and pushes the latest cached data to MongoDB.

### Refresh Identifiers Only (no new action fetch)
```bash
python moneycontrol_dividends.py --refresh-details --push-only
```
Updates symbol identifiers from the `scmas-details` endpoint while reusing cached corporate actions. Handy after corporate disclosure changes without a new action cycle.

### Pull Only Dividends For Selected Symbols
```bash
python moneycontrol_dividends.py --only SBIN,TCS,d --push-to-mongo
```
Runs the dividend section (`d`) for the listed stocks and writes the result straight to MongoDB?useful for quick event checks on a watchlist.

### Rebuild JSON Backups From Mongo Snapshot
```bash
python moneycontrol_dividends.py --push-only --json-path backup/moneycontrol_corporate_actions.json
```
Regenerates fresh JSON backups from the current Mongo cache while leaving the database untouched.

### Push Existing Mongo Snapshot (no writes)
`ash
python moneycontrol_dividends.py --push-only
`
Loads data from Mongo (or JSON backup if Mongo is empty) and exits without rewriting either store. Good for sanity-checking the cached dataset.

### Push Existing JSON Backup to Mongo
`ash
python moneycontrol_dividends.py --push-only --json-path Historic Data/moneycontrol_corporate_actions.json --push-to-mongo
`
Replays the JSON backup into MongoDB?ideal when you import the project on a new machine and want Mongo to match the shipped snapshot.

### Inspect Metadata Without Corporate Sections
`ash
python moneycontrol_dividends.py --only SBIN,TCS --refresh-details --push-only
`
Refreshes identifiers for selected stocks and updates JSON/Mongo metadata without touching corporate-action sections. Useful for symbol verification scripts.

## FAQ
**Why keep JSON and Mongo?**
Mongo stores the canonical dataset for distributed use, while JSON snapshots remain easy to diff, share, or inspect offline.

**What happens if JSON files are deleted?**
The scraper pulls everything from Mongo on the next run. If Mongo is also empty, use --refresh-metadata and --refresh-details to rebuild from the APIs.

**How are duplicates avoided?**
Per-section pagination stops once existing entries are detected, and section payloads are normalised to a single list key (e.g. d["dividend"]).

**Do SC_ prefixed keys exist in Mongo?**
No. Keys like SC_ISINID are stored without the prefix (e.g. ISINID) for easier querying. When exporting to JSON, the script restores the original names for compatibility.

## JSON Backups
- `Historic Data/moneycontrol_stock_metadata.json`
- `Historic Data/moneycontrol_corporate_actions.json`

These remain as portable snapshots; the scraper still works on a clean machine by reading directly from MongoDB.

## Streaming Mongo Writes
- When `--push-to-mongo` is supplied during a scrape, documents are queued and flushed to Mongo as soon as each `chunk-size` batch finishes.
- Both corporate-action payloads and metadata records flush independently, so a crash or manual stop preserves everything processed up to that point.
- Remaining queued items are force-flushed before exit, and the legacy end-of-run push is skipped once streaming succeeded to avoid duplicate writes.

## MongoDB Notes
- Two collections are used: `corporate_actions` and `stock_metadata` (configurable via flags).
- Documents are upserted on `id`, and identifier keys are stored without the `SC_` prefix for easier querying.
- Indexes on `id` are created automatically if they do not exist.

## Troubleshooting
- Moneycontrol occasionally returns HTTP 403 or empty payloads; the scraper retries with back-off.
- If `pymongo` is missing, Mongo operations are skipped with a warning.
- To reset the cache, drop the Mongo collections or delete the JSON backups before rerunning the script.
