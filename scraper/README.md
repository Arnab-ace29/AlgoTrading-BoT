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
5. Persist outputs to JSON (as a portable backup) and/or MongoDB (canonical store).

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
Uploads all corporate actions and metadata to MongoDB while updating the JSON backups.

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

## JSON Backups
- `Historic Data/moneycontrol_stock_metadata.json`
- `Historic Data/moneycontrol_corporate_actions.json`

These remain as portable snapshots; the scraper still works on a clean machine by reading directly from MongoDB.

## MongoDB Notes
- Two collections are used: `corporate_actions` and `stock_metadata` (configurable via flags).
- Documents are upserted on `id`, and identifier keys are stored without the `SC_` prefix for easier querying.
- Indexes on `id` are created automatically if they do not exist.

## Troubleshooting
- Moneycontrol occasionally returns HTTP 403 or empty payloads; the scraper retries with back-off.
- If `pymongo` is missing, Mongo operations are skipped with a warning.
- To reset the cache, drop the Mongo collections or delete the JSON backups before rerunning the script.
