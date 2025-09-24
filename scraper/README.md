# Moneycontrol Corporate Actions Scraper

This project scrapes corporate-action data (announcements, board meetings, dividends, splits, rights issues, and AGM/EGM notices) from the Moneycontrol public API, stores the results as JSON for backup, and keeps an up-to-date copy in MongoDB.

## Features
- Crawls Moneycontrol's stock directory (0-9, a-z search seeds) to build a clean symbol list.
- Pulls extended identifiers (ISIN, BSE/NSE codes, share count, ticker name) from the `scmas-details` endpoint for every stock.
- Incrementally fetches corporate-action sections per stock, stopping once existing data is reached.
- Normalises section payloads so there are no duplicate aliases (e.g. only `d["dividend"]`).
- Persists structured JSON snapshots in `Historic Data/` as a portable backup while MongoDB holds the canonical dataset.
- Optional MongoDB integration with chunked upserts, automatic `id` indexing, and cleaned symbol keys (SC_ prefix removed when stored).

## Requirements
- Python 3.9+
- `requests`
- `pymongo` (only when pushing to MongoDB or using Mongo as the cache source)

Install dependencies:

```bash
pip install -r requirements.txt  # or
pip install requests pymongo
```

## Usage

The entry point is `moneycontrol_dividends.py`.

```bash
python moneycontrol_dividends.py [flags]
```

### CLI Flags
| Flag | Type / Values | Description |
| --- | --- | --- |
| `--refresh-metadata` | flag | Force a fresh scrape of stock metadata even if cached locally/Mongo. |
| `--refresh-details` | flag | Force refresh of extended symbol identifiers (ISIN/BSE/NSE/ticker/share count) even if cached. |
| `--only` | comma-separated list | Restrict the run to specific stock ids (e.g. `SBIN,TCS`) and/or section codes (`an,bm,d,s,r,ae`). Section codes are case-insensitive. |
| `--push-only` | flag | Skip scraping and operate on the cached Mongo/JSON payload (combine with `--refresh-details` to refresh identifiers before pushing). |
| `--json-path` | file path | Override the default JSON backup location (`Historic Data/moneycontrol_corporate_actions.json`). |
| `--push-to-mongo` | flag | Enable MongoDB upserts. When set, the script writes the selected dataset to the target collections. |
| `--mongo-uri` | string (default `mongodb://localhost:27017`) | MongoDB connection string. |
| `--mongo-db` | string (default `moneycontrol`) | Target database for Mongo writes and reads. |
| `--mongo-collection` | string (default `corporate_actions`) | Mongo collection that stores corporate-action documents. |
| `--mongo-metadata-collection` | string (default `stock_metadata`) | Mongo collection that stores stock metadata/identifier documents. |
| `--chunk-size` | integer (default `50`) | Maximum number of stock documents per Mongo bulk write. Keep between 25 and 50 to limit data loss on crash. |

### Common Workflows

#### Full scrape + Mongo push
```bash
python moneycontrol_dividends.py --refresh-metadata --refresh-details --push-to-mongo
```

#### Incremental daily refresh (reads from Mongo cache when available)
```bash
python moneycontrol_dividends.py
```
*(Skips metadata scrape if cached, stops per-section once existing data is found, and falls back to Mongo if local JSON files are missing.)*

#### Refresh a subset and push to Mongo
```bash
python moneycontrol_dividends.py --only SBIN,TCS --refresh-details --push-to-mongo --chunk-size 25
```

#### Push existing Mongo/JSON snapshot only
```bash
python moneycontrol_dividends.py --push-only --refresh-details --push-to-mongo
```

## Output Files
- `Historic Data/moneycontrol_stock_metadata.json`
- `Historic Data/moneycontrol_corporate_actions.json`

These files are written as backups. MongoDB remains the canonical source, so the scraper works even when the JSON files are absent on a fresh machine.

## MongoDB Notes
- Requires a running MongoDB instance (defaults to `mongodb://localhost:27017`).
- Each document is upserted by its `id` field; identifier keys are stored without the `SC_` prefix in Mongo.
- Index `id_1` is created automatically if missing on either collection.

## Troubleshooting
- Moneycontrol occasionally responds with `403` or empty payloads; the script retries automatically.
- If `pymongo` is missing, Mongo reads/writes are skipped and a message is printed.
- To reset the cache, drop the Mongo collections or delete the JSON backups before re-running.
