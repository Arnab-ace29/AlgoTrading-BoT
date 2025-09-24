# Moneycontrol Corporate Actions Scraper

This project scrapes corporate-action data (announcements, board meetings, dividends, splits, rights issues, and AGM/EGM notices) from the Moneycontrol public API, stores the results as JSON, and can stream updates into a local MongoDB collection.

## Features
- Crawls Moneycontrol's stock directory (0-9, a-z search seeds) to build a clean symbol list.
- Incrementally fetches corporate-action sections per stock, stopping once existing data is reached.
- Persists structured JSON snapshots under `Historic Data/` for reuse.
- Optional MongoDB integration with chunked upserts and automatic `id` indexing.

## Requirements
- Python 3.9+
- `requests`
- `pymongo` (only when pushing to MongoDB)

Install dependencies:

```bash
pip install -r requirements.txt  # or
pip install requests pymongo
```

## Usage

All functionality is exposed via `moneycontrol_dividends.py`.

```bash
python moneycontrol_dividends.py [flags]
```

### CLI Flags
| Flag | Type / Values | Description |
| --- | --- | --- |
| `--refresh-metadata` | flag | Force a fresh scrape of stock metadata even if a cached JSON exists. |
| `--only` | comma-separated list | Restrict the run to specific stock ids (e.g. `SBIN,TCS`) and/or section codes (`an,bm,d,s,r,ae`). Section codes are case-insensitive. |
| `--push-only` | flag | Skip scraping and only load the existing JSON payload for MongoDB insertion. Useful after a previous scrape. |
| `--json-path` | file path | Override the default JSON location (`Historic Data/moneycontrol_corporate_actions.json`). Accepts relative or absolute paths. |
| `--push-to-mongo` | flag | Enable MongoDB upserts. When set, the script writes the selected dataset to the target collection. |
| `--mongo-uri` | string (default `mongodb://localhost:27017`) | MongoDB connection string. |
| `--mongo-db` | string (default `moneycontrol`) | Database name for Mongo writes. |
| `--mongo-collection` | string (default `corporate_actions`) | Collection receiving the corporate-action documents. |
| `--chunk-size` | integer (default `50`) | Maximum number of stock documents per Mongo bulk write. Keep between 25 and 50 to limit data loss on crash. |

### Common Workflows

#### Full scrape + Mongo push
```bash
python moneycontrol_dividends.py --refresh-metadata --push-to-mongo
```

#### Incremental daily refresh
```bash
python moneycontrol_dividends.py
```
*(Skips metadata scrape if cached and stops per-section once existing data is found.)*

#### Refresh a subset and push to Mongo
```bash
python moneycontrol_dividends.py --only SBIN,TCS --push-to-mongo --chunk-size 25
```

#### Push existing JSON only
```bash
python moneycontrol_dividends.py --push-only --push-to-mongo
```

## Output Files
- `Historic Data/moneycontrol_stock_metadata.json`
- `Historic Data/moneycontrol_corporate_actions.json`

These files are overwritten on each scrape unless `--push-only` is used.

## MongoDB Notes
- Requires a running MongoDB instance (defaults to `mongodb://localhost:27017`).
- Each stock document is upserted by its `id` field.
- Index `id_1` is created automatically if missing.

## Troubleshooting
- Moneycontrol occasionally responds with `403` or empty payloads; the script retries automatically.
- If `pymongo` is missing, Mongo pushes are skipped and a message is printed.
- To clear cached data, delete the JSON files under `Historic Data/` before re-running.
