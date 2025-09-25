# Stocks Fundamental Scraper

Python utility for collecting fundamentals from [Screener.in](https://www.screener.in/) for entire index lists. The tool expands parent KPIs, normalises values, appends incremental history, and can stream results straight into MongoDB collections for downstream analytics.

## Features

- Scrape multiple Screener sections (`quarters`, `profit-loss`, `balance-sheet`, `cash-flow`, `ratios`) for every company in a supplied index list.
- Expand child KPI schedules under each parent row and track parent/child relationships explicitly.
- Reads index and corporate metadata from a Moneycontrol source database (defaults to the `moneycontrol` Mongo DB) and pushes section metrics into the Screener database.
- Built-in request pacing with exponential backoff and optional proxy rotation to stay within Screener rate limits.
- Optionally export section data to JSON snapshots when `--results-dir` is supplied; otherwise persist directly to Mongo.
- Prioritises unseen or stale companies by tracking the last scrape time in Mongo and scheduling oldest entries first.
- Optional MongoDB upserts (defaulting to `mongodb://localhost:27017` database `screener`) with upsert keys based on index, slug, BSEID, NSEID, and ISINID.
- Cleans numeric strings (removing commas, percents, currency symbols) so values merge cleanly with other data sources.
- Resilient to reruns: missing sections are skipped, existing documents are updated in place, and failures are logged (optionally to `exceptions.txt` when `--results-dir` is used).

## Requirements

- Python 3.9+
- `pip install -r requirements.txt` (see below)

### Python Dependencies

| Package          | Purpose                             |
|------------------|-------------------------------------|
| `requests`       | HTTP client for Screener web/API    |
| `beautifulsoup4` | HTML parsing                         |
| `pandas`         | Table parsing & data shaping         |
| `tqdm`           | Progress bars                        |
| `pymongo`*       | MongoDB optional writer (install if using Mongo)

> *Install `pymongo` only when using the MongoDB integration: `pip install pymongo`.

You can quickly install everything with:

```bash
python -m pip install requests pandas beautifulsoup4 tqdm pymongo
```

## Input Data

Corporate metadata is expected in MongoDB (default collection `corporate_actions`). You can keep a Moneycontrol dump (`moneycontrol_corporate_actions.json`) as a fallback backup:

```
scraper/
  Historic Data/
    moneycontrol_corporate_actions.json
```

Index constituents can be provided in several ways:

1. `--index all` ? scrape every company present in the corporate action dataset.
2. `--index path/to/list.txt` ? text file with one entry per line (`BSEID,NSEID`, `BSEID`, or `NSEID`).
3. `--index path/to/index.json` ? JSON list or dict containing the same descriptor strings/dicts.
4. `--index NIFTY50 --index-file index_constituents` ? fetch constituents from the Mongo collection (falls back to JSON when provided).

Each descriptor may be a dict with `SC_BSEID`, `SC_NSEID`, `SC_ISINID` fields, or a comma-separated pair (`500325,RELIANCE`).

## Usage

```bash
cd stocks_fundamental_scraper
python scrape_screener.py --index all --limit 10
```

### Key Arguments

| Flag | Description |
|------|-------------|
| `--index` | Index name or path to the constituent list (required). |
| `--index-file` | Optional backup JSON map; Mongo is queried first using the configured collection. |
| `--corporate-actions` | Optional backup JSON file; Mongo supplies corporate metadata when this file is absent. |
| `--corporate-collection` | MongoDB collection containing source corporate metadata (default `corporate_actions`). |
| `--source-mongo-uri` | Mongo URI for the Moneycontrol source database (default `mongodb://localhost:27017`). |
| `--source-mongo-db` | Source database name holding index/corporate datasets (default `moneycontrol`). |
| `--results-dir` | Optional directory for JSON snapshots; leave unset to skip local files. |
| `--limit` | Restrict the number of companies (useful for smoke-tests); omit or set 0 to scrape all. |
| `--standalone` | Fetch standalone rather than consolidated numbers. |
| `--target-mongo-uri` | Mongo URI for the Screener target database (default `mongodb://localhost:27017`). |
| `--target-mongo-db` | Target Mongo database name (default `screener`). |
| `--min-delay` / `--max-delay` | Bounds (seconds) for spacing between outbound requests (defaults 1.0 / 2.5). |
| `--delay-jitter` | Additional random jitter (seconds) appended to each wait window (default 0.5). |
| `--retry-limit` | Maximum attempts per HTTP request before surfacing an error (default 5). |
| `--retry-backoff` | Base backoff window (seconds) for exponential retry delays (default 3.0). |
| `--retry-cap` | Upper bound (seconds) for retry backoff (default 60). |
| `--proxy-file` | Optional newline-delimited HTTP/S proxy list to rotate through while scraping. |
| `--disable-mongo` | Skip Mongo writes even if URI/DB are set.


### Interactive CLI Walkthrough

1. **Pick the universe** - start with a dry run: `python scrape_screener.py --index NIFTY50 --limit 3 --disable-mongo`.
2. **Wire the data sources** - point the scraper at Moneycontrol + Screener: `--source-mongo-uri`, `--source-mongo-db`, `--corporate-collection`, `--target-mongo-uri`, `--target-mongo-db`.
3. **Tune throttling** - choose a safe baseline (`--min-delay 3 --max-delay 6 --retry-limit 3`). Increase the values if you see frequent `429` messages; the scraper now stretches the delay automatically whenever Screener slows you down.
4. **Optionally rotate proxies** - drop one proxy per line into `proxies.txt`, then run with `--proxy-file proxies.txt` to spread traffic across endpoints you control.
5. **Persist results** - drop `--disable-mongo` and the scraper will upsert into `balance sheet`, `cash flow`, `profit loss`, `quarters`, and `ratios` collections in the target DB.
6. **Rerun without data loss** - re-launching the script adds new periods while keeping historical rows intact, thanks to the upsert keys.
7. **Let the freshness queue run** - the scraper stores `updated_at` per company and automatically runs unseen or oldest tickers before recently scraped ones.

> Tip: combine `--limit` with `--results-dir ./snapshots` during testing to inspect the JSON output without polluting Mongo.

### Examples

Scrape NIFTY 50 constituents resolved from the Mongo collection (`index_constituents`), keep consolidated figures, and write to Mongo:

```bash
python scrape_screener.py --index NIFTY50
```

Smoke-test the scraper on five companies without touching Mongo or writing local snapshots:

```bash
python scrape_screener.py --index all --limit 5 --disable-mongo
```

Switch to standalone financials:

```bash
python scrape_screener.py --index BANKNIFTY --standalone
```

## Output Structure

- If you pass `--results-dir=/path/to/snapshots`, each section is written to `/path/to/snapshots/<section>.json` (`balance-sheet`, `cash-flow`, `profit-loss`, `quarters`, `ratios`).
- Each document includes the following merge-friendly identifiers:
  - `BSEID`, `NSEID`, `ISINID`
  - `Resolved Slug`, `Slug Source`
  - `Index`, `Company Name`, `Company ID`
  - `Row Type` (`Parent`, `Child`, `Standalone`)
  - `Parent KPI`, `Child KPI`, and the metric columns (`Mar 2025`, etc.).
- If `--results-dir` is set, an `exceptions.txt` snapshot is created alongside the JSON files with pairs of BSEID,NSEID that failed to resolve or scrape.

When Mongo is enabled, the same rows are upserted into Mongo collections named:

```
balance sheet
cash flow
profit loss
quarters
ratios
```

Upsert keys mirror the JSON identifiers (`Index`, `Resolved Slug`, `BSEID`, `NSEID`, `ISINID`, `Parent KPI`, `Child KPI`, `Row Type`).

## Operational Notes

- Screener throttles heavy scrape bursts; tune `--min-delay` / `--max-delay` / `--retry-*` (and optionally `--proxy-file`) to stay within the limits.
- Scrape metadata lives in the `scrape metadata` collection (`updated_at` per BSE/NSE/ISIN) so you can monitor coverage or seed custom schedules.
- Proxy files expect one `http[s]://user:pass@host:port` entry per line; rotate only through endpoints you control and trust.
- If you notice pages missing sections, Screener may have no data or may have changed layout. The scraper logs failures and continues.
- If you use `--results-dir`, delete the snapshot files there to regenerate clean JSON on the next run.
- Use `--disable-mongo` whenever MongoDB is unavailable or credentials are incorrect.

## Contributing / Next Steps

1. Ensure your Moneycontrol datasets are refreshed periodically.
2. Add automated index downloaders or schedule scrapes via cron/task scheduler.
3. Extend output transformations (e.g., convert numeric strings to decimals) within your analytic pipeline.
4. Run `python scrape_screener.py --help` for the latest CLI reference.

Once you are ready to publish, initialise a git repository in `stocks_fundamental_scraper/`, commit the contents, and push to your preferred GitHub remote:

```bash
cd stocks_fundamental_scraper
git init
git add .
git commit -m "Initial commit: Screener index scraper"
git remote add origin <your-repo-url>
git push -u origin main
```
