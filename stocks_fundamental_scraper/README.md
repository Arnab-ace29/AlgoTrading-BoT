# Stocks Fundamental Scraper

Python utility for collecting fundamentals from [Screener.in](https://www.screener.in/) for entire index lists. The tool expands parent KPIs, normalises values, appends incremental history, and can stream results straight into MongoDB collections for downstream analytics.

## Features

- Scrape multiple Screener sections (`quarters`, `profit-loss`, `balance-sheet`, `cash-flow`, `ratios`) for every company in a supplied index list.
- Expand child KPI schedules under each parent row and track parent/child relationships explicitly.
- Optionally export section data to JSON snapshots when `--results-dir` is supplied; otherwise persist directly to Mongo.
- Optional MongoDB upserts (defaulting to `mongodb://localhost:27017` database `screener`) with upsert keys based on index, slug, BSEID, NSEID, and ISINID.
- Cleans numeric strings (removing commas, percents, currency symbols) so values merge cleanly with other data sources.
- Resilient to reruns: missing sections are skipped, existing documents are updated in place, and failed lookups are captured in `collections/exceptions.txt`.

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
| `--corporate-collection` | MongoDB collection containing corporate metadata (default `corporate_actions`). |
| `--results-dir` | Optional directory for JSON snapshots; leave unset to skip local files. |
| `--limit` | Restrict the number of companies (useful for smoke-tests). |
| `--standalone` | Fetch standalone rather than consolidated numbers. |
| `--mongo-uri` | MongoDB URI (defaults to `mongodb://localhost:27017`). |
| `--mongo-db` | MongoDB database name (defaults to `screener`). |
| `--disable-mongo` | Skip Mongo writes even if URI/DB are set.

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
- `collections/exceptions.txt` ? pairs of BSEID,NSEID that failed to resolve or scrape.

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

- Screener occasionally throttles requests; respect their terms and keep scrape volumes reasonable.
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
