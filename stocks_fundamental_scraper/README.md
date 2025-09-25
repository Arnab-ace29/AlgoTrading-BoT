# Stocks Fundamental Scraper

Python utility for collecting fundamentals from [Screener.in](https://www.screener.in/) for entire index lists. The tool expands parent KPIs, normalises values, appends incremental history, and can stream results straight into MongoDB collections for downstream analytics.

## Features

- Scrape multiple Screener sections (`quarters`, `profit-loss`, `balance-sheet`, `cash-flow`, `ratios`) for every company in a supplied index list.
- Expand child KPI schedules under each parent row and track parent/child relationships explicitly.
- Export section data to JSON collections under `collections/`, maintaining historical rows without duplicates.
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

The scraper expects a Moneycontrol corporate actions dump for resolving BSE/NSE/ISIN identifiers:

```
scraper/
  Historic Data/
    moneycontrol_corporate_actions.json
```

Index constituents can be provided in several ways:

1. `--index all` ? scrape every company present in the corporate action dataset.
2. `--index path/to/list.txt` ? text file with one entry per line (`BSEID,NSEID`, `BSEID`, or `NSEID`).
3. `--index path/to/index.json` ? JSON list or dict containing the same descriptor strings/dicts.
4. `--index NIFTY50 --index-file stocks_fundamental_scraper/index_constituents.json` ? lookup an index name inside a JSON mapping.

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
| `--index-file` | Optional JSON mapping of index names to constituent lists (defaults to `index_constituents.json`). |
| `--corporate-actions` | Path to the Moneycontrol corporate actions JSON (default `../scraper/Historic Data/moneycontrol_corporate_actions.json`). |
| `--results-dir` | Directory for JSON collections (`collections` by default). |
| `--limit` | Restrict the number of companies (useful for smoke-tests). |
| `--standalone` | Fetch standalone rather than consolidated numbers. |
| `--mongo-uri` | MongoDB URI (defaults to `mongodb://localhost:27017`). |
| `--mongo-db` | MongoDB database name (defaults to `screener`). |
| `--disable-mongo` | Skip Mongo writes even if URI/DB are set.

### Examples

Scrape NIFTY 50 constituents listed in `index_constituents.json`, keep consolidated figures, and write to Mongo:

```bash
python scrape_screener.py --index NIFTY50
```

Smoke-test the scraper on five companies, output JSON only:

```bash
python scrape_screener.py --index all --limit 5 --disable-mongo
```

Switch to standalone financials:

```bash
python scrape_screener.py --index BANKNIFTY --standalone
```

## Output Structure

- `collections/<section>.json` ? JSON array per section (`balance-sheet`, `cash-flow`, `profit-loss`, `quarters`, `ratios`).
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
- To regenerate clean JSON collections, delete the existing `collections/*.json` files and rerun.
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
