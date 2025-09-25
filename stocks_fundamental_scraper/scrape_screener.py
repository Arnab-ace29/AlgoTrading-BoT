"""Scrape Screener tables for every company in a stock index."""

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import Session
from tqdm import tqdm

try:
    from pymongo import MongoClient, ReplaceOne
except ImportError:  # pragma: no cover - optional dependency
    MongoClient = None
    ReplaceOne = None

BASE_URL = "https://www.screener.in"
SECTIONS = [
    "quarters",
    "profit-loss",
    "balance-sheet",
    "cash-flow",
    "ratios",
]
HEADERS = {"User-Agent": "Mozilla/5.0"}
OUTPUT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = None  # local JSON snapshots disabled unless explicitly requested
DEFAULT_CORPORATE_ACTIONS_PATH = Path("scraper") / "Historic Data" / "moneycontrol_corporate_actions.json"
DEFAULT_INDEX_FILE = OUTPUT_DIR / "index_constituents.json"
DEFAULT_INDEX_COLLECTION = "index_constituents"
DEFAULT_CORPORATE_COLLECTION = "corporate_actions"
DEFAULT_SOURCE_COLLECTION = "corporate_actions"
DEFAULT_SOURCE_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_SOURCE_MONGO_DB = "moneycontrol"
DEFAULT_TARGET_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_TARGET_MONGO_DB = "screener"
KEY_COLUMNS = [
    "Index",
    "Company Name",
    "Company ID",
    "Resolved Slug",
    "Slug Source",
    "BSEID",
    "NSEID",
    "ISINID",
    "Row Type",
    "Parent KPI",
    "Child KPI",
]

SECTION_COLLECTION_NAMES = {
    "balance-sheet": "balance sheet",
    "cash-flow": "cash flow",
    "profit-loss": "profit loss",
    "quarters": "quarters",
    "ratios": "ratios",
}

def extract_identifiers(entry: Dict[str, object]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(entry, dict):
        return None, None, None
    bse_candidates = (
        entry.get("SC_BSEID"),
        entry.get("SC_BSE"),
        entry.get("BSEID"),
        entry.get("BSE"),
        entry.get("bseId"),
        entry.get("bse"),
        entry.get("BSE Code"),
        entry.get("scripBSE"),
    )
    nse_candidates = (
        entry.get("SC_NSEID"),
        entry.get("SC_NSE"),
        entry.get("NSEID"),
        entry.get("NSE"),
        entry.get("nseId"),
        entry.get("nse"),
        entry.get("NSE Code"),
        entry.get("scripNSE"),
    )
    isin_candidates = (
        entry.get("SC_ISINID"),
        entry.get("ISIN"),
        entry.get("isin"),
    )
    def first_non_empty(values):
        for value in values:
            if value not in (None, ""):
                return value
        return None
    return (
        first_non_empty(bse_candidates),
        first_non_empty(nse_candidates),
        first_non_empty(isin_candidates),
    )


def normalize_label(raw: object) -> str:
    """Return a cleaned, ASCII-friendly representation of a KPI label."""
    if raw is None:
        return ""
    text = str(raw)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\xa0", " ").replace("\u202f", " ").replace("\u2009", " ")
    text = text.replace("\ufeff", "").replace("\ufffd", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def clean_metric_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).strip()
    if not text or text in {"-", "--"}:
        return ""
    text = (
        text.replace(",", "")
        .replace("%", "")
        .replace("\u20b9", "")
        .replace("Rs.", "")
    )
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    text = text.replace("\u2212", "-")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def build_record(
    parent: Optional[str],
    child: str,
    row_type: str,
    value_columns: Sequence[str],
    values: Sequence[object],
) -> Dict[str, object]:
    parent_value = parent or ""
    child_value = child
    if row_type == "Parent":
        if not parent_value:
            parent_value = child_value
        child_value = ""
    elif row_type == "Standalone":
        if not parent_value:
            parent_value = child_value
        child_value = ""
    record: Dict[str, object] = {
        "Row Type": row_type,
        "Parent KPI": parent_value,
        "Child KPI": child_value,
    }
    for column, value in zip(value_columns, values):
        record[column] = clean_metric_value(value)
    return record



def fetch_company_page(session: Session, slug: str, consolidated: bool) -> Tuple[str, BeautifulSoup]:
    url = f"{BASE_URL}/company/{slug}/"
    if consolidated:
        url += "consolidated/"
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.url.rstrip("/"), BeautifulSoup(response.text, "html.parser")

def extract_company_id(soup: BeautifulSoup) -> str:
    container = soup.find("div", id="company-info")
    if not container or "data-company-id" not in container.attrs:
        raise ValueError("Could not locate company ID on the page.")
    return container["data-company-id"]

def fetch_child_schedule(
    session: Session,
    company_id: str,
    section: str,
    parent_name: str,
) -> Dict[str, Dict[str, object]]:
    url = f"{BASE_URL}/api/company/{company_id}/schedules/"
    params = {"parent": parent_name, "section": section, "consolidated": ""}
    response = session.get(url, params=params, headers=HEADERS, timeout=30)
    if response.status_code != 200:
        return {}
    data = response.json()
    return data if isinstance(data, dict) else {}

def parse_section_table(
    session: Session,
    company_id: str,
    section: str,
    section_tag,
) -> Optional[pd.DataFrame]:
    table_tag = section_tag.find("table") if section_tag else None
    if table_tag is None:
        return None
    raw_df = pd.read_html(StringIO(str(table_tag)))[0]
    metric_column = raw_df.columns[0]
    raw_df = raw_df.rename(columns={metric_column: "Metric"})
    value_columns = list(raw_df.columns[1:])
    records: List[Dict[str, object]] = []
    for _, row in raw_df.iterrows():
        metric = normalize_label(row["Metric"])
        is_parent = isinstance(metric, str) and metric.endswith("+")
        if is_parent:
            parent_name = normalize_label(metric[:-1])
            parent_values = [row[col] for col in value_columns]
            records.append(
                build_record(parent_name or "", parent_name or metric, "Parent", value_columns, parent_values)
            )
            child_map = fetch_child_schedule(session, company_id, section, parent_name)
            if not child_map:
                continue
            for child_name, metrics in child_map.items():
                normalized_child = normalize_label(child_name)
                child_values = [metrics.get(col) for col in value_columns]
                records.append(
                    build_record(parent_name or "", normalized_child, "Child", value_columns, child_values)
                )
        else:
            row_values = [row[col] for col in value_columns]
            records.append(build_record("", metric, "Standalone", value_columns, row_values))
    if not records:
        return None
    result_df = pd.DataFrame.from_records(records)
    ordered_columns = ["Row Type", "Parent KPI", "Child KPI", *value_columns]
    return result_df[ordered_columns]

def collect_section_tables(
    session: Session,
    soup: BeautifulSoup,
    company_id: str,
) -> Dict[str, pd.DataFrame]:
    output: Dict[str, pd.DataFrame] = {}
    for section in SECTIONS:
        section_tag = soup.select_one(f"section#{section}")
        df = parse_section_table(session, company_id, section, section_tag)
        if df is not None:
            output[section] = df
    return output

def load_corporate_actions(
    path: Optional[Path],
    source_mongo: Optional["SourceMongo"],
    corporate_collection: Optional[str],
) -> List[dict]:
    if source_mongo is not None:
        actions = source_mongo.fetch_corporate_actions(corporate_collection)
        if actions:
            return actions
    if path is not None and path.exists():
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    raise FileNotFoundError(
        "Corporate actions data not found in Mongo or at the provided path."
    )

def build_corporate_lookup(corporate_actions: Iterable[dict]) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    by_bse: Dict[str, dict] = {}
    by_nse: Dict[str, dict] = {}
    for entry in corporate_actions:
        bse_id = entry.get("SC_BSEID")
        nse_id = entry.get("SC_NSEID")
        if bse_id and bse_id != "0":
            by_bse[bse_id] = entry
        if nse_id:
            by_nse[nse_id] = entry
    return by_bse, by_nse

def parse_constituent_descriptor(item) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(item, dict):
        bse = item.get("SC_BSEID") or item.get("bse") or item.get("bseid")
        nse = item.get("SC_NSEID") or item.get("nse") or item.get("nseid")
        return (bse or None, nse or None)
    value = str(item).strip()
    if not value:
        return (None, None)
    if "," in value:
        left, right = [part.strip() or None for part in value.split(",", 1)]
        return (left, right)
    return (value if value.isdigit() else None, value if not value.isdigit() else None)

@dataclass
class CompanyTarget:
    name: str
    sc_bse_id: Optional[str]
    sc_nse_id: Optional[str]
    isin_id: Optional[str]
    corporate_entry: dict

class SourceMongo:
    """Handles reads from the Moneycontrol (source) Mongo database."""

    def __init__(
        self,
        uri: str,
        db_name: str,
        collection: str,
        timeout_ms: int = 10000,
    ) -> None:
        if MongoClient is None:
            raise RuntimeError("pymongo is required for MongoDB support. Install it via 'pip install pymongo'.")
        self.client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        self.client.admin.command("ping")
        self.db = self.client[db_name]
        self.collection = collection
        self.index_collection = collection
        self.corporate_collection = collection

    def fetch_index(self, index_name: str, collection_name: Optional[str] = None) -> Optional[Sequence[object]]:
        collection = self.db[collection_name or self.collection]
        doc = (
            collection.find_one({"name": index_name})
            or collection.find_one({"index": index_name})
            or collection.find_one({"_id": index_name})
        )
        if not doc:
            return None
        if isinstance(doc, list):
            return doc
        for key in ("constituents", "members", "companies", "items", "entries", "data"):
            payload = doc.get(key)
            if isinstance(payload, list):
                return payload
        payload = doc.get(index_name)
        if isinstance(payload, list):
            return payload
        return None

    def fetch_corporate_actions(self, collection_name: Optional[str] = None) -> List[dict]:
        collection = self.db[collection_name or self.collection]
        documents = list(collection.find({}, {"_id": 0}))
        return documents

    def fetch_all_companies(self, collection_name: Optional[str] = None) -> List[dict]:
        collection = self.db[collection_name or self.collection]
        documents = list(collection.find({}, {"_id": 0}))
        records: List[dict] = []
        for doc in documents:
            if isinstance(doc, dict):
                if any(doc.get(key) for key in ("SC_BSEID", "SC_NSEID", "SC_ISINID")):
                    records.append(doc)
                for key in ("constituents", "members", "companies", "items", "entries", "data"):
                    payload = doc.get(key)
                    if isinstance(payload, list):
                        for item in payload:
                            if isinstance(item, dict):
                                records.append(item)
        return records


class TargetMongo:
    """Handles writes to the Screener (target) Mongo database."""

    def __init__(
        self,
        uri: str,
        db_name: str,
        enable_writes: bool = True,
        timeout_ms: int = 10000,
    ) -> None:
        if MongoClient is None or ReplaceOne is None:
            raise RuntimeError("pymongo is required for MongoDB support. Install it via 'pip install pymongo'.")
        self.client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        self.client.admin.command("ping")
        self.db = self.client[db_name]
        self.enable_writes = enable_writes

    def write_section(self, section: str, df: pd.DataFrame) -> None:
        if not self.enable_writes or df.empty:
            return
        collection_name = SECTION_COLLECTION_NAMES.get(section, section)
        collection = self.db[collection_name]
        records = df.to_dict("records")
        if not records:
            return
        operations = []
        for record in records:
            key = {column: record.get(column, "") for column in KEY_COLUMNS}
            operations.append(ReplaceOne(key, record, upsert=True))
        if operations:
            collection.bulk_write(operations, ordered=False)


def read_index_file(path: Path) -> Sequence[object]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return list(data.values())[0] if data else []
        if not isinstance(data, list):
            raise TypeError("Index JSON must contain a list or dict of constituents.")
        return data
    descriptors: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        descriptors.append(stripped)
    return descriptors


def load_index_descriptors(
    index_name: str,
    index_file: Optional[Path],
    source_mongo: Optional["SourceMongo"],
) -> Sequence[object]:
    if index_file and index_file.exists():
        return read_index_file(index_file)

    if source_mongo is not None:
        descriptors = source_mongo.fetch_index(index_name)
        if descriptors is not None:
            return descriptors
        if index_file and index_file.exists():
            return read_index_file(index_file)
        raise KeyError(
            f"Index '{index_name}' not found in Mongo collection '{source_mongo.index_collection}'."
        )

    path_candidate = Path(index_name)
    if path_candidate.exists():
        return read_index_file(path_candidate)

    raise FileNotFoundError(
        f"Unable to resolve index '{index_name}'. Provide a valid --index-file backup or ensure Mongo contains the mapping."
    )


def normalize_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = str(value).strip()
    if not value or value == "0":
        return None
    return value


def resolve_constituents(
    index_name: str,
    corporate_actions: List[dict],
    index_file: Optional[Path],
    source_mongo: Optional["SourceMongo"],
) -> List[CompanyTarget]:
    index_lower = index_name.lower()
    by_bse, by_nse = build_corporate_lookup(corporate_actions)
    targets: List[CompanyTarget] = []
    seen_pairs = set()
    if index_lower == "all":
        source_records: List[dict] = corporate_actions or []
        if source_mongo is not None:
            source_records = source_records or source_mongo.fetch_all_companies()
        for entry in source_records:
            bse = normalize_code(entry.get("SC_BSEID"))
            nse = normalize_code(entry.get("SC_NSEID"))
            pair = (bse or "", nse or "")
            if not pair[0] and not pair[1]:
                continue
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            targets.append(
                CompanyTarget(
                    name=entry.get("name", entry.get("shortName", entry.get("Name", ""))),
                    sc_bse_id=bse,
                    sc_nse_id=nse,
                    isin_id=normalize_code(entry.get("SC_ISINID")),
                    corporate_entry=entry,
                )
            )
        targets.sort(key=lambda item: (item.name or "", item.sc_bse_id or "", item.sc_nse_id or ""))
        return targets

    descriptors = load_index_descriptors(index_name, index_file, source_mongo)
    for descriptor in descriptors:
        bse, nse = parse_constituent_descriptor(descriptor)
        bse = normalize_code(bse)
        nse = normalize_code(nse)
        entry = None
        if bse and bse in by_bse:
            entry = by_bse[bse]
            nse = nse or normalize_code(entry.get("SC_NSEID"))
        elif nse and nse in by_nse:
            entry = by_nse[nse]
            bse = bse or normalize_code(entry.get("SC_BSEID"))
        if not entry:
            raise KeyError(f"Unable to locate corporate action entry for descriptor '{descriptor}'.")
        pair = (bse or "", nse or "")
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        targets.append(
            CompanyTarget(
                name=entry.get("name", entry.get("shortName", "")),
                sc_bse_id=bse,
                sc_nse_id=nse,
                isin_id=normalize_code(entry.get("SC_ISINID")),
                corporate_entry=entry,
            )
        )
    targets.sort(key=lambda item: (item.name or "", item.sc_bse_id or "", item.sc_nse_id or ""))
    return targets


def append_section_results(
    section: str,
    df: pd.DataFrame,
    metadata: Dict[str, object],
    results_dir: Optional[Path],
    target_mongo: Optional["TargetMongo"] = None,
) -> None:
    enriched_df = df.copy()
    for column, value in metadata.items():
        enriched_df[column] = "" if value is None else value
    ordered_value_columns = [col for col in df.columns if col not in KEY_COLUMNS]
    enriched_df = enriched_df[[*KEY_COLUMNS, *ordered_value_columns]]
    value_cols = [col for col in enriched_df.columns if col not in KEY_COLUMNS]
    for column in value_cols:
        enriched_df[column] = enriched_df[column].apply(clean_metric_value)
    enriched_df[["Parent KPI", "Child KPI"]] = enriched_df[["Parent KPI", "Child KPI"]].fillna("")
    enriched_df = enriched_df.fillna("").astype(str)

    new_records_df = enriched_df.copy()

    if results_dir is not None:
        results_dir.mkdir(parents=True, exist_ok=True)
        target_path = results_dir / f"{section}.json"
        key_columns = KEY_COLUMNS
        if target_path.exists():
            try:
                existing_df = pd.read_json(target_path, orient="records")
            except ValueError:
                existing_df = pd.DataFrame(columns=enriched_df.columns)
            else:
                existing_df = existing_df.fillna("")
                if "Company ID" not in existing_df.columns:
                    existing_df = pd.DataFrame(columns=enriched_df.columns)
                else:
                    existing_df = existing_df.astype(str)
                    existing_df = existing_df[existing_df["Company ID"].str.strip() != ""]
                rename_map = {
                    "Screener Slug": "Resolved Slug",
                    "SC_BSEID": "BSEID",
                    "SC_NSEID": "NSEID",
                    "SC_ISINID": "ISINID",
                }
                existing_df = existing_df.rename(columns={k: v for k, v in rename_map.items() if k in existing_df.columns})
            if {"Row Type", "Parent KPI", "Child KPI"}.issubset(existing_df.columns):
                mask_standalone_fix = (existing_df["Row Type"] == "Standalone") & (existing_df["Parent KPI"].astype(str).str.strip() == "")
                existing_df.loc[mask_standalone_fix, "Parent KPI"] = existing_df.loc[mask_standalone_fix, "Child KPI"]
                existing_df.loc[mask_standalone_fix, "Child KPI"] = ""
                mask_same = (existing_df["Row Type"] == "Parent") & (existing_df["Parent KPI"] == existing_df["Child KPI"])
                mask_blank = (existing_df["Row Type"] == "Parent") & (existing_df["Parent KPI"].astype(str).str.strip() == "")
                existing_df = existing_df.loc[~(mask_same | mask_blank)]
        else:
            existing_df = pd.DataFrame(columns=enriched_df.columns)

        all_columns = list(dict.fromkeys([*key_columns, *existing_df.columns, *enriched_df.columns]))
        existing_df = existing_df.reindex(columns=all_columns, fill_value="")
        enriched_df = enriched_df.reindex(columns=all_columns, fill_value="")
        existing_df.set_index(key_columns, inplace=True)
        enriched_df.set_index(key_columns, inplace=True)
        combined = existing_df.combine_first(enriched_df)
        combined.update(enriched_df)
        result_df = combined.reset_index()
        result_df = result_df.fillna("").astype(str)
        result_df.sort_values(by=KEY_COLUMNS, inplace=True)
        result_df.to_json(target_path, orient="records", force_ascii=False, indent=2)

    if target_mongo is not None:
        try:
            target_mongo.write_section(section, new_records_df)
        except Exception as exc:  # pragma: no cover - warn only
            print(
                f"Warning: Failed to write data for section '{section}' to MongoDB: {exc}",
                file=sys.stderr,
            )


def update_exception_file(path: Path, new_exceptions: Iterable[Tuple[str, str]]) -> None:
    existing: set[Tuple[str, str]] = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = [piece.strip() for piece in line.split(",", 1)]
            if not parts:
                continue
            if len(parts) == 1:
                parts.append("")
            existing.add((parts[0], parts[1]))
    for entry in new_exceptions:
        if not entry[0] and not entry[1]:
            continue
        existing.add(entry)
    if not existing:
        return
    with path.open("w", encoding="utf-8") as handle:
        for bse, nse in sorted(existing):
            handle.write(f"{bse},{nse}\n")

def scrape_company(
    session: Session,
    target: CompanyTarget,
    index_name: str,
    consolidated: bool,
    results_dir: Optional[Path],
    target_mongo: Optional["TargetMongo"] = None,
) -> bool:
    slug_candidates: List[Tuple[str, str]] = []
    if target.sc_bse_id:
        slug_candidates.append(("BSE", target.sc_bse_id))
    if target.sc_nse_id and target.sc_nse_id.upper() != "NA":
        slug_candidates.append(("NSE", target.sc_nse_id))
    last_error: Optional[Exception] = None
    for slug_source, slug in slug_candidates:
        try:
            final_url, soup = fetch_company_page(session, slug, consolidated)
            company_id = extract_company_id(soup)
            tables = collect_section_tables(session, soup, company_id)
            if not tables:
                raise ValueError("No tables found on Screener page.")
            resolved_slug = extract_slug_from_url(final_url)
            metadata = {
                "Index": index_name,
                "Company Name": target.name,
                "Company ID": company_id,
                "Resolved Slug": resolved_slug,
                "Slug Source": slug_source,
                "BSEID": target.sc_bse_id or "",
                "NSEID": target.sc_nse_id or "",
                "ISINID": target.isin_id or normalize_code(target.corporate_entry.get("SC_ISINID")) or "",
            }
            for section_name, df in tables.items():
                append_section_results(section_name, df, metadata, results_dir, target_mongo)
            return True
        except requests.HTTPError as exc:
            last_error = exc
            if exc.response is not None and exc.response.status_code == 404:
                continue
            raise
        except Exception as exc:  # pragma: no cover - safety net for data glitches
            last_error = exc
    if last_error is not None:
        bse = target.sc_bse_id or ""
        nse = target.sc_nse_id or ""
        print(
            f"Failed to scrape {target.name} (BSE:{bse} NSE:{nse}): {last_error}",
            file=sys.stderr,
        )
    return False

def read_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Screener tables into CSV files.")
    parser.add_argument("--index", required=True, help="Index name or path describing index constituents.")
    parser.add_argument(
        "--index-file",
        default=str(DEFAULT_INDEX_FILE),
        help="Optional backup JSON mapping of index names to constituents; Mongo is used when this file is absent.",
    )
    parser.add_argument(
        "--corporate-actions",
        default=str(DEFAULT_CORPORATE_ACTIONS_PATH),
        help="Optional backup JSON file for corporate actions; Mongo is used when this file is absent.",
    )
    parser.add_argument(
        "--corporate-collection",
        default=DEFAULT_CORPORATE_COLLECTION,
        help="MongoDB collection containing corporate action metadata (default: %(default)s).",
    )
    parser.add_argument(
        "--source-mongo-uri",
        default=DEFAULT_SOURCE_MONGO_URI,
        help="MongoDB connection URI for the Moneycontrol source database (default: %(default)s).",
    )
    parser.add_argument(
        "--source-mongo-db",
        default=DEFAULT_SOURCE_MONGO_DB,
        help="Source Mongo database containing index and corporate metadata (default: %(default)s).",
    )
    parser.add_argument(
        "--results-dir",
        default=None,
        help="Optional directory for JSON snapshots; leave unset to rely solely on Mongo.",
    )
    parser.add_argument(
        "--target-mongo-uri",
        default=DEFAULT_TARGET_MONGO_URI,
        help="MongoDB connection URI for the Screener target database (default: %(default)s).",
    )
    parser.add_argument(
        "--target-mongo-db",
        default=DEFAULT_TARGET_MONGO_DB,
        help="Target Mongo database where section collections live (default: %(default)s).",
    )
    parser.add_argument(
        "--disable-mongo",
        action="store_true",
        help="Skip writing scraped data to the target MongoDB.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of companies scraped (useful for testing).",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="Scrape standalone financials instead of consolidated.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = read_args(argv)
    consolidated = not args.standalone

    corporate_actions_path: Optional[Path] = None
    if args.corporate_actions:
        candidate = Path(args.corporate_actions)
        if candidate.exists():
            corporate_actions_path = candidate

    source_uri = args.source_mongo_uri or DEFAULT_SOURCE_MONGO_URI
    source_db = args.source_mongo_db or DEFAULT_SOURCE_MONGO_DB
    source_collection = args.corporate_collection or DEFAULT_SOURCE_COLLECTION

    try:
        source_mongo = SourceMongo(
            source_uri,
            source_db,
            source_collection,
        )
    except Exception as exc:
        print(f"Error connecting to source MongoDB: {exc}", file=sys.stderr)
        return 1

    try:
        corporate_actions = load_corporate_actions(
            corporate_actions_path,
            source_mongo,
            source_collection,
        )
    except Exception as exc:
        print(f"Error while loading corporate actions: {exc}", file=sys.stderr)
        return 1

    index_file_path: Optional[Path] = None
    if args.index_file:
        candidate = Path(args.index_file)
        if candidate.exists():
            index_file_path = candidate

    enable_target_writes = not args.disable_mongo
    target_mongo: Optional[TargetMongo] = None
    if enable_target_writes:
        target_uri = args.target_mongo_uri or DEFAULT_TARGET_MONGO_URI
        target_db = args.target_mongo_db or DEFAULT_TARGET_MONGO_DB
        try:
            target_mongo = TargetMongo(
                target_uri,
                target_db,
                enable_writes=True,
            )
        except Exception as exc:
            print(f"Error connecting to target MongoDB: {exc}", file=sys.stderr)
            return 1
    else:
        target_mongo = None

    try:
        targets = resolve_constituents(
            args.index,
            corporate_actions,
            index_file_path,
            source_mongo,
        )
    except Exception as exc:
        print(f"Error while resolving index constituents: {exc}", file=sys.stderr)
        return 1

    if not targets:
        print(f"No companies resolved for index '{args.index}'.", file=sys.stderr)
        return 1

    if args.limit is not None:
        targets = targets[: args.limit]

    results_dir: Optional[Path] = None
    if args.results_dir:
        results_dir = Path(args.results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)

    exceptions: List[Tuple[str, str]] = []
    processed = 0
    with requests.Session() as session:
        for target in tqdm(targets, desc=f"Scraping {args.index}", unit="company"):
            success = scrape_company(
                session,
                target,
                args.index,
                consolidated,
                results_dir,
                target_mongo,
            )
            if success:
                processed += 1
            else:
                exceptions.append((target.sc_bse_id or "", target.sc_nse_id or ""))

    if exceptions:
        if results_dir is not None:
            exception_path = results_dir / "exceptions.txt"
            update_exception_file(exception_path, exceptions)
            print(f"Recorded {len(exceptions)} failures to {exception_path}")
        else:
            print(
                f"Recorded {len(exceptions)} failures (exceptions file disabled because --results-dir was not provided)."
            )

    print(f"Scraped {processed} companies for index '{args.index}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
