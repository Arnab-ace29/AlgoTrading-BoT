import argparse
import json
import string
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests

try:  # Pymongo is optional unless Mongo push is requested.
    from pymongo import MongoClient, UpdateOne
    from pymongo.collection import Collection
except ImportError:  # pragma: no cover - handled at runtime.
    MongoClient = None  # type: ignore[assignment]
    UpdateOne = None  # type: ignore[assignment]
    Collection = None  # type: ignore[assignment]

SEARCH_URL = "https://api.moneycontrol.com/mcapi/v1/stock/search"
CORPORATE_ACTION_URL = "https://api.moneycontrol.com/mcapi/v1/stock/corporate-action"
STOCK_DETAILS_URL = "https://api.moneycontrol.com/mcapi/v1/stock/scmas-details"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonScript",
    "Accept": "application/json",
}
SEARCH_DELAY = 0.12
SECTION_DELAY = 0.12
PAGE_DELAY = 0.12
DETAIL_DELAY = 0.1
MAX_RETRIES = 3
DEFAULT_CHUNK_SIZE = 50
DEFAULT_MONGO_URI = "mongodb://localhost:27017"
DETAIL_KEYS: Tuple[str, ...] = (
    "SC_ISINID",
    "SC_BSEID",
    "SC_NOSHR",
    "SC_NSEID",
    "SC_TICKERNAME",
)


# Mapping API sections to their list payload keys and desired JSON key names.
SECTION_SPECS: Dict[str, Dict[str, object]] = {
    "an": {"list_field": "announcement", "result_key": "announcement"},
    "bm": {"list_field": "board_meeting", "result_key": "board_meeting"},
    "d": {"list_field": "dividends", "result_key": "dividend", "aliases": ("dividends",)},
    "s": {"list_field": "splits", "result_key": "splits"},
    "r": {"list_field": "rights", "result_key": "rights"},
    "ae": {"list_field": "agm_egm", "result_key": "agm_egm"},
}


def throttle(duration: float) -> None:
    """Pause execution briefly so Moneycontrol's API is not overwhelmed."""
    if duration > 0:
        time.sleep(duration)


def fetch_stock_metadata(session: requests.Session) -> Dict[str, Dict[str, object]]:
    """Collect stock metadata by looping over alphanumeric search seeds."""
    seeds = list(string.digits) + list(string.ascii_lowercase)
    records: Dict[str, Dict[str, object]] = {}
    for seed in seeds:
        payload: Dict[str, object]
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(SEARCH_URL, params={"query": seed}, headers=HEADERS, timeout=20)
                response.raise_for_status()
                payload = response.json()
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == MAX_RETRIES:
                    print(f"Failed to fetch search results for seed '{seed}': {exc}")
                    payload = {}
                else:
                    throttle(0.5 * attempt)
        for entry in payload.get("data", []) or []:
            stock_id = entry.get("id")
            if not stock_id:
                continue
            records[stock_id] = {
                "id": stock_id,
                "did": entry.get("did"),
                "shortName": entry.get("shortName"),
                "name": entry.get("name"),
                "productCategory": entry.get("PRODUCT_CATEGORY"),
                "marketcap": entry.get("marketcap"),
            }
        throttle(SEARCH_DELAY)
    return records


def fetch_stock_details(
    session: requests.Session,
    sc_id: str,
    existing_details: Optional[Dict[str, object]] = None,
    force_refresh: bool = False,
) -> Dict[str, object]:
    """Retrieve extended symbol identifiers and cache them when available."""
    existing_details = existing_details or {}
    if not force_refresh:
        if all(existing_details.get(key) not in (None, "") for key in DETAIL_KEYS):
            return {key: existing_details.get(key) for key in DETAIL_KEYS}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                STOCK_DETAILS_URL,
                params={"scId": sc_id},
                headers=HEADERS,
                timeout=20,
            )
            if response.status_code == 404:
                throttle(DETAIL_DELAY)
                return existing_details
            response.raise_for_status()
            payload = response.json()
            throttle(DETAIL_DELAY)
            break
        except Exception as exc:  # noqa: BLE001
            if attempt == MAX_RETRIES:
                print(f"Failed to fetch detail record for {sc_id}: {exc}")
                return existing_details
            throttle(0.5 * attempt)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return existing_details
    details = {key: data.get(key) for key in DETAIL_KEYS}
    return details


def update_record_with_details(
    session: requests.Session,
    record: Dict[str, object],
    force_refresh: bool = False,
) -> bool:
    """Ensure the record contains the requested SC_* detail fields."""
    sc_id = record.get("id")
    if not sc_id:
        return False
    existing_details = {key: record.get(key) for key in DETAIL_KEYS}
    details = fetch_stock_details(session, sc_id, existing_details, force_refresh=force_refresh)
    changed = False
    for key in DETAIL_KEYS:
        value = details.get(key)
        if record.get(key) != value:
            record[key] = value
            changed = True
    return changed


def request_corporate_page(
    session: requests.Session,
    sc_id: str,
    section: str,
    page: int,
) -> Tuple[int, Dict[str, object]]:
    """Request one page of corporate actions with retry/back-off on transient failures."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                CORPORATE_ACTION_URL,
                params={
                    "deviceType": "W",
                    "scId": sc_id,
                    "section": section,
                    "page": page,
                    "appVersion": "161",
                },
                headers=HEADERS,
                timeout=20,
            )
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                print(f"Failed to retrieve section '{section}' for {sc_id} page {page}: {exc}")
                return 0, {}
            throttle(0.75 * attempt)
            continue
        if response.status_code in {429, 500, 502, 503, 504, 522, 524, 403} and attempt < MAX_RETRIES:
            throttle(0.75 * attempt)
            continue
        if response.status_code in {204, 404}:
            return response.status_code, {}
        if response.status_code != 200:
            print(
                f"Unexpected status {response.status_code} for {sc_id} section '{section}' page {page}"
            )
            return response.status_code, {}
        try:
            payload = response.json()
        except ValueError:
            print(f"Non-JSON response for {sc_id} section '{section}' page {page}")
            return response.status_code, {}
        return response.status_code, payload
    return 0, {}


def extract_existing_items(existing_section: Optional[Dict[str, object]], section: str) -> List[Dict[str, object]]:
    """Return the existing stored items for the requested section (newest first)."""
    if not isinstance(existing_section, dict):
        return []
    spec = SECTION_SPECS[section]
    list_field = spec["list_field"]
    result_key = spec["result_key"]
    aliases = tuple(spec.get("aliases", ()))
    for key in (result_key, list_field, *aliases):
        items = existing_section.get(key)
        if isinstance(items, list):
            return items
    return []


def empty_section_payload(section: str) -> Dict[str, object]:
    """Build a placeholder payload when a section is skipped or unavailable."""
    spec = SECTION_SPECS[section]
    result_key = spec["result_key"]
    base_list: List[Dict[str, object]] = []
    payload: Dict[str, object] = {result_key: base_list}
    for alias in spec.get("aliases", ()):  # Mirror aliases so callers can use either key.
        payload[alias] = base_list
    payload["pageCount"] = 0
    payload["pagesFetched"] = 0
    payload["newItems"] = 0
    payload["existingItems"] = 0
    return payload


def fetch_corporate_section(
    session: requests.Session,
    sc_id: str,
    section: str,
    existing_section: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """Pull one corporate-action section and prepend only the truly new rows."""
    spec = SECTION_SPECS[section]
    list_field = spec["list_field"]
    result_key = spec["result_key"]
    aliases = tuple(spec.get("aliases", ()))

    existing_items = extract_existing_items(existing_section, section)
    existing_fingerprints = {json.dumps(entry, sort_keys=True) for entry in existing_items if isinstance(entry, dict)}

    new_items: List[Dict[str, object]] = []
    seen: set[str] = set(existing_fingerprints)
    page = 1
    pages_fetched = 0
    last_page_count: Optional[int] = None
    hit_existing = False

    while True:
        status_code, payload = request_corporate_page(session, sc_id, section, page)
        if status_code in {0, 204, 404}:
            break
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            if data not in (None, ""):
                print(
                    f"Unexpected data section for {sc_id} section '{section}' page {page}: {type(data).__name__}"
                )
            break
        page_count = data.get("pageCount")
        if isinstance(page_count, (int, float)):
            last_page_count = int(page_count)
        raw_items = data.get(list_field) or []
        if not isinstance(raw_items, list):
            print(
                f"Unexpected list payload for {sc_id} section '{section}' page {page}: {type(raw_items).__name__}"
            )
            raw_items = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            fingerprint = json.dumps(entry, sort_keys=True)
            if fingerprint in existing_fingerprints:
                hit_existing = True
                break
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            new_items.append(entry)
        pages_fetched += 1
        if hit_existing:
            break
        if last_page_count and page >= last_page_count:
            break
        if not raw_items:
            break
        page += 1
        throttle(PAGE_DELAY)

    combined_items = new_items + existing_items

    section_payload: Dict[str, object] = {result_key: combined_items}
    for alias in aliases:
        section_payload[alias] = combined_items
    if last_page_count is not None:
        section_payload["pageCount"] = last_page_count
    section_payload["pagesFetched"] = pages_fetched
    section_payload["newItems"] = len(new_items)
    section_payload["existingItems"] = len(existing_items)
    return section_payload


def load_existing_output(path: Path) -> Dict[str, Dict[str, object]]:
    """Load the previous JSON file so we can merge new results with historic data."""
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Unable to load existing corporate actions: {exc}")
        return {}
    indexed: Dict[str, Dict[str, object]] = {}
    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, dict) and entry.get("id"):
                indexed[str(entry["id"])] = entry
    return indexed


def load_existing_metadata(path: Path) -> Dict[str, Dict[str, object]]:
    """Load the cached metadata file and index it by stock id."""
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Unable to load existing metadata: {exc}")
        return {}
    records: Dict[str, Dict[str, object]] = {}
    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, dict) and entry.get("id"):
                records[str(entry["id"])] = entry
    return records


def load_corporate_payload(path: Path) -> List[Dict[str, object]]:
    """Load the corporate action JSON file into memory."""
    if not path.exists():
        print(f"Corporate action file not found at {path}")
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Unable to load corporate actions JSON: {exc}")
        return []
    if not isinstance(payload, list):
        print("Corporate actions JSON is not a list; skipping push")
        return []
    return payload


def parse_only_arg(raw_only: Optional[str]) -> Tuple[Set[str], Set[str]]:
    """Split --only tokens into targeted stock ids and section codes."""
    if not raw_only:
        return set(), set()
    stock_ids: Set[str] = set()
    sections: Set[str] = set()
    for token in (part.strip() for part in raw_only.split(",")):
        if not token:
            continue
        lower = token.lower()
        if lower in SECTION_SPECS:
            sections.add(lower)
        else:
            stock_ids.add(token.upper())
    return stock_ids, sections


def consolidated_entry(
    stock_info: Dict[str, object],
    sections: Dict[str, Dict[str, object]],
) -> Dict[str, object]:
    """Merge base stock metadata with its per-section corporate action payloads."""
    entry = dict(stock_info)
    entry.update(sections)
    return entry


def iter_chunks(items: Iterable[Dict[str, object]], chunk_size: int) -> Iterable[List[Dict[str, object]]]:
    """Yield the iterable in bounded chunks so Mongo writes stay manageable."""
    chunk: List[Dict[str, object]] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def ensure_indexes(collection: Collection) -> None:
    """Create the optimal index set for quick id-based upserts if missing."""
    if collection is None:  # pragma: no cover - defensive.
        return
    indexes = collection.index_information()
    if "id_1" not in indexes:
        collection.create_index("id", unique=True, name="id_1")


def prepare_document_for_mongo(document: Dict[str, object]) -> Dict[str, object]:
    """Strip the SC_ prefix from detail keys before persisting to Mongo."""
    prepared: Dict[str, object] = {}
    for key, value in document.items():
        new_key = key[3:] if key.startswith("SC_") else key
        prepared[new_key] = value
    return prepared


def push_documents_to_mongo(
    documents: List[Dict[str, object]],
    mongo_uri: str,
    database: str,
    collection_name: str,
    chunk_size: int,
) -> None:
    """Stream documents into MongoDB in chunky upserts for resilience."""
    if MongoClient is None or UpdateOne is None:
        raise RuntimeError("pymongo is required to push data to MongoDB. Install it via 'pip install pymongo'.")

    client = MongoClient(mongo_uri)
    collection: Collection = client[database][collection_name]
    ensure_indexes(collection)

    total_documents = len(documents)
    processed = 0
    for chunk in iter_chunks(documents, chunk_size):
        operations = []
        for doc in chunk:
            doc_id = doc.get("id")
            if not doc_id:
                continue
            prepared_doc = prepare_document_for_mongo(doc)
            operations.append(UpdateOne({"id": doc_id}, {"$set": prepared_doc}, upsert=True))
        if not operations:
            continue
        try:
            collection.bulk_write(operations, ordered=False)
        except Exception as exc:  # noqa: BLE001
            print(f"Mongo bulk_write failed: {exc}")
            continue
        processed += len(operations)
        print(f"Mongo upserted batch, total processed: {processed}/{total_documents}")


def filter_payload_by_stocks(payload: List[Dict[str, object]], stock_ids: Set[str]) -> List[Dict[str, object]]:
    """Restrict the payload to a subset of stock identifiers if requested."""
    if not stock_ids:
        return payload
    return [entry for entry in payload if entry.get("id") in stock_ids]


def main(argv: Optional[List[str]] = None) -> None:
    """Coordinate metadata scrape, per-section updates, persistence, and Mongo push."""
    parser = argparse.ArgumentParser(description="Fetch Moneycontrol corporate actions")
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="Re-scrape stock metadata instead of using the cached JSON",
    )
    parser.add_argument(
        "--refresh-details",
        action="store_true",
        help="Force refresh of symbol detail identifiers (SC_*) even if cached",
    )
    parser.add_argument(
        "--only",
        help="Comma-separated list of stock ids and/or section codes (an,bm,d,s,r,ae) to update",
    )
    parser.add_argument(
        "--push-only",
        action="store_true",
        help="Skip scraping and push the existing JSON payload to MongoDB",
    )
    parser.add_argument(
        "--json-path",
        help="Path to the corporate actions JSON file (defaults to generated file)",
    )
    parser.add_argument(
        "--push-to-mongo",
        action="store_true",
        help="Upsert the resulting payload into MongoDB",
    )
    parser.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI, help="MongoDB connection string")
    parser.add_argument("--mongo-db", default="moneycontrol", help="MongoDB database name")
    parser.add_argument("--mongo-collection", default="corporate_actions", help="MongoDB collection name")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of stock documents per Mongo bulk write (25-50 recommended)",
    )
    args = parser.parse_args(argv)

    if args.chunk_size < 1:
        parser.error("--chunk-size must be >= 1")

    requested_stock_ids, requested_sections = parse_only_arg(args.only)

    base_dir = Path(__file__).resolve().parent
    output_dir = base_dir / "Historic Data"
    output_dir.mkdir(parents=True, exist_ok=True)

    stock_metadata_path = output_dir / "moneycontrol_stock_metadata.json"
    corporate_output_path = (
        Path(args.json_path).resolve() if args.json_path else output_dir / "moneycontrol_corporate_actions.json"
    )

    session = requests.Session()

    corporate_payload: List[Dict[str, object]] = []

    if args.push_only:
        corporate_payload = load_corporate_payload(corporate_output_path)
        metadata_records = load_existing_metadata(stock_metadata_path)
        if args.refresh_details and corporate_payload:
            metadata_updated = False
            for entry in corporate_payload:
                if update_record_with_details(session, entry, force_refresh=True):
                    metadata_updated = True
                    stock_id = entry.get("id")
                    if stock_id and stock_id in metadata_records:
                        for key in DETAIL_KEYS:
                            metadata_records[stock_id][key] = entry.get(key)
            if metadata_updated:
                if metadata_records:
                    stock_metadata_path.write_text(
                        json.dumps(list(metadata_records.values()), indent=2),
                        encoding="utf-8",
                    )
                corporate_output_path.write_text(json.dumps(corporate_payload, indent=2), encoding="utf-8")

        payload_for_push = filter_payload_by_stocks(
            corporate_payload,
            requested_stock_ids,
        )
    else:
        metadata_cache = load_existing_metadata(stock_metadata_path)
        if args.refresh_metadata or not metadata_cache:
            metadata_cache = fetch_stock_metadata(session)

        if requested_stock_ids:
            missing = [stock_id for stock_id in requested_stock_ids if stock_id not in metadata_cache]
            if missing:
                print(f"Warning: requested stock ids missing from metadata: {', '.join(missing)}")
            stocks_to_process = {sid: metadata_cache[sid] for sid in requested_stock_ids if sid in metadata_cache}
        else:
            stocks_to_process = metadata_cache

        existing_by_id = load_existing_output(corporate_output_path)
        processed_ids: Set[str] = set()

        for index, (stock_id, stock_info) in enumerate(stocks_to_process.items(), start=1):
            prior_entry = existing_by_id.get(stock_id)
            if update_record_with_details(
                session,
                stock_info,
                force_refresh=args.refresh_details,
            ):
                pass
            sections_payload: Dict[str, Dict[str, object]] = {}
            should_update_stock = not requested_stock_ids or stock_id in requested_stock_ids
            for section_code in SECTION_SPECS:
                should_update_section = should_update_stock and (
                    not requested_sections or section_code in requested_sections
                )
                if should_update_section:
                    sections_payload[section_code] = fetch_corporate_section(
                        session,
                        stock_id,
                        section_code,
                        existing_section=prior_entry.get(section_code) if prior_entry else None,
                    )
                    throttle(SECTION_DELAY)
                else:
                    if prior_entry and section_code in prior_entry:
                        sections_payload[section_code] = prior_entry[section_code]
                    else:
                        sections_payload[section_code] = empty_section_payload(section_code)
            corporate_payload.append(consolidated_entry(stock_info, sections_payload))
            processed_ids.add(stock_id)
            if index % 25 == 0 and not requested_stock_ids:
                print(f"Processed {index} stocks")
            throttle(SECTION_DELAY)

        for stock_id, prior_entry in existing_by_id.items():
            if stock_id in processed_ids:
                continue
            corporate_payload.append(prior_entry)

        corporate_output_path.write_text(json.dumps(corporate_payload, indent=2), encoding="utf-8")
        stock_metadata_path.write_text(
            json.dumps(list(metadata_cache.values()), indent=2),
            encoding="utf-8",
        )

        payload_for_push = filter_payload_by_stocks(corporate_payload, requested_stock_ids)

    if args.push_to_mongo:
        if args.push_only and not payload_for_push:
            payload_for_push = filter_payload_by_stocks(
                corporate_payload,
                requested_stock_ids,
            )
        if not payload_for_push:
            print("No corporate action data available to push to MongoDB")
        else:
            print(
                f"Pushing {len(payload_for_push)} documents to MongoDB collection "
                f"{args.mongo_db}.{args.mongo_collection} in chunks of {args.chunk_size}"
            )
            push_documents_to_mongo(
                payload_for_push,
                args.mongo_uri,
                args.mongo_db,
                args.mongo_collection,
                args.chunk_size,
            )


if __name__ == "__main__":
    main()
