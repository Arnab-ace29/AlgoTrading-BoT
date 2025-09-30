"""Enrich Screener scrape metadata with additional top-ratio KPIs."""

import argparse
import sys
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:  # Optional dependency for Mongo support
    from pymongo import MongoClient
except ImportError as exc:  # pragma: no cover - runtime guard
    raise RuntimeError("pymongo is required for MongoDB support. Install it via 'pip install pymongo'.") from exc

try:  # Prefer shared constants when scrape_screener is importable
    from scrape_screener import (
        BASE_URL,
        HEADERS,
        SCRAPE_METADATA_COLLECTION,
        DEFAULT_TARGET_MONGO_URI,
        DEFAULT_TARGET_MONGO_DB,
        ProxyManager,
        load_proxy_list,
    )
except ImportError:  # Fallback when executed standalone
    BASE_URL = "https://www.screener.in"
    HEADERS = {"User-Agent": "Mozilla/5.0"}
    SCRAPE_METADATA_COLLECTION = "scrape metadata"
    DEFAULT_TARGET_MONGO_URI = "mongodb://localhost:27017"
    DEFAULT_TARGET_MONGO_DB = "screener"

    class ProxyManager:
        def __init__(self, proxies: Iterable[str]) -> None:
            self._proxies = [proxy.strip() for proxy in proxies if proxy and proxy.strip()]
            self._cursor = 0

        def for_requests(self) -> Optional[Dict[str, str]]:
            if not self._proxies:
                return None
            proxy = self._proxies[self._cursor % len(self._proxies)]
            return {"http": proxy, "https": proxy}

        def rotate(self) -> None:
            if self._proxies:
                self._cursor = (self._cursor + 1) % len(self._proxies)

        def __bool__(self) -> bool:
            return bool(self._proxies)

    def load_proxy_list(path: Path) -> List[str]:
        if not path.exists():
            raise FileNotFoundError(f"Proxy list file not found: {path}")
        proxies: List[str] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            trimmed = line.strip()
            if not trimmed or trimmed.startswith('#'):
                continue
            proxies.append(trimmed)
        return proxies

DEFAULT_TIMEOUT = 15.0
DEFAULT_DELAY = 1.0
DEFAULT_PROXY_POOL = [
    "http://159.89.132.167:8989",
    "http://64.225.8.82:9988",
    "http://45.79.17.203:3128",
    "http://165.225.240.121:80",
    "http://134.209.29.120:8080",
]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
RATIO_SCHEMA_VERSION = 2
_UNIT_TAIL_PATTERN = re.compile(r"^(?P<value>.+?)\s*(?P<unit>[A-Za-z%]+\.?)$")


class NotFoundError(RuntimeError):
    """Raised when a slug candidate does not resolve to a Screener company page."""


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate top ratios inside the scrape metadata collection.")
    parser.add_argument("--target-mongo-uri", default=DEFAULT_TARGET_MONGO_URI, help="Target MongoDB connection URI.")
    parser.add_argument("--target-mongo-db", default=DEFAULT_TARGET_MONGO_DB, help="Target MongoDB database name.")
    parser.add_argument("--collection", default=SCRAPE_METADATA_COLLECTION, help="Mongo collection storing scrape metadata.")
    parser.add_argument("--slug-field", default="Resolved Slug", help="Metadata field containing the Screener slug.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum documents to update (0 means no limit).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Seconds to sleep between successful requests.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Request timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per slug before giving up.")
    parser.add_argument("--force", action="store_true", help="Update every document even if KPIs already exist.")
    parser.add_argument("--standalone", dest="consolidated", action="store_false", help="Use standalone financial view instead of consolidated.")
    parser.set_defaults(consolidated=True)
    parser.add_argument("--proxy-file", help="Path to newline-delimited HTTP/S proxies for rotation during scraping.")
    parser.add_argument("--retry-delay-min", type=float, default=1.0, help="Minimum seconds to wait before retrying a failed slug.")
    parser.add_argument("--retry-delay-max", type=float, default=2.0, help="Maximum seconds to wait before retrying a failed slug.")
    parser.add_argument("--no-default-proxies", action="store_true", help="Disable the built-in fallback proxy pool.")
    parser.add_argument("--dry-run", action="store_true", help="Scrape KPIs but skip Mongo updates.")
    return parser.parse_args(argv)


def build_company_url(slug: str, consolidated: bool) -> str:
    tail = "consolidated" if consolidated else ""
    if tail:
        return f"{BASE_URL}/company/{slug}/{tail}"
    return f"{BASE_URL}/company/{slug}"


def _split_value_unit(text: str) -> Tuple[str, Optional[str]]:
    stripped = text.strip()
    if not stripped:
        return "", None
    match = _UNIT_TAIL_PATTERN.match(stripped)
    if match:
        value = match.group("value").strip()
        unit = match.group("unit").strip()
        return value, unit
    return stripped, None


def parse_top_ratios(html: str) -> Dict[str, Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    ratios: Dict[str, Dict[str, str]] = {}
    for item in soup.select("ul#top-ratios li"):
        key_tag = item.find("span", class_="name")
        val_tag = item.find("span", class_="number")
        if not key_tag or not val_tag:
            continue
        key = key_tag.get_text(strip=True)
        raw_val = val_tag.get_text(strip=True)
        if not key or raw_val is None:
            continue
        value, unit = _split_value_unit(raw_val)
        entry: Dict[str, str] = {"value": value, "raw": raw_val}
        if unit:
            entry["unit"] = unit
        ratios[key] = entry
    return ratios


def fetch_top_ratios_once(
    session: requests.Session,
    slug: str,
    *,
    consolidated: bool,
    timeout: float,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    url = build_company_url(slug, consolidated)
    response = session.get(url, headers=HEADERS, timeout=timeout, proxies=proxies)
    if response.status_code == 404:
        raise NotFoundError(f"Slug '{slug}' not found on Screener.")
    response.raise_for_status()
    return parse_top_ratios(response.text)


def fetch_top_ratios_with_retry(
    session: requests.Session,
    slug: str,
    *,
    consolidated: bool,
    timeout: float,
    max_retries: int,
    retry_delay: Tuple[float, float],
    proxy_manager: Optional["ProxyManager"] = None,
) -> Dict[str, Dict[str, str]]:
    min_wait, max_wait = retry_delay
    min_wait = max(0.0, min_wait)
    max_wait = max(min_wait, max_wait)
    attempt = 0
    last_error: Optional[Exception] = None

    while attempt < max_retries:
        proxies = proxy_manager.for_requests() if proxy_manager else None
        try:
            return fetch_top_ratios_once(
                session,
                slug,
                consolidated=consolidated,
                timeout=timeout,
                proxies=proxies,
            )
        except NotFoundError:
            raise
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and status not in RETRYABLE_STATUS_CODES:
                raise
            last_error = exc
            attempt += 1
        except requests.RequestException as exc:
            last_error = exc
            attempt += 1

        if attempt >= max_retries:
            break

        if proxy_manager and last_error is not None:
            if isinstance(last_error, requests.RequestException):
                proxy_manager.rotate()
            elif isinstance(last_error, requests.HTTPError):
                status = last_error.response.status_code if getattr(last_error, "response", None) is not None else None
                if status == 429:
                    proxy_manager.rotate()

        wait_time = random.uniform(min_wait, max_wait) if max_wait > 0 else 0.0
        if wait_time > 0:
            time.sleep(wait_time)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to fetch ratios for slug '{slug}' after {max_retries} attempts.")


def _clean_slug_value(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    if cleaned.startswith(BASE_URL):
        parts = cleaned.split("/company/", 1)
        cleaned = parts[-1] if len(parts) == 2 else parts[0]
    cleaned = cleaned.strip("/")
    if cleaned.endswith("/consolidated"):
        cleaned = cleaned[: -len("/consolidated")]
    return cleaned.strip()


def slug_candidates(doc: dict, slug_field: str) -> Iterator[str]:
    seen = set()
    primary_raw = doc.get(slug_field)
    if isinstance(primary_raw, str):
        primary = _clean_slug_value(primary_raw)
        if primary:
            seen.add(primary)
            yield primary
    for alt_field in ("NSEID", "BSEID", "Company ID"):
        raw_value = doc.get(alt_field)
        if isinstance(raw_value, str):
            candidate = raw_value.strip()
        elif raw_value is None:
            candidate = ""
        else:
            candidate = str(raw_value).strip()
        candidate = _clean_slug_value(candidate)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        yield candidate


def _normalise_ratio_field(label: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z]+", "_", label).strip("_").lower()
    if not slug:
        slug = "ratio"
    if slug[0].isdigit():
        slug = f"ratio_{slug}"
    return slug


def _coerce_numeric(text: str) -> Optional[Any]:
    stripped = text.strip()
    if not stripped:
        return None
    match = re.match(r"^[^\d\-\+]*([-+]?\d[\d,]*\.?\d*)\s*$", stripped)
    if not match:
        return None
    token = match.group(1).replace(",", "")
    try:
        value = float(token)
    except ValueError:
        return None
    if value.is_integer():
        return int(value)
    return value


def flatten_ratios(ratios: Dict[str, Dict[str, str]]) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, str], Dict[str, str]]:
    values: Dict[str, Any] = {}
    label_map: Dict[str, str] = {}
    unit_map: Dict[str, str] = {}
    raw_map: Dict[str, str] = {}
    used: set[str] = set()

    for label, entry in ratios.items():
        base_field = _normalise_ratio_field(label)
        candidate = base_field
        suffix = 1
        while candidate in used:
            suffix += 1
            candidate = f"{base_field}_{suffix}"
        used.add(candidate)

        value_text = entry.get("value", "").strip()
        raw_text = entry.get("raw", value_text)
        numeric = _coerce_numeric(value_text)
        values[candidate] = numeric if numeric is not None else value_text
        label_map[candidate] = label
        raw_map[candidate] = raw_text
        unit = entry.get("unit")
        if unit:
            unit_map[candidate] = unit

    return values, label_map, unit_map, raw_map


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    client = MongoClient(args.target_mongo_uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    collection = client[args.target_mongo_db][args.collection]

    retry_delay_min = max(0.0, args.retry_delay_min)
    retry_delay_max = max(retry_delay_min, args.retry_delay_max)

    proxy_manager: Optional[ProxyManager] = None
    if args.proxy_file:
        proxy_path = Path(args.proxy_file)
        try:
            proxy_values = load_proxy_list(proxy_path)
        except FileNotFoundError as exc:
            print(f"Warning: {exc}")
        else:
            if proxy_values:
                proxy_manager = ProxyManager(proxy_values)
            else:
                print(f"Warning: No usable proxies found in {proxy_path}")

    if proxy_manager is None and not args.no_default_proxies and DEFAULT_PROXY_POOL:
        proxy_manager = ProxyManager(DEFAULT_PROXY_POOL)
        print("Info: Using built-in fallback proxy list (override with --proxy-file or --no-default-proxies).", file=sys.stderr)

    if args.force:
        query: Dict[str, Any] = {}
    else:
        query = {
            "$or": [
                {"ratio_schema_version": {"$ne": RATIO_SCHEMA_VERSION}},
                {"ratio_schema_version": {"$exists": False}},
            ]
        }

    projection = {
        "_id": 1,
        args.slug_field: 1,
        "Company Name": 1,
        "NSEID": 1,
        "BSEID": 1,
        "Company ID": 1,
        "ratio_fields": 1,
    }

    cursor = collection.find(query, projection=projection)
    if args.limit > 0:
        cursor = cursor.limit(args.limit)

    processed = updated = skipped = failed = 0

    with requests.Session() as session:
        for doc in cursor:
            processed += 1
            name = doc.get("Company Name", "<unknown>")
            previous_fields = [field for field in doc.get("ratio_fields", []) if isinstance(field, str)]
            last_error: Optional[str] = None
            ratios: Optional[Dict[str, Dict[str, str]]] = None
            slug_used: Optional[str] = None

            for candidate in slug_candidates(doc, args.slug_field):
                try:
                    ratios = fetch_top_ratios_with_retry(
                        session,
                        candidate,
                        consolidated=args.consolidated,
                        timeout=args.timeout,
                        max_retries=max(1, args.max_retries),
                        retry_delay=(retry_delay_min, retry_delay_max),
                        proxy_manager=proxy_manager,
                    )
                    slug_used = candidate
                    break
                except NotFoundError as exc:
                    last_error = str(exc)
                    continue
                except Exception as exc:  # pragma: no cover - runtime resilience
                    last_error = f"{type(exc).__name__}: {exc}"
                    break

            if ratios is None:
                failed += 1
                update_doc = {"ratio_fetch_error": last_error or "Unable to fetch ratios."}
                if not args.dry_run:
                    collection.update_one({"_id": doc["_id"]}, {"$set": update_doc})
                print(f"[{processed}] Failed: {name} -> {update_doc['ratio_fetch_error']}")
                if args.delay > 0:
                    time.sleep(args.delay)
                continue

            timestamp = datetime.now(timezone.utc)

            if not ratios:
                skipped += 1
                unset_fields = {field: "" for field in previous_fields}
                unset_fields.update({"top_ratios": "", "top_ratios_error": "", "ratio_fetch_error": ""})
                set_doc = {
                    "ratio_fields": [],
                    "ratio_field_map": {},
                    "ratio_units": {},
                    "ratio_raw_values": {},
                    "ratio_updated_at": timestamp,
                    "ratio_slug": slug_used,
                    "ratio_view": "consolidated" if args.consolidated else "standalone",
                    "ratio_schema_version": RATIO_SCHEMA_VERSION,
                }
                if not args.dry_run:
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {
                            "$set": set_doc,
                            "$unset": unset_fields,
                        },
                    )
                print(f"[{processed}] Skipped: {name} -> No ratios found.")
                if args.delay > 0:
                    time.sleep(args.delay)
                continue

            value_map, label_map, unit_map, raw_map = flatten_ratios(ratios)
            ratio_fields = sorted(value_map.keys())
            unset_fields = {field: "" for field in previous_fields if field not in ratio_fields}
            unset_fields.update({"top_ratios": "", "top_ratios_error": "", "ratio_fetch_error": ""})

            set_doc: Dict[str, Any] = {
                **value_map,
                "ratio_fields": ratio_fields,
                "ratio_field_map": label_map,
                "ratio_updated_at": timestamp,
                "ratio_slug": slug_used,
                "ratio_view": "consolidated" if args.consolidated else "standalone",
                "ratio_schema_version": RATIO_SCHEMA_VERSION,
            }

            if unit_map:
                set_doc["ratio_units"] = unit_map
            else:
                unset_fields["ratio_units"] = ""

            if raw_map:
                set_doc["ratio_raw_values"] = raw_map
            else:
                unset_fields["ratio_raw_values"] = ""

            if args.dry_run:
                print(f"[{processed}] DRY RUN Updated: {name} ({slug_used}) -> {len(ratio_fields)} ratios")
            else:
                collection.update_one({"_id": doc["_id"]}, {"$set": set_doc, "$unset": unset_fields})
                updated += 1
                print(f"[{processed}] Updated: {name} ({slug_used}) -> {len(ratio_fields)} ratios")

            if args.delay > 0:
                time.sleep(args.delay)

    print(
        f"Completed. processed={processed} updated={updated} skipped={skipped} failed={failed} "
        f"(consolidated={args.consolidated})."
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
