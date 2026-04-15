from __future__ import annotations

import csv
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

CSV_HEADERS = [
    "search_query_term",
    "name",
    "price",
    "category",
    "rating",
    "review_count",
    "seller_name",
    "seller_rating",
    "platform",
    "url",
    "timestamp",
]


def _cache_root() -> Path:
    configured = os.getenv("CSV_CACHE_DIR", "search_cache")
    root = Path(__file__).resolve().parent / configured
    root.mkdir(parents=True, exist_ok=True)
    return root


def _slugify_query(query: str) -> str:
    cleaned = (query or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = cleaned.strip("_")
    return cleaned or "empty_query"


def _query_path(query: str) -> Path:
    return _cache_root() / f"{_slugify_query(query)}.csv"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return default


def _normalize_name(name: Any) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _dedup_key(item: dict[str, Any]) -> tuple[str, str, float, float, int]:
    platform = str(item.get("platform") or "").strip().lower()
    name = _normalize_name(item.get("name"))
    price = round(_safe_float(item.get("price"), 0.0), 4)
    rating = round(_safe_float(item.get("rating"), 0.0), 4)
    review_count = _safe_int(item.get("review_count"), 0)
    return (platform, name, price, rating, review_count)


def _to_row(query: str, item: dict[str, Any]) -> dict[str, str]:
    return {
        "search_query_term": query,
        "name": str(item.get("name") or "Unknown"),
        "price": str(_safe_float(item.get("price"), 0.0)),
        "category": str(item.get("category") or "Unknown"),
        "rating": str(_safe_float(item.get("rating"), 0.0)),
        "review_count": str(_safe_int(item.get("review_count"), 0)),
        "seller_name": str(item.get("seller_name") or ""),
        "seller_rating": str(_safe_float(item.get("seller_rating"), 0.0)),
        "platform": str(item.get("platform") or "Unknown"),
        "url": str(item.get("url") or ""),
        "timestamp": str(item.get("timestamp") or datetime.utcnow().isoformat()),
    }


def _row_to_item(row: dict[str, str]) -> dict[str, Any]:
    return {
        "search_query_term": row.get("search_query_term", ""),
        "name": row.get("name", "Unknown"),
        "price": _safe_float(row.get("price"), 0.0),
        "category": row.get("category", "Unknown"),
        "rating": _safe_float(row.get("rating"), 0.0),
        "review_count": _safe_int(row.get("review_count"), 0),
        "seller_name": row.get("seller_name", ""),
        "seller_rating": _safe_float(row.get("seller_rating"), 0.0),
        "platform": row.get("platform", "Unknown"),
        "url": row.get("url", ""),
        "timestamp": row.get("timestamp", ""),
    }


def _is_better_record(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    """Prefer richer records, then newer timestamps, when deduplicating."""
    candidate_reviews = _safe_int(candidate.get("review_count"), 0)
    current_reviews = _safe_int(current.get("review_count"), 0)
    if candidate_reviews != current_reviews:
        return candidate_reviews > current_reviews

    candidate_rating = _safe_float(candidate.get("rating"), 0.0)
    current_rating = _safe_float(current.get("rating"), 0.0)
    if candidate_rating != current_rating:
        return candidate_rating > current_rating

    candidate_ts = str(candidate.get("timestamp") or "")
    current_ts = str(current.get("timestamp") or "")
    return candidate_ts > current_ts


def _deduplicate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in items:
        key = _dedup_key(item)
        existing = deduped.get(key)
        if existing is None or _is_better_record(item, existing):
            deduped[key] = item
    return list(deduped.values())


def read_query_csv(query: str) -> list[dict[str, Any]]:
    path = _query_path(query)
    if not path.exists():
        return []

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        items = [_row_to_item(row) for row in reader if row]

    positive_items = [item for item in items if _safe_float(item.get("price"), 0.0) > 0.0]
    return _deduplicate_items(positive_items)


def append_query_csv_dedup(query: str, new_items: list[dict[str, Any]]) -> dict[str, int]:
    path = _query_path(query)
    existing_items = read_query_csv(query)
    existing_keys = {_dedup_key(item) for item in existing_items}

    appendable_rows: list[dict[str, str]] = []
    for item in new_items:
        if _safe_float(item.get("price"), 0.0) <= 0.0:
            continue
        key = _dedup_key(item)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        appendable_rows.append(_to_row(query, item))

    file_exists = path.exists()
    if appendable_rows:
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(appendable_rows)

    return {
        "appended": len(appendable_rows),
        "existing": len(existing_items),
        "total": len(existing_items) + len(appendable_rows),
    }


def delete_query_csv(query: str) -> bool:
    """Delete cached CSV file for a specific query if it exists."""
    path = _query_path(query)
    if not path.exists():
        return False

    try:
        path.unlink()
    except OSError:
        return False
    return True


def delete_all_query_csvs() -> int:
    """Delete all cached query CSV files and return deleted file count."""
    root = _cache_root()
    deleted_count = 0
    for path in root.glob("*.csv"):
        if not path.is_file():
            continue
        try:
            path.unlink()
            deleted_count += 1
        except OSError:
            continue

    return deleted_count


def _safe_cache_file_name(file_name: str) -> str:
    candidate = Path(file_name or "").name
    if not candidate.lower().endswith(".csv"):
        return ""
    if not re.fullmatch(r"[a-zA-Z0-9_-]+\.csv", candidate):
        return ""
    return candidate


def list_cached_csv_files() -> list[dict[str, Any]]:
    """Return metadata for cached CSV files, newest first."""
    root = _cache_root()
    entries: list[dict[str, Any]] = []

    for path in root.glob("*.csv"):
        if not path.is_file():
            continue

        record_count = 0
        query = ""
        try:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not row:
                        continue
                    record_count += 1
                    if not query:
                        query = str(row.get("search_query_term") or "").strip()
        except OSError:
            continue

        if not query:
            query = path.stem.replace("_", " ")

        modified_ts = datetime.fromtimestamp(path.stat().st_mtime)
        entries.append(
            {
                "file_name": path.name,
                "query": query,
                "record_count": record_count,
                "last_modified": modified_ts.isoformat(timespec="seconds"),
                "last_modified_display": modified_ts.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    entries.sort(key=lambda entry: entry["last_modified"], reverse=True)
    return entries


def read_cached_csv_rows(file_name: str, max_rows: int | None = 300) -> list[dict[str, Any]]:
    """Read rows from a specific cached CSV file for admin preview."""
    safe_name = _safe_cache_file_name(file_name)
    if not safe_name:
        return []

    path = _cache_root() / safe_name
    if not path.exists() or not path.is_file():
        return []

    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row:
                    continue
                rows.append(_row_to_item(row))
                if max_rows is not None and len(rows) >= max_rows:
                    break
    except OSError:
        return []

    return rows


def delete_cached_csv_file(file_name: str) -> bool:
    """Delete a cached CSV by file name."""
    safe_name = _safe_cache_file_name(file_name)
    if not safe_name:
        return False

    path = _cache_root() / safe_name
    if not path.exists() or not path.is_file():
        return False

    try:
        path.unlink()
    except OSError:
        return False

    return True


def deduplicate_all_cached_csvs() -> dict[str, int]:
    """Deduplicate all cached CSV files in-place and return cleanup stats."""
    root = _cache_root()
    stats = {
        "files_scanned": 0,
        "files_updated": 0,
        "rows_before": 0,
        "rows_after": 0,
        "rows_removed": 0,
    }

    for path in root.glob("*.csv"):
        if not path.is_file():
            continue

        stats["files_scanned"] += 1
        query_fallback = path.stem.replace("_", " ")
        raw_items: list[dict[str, Any]] = []

        try:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not row:
                        continue
                    item = _row_to_item(row)
                    if not item.get("search_query_term"):
                        item["search_query_term"] = query_fallback
                    raw_items.append(item)
        except OSError:
            continue

        before_count = len(raw_items)
        stats["rows_before"] += before_count
        if before_count == 0:
            continue

        deduped_items = _deduplicate_items(raw_items)
        after_count = len(deduped_items)
        stats["rows_after"] += after_count
        if after_count >= before_count:
            continue

        try:
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
                writer.writeheader()
                for item in deduped_items:
                    query = str(item.get("search_query_term") or query_fallback)
                    writer.writerow(_to_row(query, item))
        except OSError:
            # If rewrite fails, do not count file as updated.
            continue

        stats["files_updated"] += 1
        stats["rows_removed"] += (before_count - after_count)

    return stats


def deduplicate_cached_csv_file(file_name: str) -> dict[str, int]:
    """Deduplicate one cached CSV file and return before/after stats."""
    safe_name = _safe_cache_file_name(file_name)
    if not safe_name:
        return {"updated": 0, "rows_before": 0, "rows_after": 0, "rows_removed": 0}

    path = _cache_root() / safe_name
    if not path.exists() or not path.is_file():
        return {"updated": 0, "rows_before": 0, "rows_after": 0, "rows_removed": 0}

    query_fallback = path.stem.replace("_", " ")
    raw_items: list[dict[str, Any]] = []

    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row:
                    continue
                item = _row_to_item(row)
                if not item.get("search_query_term"):
                    item["search_query_term"] = query_fallback
                raw_items.append(item)
    except OSError:
        return {"updated": 0, "rows_before": 0, "rows_after": 0, "rows_removed": 0}

    before_count = len(raw_items)
    if before_count == 0:
        return {"updated": 0, "rows_before": 0, "rows_after": 0, "rows_removed": 0}

    deduped_items = _deduplicate_items(raw_items)
    after_count = len(deduped_items)
    removed = max(0, before_count - after_count)

    if removed == 0:
        return {"updated": 0, "rows_before": before_count, "rows_after": after_count, "rows_removed": 0}

    try:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            writer.writeheader()
            for item in deduped_items:
                query = str(item.get("search_query_term") or query_fallback)
                writer.writerow(_to_row(query, item))
    except OSError:
        return {"updated": 0, "rows_before": before_count, "rows_after": before_count, "rows_removed": 0}

    return {"updated": 1, "rows_before": before_count, "rows_after": after_count, "rows_removed": removed}


def _normalize_admin_item(item: dict[str, Any], fallback_query: str = "") -> dict[str, Any]:
    search_query_term = str(item.get("search_query_term") or fallback_query or "").strip()
    if not search_query_term:
        search_query_term = "manual"

    return {
        "search_query_term": search_query_term,
        "name": str(item.get("name") or "Unknown").strip() or "Unknown",
        "price": _safe_float(item.get("price"), 0.0),
        "category": str(item.get("category") or "General").strip() or "General",
        "rating": _safe_float(item.get("rating"), 0.0),
        "review_count": max(0, _safe_int(item.get("review_count"), 0)),
        "seller_name": str(item.get("seller_name") or "").strip(),
        "seller_rating": _safe_float(item.get("seller_rating"), 0.0),
        "platform": str(item.get("platform") or "Unknown").strip() or "Unknown",
        "url": str(item.get("url") or "").strip(),
        "timestamp": str(item.get("timestamp") or datetime.utcnow().isoformat()),
    }


def add_cached_csv_row(file_name: str, item: dict[str, Any]) -> bool:
    """Append one normalized row to a specific cache CSV file."""
    safe_name = _safe_cache_file_name(file_name)
    if not safe_name:
        return False

    path = _cache_root() / safe_name
    if not path.exists() or not path.is_file():
        return False

    fallback_query = path.stem.replace("_", " ")
    normalized = _normalize_admin_item(item, fallback_query=fallback_query)
    row = _to_row(normalized["search_query_term"], normalized)

    try:
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            if path.stat().st_size == 0:
                writer.writeheader()
            writer.writerow(row)
    except OSError:
        return False

    return True


def update_cached_csv_row(file_name: str, row_index: int, item: dict[str, Any]) -> bool:
    """Update one row in a cache CSV file by 0-based row index."""
    safe_name = _safe_cache_file_name(file_name)
    if not safe_name or row_index < 0:
        return False

    path = _cache_root() / safe_name
    if not path.exists() or not path.is_file():
        return False

    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row:
                    continue
                rows.append(_row_to_item(row))
    except OSError:
        return False

    if row_index >= len(rows):
        return False

    fallback_query = path.stem.replace("_", " ")
    normalized = _normalize_admin_item(item, fallback_query=fallback_query)
    rows[row_index] = normalized

    try:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            writer.writeheader()
            for entry in rows:
                query = str(entry.get("search_query_term") or fallback_query)
                writer.writerow(_to_row(query, entry))
    except OSError:
        return False

    return True
