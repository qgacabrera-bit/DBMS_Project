from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import pyodbc


_SCHEMA_READY = False


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


def _slugify_query(query: str) -> str:
    cleaned = (query or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = cleaned.strip("_")
    return cleaned or "empty_query"


def _safe_cache_file_name(file_name: str) -> str:
    candidate = (file_name or "").strip().split("/")[-1].split("\\")[-1]
    if not candidate.lower().endswith(".csv"):
        return ""
    if not re.fullmatch(r"[a-zA-Z0-9_-]+\.csv", candidate):
        return ""
    return candidate


def _query_slug_from_file_name(file_name: str) -> str:
    safe_name = _safe_cache_file_name(file_name)
    if not safe_name:
        return ""
    return safe_name[:-4]


def _dedup_key(item: dict[str, Any]) -> tuple[str, str, float, float, int]:
    platform = str(item.get("platform") or "").strip().lower()
    name = _normalize_name(item.get("name"))
    price = round(_safe_float(item.get("price"), 0.0), 4)
    rating = round(_safe_float(item.get("rating"), 0.0), 4)
    review_count = _safe_int(item.get("review_count"), 0)
    return (platform, name, price, rating, review_count)


def _is_better_record(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
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
    deduped: dict[tuple[str, str, float, float, int], dict[str, Any]] = {}
    for item in items:
        key = _dedup_key(item)
        existing = deduped.get(key)
        if existing is None or _is_better_record(item, existing):
            deduped[key] = item
    return list(deduped.values())


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


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return datetime.utcnow()


def _derive_base_url(url: str) -> str:
    parsed = urlparse(url or "")
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _resolve_connection_string() -> str:
    explicit = os.getenv("MSSQL_CONNECTION_STRING", "").strip()
    if explicit:
        return explicit

    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server").strip()
    server = os.getenv("MSSQL_SERVER", "localhost").strip()
    database = os.getenv("MSSQL_DATABASE", "ProductMonitoringDB").strip()
    encrypt = os.getenv("MSSQL_ENCRYPT", "no").strip()
    trust_cert = os.getenv("MSSQL_TRUST_SERVER_CERTIFICATE", "yes").strip()

    uid = os.getenv("MSSQL_UID", "").strip()
    pwd = os.getenv("MSSQL_PWD", "").strip()

    if uid and pwd:
        auth = f"UID={uid};PWD={pwd};"
    else:
        trusted = os.getenv("MSSQL_TRUSTED_CONNECTION", "yes").strip()
        auth = f"Trusted_Connection={trusted};"

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"{auth}"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
    )


def _connect() -> pyodbc.Connection:
    conn_str = _resolve_connection_string()
    timeout = _safe_int(os.getenv("MSSQL_CONNECT_TIMEOUT", "5"), 5)
    return pyodbc.connect(conn_str, timeout=timeout)


def _ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            IF OBJECT_ID(N'dbo.Platform', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Platform (
                    platform_id INT IDENTITY(1,1) PRIMARY KEY,
                    platform_name NVARCHAR(100) NOT NULL,
                    base_url NVARCHAR(500) NULL
                );
            END
            """
        )
        cursor.execute(
            """
            IF OBJECT_ID(N'dbo.Category', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Category (
                    category_id INT IDENTITY(1,1) PRIMARY KEY,
                    search_query_name NVARCHAR(255) NOT NULL,
                    category_name NVARCHAR(100) NOT NULL
                );
            END
            """
        )
        cursor.execute(
            """
            IF OBJECT_ID(N'dbo.Review', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Review (
                    review_id INT IDENTITY(1,1) PRIMARY KEY,
                    rating DECIMAL(10,8) NOT NULL DEFAULT (0),
                    review_count INT NOT NULL DEFAULT (0)
                );
            END
            """
        )
        cursor.execute(
            """
            IF OBJECT_ID(N'dbo.Product', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Product (
                    product_id INT IDENTITY(1,1) PRIMARY KEY,
                    product_name NVARCHAR(500) NOT NULL,
                    current_price DECIMAL(10,2) NOT NULL DEFAULT (0),
                    review_id INT NULL,
                    category_id INT NOT NULL,
                    platform_id INT NOT NULL,
                    product_url NVARCHAR(500) NULL,
                    date_first_scraped DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
                    CONSTRAINT FK_Product_Review FOREIGN KEY (review_id) REFERENCES dbo.Review(review_id),
                    CONSTRAINT FK_Product_Category FOREIGN KEY (category_id) REFERENCES dbo.Category(category_id),
                    CONSTRAINT FK_Product_Platform FOREIGN KEY (platform_id) REFERENCES dbo.Platform(platform_id)
                );
            END
            """
        )
        cursor.execute(
            """
            IF OBJECT_ID(N'dbo.PriceHistory', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.PriceHistory (
                    price_id INT IDENTITY(1,1) PRIMARY KEY,
                    product_id INT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    date_recorded DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
                    CONSTRAINT FK_PriceHistory_Product FOREIGN KEY (product_id) REFERENCES dbo.Product(product_id)
                );
            END
            """
        )
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'IX_Category_search_query_name'
                  AND object_id = OBJECT_ID(N'dbo.Category')
            )
            BEGIN
                CREATE INDEX IX_Category_search_query_name
                ON dbo.Category(search_query_name);
            END
            """
        )
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'IX_PriceHistory_product_id_date'
                  AND object_id = OBJECT_ID(N'dbo.PriceHistory')
            )
            BEGIN
                CREATE INDEX IX_PriceHistory_product_id_date
                ON dbo.PriceHistory(product_id, date_recorded DESC);
            END
            """
        )
        conn.commit()

    _SCHEMA_READY = True


def _ensure_platform(cursor: pyodbc.Cursor, platform_name: str, product_url: str) -> int:
    name = (platform_name or "Unknown").strip() or "Unknown"
    cursor.execute("SELECT TOP 1 platform_id FROM dbo.Platform WHERE platform_name = ?", name)
    row = cursor.fetchone()
    if row:
        return int(row.platform_id)

    cursor.execute(
        "INSERT INTO dbo.Platform (platform_name, base_url) OUTPUT INSERTED.platform_id VALUES (?, ?)",
        name,
        _derive_base_url(product_url),
    )
    inserted = cursor.fetchone()
    return int(inserted.platform_id)


def _ensure_category(cursor: pyodbc.Cursor, search_query_name: str, category_name: str) -> int:
    query_name = (search_query_name or "manual").strip() or "manual"
    cat_name = (category_name or "General").strip() or "General"

    cursor.execute(
        """
        SELECT TOP 1 category_id
        FROM dbo.Category
        WHERE search_query_name = ? AND category_name = ?
        """,
        query_name,
        cat_name,
    )
    row = cursor.fetchone()
    if row:
        return int(row.category_id)

    cursor.execute(
        """
        INSERT INTO dbo.Category (search_query_name, category_name)
        OUTPUT INSERTED.category_id
        VALUES (?, ?)
        """,
        query_name,
        cat_name,
    )
    inserted = cursor.fetchone()
    return int(inserted.category_id)


def _insert_review(cursor: pyodbc.Cursor, rating: float, review_count: int) -> int:
    cursor.execute(
        """
        INSERT INTO dbo.Review (rating, review_count)
        OUTPUT INSERTED.review_id
        VALUES (?, ?)
        """,
        max(0.0, min(5.0, _safe_float(rating, 0.0))),
        max(0, _safe_int(review_count, 0)),
    )
    inserted = cursor.fetchone()
    return int(inserted.review_id)


def _insert_product(
    cursor: pyodbc.Cursor,
    *,
    product_name: str,
    current_price: float,
    review_id: int,
    category_id: int,
    platform_id: int,
    product_url: str,
    date_first_scraped: datetime,
) -> int:
    cursor.execute(
        """
        INSERT INTO dbo.Product (
            product_name,
            current_price,
            review_id,
            category_id,
            platform_id,
            product_url,
            date_first_scraped
        )
        OUTPUT INSERTED.product_id
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (product_name or "Unknown").strip() or "Unknown",
        _safe_float(current_price, 0.0),
        review_id,
        category_id,
        platform_id,
        (product_url or "").strip(),
        date_first_scraped,
    )
    inserted = cursor.fetchone()
    return int(inserted.product_id)


def _insert_price_history(cursor: pyodbc.Cursor, product_id: int, price: float, date_recorded: datetime) -> None:
    cursor.execute(
        """
        INSERT INTO dbo.PriceHistory (product_id, price, date_recorded)
        VALUES (?, ?, ?)
        """,
        int(product_id),
        _safe_float(price, 0.0),
        date_recorded,
    )


def _cleanup_orphans(cursor: pyodbc.Cursor) -> None:
    cursor.execute(
        """
        DELETE r
        FROM dbo.Review r
        LEFT JOIN dbo.Product p ON p.review_id = r.review_id
        WHERE p.product_id IS NULL
        """
    )
    cursor.execute(
        """
        DELETE c
        FROM dbo.Category c
        LEFT JOIN dbo.Product p ON p.category_id = c.category_id
        WHERE p.product_id IS NULL
        """
    )
    cursor.execute(
        """
        DELETE pl
        FROM dbo.Platform pl
        LEFT JOIN dbo.Product p ON p.platform_id = pl.platform_id
        WHERE p.product_id IS NULL
        """
    )


def _row_to_item(row: Any) -> dict[str, Any]:
    ts = row.date_first_scraped
    if isinstance(ts, datetime):
        ts_value = ts.isoformat(timespec="seconds")
    else:
        ts_value = str(ts or "")

    return {
        "product_id": int(row.product_id),
        "search_query_term": str(row.search_query_name or ""),
        "name": str(row.product_name or "Unknown"),
        "price": _safe_float(row.current_price, 0.0),
        "category": str(row.category_name or "General"),
        "rating": _safe_float(row.rating, 0.0),
        "review_count": _safe_int(row.review_count, 0),
        "seller_name": "",
        "seller_rating": 0.0,
        "platform": str(row.platform_name or "Unknown"),
        "url": str(row.product_url or ""),
        "timestamp": ts_value,
    }


def _fetch_rows_by_query(query: str) -> list[dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []

    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                p.product_id,
                p.product_name,
                p.current_price,
                p.product_url,
                p.date_first_scraped,
                c.search_query_name,
                c.category_name,
                pl.platform_name,
                ISNULL(r.rating, 0) AS rating,
                ISNULL(r.review_count, 0) AS review_count
            FROM dbo.Product p
            INNER JOIN dbo.Category c ON c.category_id = p.category_id
            INNER JOIN dbo.Platform pl ON pl.platform_id = p.platform_id
            LEFT JOIN dbo.Review r ON r.review_id = p.review_id
            WHERE c.search_query_name = ?
            ORDER BY p.product_id ASC
            """,
            q,
        )
        rows = cursor.fetchall()

    return [_row_to_item(row) for row in rows]


def _resolve_query_from_slug(query_slug: str) -> str:
    if not query_slug:
        return ""

    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT search_query_name FROM dbo.Category")
        rows = cursor.fetchall()

    candidates = [str(row.search_query_name or "").strip() for row in rows if str(row.search_query_name or "").strip()]
    for query in candidates:
        if _slugify_query(query) == query_slug:
            return query

    fallback = query_slug.replace("_", " ").strip()
    return fallback


def read_query_csv(query: str) -> list[dict[str, Any]]:
    rows = _fetch_rows_by_query(query)
    items = [row for row in rows if _safe_float(row.get("price"), 0.0) > 0.0]
    deduped = _deduplicate_items(items)
    for item in deduped:
        item.pop("product_id", None)
    return deduped


def append_query_csv_dedup(query: str, new_items: list[dict[str, Any]]) -> dict[str, int]:
    query = (query or "").strip()
    if not query:
        return {"appended": 0, "existing": 0, "total": 0}

    existing_items = read_query_csv(query)
    existing_keys = {_dedup_key(item) for item in existing_items}

    appendable_rows: list[dict[str, Any]] = []
    for item in new_items:
        normalized = _normalize_admin_item(item, fallback_query=query)
        if _safe_float(normalized.get("price"), 0.0) <= 0.0:
            continue
        key = _dedup_key(normalized)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        normalized["search_query_term"] = query
        appendable_rows.append(normalized)

    if appendable_rows:
        _ensure_schema()
        with _connect() as conn:
            cursor = conn.cursor()
            for row in appendable_rows:
                ts = _to_datetime(row.get("timestamp"))
                platform_id = _ensure_platform(cursor, row.get("platform", ""), row.get("url", ""))
                category_id = _ensure_category(cursor, row.get("search_query_term", query), row.get("category", "General"))
                review_id = _insert_review(cursor, _safe_float(row.get("rating"), 0.0), _safe_int(row.get("review_count"), 0))
                product_id = _insert_product(
                    cursor,
                    product_name=str(row.get("name") or "Unknown"),
                    current_price=_safe_float(row.get("price"), 0.0),
                    review_id=review_id,
                    category_id=category_id,
                    platform_id=platform_id,
                    product_url=str(row.get("url") or ""),
                    date_first_scraped=ts,
                )
                _insert_price_history(cursor, product_id, _safe_float(row.get("price"), 0.0), ts)
            conn.commit()

    return {
        "appended": len(appendable_rows),
        "existing": len(existing_items),
        "total": len(existing_items) + len(appendable_rows),
    }


def delete_query_csv(query: str) -> bool:
    q = (query or "").strip()
    if not q:
        return False

    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE ph
            FROM dbo.PriceHistory ph
            INNER JOIN dbo.Product p ON p.product_id = ph.product_id
            INNER JOIN dbo.Category c ON c.category_id = p.category_id
            WHERE c.search_query_name = ?
            """,
            q,
        )
        cursor.execute(
            """
            DELETE p
            FROM dbo.Product p
            INNER JOIN dbo.Category c ON c.category_id = p.category_id
            WHERE c.search_query_name = ?
            """,
            q,
        )
        deleted = max(0, cursor.rowcount)
        cursor.execute("DELETE FROM dbo.Category WHERE search_query_name = ?", q)
        _cleanup_orphans(cursor)
        conn.commit()

    return deleted > 0


def delete_all_query_csvs() -> int:
    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dbo.PriceHistory")
        cursor.execute("DELETE FROM dbo.Product")
        deleted_products = max(0, cursor.rowcount)
        cursor.execute("DELETE FROM dbo.Review")
        cursor.execute("DELETE FROM dbo.Category")
        cursor.execute("DELETE FROM dbo.Platform")
        conn.commit()

    return deleted_products


def list_cached_csv_files() -> list[dict[str, Any]]:
    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                c.search_query_name AS query,
                COUNT(*) AS record_count,
                MAX(p.date_first_scraped) AS last_modified
            FROM dbo.Product p
            INNER JOIN dbo.Category c ON c.category_id = p.category_id
            GROUP BY c.search_query_name
            ORDER BY MAX(p.date_first_scraped) DESC
            """
        )
        rows = cursor.fetchall()

    entries: list[dict[str, Any]] = []
    for row in rows:
        query = str(row.query or "").strip()
        modified = row.last_modified
        if isinstance(modified, datetime):
            iso = modified.isoformat(timespec="seconds")
            display = modified.strftime("%Y-%m-%d %H:%M:%S")
        else:
            iso = str(modified or "")
            display = iso

        entries.append(
            {
                "file_name": f"{_slugify_query(query)}.csv",
                "query": query,
                "record_count": int(row.record_count or 0),
                "last_modified": iso,
                "last_modified_display": display,
            }
        )

    return entries


def read_cached_csv_rows(file_name: str, max_rows: int | None = 300) -> list[dict[str, Any]]:
    query_slug = _query_slug_from_file_name(file_name)
    if not query_slug:
        return []

    query = _resolve_query_from_slug(query_slug)
    rows = _fetch_rows_by_query(query)
    if max_rows is not None:
        rows = rows[:max_rows]

    for row in rows:
        row.pop("product_id", None)
    return rows


def delete_cached_csv_file(file_name: str) -> bool:
    query_slug = _query_slug_from_file_name(file_name)
    if not query_slug:
        return False
    query = _resolve_query_from_slug(query_slug)
    return delete_query_csv(query)


def deduplicate_cached_csv_file(file_name: str) -> dict[str, int]:
    query_slug = _query_slug_from_file_name(file_name)
    if not query_slug:
        return {"updated": 0, "rows_before": 0, "rows_after": 0, "rows_removed": 0}

    query = _resolve_query_from_slug(query_slug)
    rows = _fetch_rows_by_query(query)
    before = len(rows)
    if before == 0:
        return {"updated": 0, "rows_before": 0, "rows_after": 0, "rows_removed": 0}

    keep_by_key: dict[tuple[str, str, float, float, int], dict[str, Any]] = {}
    for row in rows:
        key = _dedup_key(row)
        existing = keep_by_key.get(key)
        if existing is None or _is_better_record(row, existing):
            keep_by_key[key] = row

    keep_ids = {int(row["product_id"]) for row in keep_by_key.values() if row.get("product_id") is not None}
    remove_ids = [int(row["product_id"]) for row in rows if int(row["product_id"]) not in keep_ids]

    if not remove_ids:
        return {"updated": 0, "rows_before": before, "rows_after": before, "rows_removed": 0}

    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        for product_id in remove_ids:
            cursor.execute("DELETE FROM dbo.PriceHistory WHERE product_id = ?", product_id)
            cursor.execute("DELETE FROM dbo.Product WHERE product_id = ?", product_id)
        _cleanup_orphans(cursor)
        conn.commit()

    after = before - len(remove_ids)
    return {"updated": 1, "rows_before": before, "rows_after": after, "rows_removed": len(remove_ids)}


def deduplicate_all_cached_csvs() -> dict[str, int]:
    files = list_cached_csv_files()
    stats = {
        "files_scanned": len(files),
        "files_updated": 0,
        "rows_before": 0,
        "rows_after": 0,
        "rows_removed": 0,
    }

    for entry in files:
        file_name = str(entry.get("file_name") or "")
        if not file_name:
            continue

        rows = read_cached_csv_rows(file_name, max_rows=None)
        stats["rows_before"] += len(rows)

        single = deduplicate_cached_csv_file(file_name)
        if single.get("updated"):
            stats["files_updated"] += 1
            stats["rows_removed"] += int(single.get("rows_removed") or 0)
            stats["rows_after"] += int(single.get("rows_after") or 0)
        else:
            stats["rows_after"] += int(single.get("rows_before") or 0)

    return stats


def add_cached_csv_row(file_name: str, item: dict[str, Any]) -> bool:
    query_slug = _query_slug_from_file_name(file_name)
    if not query_slug:
        return False

    fallback_query = _resolve_query_from_slug(query_slug) or query_slug.replace("_", " ")
    normalized = _normalize_admin_item(item, fallback_query=fallback_query)

    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()
        ts = _to_datetime(normalized.get("timestamp"))
        platform_id = _ensure_platform(cursor, normalized.get("platform", ""), normalized.get("url", ""))
        category_id = _ensure_category(cursor, normalized.get("search_query_term", fallback_query), normalized.get("category", "General"))
        review_id = _insert_review(cursor, _safe_float(normalized.get("rating"), 0.0), _safe_int(normalized.get("review_count"), 0))
        product_id = _insert_product(
            cursor,
            product_name=str(normalized.get("name") or "Unknown"),
            current_price=_safe_float(normalized.get("price"), 0.0),
            review_id=review_id,
            category_id=category_id,
            platform_id=platform_id,
            product_url=str(normalized.get("url") or ""),
            date_first_scraped=ts,
        )
        _insert_price_history(cursor, product_id, _safe_float(normalized.get("price"), 0.0), ts)
        conn.commit()

    return True


def update_cached_csv_row(file_name: str, row_index: int, item: dict[str, Any]) -> bool:
    query_slug = _query_slug_from_file_name(file_name)
    if not query_slug or row_index < 0:
        return False

    query = _resolve_query_from_slug(query_slug)
    rows = _fetch_rows_by_query(query)
    if row_index >= len(rows):
        return False

    target = rows[row_index]
    product_id = int(target["product_id"])
    old_price = _safe_float(target.get("price"), 0.0)

    normalized = _normalize_admin_item(item, fallback_query=query)
    ts = _to_datetime(normalized.get("timestamp"))

    _ensure_schema()
    with _connect() as conn:
        cursor = conn.cursor()

        platform_id = _ensure_platform(cursor, normalized.get("platform", ""), normalized.get("url", ""))
        category_id = _ensure_category(cursor, normalized.get("search_query_term", query), normalized.get("category", "General"))
        review_id = _insert_review(cursor, _safe_float(normalized.get("rating"), 0.0), _safe_int(normalized.get("review_count"), 0))

        new_price = _safe_float(normalized.get("price"), 0.0)
        cursor.execute(
            """
            UPDATE dbo.Product
            SET
                product_name = ?,
                current_price = ?,
                review_id = ?,
                category_id = ?,
                platform_id = ?,
                product_url = ?,
                date_first_scraped = ?
            WHERE product_id = ?
            """,
            str(normalized.get("name") or "Unknown"),
            new_price,
            review_id,
            category_id,
            platform_id,
            str(normalized.get("url") or ""),
            ts,
            product_id,
        )

        if abs(new_price - old_price) > 1e-9:
            _insert_price_history(cursor, product_id, new_price, ts)

        _cleanup_orphans(cursor)
        conn.commit()

    return True
