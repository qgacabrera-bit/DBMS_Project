from __future__ import annotations

from collections import Counter
import os
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import quote_plus

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for

from sql_cache import (
    add_cached_csv_row,
    append_query_csv_dedup,
    deduplicate_all_cached_csvs,
    deduplicate_cached_csv_file,
    delete_all_query_csvs,
    delete_cached_csv_file,
    list_cached_csv_files,
    read_cached_csv_rows,
    read_query_csv,
    update_cached_csv_row,
)
from scraper import Scraper


load_dotenv(Path(__file__).with_name(".env"))


app = Flask(__name__)
scraper = Scraper()


SORT_OPTIONS = {
    "az",
    "za",
    "price_low",
    "price_high",
    "rating",
    "review_count",
}


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def parse_csv_list(value: str | None) -> list[str]:
    if value is None:
        return []
    return [entry.strip() for entry in value.split(",") if entry.strip()]


def parse_skip_platforms_from_request() -> list[str]:
    values = request.args.getlist("skip_platforms")
    if not values:
        single = request.args.get("skip_platforms")
        values = [single] if single else []

    parsed: list[str] = []
    for value in values:
        if not value:
            continue
        parsed.extend(parse_csv_list(value))

    # preserve order while removing duplicates
    return list(dict.fromkeys(parsed))


def has_positive_price(item: Any) -> bool:
    if isinstance(item, dict):
        price = item.get("price", 0.0)
    else:
        price = getattr(item, "price", 0.0)

    try:
        return float(price or 0.0) > 0.0
    except (TypeError, ValueError):
        return False


def filter_positive_price(items: list[Any]) -> list[Any]:
    return [item for item in items if has_positive_price(item)]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return default


def extract_platform(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("platform") or "").strip()
    return str(getattr(item, "platform", "") or "").strip()


def get_platform_coverage(items: list[Any]) -> set[str]:
    """Extract unique platforms from items and cache result."""
    expected = set(scraper.SUPPORTED_PLATFORMS)
    found = {extract_platform(item) for item in items if extract_platform(item)}
    return found


def has_platform_coverage(items: list[Any]) -> bool:
    """Check if items cover all supported platforms."""
    expected = set(scraper.SUPPORTED_PLATFORMS)
    found = get_platform_coverage(items)
    return expected.issubset(found)


def fallback_products(query: str) -> list[dict[str, Any]]:
    """Return lightweight fallback rows when live scraping returns no products."""
    q = query.strip()
    encoded = quote_plus(q)
    return [
        {
            "name": f"{q.title()} (fallback) - Shopee",
            "price": 0.0,
            "category": "Unknown",
            "rating": 0.0,
            "review_count": 0,
            "seller_name": "",
            "seller_rating": 0.0,
            "platform": "Shopee",
            "url": f"https://shopee.ph/search?keyword={encoded}",
        },
        {
            "name": f"{q.title()} (fallback) - Lazada",
            "price": 0.0,
            "category": "Unknown",
            "rating": 0.0,
            "review_count": 0,
            "seller_name": "",
            "seller_rating": 0.0,
            "platform": "Lazada",
            "url": f"https://www.lazada.com.ph/catalog/?q={encoded}",
        },
        {
            "name": f"{q.title()} (fallback) - Amazon",
            "price": 0.0,
            "category": "Unknown",
            "rating": 0.0,
            "review_count": 0,
            "seller_name": "",
            "seller_rating": 0.0,
            "platform": "Amazon",
            "url": f"https://www.amazon.com/s?k={encoded}",
        },
        {
            "name": f"{q.title()} (fallback) - Google Shopping",
            "price": 0.0,
            "category": "Unknown",
            "rating": 0.0,
            "review_count": 0,
            "seller_name": "",
            "seller_rating": 0.0,
            "platform": "Google Shopping",
            "url": f"https://www.google.com/search?tbm=shop&q={encoded}",
        },
    ]


def fetch_live_results(
    query: str, 
    debug: bool = False,
    headed: bool | None = None,
    browser: str | None = None,
    challenge_wait_seconds: int | None = None,
    skip_platforms: list[str] | None = None,
    use_shopee_account_scraper: bool | None = None,
) -> dict[str, Any]:
    live_payload = scraper.fetch_live_results(
        query, 
        debug=debug,
        headed=headed,
        browser=browser,
        challenge_wait_seconds=challenge_wait_seconds,
        skip_platforms=skip_platforms,
        use_shopee_account_scraper=use_shopee_account_scraper,
    )
    if live_payload.get("results"):
        return live_payload

    # Always keep UI useful by returning direct search links when live sources are empty.
    live_payload["results"] = fallback_products(query)
    live_payload["source"] = "fallback_links"
    live_payload["persistable"] = False
    if not live_payload.get("status_message"):
        live_payload["status_message"] = "No live scraping data returned. Showing direct search links instead."
    return live_payload


def apply_filters_to_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    platform = request.args.get("platform")
    category = request.args.get("category")

    min_price = request.args.get("min_price", type=float)
    max_price = request.args.get("max_price", type=float)
    min_rating = request.args.get("min_rating", type=float)
    max_rating = request.args.get("max_rating", type=float)
    min_reviews = request.args.get("min_reviews", type=int)
    max_reviews = request.args.get("max_reviews", type=int)

    filtered: list[dict[str, Any]] = []
    for item in items:
        item_platform = str(item.get("platform") or "")
        item_category = str(item.get("category") or "")
        item_price = safe_float(item.get("price"), 0.0)
        item_rating = safe_float(item.get("rating"), 0.0)
        item_reviews = safe_int(item.get("review_count"), 0)

        if platform and item_platform != platform:
            continue
        if category and category.lower() not in item_category.lower():
            continue
        if min_price is not None and item_price < min_price:
            continue
        if max_price is not None and item_price > max_price:
            continue
        if min_rating is not None and item_rating < min_rating:
            continue
        if max_rating is not None and item_rating > max_rating:
            continue
        if min_reviews is not None and item_reviews < min_reviews:
            continue
        if max_reviews is not None and item_reviews > max_reviews:
            continue

        filtered.append(item)

    return filtered


def sort_items(items: list[dict[str, Any]], sort_key: str) -> list[dict[str, Any]]:
    if sort_key not in SORT_OPTIONS:
        sort_key = "price_low"

    if sort_key == "az":
        return sorted(items, key=lambda i: str(i.get("name") or "").lower())
    if sort_key == "za":
        return sorted(items, key=lambda i: str(i.get("name") or "").lower(), reverse=True)
    if sort_key == "price_low":
        return sorted(items, key=lambda i: safe_float(i.get("price"), 0.0))
    if sort_key == "price_high":
        return sorted(items, key=lambda i: safe_float(i.get("price"), 0.0), reverse=True)
    if sort_key == "rating":
        return sorted(items, key=lambda i: safe_float(i.get("rating"), 0.0), reverse=True)
    if sort_key == "review_count":
        return sorted(items, key=lambda i: safe_int(i.get("review_count"), 0), reverse=True)

    return items


def build_admin_stats(cache_files: list[dict[str, Any]]) -> dict[str, Any]:
    all_rows: list[dict[str, Any]] = []
    for entry in cache_files:
        file_name = str(entry.get("file_name") or "").strip()
        if not file_name:
            continue
        all_rows.extend(read_cached_csv_rows(file_name, max_rows=None))

    total_rows = len(all_rows)
    price_values = [safe_float(row.get("price"), 0.0) for row in all_rows if safe_float(row.get("price"), 0.0) > 0.0]
    average_price = (sum(price_values) / len(price_values)) if price_values else 0.0
    median_price = float(median(price_values)) if price_values else 0.0

    platform_counts = Counter(str(row.get("platform") or "Unknown") for row in all_rows)
    platform_distribution = [
        {
            "platform": platform,
            "count": count,
            "percent": round((count / total_rows) * 100.0, 1) if total_rows else 0.0,
        }
        for platform, count in sorted(platform_counts.items(), key=lambda pair: pair[1], reverse=True)
    ]

    chart_palette = ["#2563eb", "#0d9488", "#f97316", "#8b5cf6", "#e11d48", "#0891b2"]
    platform_chart_parts: list[str] = []
    cursor = 0.0
    for index, row in enumerate(platform_distribution):
        percent = float(row.get("percent") or 0.0)
        if percent <= 0:
            continue
        color = chart_palette[index % len(chart_palette)]
        start = cursor
        end = min(100.0, cursor + percent)
        row["color"] = color
        platform_chart_parts.append(f"{color} {start:.1f}% {end:.1f}%")
        cursor = end

    platform_chart_css = ", ".join(platform_chart_parts) if platform_chart_parts else "#e2e8f0 0% 100%"

    top_queries = [
        {
            "query": str(entry.get("query") or ""),
            "record_count": int(entry.get("record_count") or 0),
            "file_name": str(entry.get("file_name") or ""),
        }
        for entry in sorted(cache_files, key=lambda row: int(row.get("record_count") or 0), reverse=True)[:5]
    ]

    latest_activity = cache_files[0] if cache_files else None
    zero_price_count = sum(1 for row in all_rows if safe_float(row.get("price"), 0.0) <= 0.0)
    zero_row_files = sum(1 for entry in cache_files if int(entry.get("record_count") or 0) == 0)

    return {
        "average_price": round(average_price, 2),
        "median_price": round(median_price, 2),
        "platform_distribution": platform_distribution,
        "platform_chart_css": platform_chart_css,
        "top_queries": top_queries,
        "latest_activity": latest_activity,
        "zero_price_count": zero_price_count,
        "zero_price_ratio": round((zero_price_count / total_rows) * 100.0, 1) if total_rows else 0.0,
        "zero_row_files": zero_row_files,
    }


def _admin_row_payload_from_form() -> dict[str, Any]:
    return {
        "search_query_term": request.form.get("search_query_term", "").strip(),
        "name": request.form.get("name", "").strip(),
        "price": request.form.get("price", "").strip(),
        "category": request.form.get("category", "").strip(),
        "rating": request.form.get("rating", "").strip(),
        "review_count": request.form.get("review_count", "").strip(),
        "seller_name": request.form.get("seller_name", "").strip(),
        "seller_rating": request.form.get("seller_rating", "").strip(),
        "platform": request.form.get("platform", "").strip(),
        "url": request.form.get("url", "").strip(),
    }


@app.get("/")
def home():
    status_message = request.args.get("status_message")
    return render_template("index.html", products=[], query="", total=0, status_message=status_message, source_label="none")


@app.get("/admin")
def admin_route():
    selected_file = request.args.get("file", "").strip()
    status_message = request.args.get("status_message")

    cache_files = list_cached_csv_files()
    total_files = len(cache_files)
    total_records = sum(int(entry.get("record_count") or 0) for entry in cache_files)
    admin_stats = build_admin_stats(cache_files)

    selected_rows: list[dict[str, Any]] = []
    selected_query = ""
    if selected_file:
        selected_rows = read_cached_csv_rows(selected_file, max_rows=300)
        for entry in cache_files:
            if entry.get("file_name") == selected_file:
                selected_query = str(entry.get("query") or "")
                break

    return render_template(
        "admin.html",
        cache_files=cache_files,
        total_files=total_files,
        total_records=total_records,
        selected_file=selected_file,
        selected_query=selected_query,
        selected_rows=selected_rows,
        status_message=status_message,
        admin_stats=admin_stats,
    )


@app.post("/admin/cache/delete")
def admin_delete_cache_file_route():
    file_name = request.form.get("file_name", "").strip()
    selected = request.form.get("selected", "").strip()

    if not file_name:
        return redirect(url_for("admin_route", status_message="Select a cache file to delete."))

    deleted = delete_cached_csv_file(file_name)
    status = (
        f"Deleted cached file '{file_name}'."
        if deleted
        else f"Could not delete '{file_name}' (not found or invalid)."
    )
    return redirect(url_for("admin_route", file=selected, status_message=status))


@app.post("/admin/cache/delete_all")
def admin_delete_all_cache_files_route():
    deleted = delete_all_query_csvs()
    status = (
        f"Deleted {deleted} cached SQL record(s)."
        if deleted > 0
        else "No cached SQL records were found."
    )
    return redirect(url_for("admin_route", status_message=status))


@app.post("/admin/cache/deduplicate")
def admin_deduplicate_cache_files_route():
    stats = deduplicate_all_cached_csvs()
    status = (
        "Deduplicate complete: "
        f"{stats['rows_removed']} duplicate row(s) removed across "
        f"{stats['files_updated']} file(s) "
        f"(scanned {stats['files_scanned']} file(s))."
    )
    return redirect(url_for("admin_route", status_message=status))


@app.post("/admin/cache/deduplicate_one")
def admin_deduplicate_one_cache_file_route():
    file_name = request.form.get("file_name", "").strip()
    selected = request.form.get("selected", "").strip()

    if not file_name:
        return redirect(url_for("admin_route", file=selected, status_message="Select a cache file to deduplicate."))

    stats = deduplicate_cached_csv_file(file_name)
    if stats.get("updated"):
        status = (
            f"Deduplicated '{file_name}': removed {stats['rows_removed']} duplicate row(s) "
            f"({stats['rows_before']} -> {stats['rows_after']})."
        )
    else:
        status = f"No duplicate rows found for '{file_name}' or file is invalid."

    return redirect(url_for("admin_route", file=selected or file_name, status_message=status))


@app.post("/admin/cache/add_row")
def admin_add_cache_row_route():
    file_name = request.form.get("file_name", "").strip()
    selected = request.form.get("selected", "").strip() or file_name

    if not file_name:
        return redirect(url_for("admin_route", file=selected, status_message="Select a cache file first."))

    payload = _admin_row_payload_from_form()
    if not payload.get("name"):
        return redirect(url_for("admin_route", file=selected, status_message="Name is required for new records."))

    created = add_cached_csv_row(file_name, payload)
    status = "New row added successfully." if created else "Could not add row. Check selected file and input values."
    return redirect(url_for("admin_route", file=selected, status_message=status))


@app.post("/admin/cache/update_row")
def admin_update_cache_row_route():
    file_name = request.form.get("file_name", "").strip()
    selected = request.form.get("selected", "").strip() or file_name
    row_index = request.form.get("row_index", type=int)

    if not file_name:
        return redirect(url_for("admin_route", file=selected, status_message="Select a cache file first."))
    if row_index is None or row_index < 0:
        return redirect(url_for("admin_route", file=selected, status_message="Invalid row selection for update."))

    payload = _admin_row_payload_from_form()
    if not payload.get("name"):
        return redirect(url_for("admin_route", file=selected, status_message="Name is required when updating a row."))

    updated = update_cached_csv_row(file_name, row_index, payload)
    status = "Row updated successfully." if updated else "Could not update row. It may have changed or been removed."
    return redirect(url_for("admin_route", file=selected, status_message=status))


@app.get("/search")
def search_route():
    query = request.args.get("q", "").strip()
    live = parse_bool(request.args.get("live"), default=False)
    debug = parse_bool(request.args.get("debug"), default=False)
    headed = parse_bool(request.args.get("headed"), default=None)
    browser = request.args.get("browser") or request.args.get("playwright_browser")
    challenge_wait = request.args.get("challenge_wait_seconds") or request.args.get("challenge_wait")
    challenge_wait_seconds = int(challenge_wait) if challenge_wait else None
    skip_platforms = parse_skip_platforms_from_request()
    use_shopee_account_scraper = parse_bool(request.args.get("use_shopee_account_scraper"), default=None)

    if not query:
        return jsonify({"error": "Missing required query parameter: q"}), 400

    if not live:
        cached = read_query_csv(query)
        if cached:
            return jsonify(
                {
                    "source": "sql_cache",
                    "query": query,
                    "count": len(cached),
                    "results": cached,
                    "status_message": "Serving cached results from SQL Server.",
                    "platform_status": {},
                }
            )
        return jsonify(
            {
                "source": "sql_cache",
                "query": query,
                "count": 0,
                "results": [],
                "status_message": "No cached SQL data found. Use live=1 to fetch and persist results.",
                "platform_status": {},
            }
        )

    live_payload = fetch_live_results(
        query,
        debug=debug,
        headed=headed,
        browser=browser,
        challenge_wait_seconds=challenge_wait_seconds,
        skip_platforms=skip_platforms,
        use_shopee_account_scraper=use_shopee_account_scraper,
    )
    fresh = filter_positive_price(live_payload.get("results", []))
    sql_write_stats = {"appended": 0, "existing": 0, "total": 0}
    if fresh and live_payload.get("persistable", False):
        sql_write_stats = append_query_csv_dedup(query, fresh)

    response_payload = {
        "source": live_payload.get("source", "live"),
        "query": query,
        "count": len(fresh),
        "results": fresh,
        "status_message": live_payload.get("status_message"),
        "platform_status": live_payload.get("platform_status", {}),
        "sql_cache": sql_write_stats,
    }
    if debug and "debug" in live_payload:
        response_payload["debug"] = live_payload["debug"]
    return jsonify(response_payload)


@app.get("/debug/live_check")
def debug_live_check_route():
    query = request.args.get("q", "").strip()
    headed = parse_bool(request.args.get("headed"), default=None)
    browser = request.args.get("browser") or request.args.get("playwright_browser")
    challenge_wait = request.args.get("challenge_wait_seconds") or request.args.get("challenge_wait")
    challenge_wait_seconds = int(challenge_wait) if challenge_wait else None
    skip_platforms = parse_skip_platforms_from_request()
    use_shopee_account_scraper = parse_bool(request.args.get("use_shopee_account_scraper"), default=None)
    if not query:
        return jsonify({"error": "Missing required query parameter: q"}), 400

    live_payload = fetch_live_results(
        query, 
        debug=True,
        headed=headed,
        browser=browser,
        challenge_wait_seconds=challenge_wait_seconds,
        skip_platforms=skip_platforms,
        use_shopee_account_scraper=use_shopee_account_scraper,
    )
    filtered_results = filter_positive_price(live_payload.get("results", []))
    sql_write_stats = {"appended": 0, "existing": 0, "total": 0}
    if filtered_results and live_payload.get("persistable", False):
        sql_write_stats = append_query_csv_dedup(query, filtered_results)

    return jsonify(
        {
            "query": query,
            "source": live_payload.get("source"),
            "count": len(filtered_results),
            "status_message": live_payload.get("status_message"),
            "platform_status": live_payload.get("platform_status", {}),
            "debug": live_payload.get("debug", {}),
            "sample_results": filtered_results[:5],
            "sql_cache": sql_write_stats,
        }
    )


@app.get("/results")
def results_route():
    query = request.args.get("q", "").strip()
    live = parse_bool(request.args.get("live"), default=False)
    headed = parse_bool(request.args.get("headed"), default=None)
    browser = request.args.get("browser") or request.args.get("playwright_browser")
    challenge_wait = request.args.get("challenge_wait_seconds") or request.args.get("challenge_wait")
    challenge_wait_seconds = int(challenge_wait) if challenge_wait else None
    skip_platforms = parse_skip_platforms_from_request()
    use_shopee_account_scraper = parse_bool(request.args.get("use_shopee_account_scraper"), default=None)

    if not query:
        return render_template(
            "index.html",
            products=[],
            query="",
            total=0,
            error="Please provide a search term.",
            status_message=None,
            source_label="none",
        )

    source_label = "sql_cache"
    status_message = request.args.get("status_message")
    products: list[dict[str, Any]] = []

    if live:
        live_payload = fetch_live_results(
            query,
            headed=headed,
            browser=browser,
            challenge_wait_seconds=challenge_wait_seconds,
            skip_platforms=skip_platforms,
            use_shopee_account_scraper=use_shopee_account_scraper,
        )
        fresh = filter_positive_price(live_payload.get("results", []))
        source_label = live_payload.get("source", "live")
        if not status_message:
            status_message = live_payload.get("status_message")
        if fresh and live_payload.get("persistable", False):
            append_query_csv_dedup(query, fresh)
        products = fresh
    else:
        products = read_query_csv(query)

    products = apply_filters_to_items(products)
    sort_key = request.args.get("sort", "price_low")
    products = sort_items(products, sort_key)
    products = products[:100]

    error_message = None
    if not products:
        error_message = (
            "No products were fetched from live scraping. "
            "Try a different query or enable fallback links."
        )

    return render_template(
        "index.html",
        products=products,
        query=query,
        total=len(products),
        current_sort=sort_key,
        error=error_message,
        status_message=status_message,
        source_label=source_label,
    )


if __name__ == "__main__":
    app.run(debug=True)