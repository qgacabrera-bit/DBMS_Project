from __future__ import annotations

import csv
import base64
import http.cookiejar
import io
import json
import os
import random
import re
import sys
import time
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse
import xml.etree.ElementTree as ET

import requests
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

stealth_sync = Stealth().apply_stealth_sync


class Scraper:
    SUPPORTED_PLATFORMS = ("Shopee", "Lazada", "Amazon", "Google Shopping")

    def __init__(self, timeout: int = 15) -> None:
        self.timeout = timeout
        self.max_results = 20
        self.allow_html_scraping = self._env_bool("ENABLE_HTML_SCRAPING", default=True)
        self.enable_antibot_behavior = self._env_bool("ENABLE_ANTIBOT_BEHAVIOR", default=True)
        self.require_direct_product_url = self._env_bool("REQUIRE_DIRECT_PRODUCT_URL", default=False)
        self.shopee_cookie_json = os.getenv("SHOPEE_COOKIES_JSON", "").strip()
        self.shopee_cookie_path = os.getenv("SHOPEE_COOKIES_PATH", "").strip()
        if not self.shopee_cookie_json and not self.shopee_cookie_path:
            self.shopee_cookie_path = self._discover_shopee_cookie_path()
        
        # Playwright headed mode and browser selection
        self.headed = self._env_bool("PLAYWRIGHT_HEADED", default=False)
        self.browser = os.getenv("PLAYWRIGHT_BROWSER", "chromium").lower()
        self.challenge_wait_seconds = int(os.getenv("PLAYWRIGHT_CHALLENGE_WAIT_SECONDS", "0"))
        self.skip_platforms = self._normalize_skip_platforms(self._env_list("SKIP_PLATFORMS"))

        # Optional Shopee credentialed scraper integration (sibling shopee-scraper project).
        self.enable_shopee_account_scraper = self._env_bool("ENABLE_SHOPEE_ACCOUNT_SCRAPER", default=True)
        self.shopee_username = os.getenv("SHOPEE_USERNAME", "").strip()
        self.shopee_password = os.getenv("SHOPEE_PASSWORD", "").strip()
        self.shopee_account_numpage = max(1, int(os.getenv("SHOPEE_ACCOUNT_NUMPAGE", "1")))
        self.shopee_account_itemperpage = max(1, int(os.getenv("SHOPEE_ACCOUNT_ITEMPERPAGE", "10")))
        self.shopee_account_detail_backfill_limit = max(
            0,
            int(os.getenv("SHOPEE_ACCOUNT_DETAIL_BACKFILL_LIMIT", str(self.max_results))),
        )
        self.shopee_account_verification_wait_seconds = max(
            0,
            int(os.getenv("SHOPEE_ACCOUNT_VERIFICATION_WAIT_SECONDS", "60")),
        )

        # No-credential backup sources
        self.enable_woocommerce_source = self._env_bool("ENABLE_WOOCOMMERCE_SOURCE", default=True)
        self.woocommerce_stores = self._env_list("WOOCOMMERCE_STORES")
        self.enable_public_feeds = self._env_bool("ENABLE_PUBLIC_FEEDS", default=True)
        self.public_feed_urls = self._env_list("PUBLIC_FEED_URLS")

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        self.last_request_time = 0.0

    @staticmethod
    def _discover_shopee_cookie_path() -> str:
        cookie_path = r"C:\temp\shopee_cookies.json"
        return cookie_path if os.path.isfile(cookie_path) else ""

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_list(name: str) -> list[str]:
        value = os.getenv(name, "")
        if not value.strip():
            return []
        return [entry.strip() for entry in value.split(",") if entry.strip()]

    def _normalize_skip_platforms(self, values: list[str] | None) -> set[str]:
        if not values:
            return set()

        mapping = {
            "shopee": "Shopee",
            "lazada": "Lazada",
            "amazon": "Amazon",
            "google_shopping": "Google Shopping",
            "google-shopping": "Google Shopping",
            "google shopping": "Google Shopping",
            "googleshopping": "Google Shopping",
        }
        normalized: set[str] = set()
        for value in values:
            key = (value or "").strip().lower()
            if key in mapping:
                normalized.add(mapping[key])
        return normalized

    def _random_delay(self, min_sec: float = 0.5, max_sec: float = 2.5) -> None:
        """Add human-like random delay between requests."""
        if self.enable_antibot_behavior:
            delay = random.uniform(min_sec, max_sec)
            time.sleep(delay)

    def _enforce_request_interval(self, min_interval: float = 2.0) -> None:
        """Enforce minimum time between successive requests to appear human."""
        if self.enable_antibot_behavior:
            elapsed = time.time() - self.last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self.last_request_time = time.time()

    def _human_like_scroll(self, page) -> None:
        """Simulate human scroll behavior on page."""
        if not self.enable_antibot_behavior:
            return
        try:
            scroll_amount = random.randint(300, 800)
            page.mouse.wheel(0, scroll_amount)
            time.sleep(random.uniform(0.3, 0.8))
        except Exception:
            pass

    def _human_like_mouse_move(self, page) -> None:
        """Simulate human mouse movements with 20-step smooth interpolation."""
        if not self.enable_antibot_behavior:
            return
        try:
            start_x, start_y = random.randint(100, 900), random.randint(100, 500)
            end_x, end_y = random.randint(100, 900), random.randint(100, 500)
            page.mouse.move(start_x, start_y)
            page.mouse.move(end_x, end_y, steps=20)
            time.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass

    def fetch_live_results(
        self, 
        query: str, 
        debug: bool = False,
        headed: bool | None = None,
        browser: str | None = None,
        challenge_wait_seconds: int | None = None,
        skip_platforms: list[str] | None = None,
        use_shopee_account_scraper: bool | None = None,
    ) -> dict[str, Any]:
        """
        Fetch live results with optional per-request overrides for headed mode, browser, and challenge wait.
        
        Args:
            query: Search query string
            debug: Enable debug trace output
            headed: Override headed mode (None = use self.headed)
            browser: Override browser selection (None = use self.browser)
            challenge_wait_seconds: Override challenge wait time (None = use self.challenge_wait_seconds)
            use_shopee_account_scraper: Override account scraper toggle (None = use env config)
        """
        # Apply per-request overrides
        orig_headed = self.headed
        orig_browser = self.browser
        orig_challenge_wait = self.challenge_wait_seconds
        orig_skip_platforms = set(self.skip_platforms)
        orig_shopee_account_scraper = self.enable_shopee_account_scraper
        
        try:
            if headed is not None:
                self.headed = headed
            if browser is not None:
                self.browser = self._normalize_browser(browser)
            if challenge_wait_seconds is not None:
                self.challenge_wait_seconds = challenge_wait_seconds
            if skip_platforms is not None:
                self.skip_platforms = self._normalize_skip_platforms(skip_platforms)
            if use_shopee_account_scraper is not None:
                self.enable_shopee_account_scraper = use_shopee_account_scraper
            
            return self._do_fetch_live_results(query, debug)
        finally:
            # Restore original settings
            self.headed = orig_headed
            self.browser = orig_browser
            self.challenge_wait_seconds = orig_challenge_wait
            self.skip_platforms = orig_skip_platforms
            self.enable_shopee_account_scraper = orig_shopee_account_scraper
    
    def _normalize_browser(self, name: str) -> str:
        """Normalize browser name aliases."""
        name = name.lower().strip()
        if name in {"firefox", "mozilla"}:
            return "firefox"
        if name in {"chromium", "chrome"}:
            return "chromium"
        if name in {"webkit", "safari"}:
            return "webkit"
        return "chromium"  # default

    def _get_browser_type(self, playwright):
        """Get the correct browser type from Playwright based on self.browser setting."""
        if self.browser == "firefox":
            return playwright.firefox
        elif self.browser == "webkit":
            return playwright.webkit
        else:
            return playwright.chromium

    def _launch_browser(self, playwright, args=None):
        """Launch browser with current headed/browser settings and comprehensive anti-detection args."""
        if args is None:
            args = []
        
        # Add comprehensive anti-detection arguments
        anti_detection_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-popup-blocking",
            "--disable-default-apps",
            "--disable-preconnect",
            "--disable-component-extensions-with-background-pages",
            "--disable-component-update",
            "--disable-extensions",
            "--disable-sync",
            "--metrics-recording-only",
            "--disable-plugins-power-saver-for-prerender",
            "--disable-prerender-local-predictor",
            "--disable-prerendering",
            "--enable-features=NetworkService,NetworkServiceInProcess",
            "--disable-backgrounding-occluded-windows",
            "--disable-breakpad",
            "--disable-client-side-phishing-detection",
            "--disable-component-extensions",
            "--disable-hang-monitor",
            "--disable-ipc-flooding-protection",
            "--disable-popup-blocking",
            "--disable-prompt-on-repost",
            "--disable-renderer-backgrounding",
            "--disable-web-resources",
            "--enable-automation=false",
        ]
        
        # Merge with passed args, avoiding duplicates
        all_args = list(dict.fromkeys(anti_detection_args + args))
        
        browser_type = self._get_browser_type(playwright)
        return browser_type.launch(headless=not self.headed, args=all_args)


    def _do_fetch_live_results(self, query: str, debug: bool) -> dict[str, Any]:
        platform_status: dict[str, str] = {}
        all_results: list[dict[str, Any]] = []
        debug_trace: list[dict[str, Any]] = []

        def add_debug(stage: str, status: str, duration_ms: float, details: dict[str, Any]) -> None:
            if not debug:
                return
            debug_trace.append(
                {
                    "stage": stage,
                    "status": status,
                    "duration_ms": round(duration_ms, 2),
                    "details": details,
                }
            )

        if not self.allow_html_scraping:
            payload = {
                "results": [],
                "source": "scraping_disabled",
                "status_message": (
                    "Live scraping is currently disabled by configuration. "
                    "Enable HTML scraping to fetch live product results."
                ),
                "persistable": False,
                "platform_status": platform_status,
            }
            if debug:
                add_debug(
                    stage="config",
                    status="disabled",
                    duration_ms=0.0,
                    details={
                        "allow_html_scraping": self.allow_html_scraping,
                        "enable_antibot_behavior": self.enable_antibot_behavior,
                    },
                )
                payload["debug"] = {"query": query, "trace": debug_trace}
            return payload

        for scrape_method, platform in [
            (self.scrape_shopee, "Shopee"),
            (self.scrape_lazada, "Lazada"),
            (self.scrape_amazon, "Amazon"),
            (self.scrape_google_shopping, "Google Shopping"),
        ]:
            if platform in self.skip_platforms:
                platform_status[platform] = "skipped"
                add_debug(
                    stage=f"scrape_{platform.lower()}",
                    status="skipped",
                    duration_ms=0.0,
                    details={"reason": "requested", "platform": platform},
                )
                continue

            stage_start = time.perf_counter()
            try:
                platform_results = scrape_method(query, debug_trace=debug_trace)
                if platform_results:
                    all_results.extend(platform_results)
                    platform_status[platform] = "html_scrape_results"
                    add_debug(
                        stage=f"scrape_{platform.lower()}",
                        status="results",
                        duration_ms=(time.perf_counter() - stage_start) * 1000,
                        details={"count": len(platform_results)},
                    )
                elif platform not in platform_status:
                    platform_status[platform] = "html_scrape_no_results"
                    add_debug(
                        stage=f"scrape_{platform.lower()}",
                        status="no_results",
                        duration_ms=(time.perf_counter() - stage_start) * 1000,
                        details={"count": 0},
                    )
            except Exception as exc:
                if platform not in platform_status:
                    platform_status[platform] = "html_scrape_failed"
                add_debug(
                    stage=f"scrape_{platform.lower()}",
                    status="failed",
                    duration_ms=(time.perf_counter() - stage_start) * 1000,
                    details={"error": str(exc), "exception_type": type(exc).__name__},
                )

        if all_results:
            payload = {
                "results": all_results,
                "source": "html_scrape",
                "status_message": "Live data fetched via Playwright scraping.",
                "persistable": True,
                "platform_status": platform_status,
            }
            if debug:
                payload["debug"] = {
                    "query": query,
                    "trace": debug_trace,
                    "config": {
                        "allow_html_scraping": self.allow_html_scraping,
                        "enable_antibot_behavior": self.enable_antibot_behavior,
                        "max_results": self.max_results,
                        "playwright_headed": bool(self.headed),
                        "playwright_browser": str(self.browser),
                        "playwright_challenge_wait_seconds": int(self.challenge_wait_seconds or 0),
                        "skip_platforms": sorted(self.skip_platforms),
                    },
                }
            return payload

        # No-credential backup source: WooCommerce Store API public endpoint.
        if self.enable_woocommerce_source and self.woocommerce_stores:
            stage_start = time.perf_counter()
            woo_results = self._fetch_woocommerce_results(query)
            if woo_results:
                payload = {
                    "results": woo_results,
                    "source": "woocommerce_public",
                    "status_message": "Live data fetched from WooCommerce public product endpoints.",
                    "persistable": True,
                    "platform_status": {**platform_status, "WooCommerce": "results"},
                }
                add_debug(
                    stage="woocommerce",
                    status="results",
                    duration_ms=(time.perf_counter() - stage_start) * 1000,
                    details={"count": len(woo_results), "stores": len(self.woocommerce_stores)},
                )
                if debug:
                    payload["debug"] = {"query": query, "trace": debug_trace}
                return payload
            platform_status["WooCommerce"] = "no_results"
            add_debug(
                stage="woocommerce",
                status="no_results",
                duration_ms=(time.perf_counter() - stage_start) * 1000,
                details={"stores": len(self.woocommerce_stores)},
            )
        elif self.enable_woocommerce_source:
            platform_status["WooCommerce"] = "no_stores_configured"
            add_debug(stage="woocommerce", status="no_stores_configured", duration_ms=0.0, details={})
        else:
            platform_status["WooCommerce"] = "disabled"
            add_debug(stage="woocommerce", status="disabled", duration_ms=0.0, details={})

        if self.enable_public_feeds and self.public_feed_urls:
            stage_start = time.perf_counter()
            feed_results = self._fetch_public_feed_results(query)
            if feed_results:
                payload = {
                    "results": feed_results,
                    "source": "public_feeds",
                    "status_message": "Live data fetched from public product feeds.",
                    "persistable": True,
                    "platform_status": {**platform_status, "PublicFeed": "results"},
                }
                add_debug(
                    stage="public_feeds",
                    status="results",
                    duration_ms=(time.perf_counter() - stage_start) * 1000,
                    details={"count": len(feed_results), "feeds": len(self.public_feed_urls)},
                )
                if debug:
                    payload["debug"] = {"query": query, "trace": debug_trace}
                return payload
            platform_status["PublicFeed"] = "no_results"
            add_debug(
                stage="public_feeds",
                status="no_results",
                duration_ms=(time.perf_counter() - stage_start) * 1000,
                details={"feeds": len(self.public_feed_urls)},
            )
        elif self.enable_public_feeds:
            platform_status["PublicFeed"] = "no_feed_urls_configured"
            add_debug(stage="public_feeds", status="no_feed_urls_configured", duration_ms=0.0, details={})
        else:
            platform_status["PublicFeed"] = "disabled"
            add_debug(stage="public_feeds", status="disabled", duration_ms=0.0, details={})

        payload = {
            "results": [],
            "source": "html_scrape_no_results",
            "status_message": "No live product results were returned by scraping.",
            "persistable": False,
            "platform_status": platform_status,
        }
        if debug:
            payload["debug"] = {
                "query": query,
                "trace": debug_trace,
                "config": {
                    "allow_html_scraping": self.allow_html_scraping,
                    "enable_antibot_behavior": self.enable_antibot_behavior,
                    "playwright_headed": self.headed,
                    "playwright_browser": self.browser,
                    "challenge_wait_seconds": self.challenge_wait_seconds,
                    "skip_platforms": sorted(self.skip_platforms),
                    "enable_woocommerce_source": self.enable_woocommerce_source,
                    "enable_public_feeds": self.enable_public_feeds,
                    "woocommerce_stores": self.woocommerce_stores,
                    "public_feed_urls": self.public_feed_urls,
                },
            }
        return payload

    def _fetch_woocommerce_results(self, query: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        for store_url in self.woocommerce_stores:
            endpoint = f"{store_url.rstrip('/')}/wp-json/wc/store/products"
            try:
                response = requests.get(
                    endpoint,
                    params={"search": query, "per_page": self.max_results},
                    headers=self.headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError):
                continue

            if not isinstance(payload, list):
                continue

            store_domain = urlparse(store_url).netloc or store_url
            for item in payload:
                if not isinstance(item, dict):
                    continue

                name = self._clean_text(item.get("name"))
                if not name:
                    continue

                permalink = self._clean_text(item.get("permalink"))
                if self.require_direct_product_url and (not permalink or not self._is_direct_product_url(permalink)):
                    continue

                raw_price = ""
                prices = item.get("prices", {})
                if isinstance(prices, dict):
                    raw_price = str(prices.get("price") or "")

                # WooCommerce Store API often returns price in minor units.
                price_value = 0.0
                if raw_price.isdigit():
                    price_value = float(raw_price) / 100.0
                else:
                    price_value = self._safe_float(raw_price)

                results.append(
                    {
                        "name": name,
                        "price": price_value,
                        "category": "General",
                        "rating": 0.0,
                        "review_count": 0,
                        "seller_name": "",
                        "seller_rating": 0.0,
                        "platform": "WooCommerce",
                        "url": permalink,
                    }
                )

                if len(results) >= self.max_results:
                    return results

        return results

    def _fetch_public_feed_results(self, query: str) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        for feed_url in self.public_feed_urls:
            products.extend(self._fetch_from_feed_url(feed_url, query))
            if len(products) >= self.max_results:
                break
        return products[: self.max_results]

    def _fetch_from_feed_url(self, feed_url: str, query: str) -> list[dict[str, Any]]:
        request_url = feed_url.format(query=quote_plus(query)) if "{query}" in feed_url else feed_url
        params = None if "{query}" in feed_url else {"q": query}

        try:
            response = requests.get(request_url, params=params, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException:
            return []

        content_type = (response.headers.get("Content-Type") or "").lower()
        body = response.text

        rows: list[dict[str, Any]] = []

        if "json" in content_type or body.lstrip().startswith(("{", "[")):
            try:
                payload = response.json()
            except ValueError:
                payload = None

            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
            elif isinstance(payload, dict):
                candidate_rows = (
                    payload.get("products")
                    or payload.get("items")
                    or payload.get("results")
                    or payload.get("data")
                    or []
                )
                if isinstance(candidate_rows, list):
                    rows = [row for row in candidate_rows if isinstance(row, dict)]

        elif "xml" in content_type or body.lstrip().startswith("<"):
            rows = self._parse_xml_feed_rows(body)

        elif "csv" in content_type or "," in body.splitlines()[0] if body.splitlines() else False:
            rows = self._parse_csv_feed_rows(body)

        if not rows:
            return []

        normalized: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized_item = self._normalize_external_product(
                row,
                platform="PublicFeed",
                query=query,
            )
            if normalized_item is None:
                continue
            normalized.append(normalized_item)
            if len(normalized) >= self.max_results:
                break

        return normalized

    def _parse_xml_feed_rows(self, xml_text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return rows

        item_nodes = root.findall(".//item")
        if not item_nodes:
            item_nodes = root.findall(".//entry")
        if not item_nodes:
            item_nodes = root.findall(".//product")

        for node in item_nodes:
            entry: dict[str, Any] = {}
            for child in list(node):
                tag = child.tag.split("}")[-1].lower()
                value = (child.text or "").strip()
                if tag in {"title", "name"}:
                    entry["title"] = value
                elif tag in {"link", "url", "permalink"}:
                    entry["url"] = value
                elif tag in {"price", "sale_price"}:
                    entry["price"] = value
                elif tag in {"category", "product_type"}:
                    entry["category"] = value

            if entry:
                rows.append(entry)

            if len(rows) >= self.max_results:
                break

        return rows

    def _parse_csv_feed_rows(self, csv_text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        try:
            reader = csv.DictReader(io.StringIO(csv_text))
        except Exception:
            return rows

        key_aliases = {
            "title": ["title", "name", "product_name"],
            "url": ["url", "link", "product_url", "permalink"],
            "price": ["price", "sale_price", "regular_price"],
            "category": ["category", "product_type"],
        }

        for raw_row in reader:
            if not raw_row:
                continue

            lowered = {str(k).strip().lower(): (v or "").strip() for k, v in raw_row.items() if k}
            row: dict[str, Any] = {}
            for output_key, aliases in key_aliases.items():
                for alias in aliases:
                    if lowered.get(alias):
                        row[output_key] = lowered[alias]
                        break

            if row:
                rows.append(row)

            if len(rows) >= self.max_results:
                break

        return rows

    @staticmethod
    def _is_direct_product_url(url: str) -> bool:
        lowered = url.strip().lower()
        if not lowered.startswith(("http://", "https://")):
            return False

        disallowed_markers = (
            "google.com/search",
            "tbm=shop",
            "shopee.ph/search",
            "lazada.com.ph/catalog",
            "search_result",
        )
        return not any(marker in lowered for marker in disallowed_markers)

    def _normalize_external_product(
        self,
        item: dict[str, Any],
        platform: str,
        query: str,
    ) -> dict[str, Any] | None:
        title = str(item.get("title") or item.get("name") or "Unknown Product")
        product_url = str(
            item.get("url")
            or item.get("link")
            or item.get("product_url")
            or item.get("productUrl")
            or item.get("product_link")
            or item.get("deeplink")
            or item.get("permalink")
            or ""
        )

        if self.require_direct_product_url:
            if not product_url or not self._is_direct_product_url(product_url):
                return None
        elif product_url and not self._is_direct_product_url(product_url):
            product_url = ""

        return {
            "name": title,
            "price": self._safe_float(item.get("price", 0.0)),
            "category": str(item.get("category") or "General"),
            "rating": self._safe_float(item.get("rating", 0.0)),
            "review_count": int(item.get("review_count", 0) or 0),
            "seller_name": str(item.get("seller_name", "") or ""),
            "seller_rating": self._safe_float(item.get("seller_rating", 0.0)),
            "platform": platform,
            "url": product_url,
        }

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        if isinstance(value, dict):
            for key in ("value", "average", "rating", "score", "text", "price"):
                candidate = value.get(key)
                if candidate is not None:
                    value = candidate
                    break
            else:
                return default

        text = str(value).strip()
        if not text:
            return default

        cleaned = (
            text.replace(",", "")
            .replace("$", "")
            .replace("USD", "")
            .replace("PHP", "")
            .replace("₱", "")
            .strip()
        )
        raw = (
            text.replace("$", "")
            .replace("USD", "")
            .replace("PHP", "")
            .replace("₱", "")
            .strip()
        )

        # Handle locale-specific decimal separators (e.g., 3,7 or 1.234,56).
        if "," in raw:
            if "." in raw and raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            elif "." not in raw and re.search(r",\d{1,2}(?:\D|$)", raw):
                raw = raw.replace(",", ".")
            else:
                raw = raw.replace(",", "")

        locale_match = re.search(r"-?\d+(?:\.\d+)?", raw)
        if locale_match:
            cleaned = locale_match.group(0)

        number_match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if number_match:
            cleaned = number_match.group(0)
        try:
            return float(cleaned)
        except ValueError:
            return default

    @staticmethod
    def _safe_int(value: str | None, default: int = 0) -> int:
        if not value:
            return default
        cleaned = "".join(ch for ch in value if ch.isdigit())
        if not cleaned:
            return default
        try:
            return int(cleaned)
        except ValueError:
            return default

    @staticmethod
    def _clean_text(value: str | None) -> str:
        if not value:
            return ""
        return value.strip()

    @staticmethod
    def _extract_price_from_text(value: str) -> float:
        # Match currency-like patterns such as ₱31,490 or PHP 31,490.
        match = re.search(r"(?:PHP\s*|₱\s*)([\d,]+(?:\.\d{1,2})?)", value, flags=re.IGNORECASE)
        if not match:
            return 0.0
        return float(match.group(1).replace(",", ""))

    @staticmethod
    def _clean_shopee_mobile_name(value: str) -> str:
        text = value.replace("\u200e", " ").replace("\u200f", " ")
        text = re.sub(r"\s+", " ", text).strip()

        # Remove trailing commercial fragments that are not part of product title.
        stop_markers = [
            "₱",
            "PHP",
            " sold",
            " Days",
            " EXCLUSIVE",
            "Calamba City",
        ]
        cut_index = len(text)
        lower_text = text.lower()
        for marker in stop_markers:
            idx = lower_text.find(marker.lower())
            if idx != -1 and idx < cut_index:
                cut_index = idx

        text = text[:cut_index].strip(" -")
        return text

    @staticmethod
    def _normalize_url(base_url: str, href: str | None) -> str:
        if not href:
            return ""
        return urljoin(base_url, href)

    @staticmethod
    def _is_amazon_product_url(url: str) -> bool:
        lowered = unquote((url or "").strip().lower())
        if not lowered:
            return False
        if "amazon." not in lowered:
            return False
        return "/dp/" in lowered or "/gp/product/" in lowered

    @staticmethod
    def _is_shopee_product_url(url: str) -> bool:
        lowered = unquote((url or "").strip().lower())
        if not lowered:
            return False
        if "shopee." not in lowered:
            return False
        return "/product/" in lowered or "-i." in lowered or "/i." in lowered

    @staticmethod
    def _normalize_amazon_search_href(href: str | None) -> str:
        """Normalize Amazon search result links, including sponsored redirect wrappers."""
        if not href:
            return ""

        normalized = urljoin("https://www.amazon.com", href)
        parsed = urlparse(normalized)

        # Common Amazon wrappers for ad/sponsored cards.
        if parsed.path.startswith("/sspa/click") or parsed.path.startswith("/gp/slredirect/"):
            query_params = parse_qs(parsed.query)
            wrapped = query_params.get("url")
            if wrapped and wrapped[0]:
                decoded = unquote(wrapped[0])
                normalized = urljoin("https://www.amazon.com", decoded)

        return normalized

    @staticmethod
    def _decode_search_redirect_url(url: str) -> str:
        """Decode search-engine redirect URLs (e.g., DuckDuckGo uddg links)."""
        if not url:
            return ""

        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        uddg = query_params.get("uddg")
        if uddg:
            return unquote(uddg[0])

        # Google frequently wraps outbound destinations in /url?q=<target>.
        host = (parsed.netloc or "").lower()
        if "google." in host and parsed.path.startswith("/url"):
            wrapped_q = query_params.get("q") or query_params.get("url")
            if wrapped_q and wrapped_q[0]:
                candidate = unquote(wrapped_q[0])
                if candidate.startswith("http"):
                    return candidate

        # Bing often wraps destination URL in query param "u" with a prefixed base64 payload.
        if "bing.com" in (parsed.netloc or "").lower():
            wrapped_u = query_params.get("u")
            if wrapped_u and wrapped_u[0]:
                candidate = unquote(wrapped_u[0])
                # Common shape: a1aHR0cHM6Ly9.... where the payload after "a1" is base64(url).
                if candidate.startswith("a1"):
                    b64_payload = candidate[2:]
                    padding = "=" * (-len(b64_payload) % 4)
                    try:
                        decoded = base64.urlsafe_b64decode((b64_payload + padding).encode("ascii")).decode(
                            "utf-8",
                            errors="ignore",
                        )
                        if decoded.startswith("http"):
                            return decoded
                    except Exception:
                        pass
                if candidate.startswith("http"):
                    return candidate
        return url

    def _extract_price_from_item(self, item, selectors: list[str]) -> float:
        for selector in selectors:
            el = item.query_selector(selector)
            if not el:
                continue

            # Common pattern where numeric price lives in attribute
            attr_price = el.get_attribute("data-price") or el.get_attribute("data-value")
            if attr_price:
                parsed = self._safe_float(self._clean_text(attr_price))
                if parsed > 0:
                    return parsed

            text_price = self._safe_float(self._clean_text(el.text_content()))
            if text_price > 0:
                return text_price

        return 0.0

    def _extract_amazon_price_from_item(self, item) -> float:
        """Extract Amazon prices from the dedicated price block only.

        Amazon cards often contain unrelated numbers like ratings and review counts.
        This helper only trusts the price container itself, so cards without a real
        price stay at 0.0 instead of inheriting a random number from the listing.
        """
        price_container = item.query_selector("span.a-price")
        if not price_container:
            return 0.0

        offscreen = price_container.query_selector("span.a-offscreen")
        if offscreen:
            price = self._safe_float(self._clean_text(offscreen.text_content()))
            if price > 0:
                return price

        whole = self._clean_text((price_container.query_selector("span.a-price-whole") or price_container).text_content())
        fraction_el = price_container.query_selector("span.a-price-fraction")
        fraction = self._clean_text(fraction_el.text_content()) if fraction_el else ""

        if whole:
            candidate = whole.replace(",", "")
            if fraction.isdigit():
                candidate = f"{candidate}.{fraction}"

            price = self._safe_float(candidate)
            if price > 0:
                return price

        return 0.0

    @staticmethod
    def _parse_compact_number(value: str | None) -> int:
        if not value:
            return 0

        text = value.strip().lower().replace(",", "")
        match = re.search(r"(\d+(?:\.\d+)?)\s*([km])?", text)
        if not match:
            return 0

        base = float(match.group(1))
        unit = match.group(2)
        if unit == "k":
            base *= 1000
        elif unit == "m":
            base *= 1_000_000

        return int(base)

    def _extract_text_by_selectors(self, item, selectors: list[str]) -> str:
        for selector in selectors:
            el = item.query_selector(selector)
            if not el:
                continue
            text = self._clean_text(el.text_content())
            if text:
                return text
            attr_label = self._clean_text(el.get_attribute("aria-label"))
            if attr_label:
                return attr_label
        return ""

    def _extract_rating_from_item(self, item, selectors: list[str]) -> float:
        patterns = [
            r"([0-5](?:[\.,]\d{1,2})?)\s*/\s*5",
            r"([0-5](?:[\.,]\d)?)\s*out\s*of\s*5",
            r"([0-5](?:[\.,]\d)?)\s*stars?",
            r"([0-5](?:[\.,]\d{1,2})?)\s*(?:★|⭐|\*)+",
            r"([0-5](?:[\.,]\d{1,2})?)\s*(?:★|⭐|\*)+\s*[\d,.]+\s*[kKmM]?\+?\s*ratings?",
            r"rated\s*([0-5](?:[\.,]\d)?)",
        ]
        rating_context_pattern = (
            r"(?:rating|score|review|stars?|star-rating|average rating)"
            r"\s*(?::|\||=|▌)?\s*"
            r"([0-5](?:[\.,]\d{1,2})?)"
        )

        def parse_rating_value(raw: str) -> float:
            normalized = raw.strip().replace(",", ".")
            return max(0.0, min(5.0, float(normalized)))

        def parse_rating(text: str) -> float:
            if not text:
                return 0.0

            compact_numeric = re.fullmatch(r"\s*([0-5](?:[\.,]\d{1,2})?)\s*", text)
            if compact_numeric:
                try:
                    return parse_rating_value(compact_numeric.group(1))
                except ValueError:
                    pass

            for pattern in patterns:
                match = re.search(pattern, text, flags=re.IGNORECASE)
                if not match:
                    continue
                try:
                    return parse_rating_value(match.group(1))
                except ValueError:
                    continue

            match = re.search(rating_context_pattern, text, flags=re.IGNORECASE)
            if match:
                try:
                    return parse_rating_value(match.group(1))
                except ValueError:
                    pass
            return 0.0

        # Evaluate each selector hit instead of only the first text match.
        for selector in selectors:
            try:
                nodes = item.query_selector_all(selector)
            except Exception:
                nodes = []
            for node in nodes:
                for candidate in (
                    self._clean_text(node.text_content()),
                    self._clean_text(node.get_attribute("aria-label")),
                    self._clean_text(node.get_attribute("title")),
                ):
                    score = parse_rating(candidate)
                    if score > 0:
                        return score

        text = self._clean_text(item.text_content())
        return parse_rating(text)

    def _extract_review_count_from_item(self, item, selectors: list[str]) -> int:
        text = self._extract_text_by_selectors(item, selectors) or self._clean_text(item.text_content())
        if not text:
            return 0

        patterns = [
            r"([\d,.]+\s*[km]?)\s*ratings?",
            r"([\d,.]+\s*[km]?)\s*reviews?",
            r"\(([\d,.]+\s*[km]?)\)",
            r"([\d,.]+\s*[km]?)\s*sold",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                parsed = self._parse_compact_number(match.group(1))
                if parsed > 0:
                    return parsed

        return 0

    def _load_shopee_cookies(self) -> list[dict[str, Any]]:
        # Refresh env values at call time so runtime updates are honored.
        env_cookie_json = os.getenv("SHOPEE_COOKIES_JSON", "").strip()
        env_cookie_path = os.getenv("SHOPEE_COOKIES_PATH", "").strip()
        if env_cookie_json:
            self.shopee_cookie_json = env_cookie_json
        if env_cookie_path:
            self.shopee_cookie_path = env_cookie_path

        if not self.shopee_cookie_json and not self.shopee_cookie_path:
            self.shopee_cookie_path = self._discover_shopee_cookie_path()

        raw = ""
        source_path = ""
        if self.shopee_cookie_json:
            raw = self.shopee_cookie_json
        elif self.shopee_cookie_path:
            source_path = self.shopee_cookie_path
            try:
                with open(self.shopee_cookie_path, "r", encoding="utf-8") as fp:
                    raw = fp.read()
            except OSError:
                return []

        if not raw:
            return []

        parsed: Any = None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None

        # Support Netscape cookie export files (common browser extension format).
        if parsed is None and source_path:
            try:
                jar = http.cookiejar.MozillaCookieJar(source_path)
                jar.load(ignore_discard=True, ignore_expires=True)
                parsed = [
                    {
                        "name": c.name,
                        "value": c.value,
                        "domain": c.domain,
                        "path": c.path or "/",
                        "secure": bool(c.secure),
                        "httpOnly": False,
                    }
                    for c in jar
                ]
            except Exception:
                parsed = self._parse_netscape_cookie_text(raw)

        if parsed is None:
            return []

        if not isinstance(parsed, list):
            return []

        cookies: list[dict[str, Any]] = []
        for entry in parsed:
            if not isinstance(entry, dict):
                continue
            if not entry.get("name") or not entry.get("value"):
                continue

            cookie = {
                "name": entry.get("name"),
                "value": entry.get("value"),
                "path": entry.get("path", "/"),
                "secure": bool(entry.get("secure", True)),
                "httpOnly": bool(entry.get("httpOnly", False)),
            }

            if entry.get("domain"):
                cookie["domain"] = entry.get("domain")
            elif entry.get("url"):
                cookie["url"] = entry.get("url")
            else:
                cookie["url"] = "https://shopee.ph"

            if entry.get("sameSite") in {"Strict", "Lax", "None"}:
                cookie["sameSite"] = entry.get("sameSite")

            cookies.append(cookie)

        return cookies

    @staticmethod
    def _parse_netscape_cookie_text(raw: str) -> list[dict[str, Any]]:
        """Parse Netscape cookie format in a tolerant way for non-strict exports."""
        parsed: list[dict[str, Any]] = []
        for line in raw.splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue

            # Netscape format: domain, include_subdomains, path, secure, expires, name, value
            parts = text.split("\t")
            if len(parts) < 7:
                # Some exporters may use multiple spaces instead of tabs.
                parts = text.split()
            if len(parts) < 7:
                continue

            domain = parts[0].strip()
            path = parts[2].strip() or "/"
            secure_flag = parts[3].strip().upper() == "TRUE"
            expires_raw = parts[4].strip()
            name = parts[5].strip()
            value = "\t".join(parts[6:]).strip()

            if not name:
                continue

            cookie: dict[str, Any] = {
                "name": name,
                "value": value,
                "path": path,
                "secure": secure_flag,
                "httpOnly": False,
            }

            if domain:
                cookie["domain"] = domain
            else:
                cookie["url"] = "https://shopee.ph"

            if expires_raw.isdigit():
                expires_int = int(expires_raw)
                if expires_int > 0:
                    cookie["expires"] = expires_int

            parsed.append(cookie)

        return parsed

    @staticmethod
    def _wait_for_any_selector(page, selectors: list[str], timeout_ms: int) -> tuple[bool, str | None]:
        per_selector_timeout = max(2000, int(timeout_ms / max(1, len(selectors))))
        for selector in selectors:
            try:
                page.wait_for_selector(selector, timeout=per_selector_timeout)
                return True, selector
            except Exception:
                continue
        return False, None

    def _wait_for_challenge_resolution(
        self,
        page,
        platform: str,
        debug_trace: list[dict[str, Any]] | None,
    ) -> bool:
        wait_seconds = max(0, int(self.challenge_wait_seconds or 0))
        # If headed mode is on, ALWAYS wait even if challenge_wait_seconds is 0 (default to 120s)
        if not self.headed:
            return False
        if wait_seconds <= 0:
            wait_seconds = 120  # Default to 120 seconds in headed mode for manual solving

        # For CAPTCHA pages specifically, use a longer default wait if not set
        is_captcha = self._is_captcha_page(page)
        if is_captcha and wait_seconds < 180:
            wait_seconds = 180  # At least 3 minutes for CAPTCHA verification

        stage_name = f"playwright_{platform.lower()}"
        challenge_type = "captcha" if is_captcha else "challenge"
        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": stage_name,
                    "status": f"{challenge_type}_wait_started",
                    "duration_ms": 0.0,
                    "details": {
                        "wait_seconds": wait_seconds,
                        "url": page.url,
                        "browser": self.browser,
                        "headed": self.headed,
                        "type": challenge_type,
                    },
                }
            )

        deadline = time.time() + wait_seconds
        last_url = page.url
        while time.time() < deadline:
            page.wait_for_timeout(1000)
            current_url = page.url
            
            if platform == "Lazada":
                still_blocked = self._is_lazada_blocked_page(page)
            else:
                still_blocked = self._is_shopee_blocked_page(page)

            # For CAPTCHA, also detect URL change (indicates solve/redirect)
            if not still_blocked or (is_captcha and current_url != last_url):
                # Wait a bit more for page to fully load
                page.wait_for_timeout(2000)
                if debug_trace is not None:
                    debug_trace.append(
                        {
                            "stage": stage_name,
                            "status": f"{challenge_type}_cleared",
                            "duration_ms": 0.0,
                            "details": {"url": page.url},
                        }
                    )
                return True
            last_url = current_url

        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": stage_name,
                    "status": f"{challenge_type}_wait_expired",
                    "duration_ms": 0.0,
                    "details": {"wait_seconds": wait_seconds, "url": page.url},
                }
            )
        return False

    def _wait_for_selector_with_manual_window(
        self,
        page,
        selectors: list[str],
        timeout_ms: int,
        platform: str,
        debug_trace: list[dict[str, Any]] | None,
    ) -> tuple[bool, str | None]:
        found, matched_selector = self._wait_for_any_selector(page, selectors, timeout_ms=timeout_ms)
        if found:
            return True, matched_selector

        wait_seconds = max(0, int(self.challenge_wait_seconds or 0))
        if not self.headed or wait_seconds <= 0:
            return False, None

        stage_name = f"playwright_{platform.lower()}"
        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": stage_name,
                    "status": "selector_wait_started",
                    "duration_ms": 0.0,
                    "details": {"wait_seconds": wait_seconds, "selectors": selectors},
                }
            )

        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            page.wait_for_timeout(1000)
            found, matched_selector = self._wait_for_any_selector(page, selectors, timeout_ms=1500)
            if found:
                if debug_trace is not None:
                    debug_trace.append(
                        {
                            "stage": stage_name,
                            "status": "selector_found_after_wait",
                            "duration_ms": 0.0,
                            "details": {"selector": matched_selector},
                        }
                    )
                return True, matched_selector

        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": stage_name,
                    "status": "selector_wait_expired",
                    "duration_ms": 0.0,
                    "details": {"wait_seconds": wait_seconds, "selectors": selectors},
                }
            )
        return False, None

    @staticmethod
    def _is_lazada_blocked_page(page) -> bool:
        current_url = (page.url or "").lower()
        blocked_url_markers = ("captcha", "challenge", "security", "login", "member.lazada")
        if any(marker in current_url for marker in blocked_url_markers):
            return True

        try:
            body_text = (page.text_content("body") or "").lower()
        except Exception:
            return False

        blocked_text_markers = (
            "verify you are human",
            "slide to complete the puzzle",
            "security check",
            "access denied",
            "bot manager",
            "complete the challenge",
        )
        return any(marker in body_text for marker in blocked_text_markers)

    @staticmethod
    def _is_shopee_blocked_page(page) -> bool:
        current_url = (page.url or "").lower()
        blocked_url_markers = ("captcha", "challenge", "verify", "login", "account")
        if any(marker in current_url for marker in blocked_url_markers):
            return True

        try:
            body_text = (page.text_content("body") or "").lower()
        except Exception:
            return False

        blocked_text_markers = (
            "verify you are human",
            "complete the challenge",
            "security check",
            "access denied",
            "too many requests",
            "unusual traffic",
            "robot",
            "captcha",
            "loading issue",
            "facing some loading",
        )
        return any(marker in body_text for marker in blocked_text_markers)

    @staticmethod
    def _is_captcha_page(page) -> bool:
        """Specifically detect if current page is a CAPTCHA/verification page."""
        current_url = (page.url or "").lower()
        return "/verify/captcha" in current_url or "/verify/" in current_url

    @staticmethod
    def _is_google_consent_page(page) -> bool:
        current_url = (page.url or "").lower()
        if "consent.google" in current_url:
            return True
        try:
            body_text = (page.text_content("body") or "").lower()
        except Exception:
            return False
        markers = (
            "before you continue to google",
            "consent",
            "accept all",
            "i agree",
        )
        return any(marker in body_text for marker in markers)

    def _handle_google_consent(self, context, page, debug_trace: list[dict[str, Any]] | None) -> bool:
        """Try to bypass Google consent/interstitial pages automatically."""
        # Add a permissive consent cookie first; this resolves many interstitials.
        try:
            context.add_cookies(
                [
                    {
                        "name": "CONSENT",
                        "value": "YES+cb.20210328-17-p0.en+FX+667",
                        "domain": ".google.com",
                        "path": "/",
                        "secure": True,
                        "httpOnly": False,
                    }
                ]
            )
        except Exception:
            pass

        if not self._is_google_consent_page(page):
            return True

        button_selectors = [
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
            "form button[type='submit']",
            "input[type='submit'][value*='Agree']",
            "input[type='submit'][value*='Accept']",
        ]
        for selector in button_selectors:
            try:
                button = page.locator(selector).first
                if button and button.count() > 0:
                    button.click(timeout=2000)
                    page.wait_for_timeout(1200)
                    if debug_trace is not None:
                        debug_trace.append(
                            {
                                "stage": "google_consent",
                                "status": "accepted",
                                "duration_ms": 0.0,
                                "details": {"selector": selector},
                            }
                        )
                    return True
            except Exception:
                continue

        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": "google_consent",
                    "status": "not_resolved",
                    "duration_ms": 0.0,
                    "details": {"url": page.url},
                }
            )
        return not self._is_google_consent_page(page)

    def _scrape_shopee_api_fallback(self, query: str) -> list[dict[str, Any]]:
        """Fallback to Shopee public search API when storefront DOM selectors fail."""
        api_url = "https://shopee.ph/api/v4/search/search_items"
        param_variants = [
            {
                "by": "relevancy",
                "keyword": query,
                "limit": self.max_results,
                "newest": 0,
                "order": "desc",
                "page_type": "search",
                "scenario": "PAGE_GLOBAL_SEARCH",
                "version": 2,
            },
            {
                "by": "relevancy",
                "keyword": query,
                "limit": self.max_results,
                "newest": 0,
                "order": "desc",
                "page_type": "search",
                "scenario": "PAGE_OTHERS",
                "version": 2,
            },
        ]

        items: list[Any] = []
        session = requests.Session()
        # Reuse exported Shopee cookies for API fallback calls.
        for cookie in self._load_shopee_cookies():
            name = self._clean_text(cookie.get("name"))
            value = self._clean_text(cookie.get("value"))
            if not name or not value:
                continue
            domain = self._clean_text(cookie.get("domain")) or "shopee.ph"
            session.cookies.set(name, value, domain=domain, path=self._clean_text(cookie.get("path")) or "/")

        for params in param_variants:
            headers = {
                **self.headers,
                "Referer": f"https://shopee.ph/search?keyword={quote_plus(query)}",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://shopee.ph",
            }
            try:
                response = session.get(api_url, params=params, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError):
                continue

            if not isinstance(payload, dict):
                continue

            candidate_items = payload.get("items")
            if not isinstance(candidate_items, list):
                data = payload.get("data")
                if isinstance(data, dict):
                    candidate_items = data.get("items")
                    if not isinstance(candidate_items, list):
                        sections = data.get("sections")
                        if isinstance(sections, list):
                            flattened_items: list[Any] = []
                            for section in sections:
                                if not isinstance(section, dict):
                                    continue
                                section_items = section.get("data") or section.get("items")
                                if isinstance(section_items, list):
                                    flattened_items.extend(section_items)
                            candidate_items = flattened_items
            if isinstance(candidate_items, list) and candidate_items:
                items = candidate_items
                break

        if not items:
            return []

        products: list[dict[str, Any]] = []
        for row in items[: self.max_results]:
            if not isinstance(row, dict):
                continue
            item_basic = row.get("item_basic") or row.get("item") or row.get("item_data") or {}
            if not item_basic and isinstance(row.get("data"), dict):
                item_basic = row.get("data")
            if not isinstance(item_basic, dict):
                continue

            name = self._clean_text(item_basic.get("name"))
            if not name:
                continue

            shopid = item_basic.get("shopid")
            itemid = item_basic.get("itemid")
            raw_price = item_basic.get("price_min") or item_basic.get("price") or 0
            price_value = 0.0
            if isinstance(raw_price, (int, float)):
                # Shopee API prices are commonly in 1e5 minor units.
                price_value = float(raw_price) / 100000.0
            else:
                price_value = self._safe_float(str(raw_price))

            url = ""
            if shopid is not None and itemid is not None:
                url = f"https://shopee.ph/product/{shopid}/{itemid}"

            rating_value = self._safe_float(str(item_basic.get("rating_star") or item_basic.get("item_rating") or 0))
            rating_value = max(0.0, min(5.0, rating_value)) if rating_value > 0 else 0.0

            review_count_value = self._safe_int(str(item_basic.get("total_rating_count") or item_basic.get("cmt_count") or 0))
            if review_count_value <= 0:
                review_count_value = int(item_basic.get("historical_sold", 0) or 0)

            products.append(
                {
                    "name": name,
                    "price": price_value,
                    "category": "General",
                    "rating": rating_value,
                    "review_count": review_count_value,
                    "platform": "Shopee",
                    "url": url,
                }
            )

        return products

    def _extract_products_with_playwright(
        self,
        url: str,
        wait_selectors: list[str],
        platform: str,
        timeout_seconds: int | None = None,
        use_shopee_cookies: bool = False,
        debug_trace: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        # Enforce minimum interval between requests to avoid detection
        self._enforce_request_interval(min_interval=2.0)
        try:
            with sync_playwright() as playwright:
                browser = self._launch_browser(
                    playwright,
                    args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
                )
                context = browser.new_context(
                    user_agent=self.headers["User-Agent"],
                    locale="en-US",
                    viewport={"width": 1920, "height": 1080},
                )
                # Add comprehensive anti-detection scripts
                anti_detection_script = """
                    // Remove webdriver property
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    
                    // Spoof plugins array
                    const plugin = {
                        name: 'Chrome PDF Plugin',
                        description: 'Portable Document Format',
                        filename: 'internal-pdf-viewer'
                    };
                    const plugins = {
                        0: plugin,
                        length: 1,
                        item: () => plugin,
                        namedItem: () => plugin,
                        refresh: () => {}
                    };
                    Object.defineProperty(navigator, 'plugins', {get: () => plugins});
                    
                    // Spoof languages
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    
                    // Spoof chrome property
                    window.chrome = {
                        runtime: {
                            connect: () => null,
                            onConnect: null,
                            sendMessage: () => null
                        }
                    };
                    
                    // Remove headless indicator
                    window.chrome.runtime = undefined;
                    
                    // Spoof document properties
                    Object.defineProperty(document, 'hidden', {value: false});
                    Object.defineProperty(document, 'visibilityState', {value: 'visible'});
                """
                context.add_init_script(anti_detection_script)
                page = context.new_page()
                stealth_sync(page)
                effective_timeout = max(3, timeout_seconds or self.timeout)

                if use_shopee_cookies:
                    cookies = self._load_shopee_cookies()
                    if cookies:
                        try:
                            context.add_cookies(cookies)
                            if debug_trace is not None:
                                debug_trace.append(
                                    {
                                        "stage": "shopee_cookie_session",
                                        "status": "cookies_loaded",
                                        "duration_ms": 0.0,
                                        "details": {"count": len(cookies)},
                                    }
                                )
                        except Exception as exc:
                            if debug_trace is not None:
                                debug_trace.append(
                                    {
                                        "stage": "shopee_cookie_session",
                                        "status": "cookie_load_failed",
                                        "duration_ms": 0.0,
                                        "details": {"error": str(exc)},
                                    }
                                )
                    elif debug_trace is not None:
                        debug_trace.append(
                            {
                                "stage": "shopee_cookie_session",
                                "status": "no_cookies_configured",
                                "duration_ms": 0.0,
                                "details": {"path": self.shopee_cookie_path or ""},
                            }
                        )

                # Keep routing disabled here; abort-route callbacks can raise cancellation
                # noise during page/context teardown on some Playwright/Python versions.

                if platform == "Lazada":
                    lazada_ajax_meta: dict[str, dict[str, Any]] = {}
                    self._lazada_ajax_meta_by_url = {}

                    def _iter_lazada_candidate_rows(node: Any):
                        if isinstance(node, dict):
                            key_set = {
                                str(key).replace("_", "").replace("-", "").lower()
                                for key in node.keys()
                            }
                            if key_set & {
                                "itemid",
                                "nid",
                                "id",
                                "producturl",
                                "itemurl",
                                "producturl",
                                "url",
                                "ratingscore",
                                "ratingaverage",
                                "scoreaverage",
                                "itemratingscore",
                                "rating",
                                "reviewcount",
                                "totalreview",
                                "ratingcount",
                            }:
                                yield node
                            for value in node.values():
                                yield from _iter_lazada_candidate_rows(value)
                        elif isinstance(node, list):
                            for value in node:
                                yield from _iter_lazada_candidate_rows(value)

                    def _find_lazada_payload_value(node: Any, keys: tuple[str, ...]) -> Any:
                        if isinstance(node, dict):
                            for key, value in node.items():
                                normalized_key = str(key).replace("_", "").replace("-", "").lower()
                                if normalized_key in keys and value not in (None, ""):
                                    return value
                            for value in node.values():
                                found = _find_lazada_payload_value(value, keys)
                                if found not in (None, ""):
                                    return found
                        elif isinstance(node, list):
                            for value in node:
                                found = _find_lazada_payload_value(value, keys)
                                if found not in (None, ""):
                                    return found
                        return None

                    def _coerce_lazada_ajax_float(value: Any) -> float:
                        if isinstance(value, dict):
                            for key in (
                                "ratingScore",
                                "ratingAverage",
                                "scoreAverage",
                                "itemRatingScore",
                                "rating",
                                "value",
                                "average",
                                "score",
                                "text",
                            ):
                                if key in value:
                                    return _coerce_lazada_ajax_float(value.get(key))
                            return 0.0
                        return self._safe_float(value)

                    def _coerce_lazada_ajax_int(value: Any) -> int:
                        if isinstance(value, dict):
                            for key in ("reviewCount", "totalReview", "ratingCount", "count", "value", "text"):
                                if key in value:
                                    return self._parse_compact_number(str(value.get(key) or ""))
                            return 0
                        return self._parse_compact_number(str(value or ""))

                    def _capture_lazada_ajax_response(response) -> None:
                        try:
                            response_url = (response.url or "").lower()
                        except Exception:
                            response_url = ""
                        if "lazada.com.ph" not in response_url:
                            return

                        try:
                            payload = response.json()
                        except Exception:
                            return

                        def _store_lazada_meta(target: dict[str, dict[str, Any]], key: str, meta: dict[str, Any]) -> None:
                            if not key:
                                return

                            current = target.get(key)
                            if not current:
                                target[key] = meta
                                return

                            target[key] = {
                                "rating": max(
                                    self._safe_float(current.get("rating")),
                                    self._safe_float(meta.get("rating")),
                                ),
                                "review_count": max(
                                    self._safe_int(str(current.get("review_count") or 0)),
                                    self._safe_int(str(meta.get("review_count") or 0)),
                                ),
                            }

                        candidate_rows = list(_iter_lazada_candidate_rows(payload)) if isinstance(payload, (dict, list)) else []
                        if not candidate_rows:
                            return

                        for row in candidate_rows:
                            if not isinstance(row, dict):
                                continue

                            item_id = self._clean_text(str(row.get("itemId") or row.get("nid") or row.get("item_id") or row.get("id") or ""))
                            if not item_id:
                                product_url = self._clean_text(str(row.get("productUrl") or row.get("itemUrl") or row.get("product_url") or row.get("url") or ""))
                                item_id_match = re.search(r"-i(\d+)", product_url)
                                if item_id_match:
                                    item_id = item_id_match.group(1)
                            product_url = self._clean_text(str(row.get("productUrl") or row.get("itemUrl") or row.get("product_url") or row.get("url") or ""))

                            rating_source = _find_lazada_payload_value(
                                row,
                                (
                                    "ratingscore",
                                    "ratingaverage",
                                    "scoreaverage",
                                    "itemratingscore",
                                    "rating",
                                    "itemrating",
                                    "aggregaterating",
                                    "ratinginfo",
                                    "ratingdata",
                                    "averagescore",
                                    "ratingstar",
                                ),
                            )
                            review_source = _find_lazada_payload_value(
                                row,
                                (
                                    "review",
                                    "reviewcount",
                                    "totalreview",
                                    "ratingcount",
                                    "reviewinfo",
                                    "ratinginfo",
                                    "ratingscount",
                                    "totalrating",
                                ),
                            )

                            rating_value = _coerce_lazada_ajax_float(rating_source)
                            review_count_value = _coerce_lazada_ajax_int(review_source)

                            meta = {
                                "rating": max(0.0, min(5.0, rating_value)) if rating_value > 0 else 0.0,
                                "review_count": review_count_value if review_count_value > 0 else 0,
                            }

                            if item_id:
                                _store_lazada_meta(lazada_ajax_meta, item_id, meta)
                            if product_url:
                                _store_lazada_meta(self._lazada_ajax_meta_by_url, product_url.lower(), meta)

                    page.on("response", _capture_lazada_ajax_response)

                    # Prime a real Lazada session before opening search results.
                    try:
                        page.goto(
                            "https://www.lazada.com.ph/",
                            wait_until="domcontentloaded",
                            timeout=effective_timeout * 1000,
                        )
                        page.wait_for_timeout(random.randint(700, 1400))
                    except Exception:
                        pass

                # Add human-like delay before navigating to target
                if platform in {"Shopee", "ShopeeMobile"}:
                    # Extra delay for Shopee to appear more human
                    page.wait_for_timeout(random.randint(1000, 2500))
                    # Simulate mouse movement
                    self._human_like_mouse_move(page)

                wait_state = "networkidle" if platform == "Lazada" else "domcontentloaded"
                page.goto(url, wait_until=wait_state, timeout=effective_timeout * 1000)

                if platform == "Google Shopping":
                    self._handle_google_consent(context, page, debug_trace)

                if platform == "Lazada" and self._is_lazada_blocked_page(page):
                    if not self._wait_for_challenge_resolution(page, platform, debug_trace):
                        if debug_trace is not None:
                            debug_trace.append(
                                {
                                    "stage": "playwright_lazada",
                                    "status": "blocked",
                                    "duration_ms": 0.0,
                                    "details": {"url": page.url},
                                }
                            )
                        return []

                if platform in {"Shopee", "ShopeeMobile"} and self._is_shopee_blocked_page(page):
                    if not self._wait_for_challenge_resolution(page, platform, debug_trace):
                        if debug_trace is not None:
                            debug_trace.append(
                                {
                                    "stage": f"playwright_{platform.lower()}",
                                    "status": "blocked",
                                    "duration_ms": 0.0,
                                    "details": {"url": page.url},
                                }
                            )
                        return []

                # Human-like delay after page load
                self._random_delay(1.0, 2.5)

                # Simulate human scroll behavior
                if self.enable_antibot_behavior:
                    self._human_like_scroll(page)
                    self._random_delay(0.5, 1.5)
                    self._human_like_mouse_move(page)
                    self._random_delay(0.5, 1.0)

                # Wait for any of the candidate selectors.
                if wait_selectors:
                    found, matched_selector = self._wait_for_selector_with_manual_window(
                        page,
                        wait_selectors,
                        timeout_ms=effective_timeout * 1000,
                        platform=platform,
                        debug_trace=debug_trace,
                    )
                    if not found and debug_trace is not None:
                        timeout_details: dict[str, Any] = {"selectors": wait_selectors}
                        if platform == "Google Shopping":
                            try:
                                timeout_details["url"] = page.url
                            except Exception:
                                pass
                            try:
                                timeout_details["title"] = self._clean_text(page.title())
                            except Exception:
                                pass
                            try:
                                body_text = self._clean_text(page.text_content("body"))
                                timeout_details["body_len"] = len(body_text)
                            except Exception:
                                pass
                        debug_trace.append(
                            {
                                "stage": f"playwright_{platform.lower()}",
                                "status": "selector_timeout",
                                "duration_ms": 0.0,
                                "details": timeout_details,
                            }
                        )
                    elif matched_selector and debug_trace is not None:
                        debug_trace.append(
                            {
                                "stage": f"playwright_{platform.lower()}",
                                "status": "selector_matched",
                                "duration_ms": 0.0,
                                "details": {"selector": matched_selector},
                            }
                        )

                # Gradual wait instead of instant fixed delay (more human-like)
                if self.enable_antibot_behavior:
                    page.wait_for_timeout(random.randint(800, 1500))
                else:
                    page.wait_for_timeout(1200)

                if platform == "Shopee":
                    return self._extract_shopee_from_page(page)
                if platform == "ShopeeMobile":
                    return self._extract_shopee_mobile_from_page(page)
                if platform == "Lazada":
                    return self._extract_lazada_from_page(page, lazada_ajax_meta=lazada_ajax_meta)
                if platform == "Amazon":
                    return self._extract_amazon_from_page(page)
                if platform == "Google Shopping":
                    return self._extract_google_shopping_from_page(page)
                return []
        except Exception as exc:
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": f"playwright_{platform.lower()}",
                        "status": "exception",
                        "duration_ms": 0.0,
                        "details": {"error": str(exc)},
                    }
                )
            return []

    def _extract_shopee_from_page(self, page) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        items = page.query_selector_all("div.shopee-search-item-result__item")
        if not items:
            items = page.query_selector_all("li.shopee-search-item-result__item")
        if not items:
            items = page.query_selector_all("div[data-sqe='item']")
        if not items:
            link_fallback = self._extract_shopee_mobile_from_page(page)
            if link_fallback:
                return link_fallback[: self.max_results]

        detail_fetch_count = 0
        detail_fetch_limit = min(6, self.max_results)

        for item in items[: self.max_results]:
            try:
                name_el = item.query_selector("div[data-sqe='name']")
                if not name_el:
                    name_el = item.query_selector("a[data-sqe='link']")
                url_el = item.query_selector("a")
                name = self._clean_text(name_el.text_content() if name_el else None)
                if not name:
                    continue

                href = url_el.get_attribute("href") if url_el else None
                product_url = self._normalize_url("https://shopee.ph", href)
                price = self._extract_price_from_item(
                    item,
                    selectors=[
                        "span[class*='price']",
                        "div[class*='price']",
                        "[data-price]",
                    ],
                )
                rating = self._extract_rating_from_item(
                    item,
                    selectors=[
                        "[class*='rating']",
                        "[aria-label*='rating']",
                    ],
                )
                review_count = self._extract_review_count_from_item(
                    item,
                    selectors=[
                        "[class*='rating']",
                        "[class*='sold']",
                        "[aria-label*='rating']",
                    ],
                )
                detail_meta: dict[str, Any] = {}
                needs_detail = rating <= 0
                if needs_detail and detail_fetch_count < detail_fetch_limit:
                    detail_meta = self._fetch_shopee_product_detail_metadata(product_url)
                    detail_fetch_count += 1
                if detail_meta.get("rating", 0.0) > 0:
                    rating = float(detail_meta["rating"])
                if detail_meta.get("review_count", 0) > 0:
                    review_count = int(detail_meta["review_count"])

                products.append(
                    {
                        "name": name,
                        "price": price,
                        "category": "General",
                        "rating": rating,
                        "review_count": review_count,
                        "seller_name": "",
                        "seller_rating": 0.0,
                        "platform": "Shopee",
                        "url": product_url,
                    }
                )
                if self.enable_antibot_behavior:
                    time.sleep(random.uniform(0.05, 0.15))
            except Exception:
                continue

        return products

    def _extract_shopee_mobile_from_page(self, page) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        links = page.query_selector_all("a[href*='/product/']")
        if not links:
            links = page.query_selector_all("a[href*='i.']")

        seen_urls: set[str] = set()
        detail_fetch_count = 0
        detail_fetch_limit = min(6, self.max_results)

        for link in links[: self.max_results * 3]:
            try:
                href = link.get_attribute("href")
                url = self._normalize_url("https://shopee.ph", href)
                if not url or url in seen_urls:
                    continue

                raw_text = self._clean_text(link.text_content())
                name = self._clean_shopee_mobile_name(raw_text)
                if not name or len(name) < 4:
                    name_el = link.query_selector("[class*='name'], [class*='title']")
                    name = self._clean_shopee_mobile_name(self._clean_text(name_el.text_content() if name_el else None))
                if not name:
                    continue

                price = self._extract_price_from_item(
                    link,
                    selectors=[
                        "[data-price]",
                        "[class*='price']",
                    ],
                )
                if price <= 0:
                    price = self._extract_price_from_text(raw_text)
                rating = self._extract_rating_from_item(
                    link,
                    selectors=[
                        "[class*='rating']",
                        "[aria-label*='rating']",
                    ],
                )
                review_count = self._extract_review_count_from_item(
                    link,
                    selectors=[
                        "[class*='rating']",
                        "[class*='sold']",
                    ],
                )
                detail_meta: dict[str, Any] = {}
                needs_detail = rating <= 0
                if needs_detail and detail_fetch_count < detail_fetch_limit:
                    detail_meta = self._fetch_shopee_product_detail_metadata(url)
                    detail_fetch_count += 1
                if detail_meta.get("rating", 0.0) > 0:
                    rating = float(detail_meta["rating"])
                if detail_meta.get("review_count", 0) > 0:
                    review_count = int(detail_meta["review_count"])

                seen_urls.add(url)
                products.append(
                    {
                        "name": name,
                        "price": price,
                        "category": "General",
                        "rating": rating,
                        "review_count": review_count,
                        "seller_name": "",
                        "seller_rating": 0.0,
                        "platform": "Shopee",
                        "url": url,
                    }
                )
                if len(products) >= self.max_results:
                    break
            except Exception:
                continue

        return products

    def _scrape_shopee_mobile(
        self,
        query: str,
        use_cookies: bool = True,
        mobile_base: str = "https://shopee.ph",
        debug_trace: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        mobile_url = f"{mobile_base}/search?keyword={quote_plus(query)}&smtt=0.0.9"
        return self._extract_products_with_playwright(
            url=mobile_url,
            wait_selectors=[
                "a[href*='/product/']",
                "a[href*='i.']",
                "a[href*='-i.']",
            ],
            platform="ShopeeMobile",
            timeout_seconds=8,
            use_shopee_cookies=use_cookies,
            debug_trace=debug_trace,
        )

    def _scrape_shopee_index_fallback(self, query: str) -> list[dict[str, Any]]:
        """
        Fallback path when Shopee pages are intermittently blocked.
        Uses public search index results to recover Shopee product links/titles.
        """
        products: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        search_pages = [
            f"https://duckduckgo.com/html/?q={quote_plus(f'site:shopee.ph {query}') }",
            f"https://www.bing.com/search?q={quote_plus(f'site:shopee.ph {query}')}",
        ]

        for url in search_pages:
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                html = response.text
            except requests.RequestException:
                continue

            for match in re.finditer(
                r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                html,
                flags=re.IGNORECASE | re.DOTALL,
            ):
                raw_href = unescape(match.group(1))
                href = self._decode_search_redirect_url(raw_href)
                if not href:
                    continue

                normalized_url = self._normalize_url("https://duckduckgo.com", href)
                if not self._is_shopee_product_url(normalized_url):
                    continue
                if normalized_url in seen_urls:
                    continue

                raw_title = re.sub(r"<[^>]+>", " ", unescape(match.group(2)))
                title = self._clean_text(re.sub(r"\s+", " ", raw_title))
                if not title or len(title) < 4:
                    continue
                blocked_title_markers = ("cookie", "privacy", "sign in", "captcha", "challenge")
                if any(marker in title.lower() for marker in blocked_title_markers):
                    continue

                seen_urls.add(normalized_url)
                products.append(
                    {
                        "name": title,
                        "price": self._extract_price_from_text(title),
                        "category": "General",
                        "rating": 0.0,
                        "review_count": 0,
                        "seller_name": "",
                        "seller_rating": 0.0,
                        "platform": "Shopee",
                        "url": normalized_url,
                    }
                )

                if len(products) >= self.max_results:
                    return products

        return products

    def _extract_google_shopping_from_page(self, page) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        def is_valid_google_shopping_candidate(name: str, url: str) -> bool:
            lowered_name = (name or "").strip().lower()
            lowered_url = (url or "").strip().lower()
            if not lowered_name or len(lowered_name) < 4:
                return False
            blocked_name_markers = (
                "not selected",
                "filter",
                "did you mean",
                "sponsored products",
            )
            if any(marker in lowered_name for marker in blocked_name_markers):
                return False

            blocked_url_markers = (
                "google.com/shopping/ratings",
                "google.com/shopping/merchant",
                "google.com/preferences",
                "google.com/sorry",
                "/setprefs",
                "/policies",
                "/support",
            )
            if any(marker in lowered_url for marker in blocked_url_markers):
                return False
            return True

        def clean_google_product_name(raw_name: str, card_text: str, url: str) -> str:
            name = self._clean_text(re.sub(r"\s+", " ", raw_name or ""))

            def looks_like_code_text(text: str) -> bool:
                lowered = (text or "").lower()
                if not lowered:
                    return False
                code_markers = (
                    "function(",
                    "this||self",
                    "queryselector",
                    "document.getelementbyid",
                    "var ",
                    "=>",
                    "{if(",
                    "scrollwidth",
                    "offsetheight",
                )
                return any(marker in lowered for marker in code_markers)

            def derive_name_from_url(value: str) -> str:
                parsed = urlparse(value or "")
                segments = [seg for seg in (parsed.path or "").split("/") if seg]
                for segment in reversed(segments):
                    lowered = segment.lower()
                    if lowered in {"products", "product", "collections", "shop", "p", "item"}:
                        continue
                    slug = segment.split("?")[0]
                    slug = re.sub(r"\.[a-z0-9]{1,5}$", "", slug, flags=re.IGNORECASE)
                    slug = re.sub(r"[-_]+", " ", slug)
                    slug = re.sub(r"[^a-zA-Z0-9 ]+", " ", slug)
                    slug = self._clean_text(re.sub(r"\s+", " ", slug))
                    if len(slug) >= 4:
                        return slug
                return ""

            def looks_like_price_label(text: str) -> bool:
                lowered = (text or "").strip().lower()
                if not lowered:
                    return False
                if re.fullmatch(r"(?:was|now|save|from|only)?\s*(?:₱|php|\$)?\s*[\d,.]+(?:\s*off)?", lowered, flags=re.IGNORECASE):
                    return True
                markers = (
                    "was ",
                    "now ",
                    "save ",
                    "discount",
                    "off",
                )
                has_money = bool(re.search(r"(?:₱|php|\$)\s*[\d,.]+", lowered, flags=re.IGNORECASE))
                return has_money and any(marker in lowered for marker in markers)

            # Remove merchant snippet pattern (e.g., "for ... from <merchant>").
            pattern = re.match(r"^for\s+(.*?)\s+from\s+.+$", name, flags=re.IGNORECASE)
            if pattern:
                candidate = self._clean_text(pattern.group(1))
                if candidate:
                    name = candidate
                else:
                    name = ""

            lowered_name = name.lower()
            if (
                looks_like_code_text(name)
                or lowered_name.startswith("for ")
                or " from " in lowered_name
                or lowered_name == "for from"
                or lowered_name.startswith("from ")
                or looks_like_price_label(name)
            ):
                name = ""

            # If name is weak, try extracting a stronger title line from card text.
            if len(name) < 4 or " from " in name.lower() or name.lower().startswith("for "):
                for line in (card_text or "").split("\n"):
                    candidate = self._clean_text(re.sub(r"\s+", " ", line))
                    if len(candidate) < 4:
                        continue
                    lowered = candidate.lower()
                    if any(marker in lowered for marker in [" from ", "sponsored", "free", "shipping", "not selected"]):
                        continue
                    if looks_like_code_text(candidate):
                        continue
                    if looks_like_price_label(candidate):
                        continue
                    if re.search(r"(?:₱|php|\$)\s*\d", lowered, flags=re.IGNORECASE):
                        continue
                    name = candidate
                    break

            # Last fallback: derive name from merchant URL slug.
            if len(name) < 4:
                name = derive_name_from_url(url)

            return name[:200]

        # Prefer product card containers first so price/title extraction uses the full card context.
        card_selectors = [
            "div.sh-dgr__grid-result",
            "div.sh-dlr__list-result",
            "div[data-sokoban-container]",
            "div[data-docid]",
            "div[data-cid]",
            "div:has(span.VbBaOe)",
            "div:has(span[class*='VbBaOe'])",
        ]
        cards: list[Any] = []
        for selector in card_selectors:
            try:
                cards = page.query_selector_all(selector)
            except Exception:
                cards = []
            if cards:
                break

        # Fallback to link-level extraction if card containers are unavailable.
        if not cards:
            try:
                cards = page.query_selector_all("a[href*='/shopping/product/']")
            except Exception:
                cards = []

        for card in cards[: self.max_results * 6]:
            try:
                link_el = (
                    card.query_selector("a[href*='/shopping/product/']")
                    or card.query_selector("a[href^='/url?q=http']")
                    or card.query_selector("a[href^='https://']")
                    or card.query_selector("a[href^='http://']")
                    or card.query_selector("a[href*='shopping/product']")
                    or card.query_selector("a[href*='google.com/shopping']")
                    or (card if hasattr(card, "get_attribute") else None)
                )
                href = link_el.get_attribute("href") if link_el else None
                if not href:
                    continue

                url = self._normalize_url("https://www.google.com", href)
                url = self._decode_search_redirect_url(url)
                if not url or url in seen_urls:
                    continue
                lowered_url = url.lower()
                if lowered_url.startswith("javascript:") or lowered_url.startswith("mailto:"):
                    continue
                if "google.com" in lowered_url and "/search" in lowered_url:
                    continue

                # Name extraction from common title containers used by Google Shopping cards.
                name = ""
                for selector in [
                    "a[aria-label]",
                    "h3",
                    "h4",
                    "div[role='heading']",
                    "div[class*='title']",
                    "span[class*='title']",
                ]:
                    node = card.query_selector(selector)
                    if not node:
                        continue
                    candidate = self._clean_text(node.get_attribute("aria-label") or node.text_content())
                    if candidate and len(candidate) >= 4:
                        name = candidate
                        break
                if not name:
                    name = self._clean_text(link_el.get_attribute("aria-label") if link_el else "")
                if not name:
                    name = self._clean_text(card.text_content())
                    if name:
                        name = name.split("\n")[0].strip()
                card_text_for_title = self._clean_text(card.text_content())
                name = clean_google_product_name(name, card_text_for_title, url)

                # Price extraction: screenshot-provided class `span.VbBaOe` is prioritized.
                price = self._extract_price_from_item(
                    card,
                    selectors=[
                        "span.VbBaOe",
                        "span[class*='VbBaOe']",
                        "[data-price]",
                        "span[aria-label*='₱']",
                        "span[aria-label*='PHP']",
                        "span[aria-label*='$']",
                        "span[class*='price']",
                        "div[class*='price']",
                    ],
                )
                if price <= 0:
                    card_text = self._clean_text(card.text_content())
                    price = self._extract_price_from_text(card_text)

                if not is_valid_google_shopping_candidate(name, url):
                    continue
                if price <= 0:
                    continue

                seen_urls.add(url)
                products.append(
                    {
                        "name": name,
                        "price": price,
                        "category": "General",
                        "rating": 0.0,
                        "review_count": 0,
                        "seller_name": "",
                        "seller_rating": 0.0,
                        "platform": "Google Shopping",
                        "url": url,
                    }
                )
                if len(products) >= self.max_results:
                    break
            except Exception:
                continue

        return products

    def _extract_lazada_from_page(self, page, lazada_ajax_meta: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        lazada_ajax_meta = lazada_ajax_meta or {}
        items = page.query_selector_all("div[data-qa-locator='product-item']")
        if not items:
            items = page.query_selector_all("div[data-item-id]")

        for item in items[: self.max_results]:
            try:
                name_el = item.query_selector("a[title]")
                if not name_el:
                    name_el = item.query_selector("a")
                name = self._clean_text(name_el.text_content() if name_el else None)
                if not name:
                    continue

                href = name_el.get_attribute("href") if name_el else None
                product_url = self._normalize_url("https://www.lazada.com.ph", href)
                product_url_key = self._clean_text(product_url).lower()
                item_id_match = re.search(r"-i(\d+)", product_url)
                item_id = item_id_match.group(1) if item_id_match else ""
                price = self._extract_price_from_item(
                    item,
                    selectors=[
                        "span.ooOxS",
                        "span[class*='price']",
                        "div[data-price]",
                        "[data-price]",
                    ],
                )
                rating = self._extract_rating_from_item(
                    item,
                    selectors=[
                        "span.score-average",  # PRIMARY: Actual product rating
                        "span.container-star-v2-score",  # PRIMARY: Alternative product rating selector
                        "i.ic-dynamic-badge + span",  # Star icon followed by rating number
                        "div[class*='rating'] span",  # Rating div with span containing number
                        "[class*='rating']:not([class*='seller']) span",
                        "[title*='rating']",
                        "[aria-label*='rating']",
                        "span[class*='score']:not([class*='seller'])",
                        "div[class*='star'] span",
                        ".rate-content span",  # Additional Lazada rating selector
                    ],
                )
                review_count = self._extract_review_count_from_item(
                    item,
                    selectors=[
                        "[class*='review']:not([class*='seller'])",
                        "[class*='rating']:not([class*='seller'])",
                        "[aria-label*='rating']",
                    ],
                )

                ajax_meta = lazada_ajax_meta.get(item_id, {}) if item_id else {}
                if not ajax_meta and product_url_key:
                    ajax_meta = getattr(self, "_lazada_ajax_meta_by_url", {}).get(product_url_key, {})
                ajax_rating = self._safe_float(str(ajax_meta.get("rating") or "")) if ajax_meta else 0.0
                rating = ajax_rating if ajax_rating > 0 else 0.0
                if review_count <= 0 and ajax_meta.get("review_count", 0) > 0:
                    review_count = int(ajax_meta["review_count"])
                # Extract seller info from list item (generic fallback)
                seller_name = self._extract_text_by_selectors(
                    item,
                    selectors=[
                        "[class*='seller-name']",
                        "[class*='seller']",
                        "a[href*='shop']",
                        "[data-spm*='seller']",
                    ],
                ) or ""
                seller_rating = self._extract_rating_from_item(
                    item,
                    selectors=[
                        "[class*='seller-score']",
                        "[class*='seller-rating']",
                        "[data-spm*='seller'] [class*='score']",
                    ],
                )

                products.append(
                    {
                        "name": name,
                        "price": price,
                        "category": "General",
                        "rating": rating,
                        "review_count": review_count,
                        "seller_name": seller_name,
                        "seller_rating": seller_rating,
                        "platform": "Lazada",
                        "url": product_url,
                    }
                )
                if self.enable_antibot_behavior:
                    time.sleep(random.uniform(0.05, 0.15))
            except Exception:
                continue

        return products

    def _extract_lazada_star_rating_from_item(self, item) -> float:
        """Infer Lazada list-card rating from rendered star icon classes when numeric text is absent."""
        try:
            star_nodes = item.query_selector_all("div.mdmmT i._9-ogB")
        except Exception:
            return 0.0

        if not star_nodes:
            return 0.0

        full_count = 0
        partial_count = 0
        for node in star_nodes:
            class_name = self._clean_text(node.get_attribute("class")).lower()
            if not class_name:
                continue
            if "dy1nx" in class_name:
                full_count += 1
            elif "half" in class_name or "partial" in class_name:
                partial_count += 1

        rating = float(full_count) + (0.5 * partial_count)
        return max(0.0, min(5.0, rating))

    def _fetch_lazada_product_detail_metadata(self, product_url: str) -> dict[str, Any]:
        """Fetch Lazada PDP metadata using Playwright with stealth for all extraction."""
        if not product_url:
            return {}
        
        # Use Playwright with stealth for all extraction
        return self._fetch_lazada_product_detail_metadata_playwright(product_url)

    def _extract_lazada_product_aggregate_rating_from_html(self, html: str) -> dict[str, Any]:
        """Extract product (not seller) rating metadata from JSON-LD blocks."""
        if not html:
            return {}

        scripts = re.findall(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        meta: dict[str, Any] = {}

        def iter_nodes(payload: Any):
            if isinstance(payload, list):
                for entry in payload:
                    yield from iter_nodes(entry)
                return
            if not isinstance(payload, dict):
                return
            yield payload
            graph = payload.get("@graph")
            if isinstance(graph, list):
                for entry in graph:
                    yield from iter_nodes(entry)

        for script_body in scripts:
            body = self._clean_text(unescape(script_body))
            if not body:
                continue
            try:
                payload = json.loads(body)
            except (TypeError, ValueError):
                continue

            for node in iter_nodes(payload):
                raw_type = node.get("@type")
                if isinstance(raw_type, list):
                    type_names = {str(entry).lower() for entry in raw_type}
                else:
                    type_names = {str(raw_type).lower()}
                if "product" not in type_names:
                    continue

                aggregate = node.get("aggregateRating")
                if not isinstance(aggregate, dict):
                    continue

                rating_value = self._safe_float(str(aggregate.get("ratingValue") or ""))
                if rating_value > 0:
                    meta["rating"] = max(0.0, min(5.0, rating_value))

                review_raw = (
                    aggregate.get("reviewCount")
                    or aggregate.get("ratingCount")
                    or aggregate.get("review_count")
                    or aggregate.get("ratingsTotal")
                )
                parsed_reviews = self._parse_compact_number(str(review_raw or ""))
                if parsed_reviews > 0:
                    meta["review_count"] = parsed_reviews

                if meta.get("rating", 0.0) > 0 and meta.get("review_count", 0) > 0:
                    return meta

        return meta

    def _fetch_lazada_product_detail_metadata_playwright(self, product_url: str) -> dict[str, Any]:
        if not product_url:
            return {}

        headless_attempts = [False, True]
        for headless_mode in headless_attempts:
            try:
                with sync_playwright() as playwright:
                    browser = self._launch_browser(
                        playwright,
                        args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
                    )
                    context = browser.new_context(
                        user_agent=self.headers["User-Agent"],
                        locale="en-US",
                        viewport={"width": 1920, "height": 1080},
                    )
                    context.add_init_script(
                        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                    )
                    page = context.new_page()
                    stealth_sync(page)

                    # Establish a Lazada session first to reduce redirects/challenges.
                    try:
                        page.goto(
                            "https://www.lazada.com.ph/",
                            wait_until="domcontentloaded",
                            timeout=max(8, self.timeout) * 1000,
                        )
                        page.wait_for_timeout(random.randint(800, 1500))
                    except Exception:
                        pass

                    page.goto(product_url, wait_until="domcontentloaded", timeout=max(8, self.timeout) * 1000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=4000)
                    except Exception:
                        pass
                    if self._is_lazada_blocked_page(page):
                        context.close()
                        browser.close()
                        continue

                    self._wait_for_any_selector(
                        page,
                        selectors=[
                            "#module_seller_info",
                            ".seller-name-v2__detail-name",
                            "span.score-average",
                            "span.container-star-v2-score",
                            "div.mod-rating",
                        ],
                        timeout_ms=7000,
                    )

                    rendered_html = page.content()
                    meta = self._extract_lazada_product_aggregate_rating_from_html(rendered_html)

                    # Lazada often embeds PDP rating/review in script state rather than visible DOM.
                    try:
                        script_blob = page.eval_on_selector_all(
                            "script",
                            "els => els.map(e => e.textContent || '').join('\\n')",
                        )
                    except Exception:
                        script_blob = ""

                    if script_blob and meta.get("rating", 0.0) <= 0:
                        for match in re.finditer(
                            r'"(averageScore|ratingAverage|rating_score|scoreAverage|avgRating|ratingStar)"\s*:\s*"?([0-5](?:\.\d{1,2})?)"?',
                            script_blob,
                            flags=re.IGNORECASE,
                        ):
                            key_start = max(0, match.start() - 32)
                            context_window = script_blob[key_start : match.start()].lower()
                            if "seller" in context_window:
                                continue
                            rating_value = self._safe_float(match.group(2))
                            if 0 < rating_value <= 5.0:
                                meta["rating"] = rating_value
                                break

                    if script_blob and meta.get("review_count", 0) <= 0:
                        review_match = re.search(
                            r'"(reviewCount|ratingCount|totalReviewers|total_reviews|review_count|ratingsTotal|totalRating)"\s*:\s*"?([\d,.]+\s*[kKmM]?\+?)"?',
                            script_blob,
                            flags=re.IGNORECASE,
                        )
                        if review_match:
                            parsed_reviews = self._parse_compact_number(review_match.group(2))
                            if parsed_reviews > 0:
                                meta["review_count"] = parsed_reviews

                    if meta.get("rating", 0.0) <= 0:
                        try:
                            xpath_rating = self._clean_text(
                                page.locator(
                                    "xpath=/html/body/div[6]/div/div[3]/div[2]/div/div/div[2]/div[3]/div/div/div/div/span[1]"
                                ).first.text_content(timeout=2000)
                            )
                        except Exception:
                            xpath_rating = ""
                        xpath_rating_value = self._safe_float(xpath_rating)
                        if xpath_rating_value > 0:
                            meta["rating"] = max(0.0, min(5.0, xpath_rating_value))

                    if meta.get("rating", 0.0) <= 0:
                        rating_text = self._extract_text_by_selectors(
                            page,
                            selectors=[
                                "span.score-average",
                                "span.container-star-v2-score",
                                "span[class*='rating-score']",
                                "span[class*='rating']",
                                "div.mod-rating span[class*='score']",
                                "div.pdp-review-summary span[class*='score']",
                                "[class*='pdp-review'] [class*='score']",
                                "span[class*='score']:not([class*='seller'])",
                            ],
                        )
                        rating_value = self._safe_float(rating_text)
                        if rating_value > 0:
                            meta["rating"] = max(0.0, min(5.0, rating_value))

                    if meta.get("rating", 0.0) <= 0:
                        rating_text = self._clean_text(page.text_content("body"))
                        rating_patterns = [
                            r"([0-5](?:\.\d{1,2})?)\s*/\s*5",
                            r"([0-5](?:\.\d{1,2})?)\s*out\s*of\s*5",
                            r"rating[s]?\s*[:|=]\s*([0-5](?:\.\d{1,2})?)",
                            r"score\s*[:|=]\s*([0-5](?:\.\d{1,2})?)",
                        ]
                        for pattern in rating_patterns:
                            rating_match = re.search(pattern, rating_text, flags=re.IGNORECASE)
                            if not rating_match:
                                continue
                            rating_value = self._safe_float(rating_match.group(1))
                            if 0 < rating_value <= 5.0:
                                meta["rating"] = rating_value
                                break

                    # Text-pattern fallback: search visible page text for rating number.
                    if meta.get("rating", 0.0) <= 0:
                        full_text = self._clean_text(page.text_content("body"))
                        rating_patterns = [
                            # Common Lazada compact header: "4.9 ★★★★★ 10K+ Ratings"
                            r"([0-5](?:\.\d{1,2})?)\s*(?:★|⭐|\*)+\s*[\d,.]+\s*[kKmM]?\+?\s*ratings?",
                            # Alternate compact form without visible stars.
                            r"([0-5](?:\.\d{1,2})?)\s*[\|•-]?\s*[\d,.]+\s*[kKmM]?\+?\s*ratings?",
                            r"(?:★+\s*)?([0-5](?:\.\d{1,2})?)\s*(?:star|rating|⭐)",
                        ]
                        for pattern in rating_patterns:
                            rating_pattern_match = re.search(pattern, full_text, flags=re.IGNORECASE)
                            if not rating_pattern_match:
                                continue
                            rating_value = self._safe_float(rating_pattern_match.group(1))
                            if 0 < rating_value <= 5.0:
                                meta["rating"] = rating_value
                                break

                        # Use context-aware pattern as last resort before giving up.
                        if meta.get("rating", 0.0) <= 0:
                            full_text = self._clean_text(page.text_content("body"))
                            rating_context_pattern = (
                                r"(?:rating|score|average\s+rating|product\s+rating)"
                                r"\s*(?::|\||=)?\s*"
                                r"([0-5](?:\.\d{1,2})?)"
                            )
                            context_match = re.search(rating_context_pattern, full_text, flags=re.IGNORECASE)
                            if context_match:
                                try:
                                    rating_value = max(0.0, min(5.0, float(context_match.group(1))))
                                    if rating_value > 0:
                                        meta["rating"] = rating_value
                                except ValueError:
                                    pass

                    if meta.get("review_count", 0) <= 0:
                        full_text = self._clean_text(page.text_content("body"))
                        review_count_match = re.search(
                            r"([\d,.]+\s*[kKmM]?\+?)\s*(?:Ratings?|Reviews?)",
                            full_text,
                            flags=re.IGNORECASE,
                        )
                        if review_count_match:
                            parsed_reviews = self._parse_compact_number(review_count_match.group(1))
                            if parsed_reviews > 0:
                                meta["review_count"] = parsed_reviews

                    # Extract seller information from rendered page.
                    try:
                        seller_name = self._extract_text_by_selectors(
                            page,
                            selectors=[
                                "#module_seller_info .seller-name-v2__detail-name",
                                ".seller-name-v2__detail-name",
                                "#module_seller_info a.pdp-link_size_l.pdp-link_theme_black.seller-name-v2__detail-name",
                                "[class*='seller-name']",
                                "[class*='seller'] a",
                                "a[href*='shop']",
                                "[data-spm*='seller'] span",
                            ],
                        ) or ""
                        if seller_name:
                            meta["seller_name"] = seller_name
                    except Exception:
                        pass

                    try:
                        seller_rating = self._extract_rating_from_item(
                            page,
                            selectors=[
                                "#module_seller_info .seller-name-v2__ratings span",
                                "#module_seller_info [class*='seller-rating']",
                                "[class*='seller-score']",
                                "[class*='seller-rating']",
                                "[data-spm*='seller'] [class*='score']",
                            ],
                        )
                        if seller_rating > 0:
                            meta["seller_rating"] = seller_rating
                    except Exception:
                        pass

                    context.close()
                    browser.close()
                    return meta
            except Exception:
                continue

        return {}

    def _fetch_shopee_product_detail_metadata(self, product_url: str) -> dict[str, Any]:
        """Fetch Shopee PDP metadata using Playwright with stealth for all extraction."""
        if not product_url:
            return {}
        
        # Use Playwright with stealth for all extraction
        return self._fetch_shopee_product_detail_metadata_playwright(product_url)

    def _fetch_shopee_product_detail_metadata_playwright(self, product_url: str) -> dict[str, Any]:
        if not product_url:
            return {}

        try:
            with sync_playwright() as playwright:
                browser = self._launch_browser(playwright)
                context = browser.new_context(user_agent=self.headers["User-Agent"], locale="en-US")
                page = context.new_page()
                stealth_sync(page)
                page.goto(product_url, wait_until="domcontentloaded", timeout=max(5, self.timeout) * 1000)
                page.wait_for_timeout(900)

                meta: dict[str, Any] = {}

                # 1) Parse embedded Shopee state JSON first (most stable source on PDP).
                script_blob = ""
                try:
                    script_texts = []
                    for script_el in page.query_selector_all("script"):
                        text = script_el.text_content()
                        if text:
                            script_texts.append(text)
                    script_blob = "\n".join(script_texts)
                except Exception:
                    script_blob = ""

                if script_blob:
                    rating_match = re.search(r'"rating_star"\s*:\s*([0-5](?:\.\d{1,4})?)', script_blob)
                    if rating_match:
                        rating_value = self._safe_float(rating_match.group(1))
                        if rating_value > 0:
                            meta["rating"] = max(0.0, min(5.0, rating_value))

                    count_match = re.search(r'"total_rating_count"\s*:\s*(\d+)', script_blob)
                    if count_match:
                        parsed_reviews = int(count_match.group(1))
                        if parsed_reviews > 0:
                            meta["review_count"] = parsed_reviews

                # 2) Fallback to PDP-specific rating nodes visible in rendered HTML.
                if meta.get("rating", 0.0) <= 0:
                    rating_selectors = [
                        ".product-rating-overview__rating-score",
                        "div.F9RHbS.dQEiAI.jMXp4d",
                        "div[class*='product-rating-overview'] span[class*='rating-score']",
                        "button[class*='e2p50f'] div[class*='F9RHbS']",
                        "span[class*='rating']",
                        "div[class*='product-rating']",
                    ]
                    for selector in rating_selectors:
                        rating_el = page.query_selector(selector)
                        rating_text = self._clean_text(rating_el.text_content() if rating_el else None)
                        if not rating_text:
                            continue
                        rating_value = self._safe_float(rating_text)
                        if 0 < rating_value <= 5:
                            meta["rating"] = max(0.0, min(5.0, rating_value))
                            break

                # 3) Fallback for review count using scoped rating section text to avoid seller metrics.
                if meta.get("review_count", 0) <= 0:
                    scoped_text_parts: list[str] = []
                    for selector in [".product-rating-overview", "div.flex.asFzUa", "#main"]:
                        try:
                            scoped_text = self._clean_text(page.text_content(selector))
                        except Exception:
                            scoped_text = ""
                        if scoped_text:
                            scoped_text_parts.append(scoped_text)

                    full_text = "\n".join(scoped_text_parts) or self._clean_text(page.text_content("body"))

                    review_patterns = [
                        r"([\d,.]+\s*[kKmM]?\+?)\s*ratings?",
                        r"([\d,.]+\s*[kKmM]?\+?)\s*reviews?",
                        r"5\s*star\s*\(([\d,.]+\s*[kKmM]?\+?)\)",
                    ]
                    for pattern in review_patterns:
                        review_count_match = re.search(pattern, full_text, flags=re.IGNORECASE)
                        if not review_count_match:
                            continue
                        parsed_reviews = self._parse_compact_number(review_count_match.group(1))
                        if parsed_reviews > 0:
                            meta["review_count"] = parsed_reviews
                            break

                context.close()
                browser.close()
                return meta
        except Exception:
            return {}

    def _fetch_amazon_product_detail_metadata(self, product_url: str) -> dict[str, Any]:
        """Fetch Amazon PDP metadata using Playwright with stealth for all extraction."""
        if not product_url:
            return {}
        
        # Use Playwright with stealth for all extraction
        return self._fetch_amazon_product_detail_metadata_playwright(product_url)

    def _fetch_amazon_product_detail_metadata_playwright(self, product_url: str) -> dict[str, Any]:
        if not product_url:
            return {}

        try:
            with sync_playwright() as playwright:
                browser = self._launch_browser(playwright)
                context = browser.new_context(user_agent=self.headers["User-Agent"], locale="en-US")
                page = context.new_page()
                stealth_sync(page)
                page.goto(product_url, wait_until="domcontentloaded", timeout=max(5, self.timeout) * 1000)
                page.wait_for_timeout(900)

                meta: dict[str, Any] = {}
                full_text = self._clean_text(page.text_content("body"))

                # Amazon product rating - extract from text using regex
                rating_match = re.search(r"(\d{1,2}(?:\.\d)?)\s*out\s*of\s*5\s*stars", full_text, flags=re.IGNORECASE)
                if rating_match:
                    rating_value = self._safe_float(rating_match.group(1))
                    if rating_value > 0:
                        meta["rating"] = max(0.0, min(5.0, rating_value))

                # Amazon review count
                review_match = re.search(r"([\d,]+)\s*(?:global\s*)?ratings?", full_text, flags=re.IGNORECASE)
                if review_match:
                    parsed_reviews = self._parse_compact_number(review_match.group(1))
                    if parsed_reviews > 0:
                        meta["review_count"] = parsed_reviews

                context.close()
                browser.close()
                return meta
        except Exception:
            return {}

    def _extract_amazon_from_page(self, page) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        items = page.query_selector_all("div.s-result-item[data-component-type='s-search-result']")

        detail_fetch_count = 0
        detail_fetch_limit = min(6, self.max_results)

        for item in items[: self.max_results * 2]:
            try:
                link_el = item.query_selector("h2 a[href]") or item.query_selector("a.a-link-normal[href]")
                name_el = item.query_selector("h2 a span") or item.query_selector("h2 span")

                name = self._clean_text(name_el.text_content() if name_el else None)
                if not name:
                    name = self._clean_text(link_el.get_attribute("aria-label") if link_el else None)
                if not name:
                    continue

                href = link_el.get_attribute("href") if link_el else None
                url = self._normalize_amazon_search_href(href)
                if not self._is_amazon_product_url(url):
                    continue

                price = self._extract_amazon_price_from_item(item)
                rating = self._extract_rating_from_item(
                    item,
                    selectors=[
                        "span.a-icon-alt",
                        "[aria-label*='out of 5 stars']",
                    ],
                )
                review_count = self._extract_review_count_from_item(
                    item,
                    selectors=[
                        "span.a-size-base.s-underline-text",
                        "span[aria-label*='ratings']",
                        "span[aria-label*='reviews']",
                    ],
                )
                detail_meta: dict[str, Any] = {}
                needs_detail = rating <= 0
                if needs_detail and detail_fetch_count < detail_fetch_limit:
                    detail_meta = self._fetch_amazon_product_detail_metadata(url)
                    detail_fetch_count += 1
                if detail_meta.get("rating", 0.0) > 0:
                    rating = float(detail_meta["rating"])
                if detail_meta.get("review_count", 0) > 0:
                    review_count = int(detail_meta["review_count"])

                products.append(
                    {
                        "name": name,
                        "price": price,
                        "category": "General",
                        "rating": rating,
                        "review_count": review_count,
                        "seller_name": "",
                        "seller_rating": 0.0,
                        "platform": "Amazon",
                        "url": url,
                    }
                )
                if len(products) >= self.max_results:
                    break
                if self.enable_antibot_behavior:
                    time.sleep(random.uniform(0.05, 0.15))
            except Exception:
                continue

        return products

    def _scrape_amazon_index_fallback(self, query: str) -> list[dict[str, Any]]:
        """
        Fallback path for Amazon when direct storefront extraction is blocked.
        Uses public search index results to recover product links and titles.
        """
        url = f"https://duckduckgo.com/html/?q={quote_plus(f'site:amazon.com/dp {query}') }"
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            html = response.text
        except requests.RequestException:
            return []

        products: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for match in re.finditer(
            r'<a[^>]*class="[^"]*result__a[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            raw_href = unescape(match.group(1))
            href = self._decode_search_redirect_url(raw_href)
            if not href:
                continue

            normalized_url = self._normalize_url("https://duckduckgo.com", href)
            if "amazon." not in normalized_url.lower():
                continue
            if not self._is_amazon_product_url(normalized_url):
                continue
            if normalized_url in seen_urls:
                continue

            raw_title = re.sub(r"<[^>]+>", " ", unescape(match.group(2)))
            title = self._clean_text(re.sub(r"\s+", " ", raw_title))
            if not title:
                continue

            seen_urls.add(normalized_url)
            products.append(
                {
                    "name": title,
                    "price": self._extract_price_from_text(title),
                    "category": "General",
                    "rating": 0.0,
                    "review_count": 0,
                    "seller_name": "",
                    "seller_rating": 0.0,
                    "platform": "Amazon",
                    "url": normalized_url,
                }
            )

            if len(products) >= self.max_results:
                break

        return products

    def scrape_shopee(self, query: str, debug_trace: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        account_start = time.perf_counter()
        account_products = self._scrape_shopee_account_scraper(query, debug_trace=debug_trace)
        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": "shopee_account_scraper",
                    "status": "results" if account_products else "no_results",
                    "duration_ms": round((time.perf_counter() - account_start) * 1000, 2),
                    "details": {
                        "count": len(account_products),
                        "enabled": self.enable_shopee_account_scraper,
                    },
                }
            )
        if account_products:
            self._random_delay(1.0, 2.0)
            return account_products

        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": "shopee_account_scraper",
                    "status": "account_only_mode_no_results",
                    "duration_ms": 0.0,
                    "details": {
                        "reason": "non_account_shopee_scrapers_disabled",
                        "enabled": self.enable_shopee_account_scraper,
                    },
                }
            )
        return []

    def scrape_lazada(self, query: str, debug_trace: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        url = f"https://www.lazada.com.ph/catalog/?q={quote_plus(query)}"
        products = self._extract_products_with_playwright(
            url=url,
            wait_selectors=[
                "div[data-qa-locator='product-item']",
                "div[data-item-id]",
            ],
            platform="Lazada",
            debug_trace=debug_trace,
        )

        if products:
            self._random_delay(1.5, 3.0)
        return products

    def scrape_amazon(self, query: str, debug_trace: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        url = f"https://www.amazon.com/s?k={quote_plus(query)}"
        products = self._extract_products_with_playwright(
            url=url,
            wait_selectors=[
                "div.s-result-item[data-component-type='s-search-result']",
                "h2 a[href*='/dp/']",
                "span.a-price",
            ],
            platform="Amazon",
            timeout_seconds=10,
            debug_trace=debug_trace,
        )

        if not products:
            fallback_start = time.perf_counter()
            products = self._scrape_amazon_index_fallback(query)
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": "amazon_index_fallback",
                        "status": "results" if products else "no_results",
                        "duration_ms": round((time.perf_counter() - fallback_start) * 1000, 2),
                        "details": {"count": len(products)},
                    }
                )

        if products:
            self._random_delay(1.5, 3.0)
        return products

    def _scrape_shopee_account_scraper(
        self,
        query: str,
        debug_trace: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.enable_shopee_account_scraper:
            return []
        if not self.shopee_username or not self.shopee_password:
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": "shopee_account_scraper",
                        "status": "missing_credentials",
                        "duration_ms": 0.0,
                        "details": {},
                    }
                )
            return []

        workspace_root = Path(__file__).resolve().parent.parent
        shopee_root = workspace_root / "shopee-scraper"
        if not shopee_root.is_dir():
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": "shopee_account_scraper",
                        "status": "project_not_found",
                        "duration_ms": 0.0,
                        "details": {"path": str(shopee_root)},
                    }
                )
            return []

        shopee_root_str = str(shopee_root)
        inserted_path = False
        if shopee_root_str not in sys.path:
            sys.path.insert(0, shopee_root_str)
            inserted_path = True

        try:
            from app.scraping.shopee_scraper import ShopeeScraper  # type: ignore
        except Exception as exc:
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": "shopee_account_scraper",
                        "status": "import_failed",
                        "duration_ms": 0.0,
                        "details": {"error": str(exc)},
                    }
                )
            if inserted_path and shopee_root_str in sys.path:
                sys.path.remove(shopee_root_str)
            return []

        try:
            account_scraper = ShopeeScraper(
                username=self.shopee_username,
                password=self.shopee_password,
                keyword=query,
                numpage=self.shopee_account_numpage,
                itemperpage=self.shopee_account_itemperpage,
                lightweight=True,
                verification_wait_seconds=self.shopee_account_verification_wait_seconds,
            )
            account_scraper.scrape()
            payload = getattr(account_scraper, "results_data", {}) or {}
            rows = payload.get("data", []) if isinstance(payload, dict) else []
        except Exception as exc:
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": "shopee_account_scraper",
                        "status": "run_failed",
                        "duration_ms": 0.0,
                        "details": {"error": str(exc)},
                    }
                )
            rows = []
        finally:
            if inserted_path and shopee_root_str in sys.path:
                sys.path.remove(shopee_root_str)

        if not isinstance(rows, list):
            return []

        normalized: list[dict[str, Any]] = []
        detail_fetch_count = 0
        detail_fetch_limit = min(self.shopee_account_detail_backfill_limit, self.max_results)
        for row in rows:
            if not isinstance(row, dict):
                continue

            name = self._clean_text(str(row.get("name") or ""))
            if not name:
                continue

            product_url = self._clean_text(str(row.get("url") or ""))
            if self.require_direct_product_url and (not product_url or not self._is_direct_product_url(product_url)):
                continue

            seller = row.get("seller") if isinstance(row.get("seller"), dict) else {}
            seller_name = self._clean_text(
                str(
                    seller.get("shopName")
                    or seller.get("shop_name")
                    or seller.get("username")
                    or seller.get("name")
                    or ""
                )
            )

            raw_price = row.get("price")
            price_value = 0.0
            if isinstance(raw_price, dict):
                price_range = raw_price.get("range") if isinstance(raw_price.get("range"), dict) else {}
                min_value = self._safe_float(str(price_range.get("min") or ""), 0.0)
                max_value = self._safe_float(str(price_range.get("max") or ""), 0.0)
                price_value = min_value if min_value > 0 else max_value
            elif isinstance(raw_price, (int, float, str)):
                price_value = self._safe_float(str(raw_price or 0))

            rating_blob = row.get("rating") if isinstance(row.get("rating"), dict) else {}
            rating_value = self._safe_float(str(rating_blob.get("average") if rating_blob else row.get("rating") or 0))
            rating_value = max(0.0, min(5.0, rating_value)) if rating_value > 0 else 0.0

            review_raw = ""
            if rating_blob:
                review_raw = str(rating_blob.get("reviewCount") or rating_blob.get("review_count") or "")
            if not review_raw:
                review_raw = str(row.get("review_count") or row.get("cmt_count") or 0)
            review_count_value = self._parse_compact_number(review_raw)
            if review_count_value <= 0:
                review_count_value = self._safe_int(review_raw, 0)

            detail_meta: dict[str, Any] = {}
            needs_detail = (rating_value <= 0 or review_count_value <= 0) and bool(product_url)
            if needs_detail and detail_fetch_count < detail_fetch_limit:
                try:
                    detail_meta = self._fetch_shopee_product_detail_metadata(product_url)
                except Exception:
                    detail_meta = {}
                detail_fetch_count += 1
            if rating_value <= 0 and detail_meta.get("rating", 0.0) > 0:
                rating_value = float(detail_meta["rating"])
            if review_count_value <= 0 and detail_meta.get("review_count", 0) > 0:
                review_count_value = int(detail_meta["review_count"])

            category_value = row.get("categoryPath")
            if isinstance(category_value, list):
                category = " > ".join(self._clean_text(str(entry)) for entry in category_value if self._clean_text(str(entry)))
            else:
                category = self._clean_text(str(category_value or ""))
            if not category:
                category = "General"

            normalized.append(
                {
                    "name": name,
                    "price": price_value,
                    "category": category,
                    "rating": rating_value,
                    "review_count": review_count_value,
                    "seller_name": seller_name,
                    "seller_rating": max(
                        0.0,
                        min(
                            5.0,
                            self._safe_float(
                                str(seller.get("rating") or seller.get("rating_star") or seller.get("shop_rating") or 0)
                            ),
                        ),
                    ),
                    "platform": "Shopee",
                    "url": product_url,
                }
            )

            if len(normalized) >= self.max_results:
                break

        return normalized

    def scrape_google_shopping(self, query: str, debug_trace: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        urls = [
            f"https://www.google.com/search?tbm=shop&q={quote_plus(query)}",
            f"https://www.google.com/search?tbm=shop&hl=en&gl=PH&q={quote_plus(query)}",
            f"https://www.google.com/search?tbm=shop&hl=en&gl=US&q={quote_plus(query)}",
            f"https://www.google.com/search?hl=en&gl=PH&q={quote_plus(query)}&udm=28",
        ]
        products: list[dict[str, Any]] = []
        for url in urls:
            products = self._extract_products_with_playwright(
                url=url,
                wait_selectors=[
                    "a[href*='/shopping/product/']",
                    "span.VbBaOe",
                    "div.sh-dgr__grid-result",
                    "div.sh-dlr__list-result",
                    "div[data-sokoban-container]",
                    "div[data-cid] a",
                    "a[href*='shopping']",
                    "div:has(span.VbBaOe)",
                ],
                platform="Google Shopping",
                timeout_seconds=12,
                debug_trace=debug_trace,
            )
            if products:
                self._random_delay(1.0, 2.0)
                return products

        # Fallback to lightweight HTML parsing when Playwright extraction fails.
        html = ""
        last_error = ""
        for url in urls:
            try:
                response = requests.get(
                    url,
                    headers={
                        **self.headers,
                        "Accept-Language": "en-US,en;q=0.9",
                        "Cookie": "CONSENT=YES+cb.20210328-17-p0.en+FX+667",
                    },
                    timeout=self.timeout,
                )
                response.raise_for_status()
                html = response.text
                if html:
                    break
            except requests.RequestException as exc:
                last_error = str(exc)
                continue
        if not html:
            if debug_trace is not None:
                debug_trace.append(
                    {
                        "stage": "google_shopping_fetch",
                        "status": "request_failed",
                        "duration_ms": 0.0,
                        "details": {"error": last_error},
                    }
                )
            return []

        products: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for match in re.finditer(
            r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            href = self._clean_text(unescape(match.group(1)))
            if not href:
                continue

            product_url = urljoin("https://www.google.com", href)
            product_url = self._decode_search_redirect_url(product_url)
            if product_url in seen_urls:
                continue
            lowered_url = product_url.lower()
            if lowered_url.startswith("javascript:") or lowered_url.startswith("mailto:"):
                continue
            if "google.com" in lowered_url and "/search" in lowered_url:
                continue

            title_html = unescape(match.group(2))
            title = self._clean_text(re.sub(r"<[^>]+>", " ", title_html))
            if not title or len(title) < 4:
                continue

            context_start = max(0, match.start() - 450)
            context_end = min(len(html), match.end() + 450)
            context_text = self._clean_text(re.sub(r"<[^>]+>", " ", unescape(html[context_start:context_end])))
            price = self._extract_price_from_text(context_text)
            if price <= 0:
                continue

            seen_urls.add(product_url)
            products.append(
                {
                    "name": title,
                    "price": price,
                    "category": "General",
                    "rating": 0.0,
                    "review_count": 0,
                    "seller_name": "",
                    "seller_rating": 0.0,
                    "platform": "Google Shopping",
                    "url": product_url,
                }
            )

            if len(products) >= self.max_results:
                break

        if debug_trace is not None:
            debug_trace.append(
                {
                    "stage": "google_shopping_parse",
                    "status": "results" if products else "no_results",
                    "duration_ms": 0.0,
                    "details": {"count": len(products)},
                }
            )

        if products:
            self._random_delay(1.0, 2.0)
        return products