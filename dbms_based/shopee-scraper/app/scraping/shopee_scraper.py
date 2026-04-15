from seleniumbase import SB
from datetime import datetime, timezone
import json
import re

from .abstract_scraper import AbstractScraper
from .handlers.login_handler import LoginHandler
from .handlers.search_handler import SearchHandler
from .handlers.product_scraper import ProductScraper
from .handlers.variant_scraper import VariantScraper
from .utils import ScrapeUtils


class ShopeeScraper(AbstractScraper):
    def __init__(
        self,
        username,
        password,
        keyword,
        numpage,
        itemperpage,
        lightweight=True,
        verification_wait_seconds=60,
    ):
        self.username = username
        self.password = password
        self.keyword = keyword
        self.numpage = numpage
        self.itemperpage = itemperpage
        self.lightweight = bool(lightweight)
        self.verification_wait_seconds = max(0, int(verification_wait_seconds or 0))

        # Initialize handlers
        self.login_handler = LoginHandler(
            self.username,
            self.password,
            verification_wait_seconds=self.verification_wait_seconds,
        )
        self.search_handler = SearchHandler(self.keyword)
        self.variant_scraper = VariantScraper()
        self.product_scraper = ProductScraper(self.variant_scraper)

        # Initialize result data structure
        self.results_data = {
            "data": [],
            "keyword": self.keyword,
            "sortBy": "relevance",
            "createdAt": "",
            "updatedAt": ""
        }

        self.login_url = "https://shopee.ph/buyer/login?next=https%3A%2F%2Fshopee.ph%2F"

    @staticmethod
    def _parse_compact_number(value: str) -> int:
        text = (value or "").strip().lower().replace(",", "")
        match = re.search(r"(\d+(?:\.\d+)?)\s*([km])?", text)
        if not match:
            return 0
        number = float(match.group(1))
        unit = match.group(2)
        if unit == "k":
            number *= 1000
        elif unit == "m":
            number *= 1000000
        return int(number)

    @staticmethod
    def _derive_name_from_url(url: str) -> str:
        tail = (url or "").split("/")[-1].split("?")[0]
        tail = re.sub(r"-i\.\d+\.\d+$", "", tail, flags=re.IGNORECASE)
        tail = re.sub(r"[^a-zA-Z0-9\-_ ]+", " ", tail)
        tail = re.sub(r"[-_]+", " ", tail)
        tail = re.sub(r"\s+", " ", tail).strip()
        return tail or "Shopee Product"

    def _build_lightweight_product_dict(self, link, url: str) -> dict:
        raw_text = ""
        try:
            raw_text = (link.text or "").strip()
        except Exception:
            raw_text = ""

        lines = [re.sub(r"\s+", " ", line).strip() for line in raw_text.splitlines() if line and line.strip()]

        name = ""
        for line in lines:
            lower = line.lower()
            if len(line) < 4:
                continue
            if re.search(r"(?:₱|php|฿|\$)\s*\d", line, flags=re.IGNORECASE):
                continue
            if "sold" in lower or "free shipping" in lower or "voucher" in lower:
                continue
            if "star" in lower and re.search(r"\d", lower):
                continue
            name = line
            break
        if not name:
            name = self._derive_name_from_url(url)

        price_min = "0"
        price_max = "0"
        price_match = re.search(
            r"(?:₱|php|฿|\$)\s*([\d,]+(?:\.\d{1,2})?)(?:\s*[-–]\s*(?:₱|php|฿|\$)?\s*([\d,]+(?:\.\d{1,2})?))?",
            raw_text,
            flags=re.IGNORECASE,
        )
        if price_match:
            price_min = price_match.group(1).replace(",", "")
            price_max = (price_match.group(2) or price_match.group(1)).replace(",", "")

        rating_average = None
        rating_match = re.search(r"\b([0-5](?:\.\d)?)\b\s*(?:stars?|rating)?", raw_text, flags=re.IGNORECASE)
        if rating_match:
            try:
                parsed_rating = float(rating_match.group(1))
                if 0 <= parsed_rating <= 5:
                    rating_average = parsed_rating
            except Exception:
                rating_average = None

        review_count = "0"
        review_match = re.search(r"([\d,.]+\s*[kKmM]?)\s*(?:ratings?|reviews?)", raw_text, flags=re.IGNORECASE)
        if review_match:
            review_count = str(self._parse_compact_number(review_match.group(1)))

        return {
            "id": None,
            "name": name,
            "description": "",
            "price": {
                "range": {
                    "min": price_min,
                    "max": price_max,
                },
                "currency": "PHP",
            },
            "totalQuantity": 0,
            "categoryPath": [],
            "url": url,
            "variants": [],
            "rating": {
                "average": rating_average,
                "reviewCount": review_count,
                "sold": "0",
                "starRating": {"1": "0", "2": "0", "3": "0", "4": "0", "5": "0"},
            },
            "seller": {
                "id": None,
                "name": "",
                "rating": "0",
                "responseRate": "",
                "joined": "",
                "product": "",
                "responseTime": "",
                "follower": "",
            },
        }

    def before_scrape(self):
        print("[INFO] Preparing environment before scraping.")

    def do_scrape(self):
        with SB(uc=True, test=True) as sb:
            print("[INFO] Opening Shopee website with undetected driver...")
            sb.activate_cdp_mode(self.login_url)

            # Wait for page load
            try:
                sb.cdp.wait_for_element_visible("body", timeout=30)
                print("[INFO] Successfully loaded the page.")
            except Exception as e:
                raise RuntimeError(f"[ERROR] Error loading page: {e}")

            sb.sleep(2)

            # Change language if applicable (optional - may not appear on every load)
            try:
                sb.cdp.wait_for_element_visible(
                    "div.language-selection__list-item button:contains('English')",
                    timeout=10
                )
                sb.cdp.mouse_click("div.language-selection__list-item button:contains('English')")
                sb.sleep(2)
                print("[INFO] Successfully changed language to English.")
            except Exception as e:
                print(f"[WARNING] Language selection not found or failed (continuing): {str(e)[:100]}")
                # Language selection is optional - continue with scraping

            # Perform login
            try:
                self.login_handler.login(sb)
                print("[INFO] Successfully logged in.")
            except Exception as e:
                raise RuntimeError(f"[ERROR] Login failed: {e}")

            # Perform search
            try:
                total_pages = self.search_handler.search(sb)
                if self.numpage is not None and self.numpage < total_pages:
                    total_pages = self.numpage
                search_url = sb.cdp.get_current_url()
                print("[INFO] Successfully performed search.")
            except Exception as e:
                raise RuntimeError(f"[ERROR] Failed during search: {e}")

            for page in range(0, total_pages):
                page_url = f"{search_url}&page={page}"
                if page != 0:
                    sb.cdp.get(page_url)
                    sb.sleep(2)

                # Scroll to load products
                ScrapeUtils.scroll_page(sb)

                # Get product URLs
                try:
                    products = sb.cdp.find_all(
                        "//li[contains(@class, 'shopee-search-item-result__item')]//a[contains(@class, 'contents')]",
                        timeout=20
                    )
                    product_urls = [f"https://shopee.ph{link.get_attribute('href')}" for link in products]
                    item_per_page = len(product_urls)
                    if self.itemperpage is not None and self.itemperpage < item_per_page:
                        item_per_page = self.itemperpage
                    print(f"[INFO] Successfully retrieved product URLs for page {page}.")
                except Exception as e:
                    raise RuntimeError(f"[ERROR] Failed to get product URLs: {e}")
                
                # Scrape product details
                for link, url in zip(products[:item_per_page], product_urls[:item_per_page]):
                    try:
                        if self.lightweight:
                            product_dict = self._build_lightweight_product_dict(link, url)
                        else:
                            product_obj = self.product_scraper.scrape_product_details(sb, url)
                            product_dict = {
                                "id": product_obj.id,
                                "name": product_obj.name,
                                "description": product_obj.description,
                                "price": product_obj.price,
                                "totalQuantity": product_obj.totalQuantity,
                                "categoryPath": product_obj.categoryPath,
                                "url": product_obj.url,
                                "variants": product_obj.variants,
                                "rating": product_obj.rating,
                                "seller": product_obj.seller
                            }
                        self.results_data["data"].append(product_dict)
                        print(f"[INFO] Successfully scraped product details for URL: {url}")
                    except Exception as e:
                        print(f"[ERROR] Failed to scrape product details: {e}")
                        continue

            # Update timestamps
            current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.results_data["createdAt"] = current_time
            self.results_data["updatedAt"] = current_time

            # Logout process
            try:
                sb.cdp.evaluate("""
                    let element = document.querySelector("div.navbar__username");
                    if (element) {
                        let event = new MouseEvent('mouseover', { bubbles: true });
                        element.dispatchEvent(event);
                    }
                """)
                sb.sleep(1)
                sb.cdp.mouse_click(
                    "button.navbar-account-drawer__button.navbar-account-drawer__button--complement.navbar-user-link.reset-button-style"
                )
                print("[INFO] Successfully logged out.")
            except Exception as e:
                raise RuntimeError(f"[ERROR] Logout failed: {e}")

            sb.sleep(3)
            print("[INFO] Browser closed. Scraping process completed.")

    def after_scrape(self):
        print("[INFO] Printing results...")
        print(json.dumps(self.results_data, ensure_ascii=False, indent=4))
