from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable
from urllib.request import Request, urlopen
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

import scrape_webike_cb400sf as scraper


DATA_ROOT = Path("monitor_data")
SNAPSHOT_ROOT = DATA_ROOT / "snapshots"
DIFF_ROOT = DATA_ROOT / "diffs"
RAW_ROOT = DATA_ROOT / "raw_html"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
MIN_COMPLETENESS_RATIO = 0.7
PREFECTURES = (
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
)
STORE_PREFECTURE_MAP = {
    "ユーメディア川崎": "神奈川県",
    "ユーメディア横浜新山下": "神奈川県",
    "ユーメディア湘南": "神奈川県",
    "ユーメディア相模原": "神奈川県",
    "ユーメディア藤沢": "神奈川県",
    "ユーメディア横浜青葉": "神奈川県",
    "ユーメディア厚木": "神奈川県",
    "ユーメディア小田原": "神奈川県",
    "ホンダドリーム相模原": "神奈川県",
    "川口店": "埼玉県",
    "小牧店": "愛知県",
    "広島店第2ショールーム": "広島県",
    "東松山店": "埼玉県",
    "浜松有玉店": "静岡県",
    "大宮店": "埼玉県",
    "座間店": "神奈川県",
    "上熊本店": "熊本県",
    "岡山店": "岡山県",
    "足利店": "栃木県",
    "半田店": "愛知県",
    "小倉店": "福岡県",
    "茨木鮎川店": "大阪府",
    "名古屋守山店": "愛知県",
    "神戸伊川谷店": "兵庫県",
    "新潟中央店": "新潟県",
    "川崎店": "神奈川県",
    "三郷上彦名店": "埼玉県",
    "札幌店": "北海道",
    "宇都宮店": "栃木県",
    "名古屋みなと店": "愛知県",
    "入間店": "埼玉県",
    "北九州店": "福岡県",
    "豊橋店": "愛知県",
    "港南店": "神奈川県",
    "草加店": "埼玉県",
    "富田林店": "大阪府",
    "千葉鶴沢店": "千葉県",
    "富士店": "静岡県",
    "熊本本山店": "熊本県",
    "天白店": "愛知県",
    "府中店": "東京都",
    "伏見店": "京都府",
    "京都木津川店": "京都府",
    "京都松井山手店": "京都府",
    "港北ニュータウン店": "神奈川県",
    "前橋店": "群馬県",
    "新潟店": "新潟県",
    "蕨店": "埼玉県",
    "岐阜長良店": "岐阜県",
    "浦和店": "埼玉県",
    "仙台南店": "宮城県",
    "久留米インター店": "福岡県",
    "門真店": "大阪府",
    "小山店": "栃木県",
}


@dataclass(frozen=True)
class SiteConfig:
    name: str
    homepage: str
    ready_selector: str
    parser_name: str
    initial_url: str | None = None
    page_param: str | None = None
    max_pages: int = 10
    search_phrase: str | None = None
    search_input: str | None = None
    search_submit: str | None = None


SITE_CONFIGS: list[SiteConfig] = [
    SiteConfig(
        name="webike",
        homepage="https://moto.webike.net/",
        initial_url="https://moto.webike.net/HONDA/251_400/CB400SF_SuperFour/?yarf=2019",
        ready_selector="li.li_bike_list",
        parser_name="webike",
        page_param="pdx",
        max_pages=6,
    ),
    SiteConfig(
        name="8190",
        homepage="https://www.8190.jp/",
        initial_url="https://www.8190.jp/wish/ds/bike/search/?haikiryoKbnList=%5B4%5D&bocShashuCdList=%5B189%5D&nenshikiLow=2019",
        ready_selector="div.search-card",
        parser_name="8190",
        max_pages=2,
    ),
    SiteConfig(
        name="u-media",
        homepage="https://u-media.ne.jp/",
        initial_url="https://u-media.ne.jp/bike/?car_flag=&makercd=1&type=&haikiryokbn=&shop=&price_low=&price_high=&search_text=cb400&page=1&search_order_by=",
        ready_selector="article.bikeBox",
        parser_name="u-media",
        page_param="page",
        max_pages=8,
    ),
    SiteConfig(
        name="bikekan",
        homepage="https://www.bikekan.jp/",
        initial_url="https://www.bikekan.jp/buy/search?swing_maker_id=2&series=cb400&year_min=2019&sort=1",
        ready_selector="div.c-catalog.p-grid__col",
        parser_name="bikekan",
        page_param="page",
        max_pages=6,
    ),
    SiteConfig(
        name="goobike",
        homepage="https://www.goobike.com/",
        ready_selector="div.outerDetail",
        parser_name="goobike",
        search_phrase="CB400 SUPER FOUR VTEC Revo",
        search_input='input[name="phrase"]',
        search_submit="#searchAreaBtn",
        max_pages=6,
    ),
]


PARSERS: dict[str, Callable[[BeautifulSoup], list[dict]]] = {
    "webike": scraper.parse_webike,
    "8190": scraper.parse_8190,
    "u-media": scraper.parse_umedia,
    "bikekan": scraper.parse_bikekan,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily bike monitor with browser automation and diff output.")
    parser.add_argument("--date", default=date.today().isoformat(), help="Snapshot date in YYYY-MM-DD format.")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode.")
    parser.add_argument("--site", action="append", choices=[cfg.name for cfg in SITE_CONFIGS], help="Only run selected site(s).")
    parser.add_argument("--save-raw-html", action="store_true", help="Save page HTML for debugging.")
    parser.add_argument(
        "--full-color-refresh",
        action="store_true",
        help="Fetch detail pages to refresh colors for all listings missing color.",
    )
    return parser.parse_args()


def clean_rows(rows: list[dict], min_year: int = 2019) -> list[dict]:
    filtered: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        normalized_row = normalize_row_fields(row)
        if not scraper.is_target_row(normalized_row, min_year=min_year):
            continue
        key = row_identity(normalized_row)
        if key in seen:
            continue
        seen.add(key)
        filtered.append(add_identity_fields(normalized_row))
    filtered.sort(key=lambda item: (item["来源"], item["listing_id"], item["url"]))
    return filtered


def add_identity_fields(row: dict) -> dict:
    copied = dict(row)
    source = scraper.clean_text(str(copied.get("来源", "")))
    listing_id = extract_listing_id(source, scraper.clean_text(str(copied.get("url", ""))))
    copied["listing_id"] = listing_id
    copied["listing_key"] = f"{source}:{listing_id}" if listing_id else row_identity(copied)
    return copied


def normalize_row_fields(row: dict) -> dict:
    copied = dict(row)
    copied["走行距離"] = normalize_mileage_value(str(copied.get("走行距離", "")))
    copied["販売店場所"] = scraper.clean_text(str(copied.get("販売店場所", "")))
    copied["販売店都道府県"] = normalize_prefecture_value(
        str(copied.get("販売店都道府県", "")),
        copied["販売店場所"],
    )

    listing_key = scraper.clean_text(str(copied.get("listing_key", "")))
    if listing_key and ":" in listing_key:
        copied["listing_id"] = listing_key.split(":", 1)[1]
    return copied


def normalize_mileage_value(value: str) -> str:
    text = scraper.clean_text(value)
    if not text:
        return "N/A"
    if any(token in text for token in ("走行不明", "不明", "unknown", "Unknown", "UNKNOWN")):
        return "N/A"
    digits = scraper.re.sub(r"\D", "", text)
    return digits or "N/A"


def extract_prefecture_from_text(text: str) -> str:
    cleaned = scraper.clean_text(text)
    for prefecture in PREFECTURES:
        if prefecture in cleaned:
            return prefecture
    return ""


def normalize_prefecture_value(existing_value: str, shop_text: str) -> str:
    existing = extract_prefecture_from_text(existing_value)
    if existing:
        return existing

    extracted = extract_prefecture_from_text(shop_text)
    if extracted:
        return extracted

    cleaned_shop = scraper.clean_text(shop_text)
    return STORE_PREFECTURE_MAP.get(cleaned_shop, "")


def row_identity(row: dict) -> str:
    source = scraper.clean_text(str(row.get("来源", "")))
    url = scraper.clean_text(str(row.get("url", "")))
    listing_id = extract_listing_id(source, url)
    if listing_id:
        return f"{source}:{listing_id}"
    return "|".join(
        [
            source,
            scraper.clean_text(str(row.get("タイトル", ""))),
            scraper.clean_text(str(row.get("年式", ""))),
            scraper.clean_text(str(row.get("走行距離", ""))),
            url,
        ]
    )


def extract_listing_id(source: str, url: str) -> str:
    if not url:
        return ""

    patterns = {
        "webike": r"/bike_detail/(\d+)/?",
        "8190": r"/wish/ds/bike/(\d+)/?",
        "bikekan": r"/buy/detail/(\d+)/?",
        "goobike": r"/spread/([A-Z0-9]+)/",
    }
    pattern = patterns.get(source)
    if pattern:
        match = scraper.re.search(pattern, url, scraper.re.I)
        if match:
            return match.group(1)

    parsed = urlparse(url)
    if source == "u-media":
        query_id = parse_qs(parsed.query).get("skanrino")
        if query_id:
            return query_id[0]

    path_parts = [part for part in parsed.path.split("/") if part]
    if path_parts:
        return path_parts[-1]
    return url


def parse_goobike_live(soup: BeautifulSoup) -> list[dict]:
    rows = scraper.parse_goobike(soup)
    if rows:
        return rows

    rows = []
    for card in soup.select("div.outerDetail"):
        detail_a = card.select_one('a[href*="/spread/"]')
        if not detail_a:
            continue

        title = ""
        brand = card.select_one(".BrandName")
        model = card.select_one(".CarName")
        if brand or model:
            title = scraper.clean_text(
                " ".join(
                    part
                    for part in [
                        brand.get_text(" ", strip=True) if brand else "",
                        model.get_text(" ", strip=True) if model else "",
                    ]
                    if part
                )
            )

        text = scraper.clean_text(card.get_text(" ", strip=True))
        shop = scraper.clean_text(
            " ".join(
                part
                for part in [
                    card.select_one(".shop_name").get_text(" ", strip=True) if card.select_one(".shop_name") else "",
                    card.select_one(".address").get_text(" ", strip=True) if card.select_one(".address") else "",
                ]
                if part
            )
        )

        body_price_node = card.select_one("p:has(span.totalPrice)")  # supported by bs4 cssselect
        body_price = ""
        if body_price_node:
            body_price = scraper.regex_group(
                scraper.clean_text(body_price_node.get_text(" ", strip=True)),
                r"車両価格\s*([0-9., ]+万円)",
            )
        if not body_price:
            body_price = scraper.regex_group(text, r"車両価格\s*([0-9., ]+万円)")

        total_price_node = card.select_one("span.totalPrice")
        total_price = ""
        if total_price_node:
            total_price = scraper.clean_text(total_price_node.get_text(" ", strip=True)) + "万円"
        if not total_price:
            total_price = scraper.regex_group(text, r"支払総額\s*([0-9., ]+万円)")

        rows.append(
            scraper.row_dict(
                "goobike",
                title,
                scraper.regex_group(text, r"モデル年式\s*：?\s*(\d{4}年)"),
                scraper.regex_group(text, r"走行距離\s*：?\s*([0-9,]+ ?K?m)"),
                scraper.regex_group(text, r"色系統\s*：?\s*([^\s]+)"),
                shop,
                detail_a["href"],
                body_price,
                total_price,
            )
        )
    return rows


def parse_html(config: SiteConfig, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    if config.parser_name == "goobike":
        return parse_goobike_live(soup)
    parser = PARSERS[config.parser_name]
    return parser(soup)


def build_paginated_url(url: str, param: str, page_number: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query[param] = [str(page_number)]
    if param == "pdx" and page_number <= 1:
        query.pop(param, None)
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def find_next_href(html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ('link[rel="next"]', 'a[rel="next"]', 'li.next a', 'a.next', '.next a', 'a[aria-label="次へ"]'):
        tag = soup.select_one(selector)
        if tag and tag.get("href"):
            return urljoin(current_url, tag["href"].strip())
    for link in soup.select("a[href]"):
        text = scraper.clean_text(link.get_text(" ", strip=True))
        href = link.get("href", "").strip()
        if text in {"次へ", "次のページ", "Next"} and href:
            return urljoin(current_url, href)
    return None


def save_page_html(snapshot_date: str, site_name: str, page_number: int, html: str) -> None:
    target_dir = RAW_ROOT / snapshot_date / site_name
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"page_{page_number:02d}.html").write_text(html, encoding="utf-8")


def wait_for_cards(page: Page, selector: str) -> None:
    page.wait_for_selector(selector, timeout=60000)
    page.wait_for_timeout(2500)


def human_pause(page: Page, ms: int = 1800) -> None:
    page.wait_for_timeout(ms)


def goto_with_retries(page: Page, url: str, selector: str, attempts: int = 3) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            wait_for_cards(page, selector)
            return
        except PlaywrightTimeoutError as exc:
            last_error = exc
            if attempt == attempts:
                raise
            page.wait_for_timeout(3000)
    if last_error:
        raise last_error


def open_first_page(page: Page, config: SiteConfig) -> None:
    page.goto(config.homepage, wait_until="domcontentloaded", timeout=60000)
    human_pause(page)

    if config.search_phrase and config.search_input and config.search_submit:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                page.goto(config.homepage, wait_until="domcontentloaded", timeout=60000)
                human_pause(page)
                page.locator(config.search_input).fill(config.search_phrase)
                human_pause(page, 800)
                page.locator(config.search_submit).click()
                page.wait_for_load_state("domcontentloaded", timeout=60000)
                wait_for_cards(page, config.ready_selector)
                return
            except PlaywrightTimeoutError as exc:
                last_error = exc
                if attempt == 3:
                    raise
                page.wait_for_timeout(3000)
        if last_error:
            raise last_error

    if config.initial_url:
        goto_with_retries(page, config.initial_url, config.ready_selector)
        return

    raise RuntimeError(f"{config.name}: no entrypoint configured")


def scrape_site(context: BrowserContext, config: SiteConfig, snapshot_date: str, save_raw_html: bool) -> list[dict]:
    page = context.new_page()
    all_rows: list[dict] = []
    visited_urls: set[str] = set()
    page_number = 1

    try:
        open_first_page(page, config)

        while page_number <= config.max_pages:
            current_url = page.url
            if current_url in visited_urls:
                break
            visited_urls.add(current_url)

            html = page.content()
            if save_raw_html:
                save_page_html(snapshot_date, config.name, page_number, html)

            page_rows = parse_html(config, html)
            print(f"[{config.name}] page {page_number}: {len(page_rows)} rows")
            if not page_rows:
                break
            all_rows.extend(page_rows)

            next_url = find_next_href(html, current_url)
            if not next_url and config.page_param:
                next_url = build_paginated_url(config.initial_url or current_url, config.page_param, page_number + 1)

            if not next_url or next_url in visited_urls:
                break

            try:
                goto_with_retries(page, next_url, config.ready_selector)
            except PlaywrightTimeoutError:
                break
            page_number += 1
    finally:
        page.close()

    return all_rows


def read_snapshot_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def latest_previous_snapshot(snapshot_date: str) -> Path | None:
    snapshot_files = sorted(SNAPSHOT_ROOT.glob("*/inventory.csv"))
    candidates = [path for path in snapshot_files if path.parent.name < snapshot_date]
    return candidates[-1] if candidates else None


def price_direction(old_value: str, new_value: str) -> str:
    old_num = int(old_value or 0)
    new_num = int(new_value or 0)
    if new_num > old_num:
        return "price_up"
    if new_num < old_num:
        return "price_down"
    return "price_changed"


def build_diff_rows(snapshot_date: str, previous_rows: list[dict], current_rows: list[dict]) -> tuple[list[dict], dict[str, int]]:
    previous_map = {row["listing_key"]: row for row in previous_rows}
    current_map = {row["listing_key"]: row for row in current_rows}
    diff_rows: list[dict] = []
    summary = {"new": 0, "removed": 0, "price_up": 0, "price_down": 0}

    for key, row in current_map.items():
        old_row = previous_map.get(key)
        if not old_row:
            summary["new"] += 1
            diff_rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "change_type": "new",
                    "listing_key": key,
                    "来源": row["来源"],
                    "listing_id": row["listing_id"],
                    "タイトル": row["タイトル"],
                    "url": row["url"],
                    "old_本体価格": "",
                    "new_本体価格": row["本体価格"],
                    "old_総価格": "",
                    "new_総価格": row["総価格"],
                }
            )
            continue

        old_body = str(old_row.get("本体価格", "") or "")
        new_body = str(row.get("本体価格", "") or "")
        old_total = str(old_row.get("総価格", "") or "")
        new_total = str(row.get("総価格", "") or "")
        if old_body != new_body or old_total != new_total:
            direction = price_direction(old_total or old_body, new_total or new_body)
            if direction in summary:
                summary[direction] += 1
            diff_rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "change_type": direction,
                    "listing_key": key,
                    "来源": row["来源"],
                    "listing_id": row["listing_id"],
                    "タイトル": row["タイトル"],
                    "url": row["url"],
                    "old_本体価格": old_body,
                    "new_本体価格": new_body,
                    "old_総価格": old_total,
                    "new_総価格": new_total,
                }
            )

    for key, row in previous_map.items():
        if key in current_map:
            continue
        summary["removed"] += 1
        diff_rows.append(
            {
                "snapshot_date": snapshot_date,
                "change_type": "removed",
                "listing_key": key,
                "来源": row["来源"],
                "listing_id": row["listing_id"],
                "タイトル": row["タイトル"],
                "url": row["url"],
                "old_本体価格": row["本体価格"],
                "new_本体価格": "",
                "old_総価格": row["総価格"],
                "new_総価格": "",
            }
        )

    diff_rows.sort(key=lambda item: (item["change_type"], item["来源"], item["listing_id"], item["url"]))
    return diff_rows, summary


def copy_previous_colors(previous_rows: list[dict], current_rows: list[dict]) -> int:
    previous_map = {row["listing_key"]: row for row in previous_rows}
    copied = 0
    for row in current_rows:
        if scraper.clean_text(str(row.get("色", ""))):
            continue
        previous_row = previous_map.get(row["listing_key"])
        if not previous_row:
            continue
        previous_color = scraper.clean_text(str(previous_row.get("色", "")))
        if not previous_color:
            continue
        row["色"] = previous_color
        copied += 1
    return copied


def rows_requiring_color_refresh(
    previous_rows: list[dict],
    current_rows: list[dict],
    full_color_refresh: bool,
) -> list[dict]:
    if full_color_refresh or not previous_rows:
        return [row for row in current_rows if not scraper.clean_text(str(row.get("色", "")))]

    diff_rows, _ = build_diff_rows("color-refresh", previous_rows, current_rows)
    diff_keys = {
        row["listing_key"]
        for row in diff_rows
        if row["change_type"] != "removed"
    }
    return [
        row
        for row in current_rows
        if row["listing_key"] in diff_keys and not scraper.clean_text(str(row.get("色", "")))
    ]


def normalize_color_value(color: str) -> str:
    normalized = scraper.clean_text(color).rstrip("：:")
    if normalized in {"", "-", "--", "不明", "なし", "無し", "unknown", "Unknown"}:
        return ""

    lowered = normalized.lower()
    red_aliases = (
        "キャンディークロモスフィアレッド",
        "キャンディクロモスフィアレッド",
        "red",
        "レッド",
        "レッドii",
        "赤",
        "赤白",
        "赤/白",
        "白/赤",
        "赤/白/黒",
        "トリコロール",
    )
    blue_aliases = (
        "アトモスフィアブルーメタリック",
        "アトモスファイアブルーメタリック",
        "アトモスフィアブルーm",
        "アトモスフィア",
        "ホワイトii",
        "blue",
        "ブルー",
        "ブルーii",
        "ブルー?",
        "青",
        "青/白",
        "白/青",
        "青/赤/白",
    )
    black_aliases = (
        "ダークネスブラックメタリック",
        "black",
        "ブラック",
        "黒",
    )
    silver_aliases = (
        "マットベータシルバーメタリック",
        "silver",
        "シルバー",
        "艶消し銀",
    )

    if lowered in red_aliases:
        return "キャンディークロモスフィアレッド（红白黑）"
    if lowered in blue_aliases:
        return "アトモスフィアブルーメタリック（蓝白）"
    if lowered in black_aliases:
        return "ダークネスブラックメタリック（黑）"
    if lowered in silver_aliases:
        return "マットベータシルバーメタリック（银色）"
    return normalized


def fetch_detail_html(url: str) -> str:
    request = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urlopen(request, timeout=60) as response:
        raw = response.read()
    return raw.decode("utf-8", errors="replace")


def extract_labeled_value(soup: BeautifulSoup, labels: tuple[str, ...]) -> str:
    wanted = {label.strip() for label in labels}
    for node in soup.find_all(["p", "dt", "h3", "div", "th", "td", "span", "h5"]):
        label = scraper.clean_text(node.get_text(" ", strip=True)).rstrip("：:")
        if label not in wanted:
            continue

        parent = node.parent
        if not parent:
            continue

        if parent.name in {"li", "dl", "tr", "div"}:
            texts = [scraper.clean_text(text) for text in parent.stripped_strings]
            texts = [text for text in texts if text]
            if len(texts) >= 2 and texts[0].rstrip("：:") == label:
                return texts[1]

        sibling = node.find_next_sibling()
        while sibling:
            value = scraper.clean_text(sibling.get_text(" ", strip=True))
            if value:
                return value
            sibling = sibling.find_next_sibling()
    return ""


def extract_detail_color(source: str, html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    if source == "webike":
        color_node = soup.select_one(".motorcycle-color")
        if color_node:
            return normalize_color_value(color_node.get_text(" ", strip=True))
        return normalize_color_value(extract_labeled_value(soup, ("カラー", "色", "車体色", "本体カラー")))

    if source == "8190":
        return normalize_color_value(extract_labeled_value(soup, ("色", "カラー", "車体色")))

    if source == "bikekan":
        for box in soup.select(".p-buy-product__box"):
            label_node = box.select_one(".label")
            if not label_node:
                continue
            label = scraper.clean_text(label_node.get_text(" ", strip=True)).rstrip("：:")
            if label != "カラー":
                continue
            values = [scraper.clean_text(text) for text in box.stripped_strings]
            values = [value for value in values if value and value != "カラー"]
            if values:
                return normalize_color_value(values[0])
        return normalize_color_value(extract_labeled_value(soup, ("カラー", "色")))

    if source == "u-media":
        return normalize_color_value(extract_labeled_value(soup, ("色", "カラー")))

    if source == "goobike":
        return normalize_color_value(extract_labeled_value(soup, ("色系統", "車体色", "カラー", "色")))

    return ""


def enrich_colors_from_details(rows: list[dict], previous_rows: list[dict], full_color_refresh: bool) -> tuple[int, list[str]]:
    targets = rows_requiring_color_refresh(previous_rows, rows, full_color_refresh)
    if not targets:
        print("[color] no detail refresh targets")
        return 0, []

    refreshed = 0
    errors: list[str] = []
    total = len(targets)
    for index, row in enumerate(targets, start=1):
        source = scraper.clean_text(str(row.get("来源", "")))
        url = scraper.clean_text(str(row.get("url", "")))
        listing_key = scraper.clean_text(str(row.get("listing_key", "")))
        if not url:
            errors.append(f"color:{listing_key}: missing url")
            continue

        try:
            html = fetch_detail_html(url)
            color = extract_detail_color(source, html)
            if color:
                row["色"] = color
                refreshed += 1
            else:
                errors.append(f"color:{listing_key}: color not found")
        except Exception as exc:
            errors.append(f"color:{listing_key}: {exc}")

        print(f"[color] {index}/{total} {source} -> {scraper.clean_text(str(row.get('色', '')))}")
        time.sleep(0.8)

    return refreshed, errors


def write_summary(path: Path, snapshot_date: str, summary: dict[str, int], total_rows: int, previous_path: Path | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    previous_label = previous_path.parent.name if previous_path else "none"
    lines = [
        f"snapshot_date: {snapshot_date}",
        f"compared_to: {previous_label}",
        f"total_active: {total_rows}",
        f"new: {summary['new']}",
        f"removed: {summary['removed']}",
        f"price_up: {summary['price_up']}",
        f"price_down: {summary['price_down']}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_errors(path: Path, errors: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not errors:
        if path.exists():
            path.unlink()
        return
    path.write_text("\n".join(errors) + "\n", encoding="utf-8")


def inventory_fieldnames() -> list[str]:
    return [
        "来源",
        "listing_id",
        "listing_key",
        "タイトル",
        "年式",
        "走行距離",
        "色",
        "販売店場所",
        "販売店都道府県",
        "url",
        "本体価格",
        "総価格",
    ]


def diff_fieldnames() -> list[str]:
    return [
        "snapshot_date",
        "change_type",
        "listing_key",
        "来源",
        "listing_id",
        "タイトル",
        "url",
        "old_本体価格",
        "new_本体価格",
        "old_総価格",
        "new_総価格",
    ]


def selected_configs(selected_sites: list[str] | None) -> list[SiteConfig]:
    if not selected_sites:
        return SITE_CONFIGS
    selected = set(selected_sites)
    return [config for config in SITE_CONFIGS if config.name in selected]


def run(args: argparse.Namespace) -> int:
    snapshot_date = args.date
    configs = selected_configs(args.site)
    if not configs:
        print("No site selected.")
        return 1

    previous_snapshot = latest_previous_snapshot(snapshot_date)
    previous_rows = read_snapshot_csv(previous_snapshot) if previous_snapshot else []
    previous_by_source: dict[str, list[dict]] = {}
    for row in previous_rows:
        previous_by_source.setdefault(row["来源"], []).append(row)

    all_rows: list[dict] = []
    errors: list[str] = []
    with sync_playwright() as playwright:
        browser: Browser = playwright.chromium.launch(
            headless=not args.headful,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=DEFAULT_USER_AGENT,
            viewport={"width": 1440, "height": 2200},
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        try:
            for config in configs:
                try:
                    rows = scrape_site(context, config, snapshot_date, args.save_raw_html)
                    site_rows = clean_rows(rows, min_year=2019)
                    fallback_rows = previous_by_source.get(config.name, [])
                    if fallback_rows and len(site_rows) < max(1, int(len(fallback_rows) * MIN_COMPLETENESS_RATIO)):
                        errors.append(
                            f"{config.name}: suspiciously low row count ({len(site_rows)} < {len(fallback_rows)} * {MIN_COMPLETENESS_RATIO})"
                        )
                        print(f"[{config.name}] low row count -> using previous snapshot rows: {len(fallback_rows)}")
                        all_rows.extend(fallback_rows)
                    else:
                        all_rows.extend(site_rows)
                except Exception as exc:
                    fallback_rows = previous_by_source.get(config.name, [])
                    errors.append(f"{config.name}: {exc}")
                    if fallback_rows:
                        print(f"[{config.name}] error -> using previous snapshot rows: {len(fallback_rows)}")
                        all_rows.extend(fallback_rows)
                    else:
                        print(f"[{config.name}] error -> no previous snapshot fallback")
        finally:
            context.close()
            browser.close()

    cleaned_rows = clean_rows(all_rows, min_year=2019)
    copied_colors = copy_previous_colors(previous_rows, cleaned_rows)
    if copied_colors:
        print(f"[color] copied from previous snapshot: {copied_colors}")

    refreshed_colors, color_errors = enrich_colors_from_details(
        cleaned_rows,
        previous_rows,
        full_color_refresh=args.full_color_refresh,
    )
    if refreshed_colors:
        print(f"[color] refreshed from detail pages: {refreshed_colors}")
    errors.extend(color_errors)

    snapshot_path = SNAPSHOT_ROOT / snapshot_date / "inventory.csv"
    write_csv(snapshot_path, cleaned_rows, inventory_fieldnames())

    diff_rows, summary = build_diff_rows(snapshot_date, previous_rows, cleaned_rows)

    diff_dir = DIFF_ROOT / snapshot_date
    write_csv(diff_dir / "diff.csv", diff_rows, diff_fieldnames())
    write_summary(diff_dir / "summary.txt", snapshot_date, summary, len(cleaned_rows), previous_snapshot)
    write_errors(diff_dir / "errors.txt", errors)

    print(f"saved inventory: {snapshot_path} ({len(cleaned_rows)} rows)")
    print(f"saved diff: {diff_dir / 'diff.csv'} ({len(diff_rows)} rows)")
    print(
        "summary:",
        f"new={summary['new']}",
        f"removed={summary['removed']}",
        f"price_up={summary['price_up']}",
        f"price_down={summary['price_down']}",
    )
    if errors:
        print(f"errors: {len(errors)} -> {diff_dir / 'errors.txt'}")
    return 0


if __name__ == "__main__":
    sys.exit(run(parse_args()))
