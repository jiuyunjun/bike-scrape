import csv
import email
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag


BASE_URL = "https://moto.webike.net"
UMEDIA_BASE_URL = "https://u-media.ne.jp/bike/"
BIKENET8190_BASE_URL = "https://www.8190.jp"
BDS_BIKESENSOR_BASE_URL = "https://www.bds-bikesensor.net"
DATA_DIR = "data"
OUTPUT_CSV = "cb400sf_2019plus.csv"


def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_year(text: str):
    if not text:
        return None
    m = re.search(r"(\d{4})", text)
    return int(m.group(1)) if m else None


def read_mhtml_html(path: Path) -> tuple[str, str]:
    raw_text = path.read_text(encoding="utf-8", errors="replace")

    snapshot_url = ""
    for line in raw_text.splitlines():
        if line.startswith("Snapshot-Content-Location:"):
            snapshot_url = line.split(": ", 1)[1].strip()
            break

    with path.open("rb") as f:
        msg = email.message_from_binary_file(f)

    payload = None
    charset = None
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            payload = part.get_payload(decode=True)
            charset = part.get_content_charset()
            break

    if payload is None:
        raise ValueError(f"no text/html part found in {path}")

    encodings = []
    if "goobike.com" in snapshot_url:
        encodings.extend(["euc_jp", "cp932", "utf-8"])
    else:
        encodings.extend([charset or "utf-8", "utf-8", "cp932", "euc_jp"])

    html = None
    for enc in encodings:
        try:
            html = payload.decode(enc)
            break
        except Exception:
            continue

    if html is None:
        html = payload.decode("utf-8", errors="replace")

    return snapshot_url, html


def parse_price_text(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""

    compact = text.replace(" ", "").replace(",", "")
    compact = compact.replace("￥", "").replace("¥", "")

    if "万円" in compact:
        number_text = compact.replace("万円", "").replace("円", "")
        try:
            value = Decimal(number_text) * Decimal("10000")
            return str(int(value.quantize(Decimal("1"))))
        except InvalidOperation:
            pass

    return re.sub(r"\D", "", compact)


def regex_group(text: str, pattern: str) -> str:
    m = re.search(pattern, text, re.I)
    return clean_text(m.group(1)) if m else ""


def find_value_in_pairs(container: Tag, label: str) -> str:
    for li in container.find_all("li"):
        ps = li.find_all("p")
        if len(ps) < 2:
            continue
        left = clean_text(ps[0].get_text(" ", strip=True))
        if left == label:
            return clean_text(ps[1].get_text(" ", strip=True))
    return ""


def row_dict(
    source: str,
    title: str,
    year_text: str,
    mileage: str,
    color: str,
    shop: str,
    url: str,
    body_price: str,
    total_price: str,
) -> dict:
    year = extract_year(year_text)
    return {
        "来源": source,
        "タイトル": clean_text(title),
        "年式": year if year is not None else clean_text(year_text),
        "走行距離": clean_text(mileage),
        "色": clean_text(color),
        "販売店場所": clean_text(shop),
        "url": clean_text(url),
        "本体価格": parse_price_text(body_price),
        "総価格": parse_price_text(total_price),
    }


def parse_webike(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for card in soup.select("li.li_bike_list"):
        detail_a = card.select_one('a.flex[href*="/bike_detail/"]') or card.select_one('a[href*="/bike_detail/"]')
        if not detail_a:
            continue

        title_node = card.select_one("h2 strong")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        catch_copy_node = card.select_one("p.catch-copy")
        catch_copy = clean_text(catch_copy_node.get_text(" ", strip=True)) if catch_copy_node else ""
        if catch_copy:
            title = f"{title} {catch_copy}".strip()

        price_box = card.select_one(".price-info")
        body_price = find_value_in_pairs(price_box, "本体価格") if price_box else ""
        total_price = find_value_in_pairs(price_box, "支払総額") if price_box else ""

        distance_box = card.select_one(".box-distace")
        mileage = find_value_in_pairs(distance_box, "走行距離") if distance_box else ""
        year_text = find_value_in_pairs(distance_box, "年式") if distance_box else ""

        shop_address_node = card.select_one(".bike_shop-address")
        shop = clean_text(shop_address_node.get_text(" ", strip=True)) if shop_address_node else ""

        rows.append(
            row_dict(
                "webike",
                title,
                year_text,
                mileage,
                "",
                shop,
                urljoin(BASE_URL, detail_a["href"]),
                body_price,
                total_price,
            )
        )
    return rows


def parse_umedia(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for card in soup.select("article.bikeBox"):
        detail_a = card.select_one('a[href*="detail.php?skanrino="]')
        if not detail_a:
            continue

        text = clean_text(card.get_text(" ", strip=True))
        title_node = card.select_one("h4 a")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        shop_node = card.select_one("p.shop a")
        shop = clean_text(shop_node.get_text(" ", strip=True)) if shop_node else ""

        rows.append(
            row_dict(
                "u-media",
                title,
                regex_group(text, r"年式\s*(\d{4}年)"),
                regex_group(text, r"走行距離\s*([0-9,]+km)"),
                regex_group(text, r"色\s*([^\s]+)"),
                shop,
                urljoin(UMEDIA_BASE_URL, detail_a["href"]),
                regex_group(text, r"車輌本体価格[:：]\s*([0-9,]+円)"),
                "",
            )
        )
    return rows


def parse_bikekan(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for card in soup.select("div.c-catalog.p-grid__col"):
        detail_a = card.select_one('a[href*="/buy/detail/"]')
        if not detail_a:
            continue

        title = clean_text(card.select_one(".section.-title span").get_text(" ", strip=True)) if card.select_one(".section.-title span") else ""
        maker = clean_text(card.select_one(".section.-maker span").get_text(" ", strip=True)) if card.select_one(".section.-maker span") else ""
        if maker and maker not in title:
            title = f"{maker} {title}".strip()

        body_price = ""
        total_price = ""
        cols = card.select("dl.data .col")
        if len(cols) >= 1:
            body_price = parse_price_text(clean_text(cols[0].get_text(" ", strip=True)).replace("車両価格", ""))
        if len(cols) >= 2:
            total_price = parse_price_text(clean_text(cols[1].get_text(" ", strip=True)).replace("支払総額", ""))

        year_text = ""
        mileage = ""
        tds = card.select("table.spec tbody td")
        if len(tds) >= 2:
            year_text = clean_text(tds[0].get_text(" ", strip=True))
            mileage = clean_text(tds[1].get_text(" ", strip=True))

        shop_node = card.select_one(".c-button-tel .label")
        shop = clean_text(shop_node.get_text(" ", strip=True)).replace("取扱店舗：", "") if shop_node else ""

        rows.append(
            row_dict(
                "bikekan",
                title,
                year_text,
                mileage,
                "",
                shop,
                detail_a["href"],
                body_price,
                total_price,
            )
        )
    return rows


def parse_8190(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for card in soup.select("div.search-card"):
        detail_a = card.select_one('a.name[href*="/wish/ds/bike/"]')
        if not detail_a:
            continue

        text = clean_text(card.get_text(" ", strip=True))
        shop = regex_group(text, r"店舗在庫\s*(.+?)(?:お問い合わせ|来店予約|オンライン商談予約|$)")

        rows.append(
            row_dict(
                "8190",
                clean_text(detail_a.get_text(" ", strip=True)),
                regex_group(text, r"モデル年\s*(\d{4}年)"),
                regex_group(text, r"走行距離\s*([0-9,]+km)"),
                "",
                shop,
                urljoin(BIKENET8190_BASE_URL, detail_a["href"]),
                regex_group(text, r"車両本体価格\s*(￥[0-9,]+)"),
                "",
            )
        )
    return rows


def parse_goobike(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for card in soup.select("div.bike_sec"):
        detail_a = card.select_one("a.detail_kakaku_link")
        if not detail_a:
            continue

        title_node = card.select_one(".model_title h4 a")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        text = clean_text(card.get_text(" ", strip=True))

        price_tds = card.select(".detail_cont .cont01 table tr:nth-of-type(2) td")
        body_price = clean_text(price_tds[0].get_text(" ", strip=True)) if len(price_tds) >= 1 else ""
        total_price = clean_text(price_tds[1].get_text(" ", strip=True)) if len(price_tds) >= 2 else ""

        shop_name = ""
        shop_address = ""
        shop_link = card.select_one('a.s_info01[href*="/shop/client_"]')
        if shop_link:
            shop_name = clean_text(shop_link.get_text(" ", strip=True))
        area_node = card.select_one(".name_icon")
        if area_node:
            shop_address = clean_text(area_node.get_text(" ", strip=True))
        shop = clean_text(" ".join(x for x in [shop_name, shop_address] if x))

        rows.append(
            row_dict(
                "goobike",
                title,
                regex_group(text, r"モデル年式\s*(\d{4}年)"),
                regex_group(text, r"走行距離\s*([0-9,]+K?m)"),
                regex_group(text, r"色系統\s*([^\s]+)"),
                shop,
                detail_a["href"],
                body_price,
                total_price,
            )
        )
    return rows


def parse_bds_bikesensor(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for card in soup.select("li.c-search_block_list_item.type_bike"):
        detail_a = card.select_one('a[href*="/bike/detail/"]')
        if not detail_a:
            continue

        maker = clean_text(card.select_one(".c-search_block_text h2").get_text(" ", strip=True)) if card.select_one(".c-search_block_text h2") else ""
        title_node = card.select_one("h2.c-search_block_title a")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        if maker and maker not in title:
            title = f"{maker} {title}".strip()

        lead_node = card.select_one(".c-search_block_lead a")
        lead = clean_text(lead_node.get_text(" ", strip=True)) if lead_node else ""
        if lead:
            title = f"{title} {lead}".strip()

        body_price = ""
        total_price = ""
        for price_block in card.select(".c-search_block_price"):
            label = clean_text(price_block.select_one(".c-search_block_price_title").get_text(" ", strip=True)) if price_block.select_one(".c-search_block_price_title") else ""
            value = clean_text(price_block.select_one(".c-search_block_price_text").get_text(" ", strip=True)) if price_block.select_one(".c-search_block_price_text") else ""
            if "本体価格" in label:
                body_price = value
            elif "お支払総額" in label:
                total_price = value

        year_text = ""
        mileage = ""
        region = ""
        for col in card.select(".c-search_status_col"):
            head = clean_text(col.select_one(".c-search_status_head").get_text(" ", strip=True)) if col.select_one(".c-search_status_head") else ""
            content = clean_text(col.select_one(".c-search_status_content").get_text(" ", strip=True)) if col.select_one(".c-search_status_content") else ""
            if head == "モデル年":
                year_text = content
            elif head == "距離":
                mileage = content
            elif head == "地域":
                region = content

        shop_name = clean_text(card.select_one(".c-search_block_bottom_title01").get_text(" ", strip=True)) if card.select_one(".c-search_block_bottom_title01") else ""
        shop_address = ""
        for row in card.select(".c-search_block_bottom_info tr"):
            th = clean_text(row.select_one("th").get_text(" ", strip=True)) if row.select_one("th") else ""
            td = clean_text(row.select_one("td").get_text(" ", strip=True)) if row.select_one("td") else ""
            if th == "住所":
                shop_address = td
                break

        shop = clean_text(" ".join(part for part in [shop_address or region, shop_name] if part))

        rows.append(
            row_dict(
                "bds-bikesensor",
                title,
                year_text,
                mileage,
                "",
                shop,
                urljoin(BDS_BIKESENSOR_BASE_URL, detail_a["href"]),
                body_price,
                total_price,
            )
        )
    return rows


def parse_mhtml_file(path: Path) -> list[dict]:
    snapshot_url, html = read_mhtml_html(path)
    soup = BeautifulSoup(html, "html.parser")

    if "moto.webike.net" in snapshot_url:
        return parse_webike(soup)
    if "u-media.ne.jp" in snapshot_url:
        return parse_umedia(soup)
    if "bikekan.jp" in snapshot_url:
        return parse_bikekan(soup)
    if "8190.jp" in snapshot_url:
        return parse_8190(soup)
    if "goobike.com" in snapshot_url:
        return parse_goobike(soup)
    if "bds-bikesensor.net" in snapshot_url:
        return parse_bds_bikesensor(soup)

    return []


def is_target_row(row: dict, min_year: int = 2019) -> bool:
    title = clean_text(str(row.get("タイトル", ""))).lower()
    year = row.get("年式")
    return "cb400" in title and isinstance(year, int) and year >= min_year


def row_key(row: dict) -> tuple:
    url = clean_text(str(row.get("url", "")))
    if url:
        return ("url", url)

    return (
        "fingerprint",
        clean_text(str(row.get("タイトル", ""))).lower(),
        clean_text(str(row.get("年式", ""))),
        clean_text(str(row.get("走行距離", ""))).lower(),
        clean_text(str(row.get("本体価格", ""))).lower(),
        clean_text(str(row.get("販売店場所", ""))).lower(),
    )


def crawl_from_local_mhtml(data_dir: str = DATA_DIR, min_year: int = 2019) -> list[dict]:
    rows = []
    seen = set()
    mhtml_files = sorted(Path(data_dir).glob("*.mhtml"))

    for idx, path in enumerate(mhtml_files, start=1):
        print(f"[LOCAL] file {idx}/{len(mhtml_files)}")
        file_rows = parse_mhtml_file(path)
        print(f"  -> parsed rows: {len(file_rows)}")

        for row in file_rows:
            if not is_target_row(row, min_year=min_year):
                continue

            key = row_key(row)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    return rows


def save_csv(rows: list[dict], filename: str = OUTPUT_CSV) -> None:
    fieldnames = [
        "来源",
        "タイトル",
        "年式",
        "走行距離",
        "色",
        "販売店場所",
        "url",
        "本体価格",
        "総価格",
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"saved: {filename} ({len(rows)} rows)")


if __name__ == "__main__":
    rows = crawl_from_local_mhtml(DATA_DIR, min_year=2019)
    save_csv(rows)
