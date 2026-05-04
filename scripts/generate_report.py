from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo


# ============================================================
# Taiwan ETF Chip Report - TRUE TWSE ETF Master Version
# ------------------------------------------------------------
# Purpose:
#   1. Fetch real TWSE ETF master data from official TWSE sources.
#   2. Do NOT use sample ETF data.
#   3. Do NOT silently output fake rows when the official source cannot be parsed.
#   4. Save official ETF master snapshots:
#        data/etf_master_latest.json
#        raw/etf_master/YYYY-MM-DD/twse_etf_master.json
#   5. If raw/holdings snapshots are already available, calculate ETF holding diffs.
#      If not, output a master-only report with empty top_stock_changes.
#
# Important:
#   - TWSE ETF master = ETF list/basic product data.
#   - Daily stock holding changes require issuer PCF / holdings crawlers.
#   - This file intentionally does not generate fake stock changes.
# ============================================================


# ----------------------------
# Paths
# ----------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_DIR = ROOT / "history"
RAW_MASTER_DIR = ROOT / "raw" / "etf_master"
RAW_HOLDINGS_DIR = ROOT / "raw" / "holdings"
RAW_DEBUG_DIR = ROOT / "raw" / "debug"

for folder in [DATA_DIR, HISTORY_DIR, RAW_MASTER_DIR, RAW_HOLDINGS_DIR, RAW_DEBUG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Config
# ----------------------------
TAIPEI_TZ = ZoneInfo("Asia/Taipei")

TOP_N_STOCKS = int(os.getenv("TOP_N_STOCKS", "10"))

# This is a guardrail. TWSE listed ETFs are far more than 50.
# If parsing returns only 2 or 5 rows, something is wrong and the workflow should fail.
MIN_TWSE_ETF_MASTER_ROWS = int(os.getenv("MIN_TWSE_ETF_MASTER_ROWS", "50"))

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_SLEEP_SECONDS = float(os.getenv("HTTP_SLEEP_SECONDS", "0.35"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Set to 1 only when you want to keep the website rendering even if TWSE changed its page.
# Default is strict because the goal is real data, not merely a green workflow.
ALLOW_STALE_MASTER_ON_FETCH_FAIL = os.getenv("ALLOW_STALE_MASTER_ON_FETCH_FAIL", "0").strip() == "1"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("twse-etf-master")


# ----------------------------
# Official TWSE sources
# ----------------------------
# Primary source: TWSE ETF product list. Search-indexed TWSE content exposes:
#   上市日期, 證券代號, 證券簡稱, 發行人, 標的指數
#
# Accessibility pages are included because they are often more static than the
# main frontend pages and are still official TWSE pages.
TWSE_MASTER_URLS = [
    {
        "name": "TWSE ETF product list accessibility zh",
        "url": "https://accessibility.twse.com.tw/zh/products/securities/etf/products/list.html",
        "kind": "product_list",
    },
    {
        "name": "TWSE ETF product list zh",
        "url": "https://www.twse.com.tw/zh/products/securities/etf/products/list.html",
        "kind": "product_list",
    },
    {
        "name": "TWSE ETF product list en",
        "url": "https://www.twse.com.tw/en/products/securities/etf/products/list.html",
        "kind": "product_list",
    },
    {
        "name": "TWSE ETFortune screener en",
        "url": "https://www.twse.com.tw/en/ETFortune-institute/products",
        "kind": "etfortune",
    },
    {
        "name": "TWSE ETFortune screener zh",
        "url": "https://www.twse.com.tw/zh/ETFortune/products",
        "kind": "etfortune",
    },
]

# Supplementary category pages. These may not contain full AUM/issuer fields,
# but they can help recover category labels or active ETF codes.
TWSE_CATEGORY_URLS = [
    {
        "name": "domestic_equity",
        "url": "https://www.twse.com.tw/zh/ETF/domestic",
        "category": "國內成分證券ETF",
    },
    {
        "name": "active",
        "url": "https://www.twse.com.tw/en/products/securities/etf/products/active-list.html",
        "category": "主動式ETF",
    },
    {
        "name": "foreign_equity",
        "url": "https://www.twse.com.tw/en/products/securities/etf/products/foreign.html",
        "category": "國外成分證券ETF",
    },
    {
        "name": "leveraged_inverse",
        "url": "https://www.twse.com.tw/en/products/securities/etf/products/li.html",
        "category": "槓桿反向ETF",
    },
    {
        "name": "bond_fixed_income",
        "url": "https://www.twse.com.tw/en/products/securities/etf/products/bfIncome.html",
        "category": "債券及固定收益ETF",
    },
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# ============================================================
# Utilities
# ============================================================
def now_taipei() -> datetime:
    return datetime.now(TAIPEI_TZ)


def date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        text = clean_text(value).replace(",", "").replace("%", "")
        if text in {"", "-", "--", "N/A", "nan", "None"}:
            return default
        return float(text)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        text = clean_text(value).replace(",", "")
        if text in {"", "-", "--", "N/A", "nan", "None"}:
            return default
        return int(float(text))
    except Exception:
        return default


def normalize_code(value: Any) -> str:
    return clean_text(value).upper()


def is_twse_etf_code(value: Any) -> bool:
    code = normalize_code(value)
    # Covers 0050, 00631L, 00632R, 00980A, etc.
    return bool(re.fullmatch(r"\d{4,6}[A-Z]?", code))


def direction_from_value(value: float) -> str:
    if value > 0:
        return "buy"
    if value < 0:
        return "sell"
    return "neutral"


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(path.parent),
        delete=False,
        suffix=".tmp",
    ) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def http_get(url: str) -> str:
    logger.info("GET %s", url)
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    time.sleep(HTTP_SLEEP_SECONDS)
    return response.text


def debug_save_html(report_date: str, source_name: str, html: str) -> None:
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", source_name)[:80]
    path = RAW_DEBUG_DIR / report_date / f"{safe_name}.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8", errors="ignore")


# ============================================================
# TWSE ETF Master Fetching
# ============================================================
def fetch_twse_etf_master(report_date: str) -> List[Dict[str, Any]]:
    """
    Fetch and validate TWSE ETF master from official TWSE pages.

    This function is strict:
      - It fails if no source returns enough ETF rows.
      - It does not fabricate ETF rows.
      - Optional stale-cache mode must be explicitly enabled by env var.
    """
    all_rows: List[Dict[str, Any]] = []
    source_stats: List[Dict[str, Any]] = []

    for source in TWSE_MASTER_URLS:
        try:
            html = http_get(source["url"])
            debug_save_html(report_date, source["name"], html)

            if source["kind"] == "product_list":
                rows = parse_twse_product_list_html(html, source["url"], source["name"])
            elif source["kind"] == "etfortune":
                rows = parse_twse_etfortune_html(html, source["url"], source["name"])
            else:
                rows = []

            rows = dedupe_etf_rows(rows)
            source_stats.append({"source": source["name"], "rows": len(rows)})
            logger.info("Parsed %s rows from %s", len(rows), source["name"])
            all_rows.extend(rows)

        except Exception as exc:
            source_stats.append({"source": source["name"], "rows": 0, "error": str(exc)})
            logger.warning("Failed source %s: %s", source["name"], exc)

    # Supplement category data after main parsing.
    category_rows: List[Dict[str, Any]] = []
    for source in TWSE_CATEGORY_URLS:
        try:
            html = http_get(source["url"])
            debug_save_html(report_date, f"category_{source['name']}", html)
            rows = parse_category_page_html(html, source["url"], source["category"])
            logger.info("Parsed %s category rows from %s", len(rows), source["name"])
            category_rows.extend(rows)
        except Exception as exc:
            logger.warning("Failed category page %s: %s", source["name"], exc)

    merged = merge_master_and_category_rows(all_rows, category_rows)
    merged = dedupe_etf_rows(merged)

    # Strong validation: 0050 must exist and row count must be credible.
    has_0050 = any(row["etf_code"] == "0050" for row in merged)
    if len(merged) >= MIN_TWSE_ETF_MASTER_ROWS and has_0050:
        return sorted(merged, key=lambda x: x["etf_code"])

    msg = (
        f"TWSE ETF master parse did not pass validation. "
        f"rows={len(merged)}, has_0050={has_0050}, "
        f"min_required={MIN_TWSE_ETF_MASTER_ROWS}, source_stats={source_stats}. "
        f"Debug HTML saved under raw/debug/{report_date}/."
    )

    if ALLOW_STALE_MASTER_ON_FETCH_FAIL:
        cached = load_cached_master()
        if cached:
            logger.warning("%s Use stale cached master because ALLOW_STALE_MASTER_ON_FETCH_FAIL=1.", msg)
            return cached

    raise RuntimeError(msg)


def parse_twse_product_list_html(html: str, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    # 1) Table parser. This is the preferred path for product list pages.
    rows.extend(parse_master_tables(html, source_url, source_name))

    # 2) Text parser. Useful when HTML table is rendered as text in accessibility pages.
    rows.extend(parse_product_list_text(html, source_url, source_name))

    # 3) Link parser for pages with etfInfo links.
    rows.extend(parse_etf_links(html, source_url, source_name))

    return dedupe_etf_rows(rows)


def parse_twse_etfortune_html(html: str, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    rows.extend(parse_master_tables(html, source_url, source_name))
    rows.extend(parse_etfortune_text(html, source_url, source_name))
    rows.extend(parse_etf_links(html, source_url, source_name))

    return dedupe_etf_rows(rows)


def parse_master_tables(html: str, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    try:
        tables = pd.read_html(StringIO(html), displayed_only=False)
    except Exception:
        return rows

    for table_index, df in enumerate(tables):
        if df.empty:
            continue

        df = normalize_dataframe_columns(df)
        rows_from_named = parse_named_columns_dataframe(df, source_url, f"{source_name} table {table_index}")
        if rows_from_named:
            rows.extend(rows_from_named)
            continue

        rows.extend(parse_loose_dataframe(df, source_url, f"{source_name} table {table_index}"))

    return rows


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            clean_text(" ".join(str(x) for x in col if str(x) != "nan"))
            for col in df.columns
        ]
    else:
        df.columns = [clean_text(c) for c in df.columns]

    return df


def find_column(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    normalized = {clean_text(c).lower(): c for c in df.columns}

    for name in names:
        key = name.lower()
        if key in normalized:
            return normalized[key]

    for c in df.columns:
        c_norm = clean_text(c).lower()
        for name in names:
            if name.lower() in c_norm:
                return c

    return None


def parse_named_columns_dataframe(df: pd.DataFrame, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    code_col = find_column(df, ["證券代號", "ETF Code", "Security Code", "Code", "股票代號", "代號"])
    name_col = find_column(df, ["證券簡稱", "Name of ETF", "ETF Name", "Name", "名稱", "ETF名稱"])
    listing_col = find_column(df, ["上市日期", "Listing Date", "掛牌日期"])
    issuer_col = find_column(df, ["發行人", "Issuer", "發行公司", "總代理人"])
    benchmark_col = find_column(df, ["標的指數", "Benchmark", "追蹤指數", "Index"])
    aum_col = find_column(df, ["AUM", "資產規模"])
    close_col = find_column(df, ["Closing Price", "收盤價"])
    beneficiary_col = find_column(df, ["Beneficiary", "受益人"])
    trading_value_col = find_column(df, ["Average Daily Trading Value", "成交值"])
    trading_volume_col = find_column(df, ["Average Daily Trading Volume", "成交量"])

    if not code_col or not name_col:
        return []

    rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        code = normalize_code(row.get(code_col))
        name = clean_text(row.get(name_col))

        if not is_twse_etf_code(code) or not name:
            continue

        rows.append(
            make_etf_master_row(
                etf_code=code,
                etf_name=name,
                listing_date=clean_text(row.get(listing_col)) if listing_col else "",
                issuer=clean_text(row.get(issuer_col)) if issuer_col else infer_issuer_from_name(name),
                benchmark=clean_text(row.get(benchmark_col)) if benchmark_col else "",
                source=source_name,
                source_url=source_url,
                aum_yi=safe_float(row.get(aum_col)) if aum_col else 0.0,
                close=safe_float(row.get(close_col)) if close_col else 0.0,
                beneficiaries=safe_int(row.get(beneficiary_col)) if beneficiary_col else 0,
                avg_daily_trading_value_ytd_million=safe_float(row.get(trading_value_col)) if trading_value_col else 0.0,
                avg_daily_trading_volume_ytd_shares=safe_int(row.get(trading_volume_col)) if trading_volume_col else 0,
            )
        )

    return rows


def parse_loose_dataframe(df: pd.DataFrame, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        cells = [clean_text(x) for x in row.tolist()]
        cells = [x for x in cells if x and x.lower() != "nan"]
        if not cells:
            continue

        code_index = None
        code = ""
        for i, cell in enumerate(cells):
            if is_twse_etf_code(cell):
                code_index = i
                code = normalize_code(cell)
                break

        if code_index is None:
            joined = " ".join(cells)
            match = re.search(r"\b(\d{4,6}[A-Z]?)\b", joined)
            if match and is_twse_etf_code(match.group(1)):
                code = normalize_code(match.group(1))
                code_index = -1
            else:
                continue

        # Common product list order:
        # listing date, code, name, issuer, benchmark
        listing_date = ""
        name = ""
        issuer = ""
        benchmark = ""

        if code_index is not None and code_index >= 0:
            if code_index - 1 >= 0 and looks_like_date(cells[code_index - 1]):
                listing_date = cells[code_index - 1]
            if code_index + 1 < len(cells):
                name = cells[code_index + 1]
            if code_index + 2 < len(cells):
                issuer = cells[code_index + 2]
            if code_index + 3 < len(cells):
                benchmark = cells[code_index + 3]
        else:
            # Fallback: choose first non-date/non-code cell as name.
            for cell in cells:
                if not is_twse_etf_code(cell) and not looks_like_date(cell) and not looks_like_number_only(cell):
                    name = cell
                    break

        if not name:
            continue

        rows.append(
            make_etf_master_row(
                etf_code=code,
                etf_name=name,
                listing_date=listing_date,
                issuer=issuer or infer_issuer_from_name(name),
                benchmark=benchmark,
                source=source_name,
                source_url=source_url,
            )
        )

    return rows


def parse_product_list_text(html: str, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    rows: List[Dict[str, Any]] = []

    for line in text.splitlines():
        line = clean_text(line)
        if not line:
            continue

        patterns = [
            # 2003.06.30, 0050, 元大台灣50, 元大證券投資信託股份有限公司, 富時...
            r"^(?P<date>\d{4}[./-]\d{1,2}[./-]\d{1,2})\s*[,，]\s*(?P<code>\d{4,6}[A-Z]?)\s*[,，]\s*(?P<name>[^,，]+)(?:\s*[,，]\s*(?P<issuer>[^,，]+))?(?:\s*[,，]\s*(?P<benchmark>.+))?$",
            # 0050, 元大台灣50, 元大證券投資信託股份有限公司, 富時...
            r"^(?P<code>\d{4,6}[A-Z]?)\s*[,，]\s*(?P<name>[^,，]+)(?:\s*[,，]\s*(?P<issuer>[^,，]+))?(?:\s*[,，]\s*(?P<benchmark>.+))?$",
            # 0050 元大台灣50
            r"^(?P<code>\d{4,6}[A-Z]?)\s+(?P<name>.{2,80})$",
        ]

        for pattern in patterns:
            match = re.match(pattern, line)
            if not match:
                continue

            code = normalize_code(match.groupdict().get("code", ""))
            name = clean_text(match.groupdict().get("name", ""))

            if not is_twse_etf_code(code) or not name or looks_like_number_only(name):
                continue

            rows.append(
                make_etf_master_row(
                    etf_code=code,
                    etf_name=name,
                    listing_date=clean_text(match.groupdict().get("date", "")),
                    issuer=clean_text(match.groupdict().get("issuer", "")) or infer_issuer_from_name(name),
                    benchmark=clean_text(match.groupdict().get("benchmark", "")),
                    source=f"{source_name} text",
                    source_url=source_url,
                )
            )
            break

    return rows


def parse_etfortune_text(html: str, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    # ETFortune pages may render rows client-side. Try to extract from visible text/links only.
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    rows: List[Dict[str, Any]] = []

    # ETF detail links usually include /ETFortune/etfInfo/0050.
    rows.extend(parse_etf_links(html, source_url, source_name))

    # Conservative code/name lines.
    for line in text.splitlines():
        line = clean_text(line)
        match = re.match(r"^(?P<code>\d{4,6}[A-Z]?)\s+(?P<name>.{2,80})$", line)
        if not match:
            continue

        code = normalize_code(match.group("code"))
        name = clean_text(match.group("name"))
        if is_twse_etf_code(code) and name and not looks_like_number_only(name):
            rows.append(
                make_etf_master_row(
                    etf_code=code,
                    etf_name=name,
                    issuer=infer_issuer_from_name(name),
                    source=f"{source_name} text",
                    source_url=source_url,
                )
            )

    return rows


def parse_etf_links(html: str, source_url: str, source_name: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, Any]] = []

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = clean_text(a.get_text(" ", strip=True))

        code = ""
        for pattern in [
            r"/etfInfo/([0-9]{4,6}[A-Z]?)",
            r"etfInfo/([0-9]{4,6}[A-Z]?)",
            r"content\.html\?([0-9]{4,6}[A-Z]?)",
        ]:
            match = re.search(pattern, href)
            if match and is_twse_etf_code(match.group(1)):
                code = normalize_code(match.group(1))
                break

        if not code:
            continue

        name = text
        if not name or name == code:
            parent_text = clean_text(a.parent.get_text(" ", strip=True)) if a.parent else ""
            name = parent_text.replace(code, "").strip(" ,，;:-")

        rows.append(
            make_etf_master_row(
                etf_code=code,
                etf_name=name,
                issuer=infer_issuer_from_name(name),
                source=f"{source_name} link",
                source_url=source_url,
            )
        )

    return rows


def parse_category_page_html(html: str, source_url: str, category: str) -> List[Dict[str, Any]]:
    rows = []
    rows.extend(parse_master_tables(html, source_url, f"TWSE category {category}"))
    rows.extend(parse_product_list_text(html, source_url, f"TWSE category {category}"))
    rows.extend(parse_etf_links(html, source_url, f"TWSE category {category}"))

    for row in rows:
        row["category"] = row.get("category") or category

    return dedupe_etf_rows(rows)


def make_etf_master_row(
    etf_code: str,
    etf_name: str,
    source: str,
    source_url: str,
    listing_date: str = "",
    issuer: str = "",
    benchmark: str = "",
    category: str = "",
    aum_yi: float = 0.0,
    close: float = 0.0,
    beneficiaries: int = 0,
    avg_daily_trading_value_ytd_million: float = 0.0,
    avg_daily_trading_volume_ytd_shares: int = 0,
) -> Dict[str, Any]:
    etf_name = clean_text(etf_name)

    return {
        "etf_code": normalize_code(etf_code),
        "etf_name": etf_name,
        "listing_date": clean_text(listing_date),
        "benchmark": clean_text(benchmark),
        "issuer": clean_text(issuer) or infer_issuer_from_name(etf_name),
        "market": "TWSE",
        "source": source,
        "source_url": source_url,
        "category": clean_text(category),
        "aum_yi": round(float(aum_yi or 0.0), 4),
        "close": round(float(close or 0.0), 4),
        "avg_daily_trading_value_ytd_million": round(float(avg_daily_trading_value_ytd_million or 0.0), 4),
        "avg_daily_trading_volume_ytd_shares": int(avg_daily_trading_volume_ytd_shares or 0),
        "beneficiaries": int(beneficiaries or 0),
        "top10_weight_pct": 0.0,
    }


def dedupe_etf_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        code = normalize_code(row.get("etf_code"))
        if not is_twse_etf_code(code):
            continue

        row["etf_code"] = code
        row["etf_name"] = clean_text(row.get("etf_name", ""))

        if not row["etf_name"]:
            continue

        if code not in best or row_quality_score(row) > row_quality_score(best[code]):
            best[code] = row
        else:
            # merge missing non-numeric fields into existing row
            existing = best[code]
            for key in ["listing_date", "benchmark", "issuer", "category"]:
                if not existing.get(key) and row.get(key):
                    existing[key] = row[key]

    return sorted(best.values(), key=lambda x: x["etf_code"])


def merge_master_and_category_rows(master_rows: List[Dict[str, Any]], category_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged = {row["etf_code"]: dict(row) for row in dedupe_etf_rows(master_rows)}

    for cat_row in dedupe_etf_rows(category_rows):
        code = cat_row["etf_code"]
        if code not in merged:
            merged[code] = cat_row
            continue

        # Only enrich category/name/issuer if missing.
        for key in ["category", "issuer", "benchmark", "listing_date"]:
            if not merged[code].get(key) and cat_row.get(key):
                merged[code][key] = cat_row[key]

    return sorted(merged.values(), key=lambda x: x["etf_code"])


def row_quality_score(row: Dict[str, Any]) -> int:
    score = 0
    for key in ["etf_name", "listing_date", "issuer", "benchmark", "category"]:
        if row.get(key):
            score += 2
    for key in ["aum_yi", "close", "beneficiaries", "avg_daily_trading_value_ytd_million"]:
        if safe_float(row.get(key)) > 0:
            score += 3
    if row.get("source", "").lower().find("product list") >= 0:
        score += 2
    if row.get("source", "").lower().find("etfortune") >= 0:
        score += 3
    return score


def looks_like_date(text: str) -> bool:
    return bool(re.fullmatch(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", clean_text(text)))


def looks_like_number_only(text: str) -> bool:
    return bool(re.fullmatch(r"[\d,.\-% ]+", clean_text(text)))


def infer_issuer_from_name(etf_name: str) -> str:
    mapping = [
        ("元大", "元大投信"),
        ("Yuanta", "元大投信"),
        ("富邦", "富邦投信"),
        ("Fubon", "富邦投信"),
        ("國泰", "國泰投信"),
        ("Cathay", "國泰投信"),
        ("群益", "群益投信"),
        ("Capital", "群益投信"),
        ("復華", "復華投信"),
        ("Fuh Hwa", "復華投信"),
        ("永豐", "永豐投信"),
        ("SinoPac", "永豐投信"),
        ("凱基", "凱基投信"),
        ("KGI", "凱基投信"),
        ("中信", "中國信託投信"),
        ("CTBC", "中國信託投信"),
        ("台新", "台新投信"),
        ("Taishin", "台新投信"),
        ("統一", "統一投信"),
        ("Uni-President", "統一投信"),
        ("兆豐", "兆豐投信"),
        ("Mega", "兆豐投信"),
        ("野村", "野村投信"),
        ("Nomura", "野村投信"),
        ("第一金", "第一金投信"),
        ("First", "第一金投信"),
        ("大華", "大華銀投信"),
        ("UOB", "大華銀投信"),
        ("街口", "街口投信"),
        ("JKO", "街口投信"),
        ("玉山", "玉山投信"),
        ("E.SUN", "玉山投信"),
        ("聯博", "聯博投信"),
        ("AllianceBernstein", "聯博投信"),
        ("貝萊德", "貝萊德投信"),
        ("BlackRock", "貝萊德投信"),
        ("摩根", "摩根投信"),
        ("J.P. Morgan", "摩根投信"),
    ]

    low = etf_name.lower()
    for key, issuer in mapping:
        if key.lower() in low:
            return issuer
    return ""


def load_cached_master() -> List[Dict[str, Any]]:
    path = DATA_DIR / "etf_master_latest.json"
    if not path.exists():
        return []
    data = read_json(path)
    if not isinstance(data, list):
        return []
    return dedupe_etf_rows(data)


def save_master(report_date: str, master: List[Dict[str, Any]]) -> None:
    atomic_write_json(DATA_DIR / "etf_master_latest.json", master)
    atomic_write_json(RAW_MASTER_DIR / report_date / "twse_etf_master.json", master)


# ============================================================
# Holdings snapshots and diff
# ============================================================
def holdings_folder(trade_date: str) -> Path:
    return RAW_HOLDINGS_DIR / trade_date


def load_holdings_snapshot(trade_date: str) -> Dict[str, Dict[str, Any]]:
    folder = holdings_folder(trade_date)
    if not folder.exists():
        return {}

    output: Dict[str, Dict[str, Any]] = {}

    for path in sorted(folder.glob("*.json")):
        try:
            payload = read_json(path)
            etf_code = normalize_code(payload.get("etf_code", path.stem))
            output[etf_code] = payload
        except Exception as exc:
            logger.warning("Failed to read holdings snapshot %s: %s", path, exc)

    return output


def previous_available_holding_date(report_date: str, max_lookback_days: int = 10) -> Optional[str]:
    current = datetime.strptime(report_date, "%Y-%m-%d").date()
    for i in range(1, max_lookback_days + 1):
        candidate = date_str(current - timedelta(days=i))
        folder = holdings_folder(candidate)
        if folder.exists() and any(folder.glob("*.json")):
            return candidate
    return None


def normalize_holding_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "stock_code": clean_text(row.get("stock_code", "")),
        "stock_name": clean_text(row.get("stock_name", "")),
        "shares": safe_float(row.get("shares", 0)),
        "weight_pct": safe_float(row.get("weight_pct", 0)),
        "close": safe_float(row.get("close", 0)),
        "market_value": safe_float(row.get("market_value", 0)),
    }


def calculate_etf_stock_diffs(
    today_holdings: Dict[str, Dict[str, Any]],
    previous_holdings: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    diffs: List[Dict[str, Any]] = []

    for etf_code, today_payload in sorted(today_holdings.items()):
        previous_payload = previous_holdings.get(etf_code)
        if not previous_payload:
            continue

        etf_name = today_payload.get("etf_name") or previous_payload.get("etf_name") or ""
        today_rows = [normalize_holding_row(x) for x in today_payload.get("holdings", [])]
        previous_rows = [normalize_holding_row(x) for x in previous_payload.get("holdings", [])]

        today_map = {row["stock_code"]: row for row in today_rows if row["stock_code"]}
        previous_map = {row["stock_code"]: row for row in previous_rows if row["stock_code"]}

        for stock_code in sorted(set(today_map) | set(previous_map)):
            today_row = today_map.get(stock_code, {})
            prev_row = previous_map.get(stock_code, {})

            stock_name = today_row.get("stock_name") or prev_row.get("stock_name") or ""
            today_shares = safe_float(today_row.get("shares", 0))
            prev_shares = safe_float(prev_row.get("shares", 0))
            delta_shares = today_shares - prev_shares

            if abs(delta_shares) < 1:
                continue

            price = safe_float(today_row.get("close", 0)) or safe_float(prev_row.get("close", 0))
            today_weight = safe_float(today_row.get("weight_pct", 0))
            prev_weight = safe_float(prev_row.get("weight_pct", 0))

            diffs.append(
                {
                    "etf_code": etf_code,
                    "etf_name": etf_name,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "delta_shares": round(delta_shares, 0),
                    "delta_lot": round(delta_shares / 1000, 1),
                    "delta_value_yi": round(delta_shares * price / 100_000_000, 4),
                    "weight_delta_pct": round(today_weight - prev_weight, 4),
                }
            )

    return diffs


# ============================================================
# Report sections
# ============================================================
def make_stock_signal(stock_name: str, direction: str, etf_count: int, value_yi: float) -> str:
    abs_value = abs(value_yi)
    if direction == "buy" and etf_count >= 5:
        return f"{stock_name} 被 {etf_count} 檔 ETF 同步加碼，估算買盤 {abs_value:.2f} 億元，屬於高共識買盤。"
    if direction == "buy" and etf_count >= 2:
        return f"{stock_name} 被 {etf_count} 檔 ETF 加碼，估算買盤 {abs_value:.2f} 億元，建議觀察是否延續。"
    if direction == "sell" and etf_count >= 4:
        return f"{stock_name} 遭 {etf_count} 檔 ETF 同步減碼，估算賣壓 {abs_value:.2f} 億元，短線籌碼偏弱。"
    if direction == "sell" and etf_count >= 2:
        return f"{stock_name} 遭 {etf_count} 檔 ETF 減碼，估算賣壓 {abs_value:.2f} 億元，需觀察是否擴散。"
    return f"{stock_name} 今日 ETF 籌碼變化有限，暫列觀察。"


def aggregate_stock_changes(etf_stock_diffs: List[Dict[str, Any]], top_n: Optional[int] = None) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in etf_stock_diffs:
        grouped[row["stock_code"]].append(row)

    output: List[Dict[str, Any]] = []
    for stock_code, rows in grouped.items():
        stock_name = rows[0]["stock_name"]
        total_lot = sum(safe_float(x["delta_lot"]) for x in rows)
        total_value_yi = sum(safe_float(x["delta_value_yi"]) for x in rows)
        total_weight_delta = sum(safe_float(x["weight_delta_pct"]) for x in rows)
        direction = direction_from_value(total_value_yi)

        participants = []
        for x in sorted(rows, key=lambda r: abs(safe_float(r["delta_value_yi"])), reverse=True):
            delta_value_yi = safe_float(x["delta_value_yi"])
            participant_direction = direction_from_value(delta_value_yi)
            if participant_direction == "neutral":
                continue
            participants.append(
                {
                    "etf_code": x["etf_code"],
                    "etf_name": x["etf_name"],
                    "direction": participant_direction,
                    "delta_lot": round(safe_float(x["delta_lot"]), 1),
                    "delta_value_yi": round(delta_value_yi, 4),
                    "weight_delta_pct": round(safe_float(x["weight_delta_pct"]), 4),
                }
            )

        output.append(
            {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "direction": direction,
                "delta_lot": round(total_lot, 1),
                "delta_value_yi": round(total_value_yi, 2),
                "etf_count": len(participants),
                "weight_delta_pct": round(total_weight_delta, 4),
                "signal": make_stock_signal(stock_name, direction, len(participants), total_value_yi),
                "participating_etfs": participants,
            }
        )

    output.sort(key=lambda x: abs(safe_float(x["delta_value_yi"])), reverse=True)
    return output[:top_n] if top_n is not None else output


def build_etf_rankings(master: List[Dict[str, Any]], diffs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    diffs_by_etf: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in diffs:
        diffs_by_etf[row["etf_code"]].append(row)

    rankings = []
    for etf in master:
        code = etf["etf_code"]
        rows = diffs_by_etf.get(code, [])

        buy_value_yi = sum(max(0.0, safe_float(x["delta_value_yi"])) for x in rows)
        sell_value_yi = abs(sum(min(0.0, safe_float(x["delta_value_yi"])) for x in rows))
        net_flow_yi = buy_value_yi - sell_value_yi
        aum_yi = safe_float(etf.get("aum_yi"))
        turnover_pct = (buy_value_yi + sell_value_yi) / aum_yi * 100 if aum_yi > 0 and rows else 0.0

        rankings.append(
            {
                "etf_code": code,
                "etf_name": etf.get("etf_name", ""),
                "aum_yi": round(aum_yi, 1),
                "buy_value_yi": round(buy_value_yi, 2),
                "sell_value_yi": round(sell_value_yi, 2),
                "net_flow_yi": round(net_flow_yi, 2),
                "turnover_pct": round(turnover_pct, 2),
                "top10_weight_pct": round(safe_float(etf.get("top10_weight_pct")), 1),
            }
        )

    rankings.sort(
        key=lambda x: (
            abs(safe_float(x["buy_value_yi"]) + safe_float(x["sell_value_yi"])),
            safe_float(x["aum_yi"]),
            x["etf_code"],
        ),
        reverse=True,
    )
    return rankings


def build_kpis(all_stock_changes: List[Dict[str, Any]], top_stock_changes: List[Dict[str, Any]]) -> Dict[str, Any]:
    net = sum(safe_float(x["delta_value_yi"]) for x in all_stock_changes)
    consensus_buy = sum(
        1 for x in all_stock_changes
        if safe_float(x["delta_value_yi"]) > 0 and safe_int(x.get("etf_count")) >= 2
    )
    consensus_sell = sum(
        1 for x in all_stock_changes
        if safe_float(x["delta_value_yi"]) < 0 and safe_int(x.get("etf_count")) >= 2
    )
    total_abs = sum(abs(safe_float(x["delta_value_yi"])) for x in all_stock_changes) or 1.0
    top3_abs = sum(abs(safe_float(x["delta_value_yi"])) for x in top_stock_changes[:3])
    concentration_score = round(top3_abs / total_abs * 100) if all_stock_changes else 0

    return {
        "net_change_value_yi": round(net, 1),
        "consensus_buy_count": consensus_buy,
        "consensus_sell_count": consensus_sell,
        "concentration_score": concentration_score,
    }


def build_stock_radar(top_stock_changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in top_stock_changes:
        participants = item.get("participating_etfs", [])
        buy_etfs = sum(1 for x in participants if x.get("direction") == "buy")
        sell_etfs = sum(1 for x in participants if x.get("direction") == "sell")
        if item["direction"] == "buy" and buy_etfs >= 5:
            event_type = "高共識加碼"
        elif item["direction"] == "buy":
            event_type = "共識加碼"
        elif item["direction"] == "sell" and sell_etfs >= 4:
            event_type = "高共識減碼"
        elif item["direction"] == "sell":
            event_type = "共識減碼"
        else:
            event_type = "觀察"

        rows.append(
            {
                "stock_code": item["stock_code"],
                "stock_name": item["stock_name"],
                "buy_etfs": buy_etfs,
                "sell_etfs": sell_etfs,
                "net_value_yi": item["delta_value_yi"],
                "streak_days": 1,
                "event_type": event_type,
                "note": item["signal"],
            }
        )
    return rows


def holdings_quality(master_count: int, today_holdings_count: int) -> Dict[str, Any]:
    ratio = today_holdings_count / master_count if master_count else 0.0
    return {
        "tracked_etfs": master_count,
        "covered_etfs": today_holdings_count,
        "coverage_ratio": round(ratio, 4),
        "is_ready": ratio >= 0.85,
    }


def build_events(top_changes: List[Dict[str, Any]], quality: Dict[str, Any], master_count: int) -> List[Dict[str, Any]]:
    events = []

    buys = [x for x in top_changes if safe_float(x["delta_value_yi"]) > 0]
    sells = [x for x in top_changes if safe_float(x["delta_value_yi"]) < 0]

    if buys:
        top = buys[0]
        events.append(
            {
                "time": "盤後",
                "title": f"{top['stock_name']} 成為今日 ETF 最大共識買盤",
                "desc": f"估算加碼 {top['delta_value_yi']} 億元，參與 ETF {top['etf_count']} 檔。",
            }
        )

    if sells:
        top = sorted(sells, key=lambda x: safe_float(x["delta_value_yi"]))[0]
        events.append(
            {
                "time": "盤後",
                "title": f"{top['stock_name']} 出現 ETF 共識減碼",
                "desc": f"估算減碼 {abs(safe_float(top['delta_value_yi'])):.2f} 億元，參與 ETF {top['etf_count']} 檔。",
            }
        )

    events.append(
        {
            "time": "資料檢查",
            "title": "TWSE ETF Master 已成功更新",
            "desc": f"本次從 TWSE 官方來源解析並驗證 {master_count} 檔上市 ETF master。",
        }
    )
    events.append(
        {
            "time": "資料檢查",
            "title": "ETF 持股快照覆蓋率",
            "desc": f"今日已覆蓋 {quality['covered_etfs']}/{quality['tracked_etfs']} 檔 ETF，覆蓋率 {quality['coverage_ratio']:.1%}。",
        }
    )
    return events


def build_ai_report(top_changes: List[Dict[str, Any]], kpis: Dict[str, Any], master_count: int) -> Dict[str, Any]:
    if not top_changes:
        return {
            "headline": f"TWSE ETF master 已成功更新，本次追蹤 {master_count} 檔上市 ETF。",
            "summary": (
                "目前已接上正式 TWSE ETF master，不再使用 sample ETF 清單。"
                "此階段只提供 ETF 清單、名稱、發行人、上市日期、標的指數等 master data。"
                "每日個股層級的 ETF 加減碼需要下一步接上各投信 PCF / 每日持股揭露後才會產生。"
            ),
            "watchlist": [],
            "risk": "目前個股籌碼變化尚未接上正式持股快照，請勿將空白的 top_stock_changes 解讀為沒有 ETF 調倉。",
        }

    net = safe_float(kpis["net_change_value_yi"])
    bias = "偏多" if net > 0 else "偏空" if net < 0 else "中性"
    watchlist = [
        f"{x['stock_code']} {x['stock_name']}：ETF {'加碼' if safe_float(x['delta_value_yi']) > 0 else '減碼'} {abs(safe_float(x['delta_value_yi'])):.2f} 億，參與 ETF {x['etf_count']} 檔"
        for x in top_changes[:5]
    ]
    return {
        "headline": f"今日全市場 ETF 籌碼{bias}，前十大變動淨額 {net:.1f} 億元。",
        "summary": f"本報告以 TWSE ETF master 與 raw/holdings 快照計算，前十大變動集中度為 {kpis['concentration_score']}%。",
        "watchlist": watchlist,
        "risk": "若持股快照覆蓋率未達 100%，請避免將報告視為完整市場結論。",
    }


def build_data_sources() -> List[Dict[str, Any]]:
    return [
        {
            "name": "TWSE ETF 商品資訊上市清單",
            "type": "ETF master",
            "update_freq": "依 TWSE 官方頁面更新",
            "status": "ready",
            "fields": ["listing_date", "etf_code", "etf_name", "issuer", "benchmark"],
        },
        {
            "name": "TWSE ETF e添富投資篩選器",
            "type": "ETF master / AUM / close / issuer",
            "update_freq": "每日或依官方頁面更新",
            "status": "ready",
            "fields": ["etf_code", "etf_name", "listing_date", "benchmark", "aum_yi", "close", "issuer"],
        },
        {
            "name": "各投信 PCF / 每日持股揭露",
            "type": "每日持股核心資料",
            "update_freq": "每日盤前或盤後",
            "status": "watch",
            "fields": ["stock_code", "shares", "weight", "cash_component", "creation_unit"],
        },
        {
            "name": "raw/holdings snapshot",
            "type": "本系統標準化後持股快照",
            "update_freq": "每日",
            "status": "watch",
            "fields": ["trade_date", "etf_code", "stock_code", "shares", "weight_pct", "close"],
        },
    ]


# ============================================================
# Report
# ============================================================
def build_report() -> Dict[str, Any]:
    now = now_taipei()
    report_date = date_str(now.date())

    master = fetch_twse_etf_master(report_date)
    save_master(report_date, master)

    today_holdings = load_holdings_snapshot(report_date)
    prev_date = previous_available_holding_date(report_date)
    prev_holdings = load_holdings_snapshot(prev_date) if prev_date else {}

    diffs = calculate_etf_stock_diffs(today_holdings, prev_holdings) if today_holdings and prev_holdings else []
    all_stock_changes = aggregate_stock_changes(diffs, top_n=None)
    top_stock_changes = all_stock_changes[:TOP_N_STOCKS]
    kpis = build_kpis(all_stock_changes, top_stock_changes)
    quality = holdings_quality(len(master), len(today_holdings))

    net = safe_float(kpis["net_change_value_yi"])
    if top_stock_changes:
        market_bias = "偏多" if net > 0 else "偏空" if net < 0 else "中性"
    else:
        market_bias = "Master 已更新"

    return {
        "meta": {
            "report_date": report_date,
            "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "Asia/Taipei",
            "tracked_etfs": len(master),
            "covered_etfs": quality["covered_etfs"],
            "coverage_ratio": quality["coverage_ratio"],
            "universe": "TWSE listed ETFs",
            "market_bias": market_bias,
            "snapshot_mode": "production_twse_master",
            "previous_snapshot_date": prev_date or "",
            "sample_mode": False,
        },
        "data_quality": quality,
        "kpis": kpis,
        "top_stock_changes": top_stock_changes,
        "stock_radar": build_stock_radar(top_stock_changes),
        "etf_rankings": build_etf_rankings(master, diffs),
        "events": build_events(top_stock_changes, quality, len(master)),
        "data_sources": build_data_sources(),
        "ai_report": build_ai_report(top_stock_changes, kpis, len(master)),
    }


def persist_report(report: Dict[str, Any]) -> None:
    report_date = report["meta"]["report_date"]
    atomic_write_json(DATA_DIR / "latest_report.json", report)
    atomic_write_json(HISTORY_DIR / f"{report_date}.json", report)
    logger.info("Wrote %s", DATA_DIR / "latest_report.json")
    logger.info("Wrote %s", HISTORY_DIR / f"{report_date}.json")


def main() -> int:
    try:
        report = build_report()
        persist_report(report)
        logger.info("Completed successfully.")
        return 0
    except Exception as exc:
        logger.exception("Generation failed: %s", exc)
        # Strict behavior: fail workflow when true TWSE master cannot be fetched/validated.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
