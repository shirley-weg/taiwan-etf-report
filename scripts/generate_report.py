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
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo


# ============================================================
# Taiwan ETF Chip Report - TWSE Master Production Version
# ------------------------------------------------------------
# This version removes sample ETF master data.
#
# What this version does now:
#   1. Fetches real TWSE ETF master data from official TWSE pages.
#   2. Saves:
#        - raw/etf_master/YYYY-MM-DD/twse_etf_master.json
#        - data/etf_master_latest.json
#   3. If raw/holdings snapshots exist, calculates real holding diffs.
#   4. If holdings snapshots do not exist yet, outputs a master-only report.
#      It does NOT create fake top_stock_changes.
#   5. Outputs:
#        - data/latest_report.json
#        - history/YYYY-MM-DD.json
#
# Important:
#   - This step connects "TWSE ETF master".
#   - It does NOT yet connect issuer PCF crawlers.
#   - Therefore top_stock_changes will remain empty until real ETF holdings
#     snapshots are available under raw/holdings/YYYY-MM-DD/{etf_code}.json.
# ============================================================


# ----------------------------
# Runtime configuration
# ----------------------------
ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = ROOT / "data"
HISTORY_DIR = ROOT / "history"
RAW_HOLDINGS_DIR = ROOT / "raw" / "holdings"
RAW_MASTER_DIR = ROOT / "raw" / "etf_master"

DATA_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)
RAW_HOLDINGS_DIR.mkdir(parents=True, exist_ok=True)
RAW_MASTER_DIR.mkdir(parents=True, exist_ok=True)

TAIPEI_TZ = ZoneInfo("Asia/Taipei")

TOP_N_STOCKS = int(os.getenv("TOP_N_STOCKS", "10"))
MIN_HOLDINGS_COVERAGE_RATIO = float(os.getenv("MIN_HOLDINGS_COVERAGE_RATIO", "0.85"))
ALLOW_MASTER_ONLY_REPORT = os.getenv("ALLOW_MASTER_ONLY_REPORT", "1").strip() == "1"
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_SLEEP_SECONDS = float(os.getenv("HTTP_SLEEP_SECONDS", "0.25"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("twse-etf-report")


# ----------------------------
# Official TWSE source URLs
# ----------------------------
TWSE_ETFORTUNE_PRODUCTS_URLS = [
    "https://www.twse.com.tw/en/ETFortune-institute/products",
    "https://www.twse.com.tw/zh/ETFortune/products",
]

TWSE_STATIC_PRODUCT_PAGES = {
    "domestic_equity": "https://www.twse.com.tw/en/products/securities/etf/products/domestic.html",
    "foreign_equity": "https://www.twse.com.tw/en/products/securities/etf/products/foreign.html",
    "leveraged_inverse": "https://www.twse.com.tw/en/products/securities/etf/products/li.html",
    "vanilla_futures": "https://www.twse.com.tw/en/products/securities/etf/products/vanilla-futures.html",
    "leveraged_inverse_futures": "https://www.twse.com.tw/en/products/securities/etf/products/li-futures.html",
    "bond_fixed_income": "https://www.twse.com.tw/en/products/securities/etf/products/bfIncome.html",
    "active": "https://www.twse.com.tw/en/products/securities/etf/products/active-list.html",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


# ============================================================
# Utility functions
# ============================================================
def now_taipei() -> datetime:
    return datetime.now(TAIPEI_TZ)


def to_date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def previous_business_day(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "" or str(value).strip() in {"-", "--", "N/A", "nan"}:
            return default
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "" or str(value).strip() in {"-", "--", "N/A", "nan"}:
            return default
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return default


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


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_code(value: Any) -> str:
    return clean_text(value).upper()


def parse_twse_number(value: Any) -> float:
    return safe_float(value, 0.0)


# ============================================================
# TWSE ETF master crawler
# ============================================================
def fetch_all_twse_etfs() -> List[Dict[str, Any]]:
    """
    Fetch real TWSE ETF master.

    Priority:
      1. ETFortune screener table, because it contains richer fields
         such as AUM, closing price, beneficiary and issuer.
      2. Static TWSE product pages, because they are simpler and stable,
         but usually contain fewer numeric fields.

    Return schema:
      {
        etf_code,
        etf_name,
        listing_date,
        benchmark,
        issuer,
        market,
        source,
        category,
        aum_yi,
        close,
        avg_daily_trading_value_ytd_million,
        avg_daily_trading_volume_ytd_shares,
        beneficiaries,
        top10_weight_pct
      }
    """
    errors: List[str] = []

    for url in TWSE_ETFORTUNE_PRODUCTS_URLS:
        try:
            rows = fetch_twse_etfortune_products(url)
            if rows:
                logger.info("Loaded %s TWSE ETF rows from ETFortune.", len(rows))
                return sort_and_dedupe_etfs(rows)
        except Exception as exc:
            logger.warning("ETFortune fetch failed for %s: %s", url, exc)
            errors.append(f"{url}: {exc}")

    try:
        rows = fetch_twse_static_product_pages()
        if rows:
            logger.info("Loaded %s TWSE ETF rows from static TWSE product pages.", len(rows))
            return sort_and_dedupe_etfs(rows)
    except Exception as exc:
        logger.warning("Static TWSE product pages fetch failed: %s", exc)
        errors.append(f"static product pages: {exc}")

    raise RuntimeError(
        "Unable to fetch TWSE ETF master from official TWSE sources. "
        "Errors: " + " | ".join(errors)
    )


def fetch_twse_etfortune_products(url: str) -> List[Dict[str, Any]]:
    """
    Try to parse the official ETFortune screener table.
    This page may be rendered differently by TWSE, so we accept table parsing
    only when actual ETF rows are present.
    """
    html = http_get(url)

    tables = pd.read_html(StringIO(html))
    rows: List[Dict[str, Any]] = []

    for table in tables:
        if table.empty:
            continue

        table.columns = [clean_text(c) for c in table.columns]
        candidate = normalize_etfortune_dataframe(table, source_url=url)
        if candidate:
            rows.extend(candidate)

    return rows


def normalize_etfortune_dataframe(df: pd.DataFrame, source_url: str) -> List[Dict[str, Any]]:
    """
    Normalize ETFortune screener table.

    Supports English and Chinese column names.
    """
    columns = {clean_text(c).lower(): c for c in df.columns}

    def find_col(possible_names: List[str]) -> Optional[str]:
        lower_map = {clean_text(c).lower(): c for c in df.columns}
        for name in possible_names:
            key = name.lower()
            if key in lower_map:
                return lower_map[key]

        for c in df.columns:
            c_norm = clean_text(c).lower()
            for name in possible_names:
                if name.lower() in c_norm:
                    return c
        return None

    code_col = find_col(["ETF Code", "股票代號", "證券代號", "代號"])
    name_col = find_col(["ETF Name", "ETF 名稱", "名稱", "證券名稱"])
    listing_col = find_col(["Listing Date", "上市日期", "上櫃日期"])
    benchmark_col = find_col(["Benchmark", "標的指數", "追蹤指數"])
    aum_col = find_col(["AUM", "資產規模"])
    close_col = find_col(["Closing Price", "收盤價"])
    trading_value_col = find_col(["Average Daily Trading Value", "成交值"])
    trading_volume_col = find_col(["Average Daily Trading Volume", "成交量"])
    beneficiary_col = find_col(["Beneficiary", "受益人"])
    issuer_col = find_col(["Issuer", "發行人", "投信"])

    if not code_col or not name_col:
        return []

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        etf_code = normalize_code(row.get(code_col))
        etf_name = clean_text(row.get(name_col))

        if not re.fullmatch(r"\d{4,6}[A-Z]?", etf_code):
            continue

        rows.append(
            {
                "etf_code": etf_code,
                "etf_name": etf_name,
                "listing_date": clean_text(row.get(listing_col)) if listing_col else "",
                "benchmark": clean_text(row.get(benchmark_col)) if benchmark_col else "",
                "issuer": clean_text(row.get(issuer_col)) if issuer_col else infer_issuer_from_name(etf_name),
                "market": "TWSE",
                "source": "TWSE ETFortune",
                "source_url": source_url,
                "category": "",
                "aum_yi": parse_twse_number(row.get(aum_col)) if aum_col else 0.0,
                "close": parse_twse_number(row.get(close_col)) if close_col else 0.0,
                "avg_daily_trading_value_ytd_million": parse_twse_number(row.get(trading_value_col)) if trading_value_col else 0.0,
                "avg_daily_trading_volume_ytd_shares": safe_int(row.get(trading_volume_col)) if trading_volume_col else 0,
                "beneficiaries": safe_int(row.get(beneficiary_col)) if beneficiary_col else 0,
                "top10_weight_pct": 0.0,
            }
        )

    return rows


def fetch_twse_static_product_pages() -> List[Dict[str, Any]]:
    """
    Fallback parser for official static TWSE product category pages.

    These pages usually expose ETF code/name as simple product lists.
    Numeric fields such as AUM/close may not exist here; those remain 0
    until ETFortune screener or another official numeric source is available.
    """
    rows: List[Dict[str, Any]] = []

    for category, url in TWSE_STATIC_PRODUCT_PAGES.items():
        try:
            html = http_get(url)
            category_rows = parse_static_product_page(html, url, category)
            rows.extend(category_rows)
        except Exception as exc:
            logger.warning("Failed to parse static product page %s: %s", url, exc)

    return rows


def parse_static_product_page(html: str, source_url: str, category: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: Dict[str, Dict[str, Any]] = {}

    # Most TWSE ETF product pages link to content.html?CODE=
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        match = re.search(r"content\.html\?([0-9A-Z]+)=?", href)
        if not match:
            continue

        code = normalize_code(match.group(1))
        name = clean_text(a.get_text(" ", strip=True))

        if not name or name == code:
            parent_text = clean_text(a.parent.get_text(" ", strip=True)) if a.parent else ""
            name = parent_text.replace(code, "").strip(" ,;:-")

        if not re.fullmatch(r"\d{4,6}[A-Z]?", code):
            continue

        rows[code] = {
            "etf_code": code,
            "etf_name": name,
            "listing_date": "",
            "benchmark": "",
            "issuer": infer_issuer_from_name(name),
            "market": "TWSE",
            "source": "TWSE static product page",
            "source_url": source_url,
            "category": category,
            "aum_yi": 0.0,
            "close": 0.0,
            "avg_daily_trading_value_ytd_million": 0.0,
            "avg_daily_trading_volume_ytd_shares": 0,
            "beneficiaries": 0,
            "top10_weight_pct": 0.0,
        }

    # Also parse simple text pattern like "00682U, Yuanta ..."
    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        match = re.match(r"^([0-9]{4,6}[A-Z]?)\s*[,，]\s*(.+)$", clean_text(line))
        if match:
            code = normalize_code(match.group(1))
            name = clean_text(match.group(2))
            rows.setdefault(
                code,
                {
                    "etf_code": code,
                    "etf_name": name,
                    "listing_date": "",
                    "benchmark": "",
                    "issuer": infer_issuer_from_name(name),
                    "market": "TWSE",
                    "source": "TWSE static product page",
                    "source_url": source_url,
                    "category": category,
                    "aum_yi": 0.0,
                    "close": 0.0,
                    "avg_daily_trading_value_ytd_million": 0.0,
                    "avg_daily_trading_volume_ytd_shares": 0,
                    "beneficiaries": 0,
                    "top10_weight_pct": 0.0,
                },
            )

    return list(rows.values())


def infer_issuer_from_name(etf_name: str) -> str:
    """
    Best-effort issuer inference for static pages when issuer is not present.
    ETFortune table should provide issuer directly; this is only fallback.
    """
    mapping = {
        "元大": "元大投信",
        "Yuanta": "元大投信",
        "富邦": "富邦投信",
        "Fubon": "富邦投信",
        "國泰": "國泰投信",
        "Cathay": "國泰投信",
        "群益": "群益投信",
        "Capital": "群益投信",
        "復華": "復華投信",
        "Fuh Hwa": "復華投信",
        "永豐": "永豐投信",
        "Sinopac": "永豐投信",
        "凱基": "凱基投信",
        "KGI": "凱基投信",
        "中信": "中國信託投信",
        "CTBC": "中國信託投信",
        "台新": "台新投信",
        "Taishin": "台新投信",
        "統一": "統一投信",
        "Uni-President": "統一投信",
        "兆豐": "兆豐投信",
        "Mega": "兆豐投信",
        "野村": "野村投信",
        "Nomura": "野村投信",
        "第一金": "第一金投信",
        "First": "第一金投信",
        "大華": "大華銀投信",
        "JKO": "街口投信",
        "玉山": "玉山投信",
        "E.SUN": "玉山投信",
        "聯博": "聯博投信",
        "AllianceBernstein": "聯博投信",
        "貝萊德": "貝萊德投信",
        "BlackRock": "貝萊德投信",
        "摩根": "摩根投信",
        "J.P. Morgan": "摩根投信",
    }

    for key, issuer in mapping.items():
        if key.lower() in etf_name.lower():
            return issuer

    return ""


def sort_and_dedupe_etfs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        code = normalize_code(row.get("etf_code"))
        if not code:
            continue

        existing = deduped.get(code)
        if not existing:
            deduped[code] = row
            continue

        # Prefer richer row with AUM or issuer.
        existing_score = int(bool(existing.get("aum_yi"))) + int(bool(existing.get("issuer")))
        row_score = int(bool(row.get("aum_yi"))) + int(bool(row.get("issuer")))
        if row_score > existing_score:
            deduped[code] = row

    return sorted(deduped.values(), key=lambda x: x["etf_code"])


def save_etf_master_snapshot(report_date: str, etf_master: List[Dict[str, Any]]) -> None:
    path = RAW_MASTER_DIR / report_date / "twse_etf_master.json"
    atomic_write_json(path, etf_master)
    atomic_write_json(DATA_DIR / "etf_master_latest.json", etf_master)


# ============================================================
# Raw holdings snapshot layer
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
            etf_code = str(payload.get("etf_code", path.stem)).upper()
            output[etf_code] = payload
        except Exception as exc:
            logger.warning("Failed to read holdings snapshot %s: %s", path, exc)

    return output


def get_previous_available_holding_date(report_date: str, max_lookback_days: int = 10) -> Optional[str]:
    current = datetime.strptime(report_date, "%Y-%m-%d").date()

    for i in range(1, max_lookback_days + 1):
        candidate = to_date_str(current - timedelta(days=i))
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
    yesterday_holdings: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    diffs: List[Dict[str, Any]] = []

    for etf_code, today_payload in sorted(today_holdings.items()):
        yesterday_payload = yesterday_holdings.get(etf_code)
        if not yesterday_payload:
            continue

        etf_name = today_payload.get("etf_name") or yesterday_payload.get("etf_name") or ""
        today_rows = [normalize_holding_row(x) for x in today_payload.get("holdings", [])]
        yesterday_rows = [normalize_holding_row(x) for x in yesterday_payload.get("holdings", [])]

        today_map = {row["stock_code"]: row for row in today_rows if row["stock_code"]}
        yesterday_map = {row["stock_code"]: row for row in yesterday_rows if row["stock_code"]}

        for stock_code in sorted(set(today_map) | set(yesterday_map)):
            today_row = today_map.get(stock_code, {})
            yesterday_row = yesterday_map.get(stock_code, {})

            stock_name = today_row.get("stock_name") or yesterday_row.get("stock_name") or ""
            today_shares = safe_float(today_row.get("shares", 0))
            yesterday_shares = safe_float(yesterday_row.get("shares", 0))
            delta_shares = today_shares - yesterday_shares

            if abs(delta_shares) < 1:
                continue

            price = safe_float(today_row.get("close", 0)) or safe_float(yesterday_row.get("close", 0))
            today_weight = safe_float(today_row.get("weight_pct", 0))
            yesterday_weight = safe_float(yesterday_row.get("weight_pct", 0))

            diffs.append(
                {
                    "etf_code": etf_code,
                    "etf_name": etf_name,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "delta_shares": round(delta_shares, 0),
                    "delta_lot": round(delta_shares / 1000, 1),
                    "delta_value_yi": round(delta_shares * price / 100_000_000, 4),
                    "weight_delta_pct": round(today_weight - yesterday_weight, 4),
                }
            )

    return diffs


# ============================================================
# Aggregation and report sections
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


def build_etf_rankings(
    etf_master: List[Dict[str, Any]],
    etf_stock_diffs: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    diffs_by_etf: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in etf_stock_diffs:
        diffs_by_etf[row["etf_code"]].append(row)

    output: List[Dict[str, Any]] = []

    for etf in etf_master:
        etf_code = etf["etf_code"]
        rows = diffs_by_etf.get(etf_code, [])

        buy_value_yi = sum(max(0.0, safe_float(x["delta_value_yi"])) for x in rows)
        sell_value_yi = abs(sum(min(0.0, safe_float(x["delta_value_yi"])) for x in rows))
        net_flow_yi = buy_value_yi - sell_value_yi

        aum_yi = safe_float(etf.get("aum_yi"))
        turnover_pct = (buy_value_yi + sell_value_yi) / aum_yi * 100 if aum_yi > 0 and rows else 0.0

        output.append(
            {
                "etf_code": etf_code,
                "etf_name": etf.get("etf_name", ""),
                "aum_yi": round(aum_yi, 1),
                "buy_value_yi": round(buy_value_yi, 2),
                "sell_value_yi": round(sell_value_yi, 2),
                "net_flow_yi": round(net_flow_yi, 2),
                "turnover_pct": round(turnover_pct, 2),
                "top10_weight_pct": round(safe_float(etf.get("top10_weight_pct")), 1),
            }
        )

    output.sort(
        key=lambda x: (
            abs(safe_float(x["buy_value_yi"]) + safe_float(x["sell_value_yi"])),
            safe_float(x["aum_yi"]),
        ),
        reverse=True,
    )
    return output


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
    output: List[Dict[str, Any]] = []

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

        output.append(
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

    return output


def build_events(
    top_stock_changes: List[Dict[str, Any]],
    quality: Dict[str, Any],
    master_count: int,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    buys = [x for x in top_stock_changes if safe_float(x["delta_value_yi"]) > 0]
    sells = [x for x in top_stock_changes if safe_float(x["delta_value_yi"]) < 0]

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
            "title": "TWSE ETF Master 已更新",
            "desc": f"本次從 TWSE 官方來源取得 {master_count} 檔上市 ETF master。"
        }
    )

    events.append(
        {
            "time": "資料檢查",
            "title": "ETF 持股快照覆蓋率",
            "desc": (
                f"今日已覆蓋 {quality['covered_etfs']}/{quality['tracked_etfs']} 檔 ETF，"
                f"覆蓋率 {quality['coverage_ratio']:.1%}。"
            ),
        }
    )

    return events


def build_ai_report(
    top_stock_changes: List[Dict[str, Any]],
    kpis: Dict[str, Any],
    quality: Dict[str, Any],
    master_count: int,
) -> Dict[str, Any]:
    net = safe_float(kpis["net_change_value_yi"])
    bias = "偏多" if net > 0 else "偏空" if net < 0 else "中性"

    if not top_stock_changes:
        return {
            "headline": f"TWSE ETF master 已更新，本次追蹤 {master_count} 檔上市 ETF。",
            "summary": (
                "目前已切換為正式 TWSE ETF master 資料源，不再使用 sample ETF 清單。"
                "由於尚未接上各投信 PCF / 每日持股 crawler，今日個股層級的 ETF 持股差分暫無資料。"
                "下一步應接上 issuer PCF adapters，產生 raw/holdings/YYYY-MM-DD/{etf_code}.json 後，"
                "系統即可自動計算 top_stock_changes 與 participating_etfs。"
            ),
            "watchlist": [],
            "risk": "目前只有 ETF master 為正式資料；個股籌碼分析須等持股快照資料完整後才可使用。",
        }

    buys = [x for x in top_stock_changes if safe_float(x["delta_value_yi"]) > 0]
    sells = [x for x in top_stock_changes if safe_float(x["delta_value_yi"]) < 0]

    headline = f"今日全市場 ETF 籌碼{bias}，前十大變動淨額 {net:.1f} 億元。"

    summary_parts = [
        f"本報告以 TWSE 上市 ETF master 為基礎，今日持股快照覆蓋率為 {quality['coverage_ratio']:.1%}。",
        f"跨 ETF 持股差分顯示，共識加碼股票 {kpis['consensus_buy_count']} 檔，共識減碼股票 {kpis['consensus_sell_count']} 檔。",
        f"前三大變動占整體變動絕對值 {kpis['concentration_score']}%，可用來衡量今日 ETF 資金是否集中於少數權值股。",
    ]

    if buys:
        top = buys[0]
        summary_parts.append(
            f"最大買盤為 {top['stock_code']} {top['stock_name']}，估算加碼 {top['delta_value_yi']} 億元，參與 ETF {top['etf_count']} 檔。"
        )

    if sells:
        top = sorted(sells, key=lambda x: safe_float(x["delta_value_yi"]))[0]
        summary_parts.append(
            f"最大賣壓為 {top['stock_code']} {top['stock_name']}，估算減碼 {abs(safe_float(top['delta_value_yi'])):.2f} 億元，參與 ETF {top['etf_count']} 檔。"
        )

    watchlist = []
    for x in top_stock_changes[:5]:
        direction = "加碼" if safe_float(x["delta_value_yi"]) > 0 else "減碼"
        watchlist.append(
            f"{x['stock_code']} {x['stock_name']}：ETF {direction} {abs(safe_float(x['delta_value_yi'])):.2f} 億，參與 ETF {x['etf_count']} 檔"
        )

    return {
        "headline": headline,
        "summary": "".join(summary_parts),
        "watchlist": watchlist,
        "risk": "若持股快照覆蓋率未達 100%，請避免將報告視為完整市場結論。",
    }


def build_data_sources() -> List[Dict[str, Any]]:
    return [
        {
            "name": "TWSE ETF e添富 / ETFortune",
            "type": "ETF master / AUM / close / issuer",
            "update_freq": "每日或依官方更新",
            "status": "ready",
            "fields": ["etf_code", "etf_name", "listing_date", "benchmark", "aum_yi", "close", "issuer"],
        },
        {
            "name": "TWSE ETF product pages",
            "type": "ETF category / product list fallback",
            "update_freq": "依官方頁面更新",
            "status": "ready",
            "fields": ["etf_code", "etf_name", "category", "issuer"],
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


def calculate_snapshot_quality(
    etf_master: List[Dict[str, Any]],
    today_holdings: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    tracked = len(etf_master)
    covered = len(today_holdings)
    coverage = covered / tracked if tracked else 0.0

    return {
        "tracked_etfs": tracked,
        "covered_etfs": covered,
        "coverage_ratio": round(coverage, 4),
        "min_required_coverage_ratio": MIN_HOLDINGS_COVERAGE_RATIO,
        "is_ready": coverage >= MIN_HOLDINGS_COVERAGE_RATIO,
        "allow_master_only_report": ALLOW_MASTER_ONLY_REPORT,
    }


# ============================================================
# Report generation
# ============================================================
def build_report() -> Optional[Dict[str, Any]]:
    now = now_taipei()
    report_date = to_date_str(now.date())

    logger.info("Report date: %s", report_date)

    etf_master = fetch_all_twse_etfs()
    save_etf_master_snapshot(report_date, etf_master)

    today_holdings = load_holdings_snapshot(report_date)
    previous_snapshot_date = get_previous_available_holding_date(report_date)

    if previous_snapshot_date:
        yesterday_holdings = load_holdings_snapshot(previous_snapshot_date)
    else:
        yesterday_holdings = {}

    quality = calculate_snapshot_quality(etf_master, today_holdings)

    if not quality["is_ready"] and not ALLOW_MASTER_ONLY_REPORT:
        logger.warning("Holdings snapshots are not ready. Skip report.")
        return None

    etf_stock_diffs = calculate_etf_stock_diffs(today_holdings, yesterday_holdings) if today_holdings and yesterday_holdings else []
    all_stock_changes = aggregate_stock_changes(etf_stock_diffs, top_n=None)
    top_stock_changes = all_stock_changes[:TOP_N_STOCKS]

    etf_rankings = build_etf_rankings(etf_master, etf_stock_diffs)
    kpis = build_kpis(all_stock_changes, top_stock_changes)
    stock_radar = build_stock_radar(top_stock_changes)
    events = build_events(top_stock_changes, quality, master_count=len(etf_master))
    ai_report = build_ai_report(top_stock_changes, kpis, quality, master_count=len(etf_master))

    net = safe_float(kpis["net_change_value_yi"])
    market_bias = "偏多" if net > 0 else "偏空" if net < 0 else "Master 已更新"

    return {
        "meta": {
            "report_date": report_date,
            "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "Asia/Taipei",
            "tracked_etfs": len(etf_master),
            "covered_etfs": quality["covered_etfs"],
            "coverage_ratio": quality["coverage_ratio"],
            "universe": "TWSE listed ETFs",
            "market_bias": market_bias,
            "snapshot_mode": "production_twse_master",
            "previous_snapshot_date": previous_snapshot_date or "",
            "sample_mode": False,
        },
        "data_quality": quality,
        "kpis": kpis,
        "top_stock_changes": top_stock_changes,
        "stock_radar": stock_radar,
        "etf_rankings": etf_rankings,
        "events": events,
        "data_sources": build_data_sources(),
        "ai_report": ai_report,
    }


def persist_report(report: Dict[str, Any]) -> None:
    report_date = report["meta"]["report_date"]

    latest_path = DATA_DIR / "latest_report.json"
    history_path = HISTORY_DIR / f"{report_date}.json"

    atomic_write_json(latest_path, report)
    atomic_write_json(history_path, report)

    logger.info("Generated %s", latest_path)
    logger.info("Generated %s", history_path)


def main() -> int:
    try:
        report = build_report()

        if report is None:
            logger.info("No report generated.")
            return 0

        persist_report(report)
        logger.info("Completed.")
        return 0

    except Exception as exc:
        logger.exception("Report generation failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
