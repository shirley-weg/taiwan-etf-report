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
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo


# ============================================================
# Taiwan ETF Chip Report - Official TWSE Master + Real PCF Crawler
# ------------------------------------------------------------
# 正式資料來源分層：
#   1. TWSE ISIN：抓 ETF master universe，不用 sample。
#   2. TWSE ETF 商品頁：自動探索各 ETF 的「申購買回清單 PCF」連結。
#   3. 各投信 PCF 網頁：抓真實 PCF / 每日持股揭露。
#   4. raw/holdings/YYYY-MM-DD/{etf_code}.json：標準化每日持股快照。
#   5. 今日快照 - 前一交易日快照：計算 top_stock_changes / participating_etfs。
#
# 嚴格原則：
#   - 不產生 sample ETF。
#   - 不產生假的 holdings。
#   - 抓不到某 ETF 的 PCF 會記錄在 data/pcf_fetch_status.json。
#   - 只有真實解析到 holdings 的 ETF 才會進入 raw/holdings。
#
# 重要限制：
#   - 各投信網站格式不同，本版以「TWSE 商品頁 PCF 連結探索 + 通用 HTML table parser」為主。
#   - 對主流投信提供 URL fallback pattern：元大、富邦、國泰、群益、復華、永豐。
#   - 若某家投信網站改版，請在 data/pcf_source_registry.json 補手動 URL。
# ============================================================


# ----------------------------
# Paths
# ----------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_DIR = ROOT / "history"
RAW_MASTER_DIR = ROOT / "raw" / "etf_master"
RAW_HOLDINGS_DIR = ROOT / "raw" / "holdings"
RAW_PCF_DIR = ROOT / "raw" / "pcf"
RAW_DEBUG_DIR = ROOT / "raw" / "debug"

for folder in [DATA_DIR, HISTORY_DIR, RAW_MASTER_DIR, RAW_HOLDINGS_DIR, RAW_PCF_DIR, RAW_DEBUG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Config
# ----------------------------
TAIPEI_TZ = ZoneInfo("Asia/Taipei")

TOP_N_STOCKS = int(os.getenv("TOP_N_STOCKS", "10"))
MIN_TWSE_ETF_MASTER_ROWS = int(os.getenv("MIN_TWSE_ETF_MASTER_ROWS", "50"))

# PCF 覆蓋率門檻。
# 初期接真實來源時，建議先設 0.10 或 0.20，確認主流投信可解析。
# 等你補齊 source registry 後，再提高至 0.80 / 0.90。
MIN_PCF_COVERAGE_RATIO = float(os.getenv("MIN_PCF_COVERAGE_RATIO", "0.10"))
STRICT_PCF_COVERAGE = os.getenv("STRICT_PCF_COVERAGE", "0").strip() == "1"

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_SLEEP_SECONDS = float(os.getenv("HTTP_SLEEP_SECONDS", "0.35"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("etf-pcf-report")


# ----------------------------
# Official / issuer source URLs
# ----------------------------
TWSE_ISIN_CLASS_URL = "https://isin.twse.com.tw/isin/class_main.jsp"
TWSE_ISIN_PUBLIC_LIST_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"

# TWSE ETF product detail page. This page usually provides a PCF link.
TWSE_PRODUCT_CONTENT_URLS = [
    "https://wwwc.twse.com.tw/zh/products/securities/etf/products/content.html?{code}=",
    "https://www.twse.com.tw/zh/products/securities/etf/products/content.html?{code}=",
]

# Optional ETF master enrichment sources.
TWSE_ETF_PRODUCT_LIST_URLS = [
    "https://www.twse.com.tw/zh/products/securities/etf/products/list.html",
    "https://www.twse.com.tw/en/products/securities/etf/products/list.html",
    "https://www.twse.com.tw/zh/ETFortune/products",
    "https://www.twse.com.tw/en/ETFortune-institute/products",
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,application/json",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# ============================================================
# Utility
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


def normalize_code(value: Any) -> str:
    return clean_text(value).upper()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        text = clean_text(value)
        text = text.replace(",", "").replace("%", "").replace("NT$", "").replace("$", "")
        text = re.sub(r"[^0-9.\-]", "", text)
        if text in {"", "-", "--", "N/A", "nan", "None"}:
            return default
        return float(text)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(safe_float(value, default)))
    except Exception:
        return default


def is_etf_code(value: Any) -> bool:
    return bool(re.fullmatch(r"\d{4,6}[A-Z]?", normalize_code(value)))


def is_stock_code(value: Any) -> bool:
    # 只把台股普通股/上市櫃股票納入個股籌碼統計；ETF、債券、期貨、外股先排除。
    return bool(re.fullmatch(r"\d{4}", clean_text(value)))


def is_isin(value: Any) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", clean_text(value)))


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


def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    logger.info("GET %s params=%s", url, params)
    response = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    time.sleep(HTTP_SLEEP_SECONDS)
    return response.text


def debug_save(report_date: str, name: str, content: str, suffix: str = "html") -> None:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", name)[:90]
    path = RAW_DEBUG_DIR / report_date / f"{safe}.{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", errors="ignore")


def looks_like_date(text: Any) -> bool:
    return bool(re.fullmatch(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", clean_text(text)))


def normalize_date(text: Any) -> str:
    text = clean_text(text)
    return text.replace("/", ".").replace("-", ".")


def looks_like_number(text: Any) -> bool:
    return bool(re.fullmatch(r"[-+]?\d[\d,]*(?:\.\d+)?%?", clean_text(text)))


# ============================================================
# TWSE ETF master
# ============================================================
def fetch_twse_etf_master(report_date: str) -> List[Dict[str, Any]]:
    errors: List[str] = []

    try:
        master = fetch_from_isin_class_main(report_date)
        logger.info("ISIN class_main parsed %s ETF rows.", len(master))
    except Exception as exc:
        errors.append(f"class_main: {exc}")
        logger.warning("class_main failed: %s", exc)
        master = []

    if len(master) < MIN_TWSE_ETF_MASTER_ROWS or not contains_0050(master):
        try:
            fallback = fetch_from_isin_public_list(report_date)
            logger.info("ISIN public list parsed %s ETF rows.", len(fallback))
            if len(fallback) > len(master):
                master = fallback
        except Exception as exc:
            errors.append(f"C_public: {exc}")
            logger.warning("C_public failed: %s", exc)

    master = dedupe_master_rows(master)

    if len(master) < MIN_TWSE_ETF_MASTER_ROWS or not contains_0050(master):
        raise RuntimeError(
            "TWSE ETF master validation failed. "
            f"rows={len(master)}, has_0050={contains_0050(master)}, "
            f"min_required={MIN_TWSE_ETF_MASTER_ROWS}, errors={errors}."
        )

    try:
        enrichments = fetch_twse_product_enrichment(report_date)
        master = merge_enrichment(master, enrichments)
    except Exception as exc:
        logger.warning("TWSE product enrichment failed, keep ISIN master only: %s", exc)

    return sorted(dedupe_master_rows(master), key=lambda x: x["etf_code"])


def fetch_from_isin_class_main(report_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_codes: set[str] = set()
    empty_pages = 0

    for page in range(1, 30):
        params = {
            "Page": page,
            "chklike": "Y",
            "market": "1",
            "issuetype": "",
            "industry_code": "",
            "isincode": "",
            "owncode": "",
            "stockname": "",
        }
        html = http_get(TWSE_ISIN_CLASS_URL, params=params)
        debug_save(report_date, f"isin_class_main_page_{page}", html)

        page_rows = parse_isin_class_main_html(html)
        page_rows = [row for row in page_rows if row["etf_code"] not in seen_codes]

        for row in page_rows:
            seen_codes.add(row["etf_code"])

        rows.extend(page_rows)
        logger.info("class_main page=%s parsed_new_rows=%s total=%s", page, len(page_rows), len(rows))

        if not page_rows:
            empty_pages += 1
            if empty_pages >= 2:
                break
        else:
            empty_pages = 0

    return dedupe_master_rows(rows)


def parse_isin_class_main_html(html: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    try:
        tables = pd.read_html(StringIO(html), displayed_only=False)
        for df in tables:
            rows.extend(parse_isin_class_dataframe(df))
    except Exception:
        pass

    if not rows:
        soup = BeautifulSoup(html, "html.parser")
        for tr in soup.find_all("tr"):
            cells = [clean_text(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
            parsed = parse_isin_class_cells(cells)
            if parsed:
                rows.append(parsed)

    return dedupe_master_rows(rows)


def parse_isin_class_dataframe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [clean_text(" ".join(str(x) for x in col if str(x) != "nan")) for col in df.columns]
    else:
        df.columns = [clean_text(c) for c in df.columns]

    if len(df) > 0:
        first_row = [clean_text(x) for x in df.iloc[0].tolist()]
        if any("有價證券代號" in x for x in first_row) or any("ISIN" in x for x in first_row):
            df.columns = first_row
            df = df.iloc[1:].copy()

    for _, row in df.iterrows():
        cells = [clean_text(x) for x in row.tolist()]
        parsed = parse_isin_class_cells(cells)
        if parsed:
            rows.append(parsed)

    return rows


def parse_isin_class_cells(cells: List[str]) -> Optional[Dict[str, Any]]:
    cells = [clean_text(x) for x in cells if clean_text(x)]
    if len(cells) < 7:
        return None

    code = ""
    isin = ""
    for cell in cells:
        if not code and is_etf_code(cell):
            code = normalize_code(cell)
        if not isin and is_isin(cell):
            isin = clean_text(cell)

    if not code:
        return None

    code_idx = next((i for i, x in enumerate(cells) if normalize_code(x) == code), -1)
    name = cells[code_idx + 1] if 0 <= code_idx + 1 < len(cells) else ""

    market = ""
    security_type = ""
    listing_date = ""
    cfi_code = ""
    remark = ""

    if code_idx >= 0:
        maybe_market = cells[code_idx + 2] if code_idx + 2 < len(cells) else ""
        maybe_type = cells[code_idx + 3] if code_idx + 3 < len(cells) else ""
        if "上市" in maybe_market or "上櫃" in maybe_market:
            market = maybe_market
            security_type = maybe_type
            listing_date = cells[code_idx + 5] if code_idx + 5 < len(cells) else ""
            cfi_code = cells[code_idx + 6] if code_idx + 6 < len(cells) else ""
            remark = cells[code_idx + 7] if code_idx + 7 < len(cells) else ""

    joined = " ".join(cells)
    if "ETF" not in joined:
        return None
    if market and "上市" not in market:
        return None
    if security_type and "ETF" not in security_type:
        return None
    if not name or name in {"ETF", "上市"}:
        return None

    return make_master_row(
        etf_code=code,
        etf_name=name,
        listing_date=normalize_date(listing_date),
        issuer=infer_issuer_from_name(name),
        benchmark="",
        market="TWSE",
        source="TWSE ISIN class_main",
        source_url=TWSE_ISIN_CLASS_URL,
        isin=isin,
        security_type=security_type or "ETF",
        cfi_code=cfi_code,
        remark=remark,
    )


def fetch_from_isin_public_list(report_date: str) -> List[Dict[str, Any]]:
    html = http_get(TWSE_ISIN_PUBLIC_LIST_URL)
    debug_save(report_date, "isin_C_public_strMode_2", html)

    rows: List[Dict[str, Any]] = []
    soup = BeautifulSoup(html, "html.parser")
    current_section = ""

    for tr in soup.find_all("tr"):
        cells = [clean_text(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
        cells = [x for x in cells if x]
        if not cells:
            continue

        if len(cells) == 1 and not is_etf_code(cells[0]):
            current_section = cells[0]
            continue

        if len(cells) < 3:
            continue

        code, name = split_code_name(cells[0])
        if not code or not name:
            continue

        joined = " ".join(cells)
        if current_section != "ETF" and "ETF" not in joined:
            continue

        isin = next((x for x in cells if is_isin(x)), "")
        listing_date = cells[2] if len(cells) > 2 else ""
        cfi_code = cells[5] if len(cells) > 5 else ""
        remark = cells[6] if len(cells) > 6 else ""

        rows.append(
            make_master_row(
                etf_code=code,
                etf_name=name,
                listing_date=normalize_date(listing_date),
                issuer=infer_issuer_from_name(name),
                benchmark="",
                market="TWSE",
                source="TWSE ISIN C_public",
                source_url=TWSE_ISIN_PUBLIC_LIST_URL,
                isin=isin,
                security_type="ETF",
                cfi_code=cfi_code,
                remark=remark,
            )
        )

    return dedupe_master_rows(rows)


def split_code_name(text: str) -> Tuple[str, str]:
    text = clean_text(text)
    match = re.match(r"^(\d{4,6}[A-Z]?)\s+(.+)$", text)
    if not match:
        return "", ""
    code = normalize_code(match.group(1))
    name = clean_text(match.group(2))
    return (code, name) if is_etf_code(code) else ("", "")


def make_master_row(
    etf_code: str,
    etf_name: str,
    listing_date: str,
    issuer: str,
    benchmark: str,
    market: str,
    source: str,
    source_url: str,
    isin: str = "",
    security_type: str = "ETF",
    cfi_code: str = "",
    remark: str = "",
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
        "isin": clean_text(isin),
        "listing_date": clean_text(listing_date),
        "benchmark": clean_text(benchmark),
        "issuer": clean_text(issuer) or infer_issuer_from_name(etf_name),
        "market": market,
        "security_type": security_type,
        "source": source,
        "source_url": source_url,
        "category": clean_text(category),
        "aum_yi": round(float(aum_yi or 0.0), 4),
        "close": round(float(close or 0.0), 4),
        "avg_daily_trading_value_ytd_million": round(float(avg_daily_trading_value_ytd_million or 0.0), 4),
        "avg_daily_trading_volume_ytd_shares": int(avg_daily_trading_volume_ytd_shares or 0),
        "beneficiaries": int(beneficiaries or 0),
        "top10_weight_pct": 0.0,
        "cfi_code": clean_text(cfi_code),
        "remark": clean_text(remark),
    }


def dedupe_master_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        row = dict(row)
        code = normalize_code(row.get("etf_code"))
        if not is_etf_code(code):
            continue

        row["etf_code"] = code
        row["etf_name"] = clean_text(row.get("etf_name", ""))
        if not row["etf_name"]:
            continue

        if code not in best:
            best[code] = row
            continue

        if row_quality_score(row) > row_quality_score(best[code]):
            merged = {**best[code], **row}
            for key in best[code]:
                if not merged.get(key) and best[code].get(key):
                    merged[key] = best[code][key]
            best[code] = merged
        else:
            for key, value in row.items():
                if not best[code].get(key) and value:
                    best[code][key] = value

    return sorted(best.values(), key=lambda x: x["etf_code"])


def row_quality_score(row: Dict[str, Any]) -> int:
    score = 0
    for key in ["etf_name", "isin", "listing_date", "issuer", "benchmark", "security_type", "cfi_code", "remark"]:
        if row.get(key):
            score += 2
    for key in ["aum_yi", "close", "beneficiaries", "avg_daily_trading_value_ytd_million"]:
        if safe_float(row.get(key)) > 0:
            score += 4
    if "class_main" in row.get("source", ""):
        score += 5
    if "C_public" in row.get("source", ""):
        score += 3
    return score


def contains_0050(rows: List[Dict[str, Any]]) -> bool:
    return any(row.get("etf_code") == "0050" for row in rows)


def infer_issuer_from_name(etf_name: str) -> str:
    mapping = [
        ("元大", "元大投信"),
        ("富邦", "富邦投信"),
        ("國泰", "國泰投信"),
        ("群益", "群益投信"),
        ("復華", "復華投信"),
        ("永豐", "永豐投信"),
        ("凱基", "凱基投信"),
        ("中信", "中國信託投信"),
        ("台新", "台新投信"),
        ("統一", "統一投信"),
        ("兆豐", "兆豐投信"),
        ("野村", "野村投信"),
        ("第一金", "第一金投信"),
        ("大華", "大華銀投信"),
        ("街口", "街口投信"),
        ("玉山", "玉山投信"),
        ("聯博", "聯博投信"),
        ("貝萊德", "貝萊德投信"),
        ("摩根", "摩根投信"),
        ("Yuanta", "元大投信"),
        ("Fubon", "富邦投信"),
        ("Cathay", "國泰投信"),
        ("Capital", "群益投信"),
        ("Fuh Hwa", "復華投信"),
        ("SinoPac", "永豐投信"),
        ("KGI", "凱基投信"),
        ("CTBC", "中國信託投信"),
        ("Taishin", "台新投信"),
        ("Uni-President", "統一投信"),
        ("Mega", "兆豐投信"),
        ("Nomura", "野村投信"),
        ("First", "第一金投信"),
        ("UOB", "大華銀投信"),
        ("JKO", "街口投信"),
        ("E.SUN", "玉山投信"),
        ("BlackRock", "貝萊德投信"),
        ("J.P. Morgan", "摩根投信"),
        ("AllianceBernstein", "聯博投信"),
    ]
    lower = etf_name.lower()
    for key, issuer in mapping:
        if key.lower() in lower:
            return issuer
    return ""


# ============================================================
# Optional master enrichment
# ============================================================
def fetch_twse_product_enrichment(report_date: str) -> Dict[str, Dict[str, Any]]:
    enrich: Dict[str, Dict[str, Any]] = {}

    for url in TWSE_ETF_PRODUCT_LIST_URLS:
        try:
            html = http_get(url)
            debug_save(report_date, f"enrich_{url_to_name(url)}", html)
            rows = parse_product_or_etfortune_enrichment(html, url)
            for row in rows:
                code = row["etf_code"]
                if code not in enrich or row_quality_score(row) > row_quality_score(enrich[code]):
                    enrich[code] = row
            logger.info("Enrichment %s parsed %s rows.", url, len(rows))
        except Exception as exc:
            logger.warning("Enrichment source failed %s: %s", url, exc)

    return enrich


def url_to_name(url: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", url)[-70:]


def parse_product_or_etfortune_enrichment(html: str, source_url: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    try:
        tables = pd.read_html(StringIO(html), displayed_only=False)
        for df in tables:
            rows.extend(parse_enrichment_dataframe(df, source_url))
    except Exception:
        pass

    return dedupe_master_rows(rows)


def parse_enrichment_dataframe(df: pd.DataFrame, source_url: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    df = flatten_df_columns(df)

    code_col = find_col(df, ["股票代號", "ETF Code", "證券代號"])
    name_col = find_col(df, ["ETF名稱", "ETF Name", "證券簡稱", "名稱"])
    listing_col = find_col(df, ["上市日期", "Listing Date"])
    benchmark_col = find_col(df, ["標的指數", "Benchmark"])
    issuer_col = find_col(df, ["發行人", "Issuer"])
    aum_col = find_col(df, ["資產規模", "AUM"])
    close_col = find_col(df, ["收盤價", "Closing Price"])
    value_col = find_col(df, ["成交值", "Trading Value"])
    volume_col = find_col(df, ["成交量", "Trading Volume"])
    beneficiary_col = find_col(df, ["受益人", "Beneficiary"])

    if not code_col or not name_col:
        return []

    for _, row in df.iterrows():
        code = normalize_code(row.get(code_col))
        name = clean_text(row.get(name_col))
        if not is_etf_code(code) or not name:
            continue

        rows.append(
            make_master_row(
                etf_code=code,
                etf_name=name,
                listing_date=normalize_date(row.get(listing_col, "")) if listing_col else "",
                issuer=clean_text(row.get(issuer_col, "")) if issuer_col else infer_issuer_from_name(name),
                benchmark=clean_text(row.get(benchmark_col, "")) if benchmark_col else "",
                market="TWSE",
                source="TWSE ETF product enrichment",
                source_url=source_url,
                aum_yi=safe_float(row.get(aum_col)) if aum_col else 0.0,
                close=safe_float(row.get(close_col)) if close_col else 0.0,
                beneficiaries=safe_int(row.get(beneficiary_col)) if beneficiary_col else 0,
                avg_daily_trading_value_ytd_million=safe_float(row.get(value_col)) if value_col else 0.0,
                avg_daily_trading_volume_ytd_shares=safe_int(row.get(volume_col)) if volume_col else 0,
            )
        )

    return rows


def merge_enrichment(master: List[Dict[str, Any]], enrich: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []

    for row in master:
        code = row["etf_code"]
        extra = enrich.get(code)
        if not extra:
            output.append(row)
            continue

        merged = dict(row)
        for key in [
            "etf_name",
            "listing_date",
            "benchmark",
            "issuer",
            "category",
            "aum_yi",
            "close",
            "avg_daily_trading_value_ytd_million",
            "avg_daily_trading_volume_ytd_shares",
            "beneficiaries",
        ]:
            if not merged.get(key) and extra.get(key):
                merged[key] = extra[key]
            elif key in ["aum_yi", "close", "avg_daily_trading_value_ytd_million", "avg_daily_trading_volume_ytd_shares", "beneficiaries"]:
                if safe_float(merged.get(key)) == 0 and safe_float(extra.get(key)) != 0:
                    merged[key] = extra[key]
        output.append(merged)

    return dedupe_master_rows(output)


# ============================================================
# PCF source discovery
# ============================================================
def load_manual_pcf_registry() -> Dict[str, str]:
    """
    Optional file:
      data/pcf_source_registry.json

    Format:
      {
        "0050": "https://www.yuantaetfs.com/tradeInfo/pcf/0050",
        "006208": "https://websys.fsit.com.tw/FubonETF/Trade/Pcf.aspx?lan=TW&stkId=006208"
      }

    手動 registry 的優先權最高。若某家投信網站需要特殊 URL，直接補這個檔案。
    """
    path = DATA_DIR / "pcf_source_registry.json"
    if not path.exists():
        return {}

    data = read_json(path)
    if not isinstance(data, dict):
        return {}

    return {normalize_code(k): clean_text(v) for k, v in data.items() if clean_text(v)}


def discover_pcf_url_from_twse_product_page(etf_code: str, report_date: str) -> Optional[str]:
    for template in TWSE_PRODUCT_CONTENT_URLS:
        url = template.format(code=etf_code)
        try:
            html = http_get(url)
            debug_save(report_date, f"twse_product_{etf_code}", html)
            found = extract_pcf_link_from_html(html, base_url=url)
            if found:
                logger.info("TWSE product page discovered PCF url for %s: %s", etf_code, found)
                return found
        except Exception as exc:
            logger.warning("TWSE product page discovery failed for %s url=%s: %s", etf_code, url, exc)
    return None


def extract_pcf_link_from_html(html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    candidates: List[str] = []
    keywords = [
        "申購買回清單",
        "申購買回清單PCF",
        "PCF",
        "pcf",
        "buyback",
        "purchase",
        "Pcf.aspx",
        "tradeInfo/pcf",
        "trade_list",
        "purchase?code",
    ]

    for a in soup.find_all("a", href=True):
        href = clean_text(a.get("href", ""))
        text = clean_text(a.get_text(" ", strip=True))
        joined = f"{href} {text}"
        if any(k in joined for k in keywords):
            candidates.append(urljoin(base_url, href))

    # Also parse onclick / data-url.
    for tag in soup.find_all(True):
        for attr in ["onclick", "data-url", "data-href", "data-link"]:
            val = clean_text(tag.get(attr, ""))
            if not val:
                continue
            if any(k in val for k in keywords):
                urls = re.findall(r"https?://[^'\"\s)]+", val)
                candidates.extend(urls)

    cleaned = []
    for c in candidates:
        if c and c not in cleaned:
            cleaned.append(c)

    # Prefer real issuer URLs over TWSE anchor.
    for c in cleaned:
        if not any(domain in c for domain in ["twse.com.tw", "wwwc.twse.com.tw"]):
            return c

    return cleaned[0] if cleaned else None


def issuer_fallback_urls(etf: Dict[str, Any]) -> List[str]:
    """
    Known issuer URL patterns. These are real issuer websites, not sample.
    Not every issuer uses a code-only URL; for those, TWSE product page discovery or
    data/pcf_source_registry.json is required.
    """
    code = etf["etf_code"]
    issuer_name = f"{etf.get('issuer','')} {etf.get('etf_name','')}"
    urls: List[str] = []

    if contains_any(issuer_name, ["元大", "Yuanta"]):
        urls.append(f"https://www.yuantaetfs.com/tradeInfo/pcf/{code}")

    if contains_any(issuer_name, ["富邦", "Fubon"]):
        urls.append(f"https://websys.fsit.com.tw/FubonETF/Trade/Pcf.aspx?lan=TW&stkId={code}")

    if contains_any(issuer_name, ["國泰", "Cathay"]):
        # 國泰網站常用 internal code；若 TWSE product page 找不到，base purchase page 仍可解析預設 ETF。
        urls.append("https://www.cathaysite.com.tw/ETF/purchase")

    if contains_any(issuer_name, ["群益", "Capital"]):
        # 群益常用 /etf/product/detail/{id}/buyback；id 需靠 TWSE page 或手動 registry 發現。
        urls.append("https://www.capitalfund.com.tw/etf/transaction/buyback")

    if contains_any(issuer_name, ["復華", "Fuh Hwa"]):
        urls.append("https://www.fhtrust.com.tw/ETF/trade_list")

    if contains_any(issuer_name, ["永豐", "SinoPac"]):
        urls.append("https://sitc.sinopac.com/SinopacEtfs/Etfs/Pcf")

    return urls


def contains_any(text: str, needles: List[str]) -> bool:
    low = text.lower()
    return any(n.lower() in low for n in needles)


def resolve_pcf_url(etf: Dict[str, Any], manual_registry: Dict[str, str], report_date: str) -> Optional[str]:
    code = etf["etf_code"]

    if manual_registry.get(code):
        return manual_registry[code]

    discovered = discover_pcf_url_from_twse_product_page(code, report_date)
    if discovered:
        return discovered

    fallbacks = issuer_fallback_urls(etf)
    return fallbacks[0] if fallbacks else None


# ============================================================
# PCF parsing
# ============================================================
def fetch_all_pcf_holdings(master: List[Dict[str, Any]], report_date: str) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    manual_registry = load_manual_pcf_registry()
    today_holdings: Dict[str, Dict[str, Any]] = {}
    statuses: List[Dict[str, Any]] = []

    for idx, etf in enumerate(master, 1):
        code = etf["etf_code"]
        name = etf.get("etf_name", "")
        issuer = etf.get("issuer", "")

        logger.info("[%s/%s] Fetch PCF for %s %s", idx, len(master), code, name)

        pcf_url = resolve_pcf_url(etf, manual_registry, report_date)
        if not pcf_url:
            statuses.append(
                {
                    "etf_code": code,
                    "etf_name": name,
                    "issuer": issuer,
                    "status": "no_pcf_url",
                    "pcf_url": "",
                    "holdings_count": 0,
                    "error": "No PCF URL discovered. Add data/pcf_source_registry.json entry.",
                }
            )
            continue

        try:
            payload = fetch_one_pcf(etf, pcf_url, report_date)
            holdings = payload.get("holdings", [])

            if not holdings:
                statuses.append(
                    {
                        "etf_code": code,
                        "etf_name": name,
                        "issuer": issuer,
                        "status": "parsed_zero_holdings",
                        "pcf_url": pcf_url,
                        "holdings_count": 0,
                        "error": "PCF page fetched but no Taiwan stock holdings parsed.",
                    }
                )
                continue

            save_holding_snapshot(report_date, code, payload)
            today_holdings[code] = payload

            statuses.append(
                {
                    "etf_code": code,
                    "etf_name": name,
                    "issuer": issuer,
                    "status": "ok",
                    "pcf_url": pcf_url,
                    "holdings_count": len(holdings),
                    "error": "",
                }
            )

        except Exception as exc:
            logger.warning("PCF fetch failed for %s %s: %s", code, pcf_url, exc)
            statuses.append(
                {
                    "etf_code": code,
                    "etf_name": name,
                    "issuer": issuer,
                    "status": "error",
                    "pcf_url": pcf_url,
                    "holdings_count": 0,
                    "error": str(exc),
                }
            )

    atomic_write_json(DATA_DIR / "pcf_fetch_status.json", statuses)
    atomic_write_json(RAW_PCF_DIR / report_date / "pcf_fetch_status.json", statuses)

    return today_holdings, statuses


def fetch_one_pcf(etf: Dict[str, Any], pcf_url: str, report_date: str) -> Dict[str, Any]:
    code = etf["etf_code"]
    html = http_get(pcf_url)

    debug_save(report_date, f"pcf_{code}_{url_to_name(pcf_url)}", html)

    holdings = parse_holdings_from_pcf_html(html, pcf_url)
    meta = parse_pcf_meta_from_html(html)

    return {
        "trade_date": meta.get("trade_date") or report_date,
        "etf_code": code,
        "etf_name": etf.get("etf_name", ""),
        "issuer": etf.get("issuer", ""),
        "market": etf.get("market", "TWSE"),
        "source": "issuer_pcf",
        "source_url": pcf_url,
        "pcf_meta": meta,
        "holdings": holdings,
    }


def parse_pcf_meta_from_html(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    dates = re.findall(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}", text)
    trade_date = normalize_date(dates[0]) if dates else ""

    def find_number_after(label_patterns: List[str]) -> float:
        for label in label_patterns:
            pattern = label + r".{0,40}?([-+]?\$?NT\$?\s*[\d,]+(?:\.\d+)?)"
            m = re.search(pattern, text)
            if m:
                return safe_float(m.group(1))
        return 0.0

    return {
        "trade_date": trade_date,
        "fund_nav": find_number_after(["基金淨資產價值", "Fund Net Asset Value"]),
        "outstanding_units": find_number_after(["已發行受益權單位總數", "Total Outstanding Shares"]),
        "unit_diff": find_number_after(["與前日已發行單位差異數", "Net Change in Outstanding Shares"]),
        "nav_per_unit": find_number_after(["每受益權單位淨資產價值", "NAV Per Share"]),
        "creation_unit": find_number_after(["每.*?申購.*?基數之受益權單位數", "Creation/Redemption Units"]),
    }


def parse_holdings_from_pcf_html(html: str, source_url: str) -> List[Dict[str, Any]]:
    holdings: List[Dict[str, Any]] = []

    # 1) HTML tables
    try:
        tables = pd.read_html(StringIO(html), displayed_only=False)
        for idx, df in enumerate(tables):
            parsed = parse_holdings_dataframe(df, source_url, f"table_{idx}")
            holdings.extend(parsed)
    except Exception as exc:
        logger.info("No parseable PCF tables from %s: %s", source_url, exc)

    # 2) Text fallback
    if not holdings:
        holdings.extend(parse_holdings_text_fallback(html, source_url))

    return dedupe_holdings(holdings)


def flatten_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [clean_text(" ".join(str(x) for x in col if str(x) != "nan")) for col in df.columns]
    else:
        df.columns = [clean_text(c) for c in df.columns]

    if len(df) > 0:
        first_row = [clean_text(x) for x in df.iloc[0].tolist()]
        if any("股票代號" in x for x in first_row) or any("股數" in x for x in first_row) or any("持股權重" in x for x in first_row):
            df.columns = first_row
            df = df.iloc[1:].copy()

    return df


def find_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    for col in df.columns:
        col_l = clean_text(col).lower()
        for name in names:
            if name.lower() in col_l:
                return col
    return None


def parse_holdings_dataframe(df: pd.DataFrame, source_url: str, table_name: str) -> List[Dict[str, Any]]:
    df = flatten_df_columns(df)
    rows: List[Dict[str, Any]] = []

    code_col = find_col(df, ["股票代號", "證券代號", "成分股代號", "Stock Code", "代號"])
    name_col = find_col(df, ["股票名稱", "證券名稱", "成分股名稱", "Stock Name", "名稱"])
    weight_col = find_col(df, ["持股權重", "權重", "比重", "Weight"])
    shares_col = find_col(df, ["股數", "持有股數", "數量", "Shares", "Units"])
    price_col = find_col(df, ["收盤價", "價格", "Price", "Close"])
    value_col = find_col(df, ["市值", "金額", "Market Value", "Value"])

    # Named columns path
    if code_col:
        for _, row in df.iterrows():
            code = clean_text(row.get(code_col))
            if not is_stock_code(code):
                continue

            stock_name = clean_text(row.get(name_col)) if name_col else infer_stock_name_from_row(row)
            shares = safe_float(row.get(shares_col)) if shares_col else infer_shares_from_row(row)
            weight = safe_float(row.get(weight_col)) if weight_col else infer_weight_from_row(row)
            price = safe_float(row.get(price_col)) if price_col else 0.0
            market_value = safe_float(row.get(value_col)) if value_col else 0.0

            if shares <= 0 and market_value <= 0:
                continue

            if price <= 0 and shares > 0 and market_value > 0:
                price = market_value / shares

            rows.append(
                {
                    "stock_code": code,
                    "stock_name": stock_name,
                    "shares": round(shares, 4),
                    "weight_pct": round(weight, 6),
                    "close": round(price, 6),
                    "market_value": round(market_value, 4),
                    "source_table": table_name,
                }
            )

        if rows:
            return rows

    # Loose row scan path
    for _, row in df.iterrows():
        cells = [clean_text(x) for x in row.tolist()]
        cells = [x for x in cells if x and x.lower() != "nan"]
        if not cells:
            continue

        code_idx = next((i for i, x in enumerate(cells) if is_stock_code(x)), None)
        if code_idx is None:
            continue

        code = cells[code_idx]
        stock_name = cells[code_idx + 1] if code_idx + 1 < len(cells) else ""

        numeric_after = [safe_float(x) for x in cells[code_idx + 2:] if looks_like_number(x)]
        pct_candidates = [safe_float(x) for x in cells[code_idx + 2:] if "%" in x or 0 < safe_float(x) <= 100]

        weight = pct_candidates[0] if pct_candidates else 0.0
        shares = max(numeric_after) if numeric_after else 0.0

        # Avoid treating market value as shares when both appear. If a table gives huge numbers,
        # this may still require issuer-specific adjustment. The named-column path handles most cases.
        if shares <= 0:
            continue

        rows.append(
            {
                "stock_code": code,
                "stock_name": stock_name,
                "shares": round(shares, 4),
                "weight_pct": round(weight, 6),
                "close": 0.0,
                "market_value": 0.0,
                "source_table": table_name,
            }
        )

    return rows


def infer_stock_name_from_row(row: pd.Series) -> str:
    for value in row.tolist():
        text = clean_text(value)
        if text and not is_stock_code(text) and not looks_like_number(text) and "股票" not in text and "權重" not in text:
            return text
    return ""


def infer_shares_from_row(row: pd.Series) -> float:
    nums = [safe_float(x) for x in row.tolist() if looks_like_number(x)]
    nums = [x for x in nums if x > 1000]
    return max(nums) if nums else 0.0


def infer_weight_from_row(row: pd.Series) -> float:
    for x in row.tolist():
        text = clean_text(x)
        val = safe_float(text)
        if "%" in text and 0 <= val <= 100:
            return val
    return 0.0


def parse_holdings_text_fallback(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    lines = [clean_text(x) for x in soup.get_text("\n", strip=True).splitlines()]
    lines = [x for x in lines if x]

    rows = []
    for i, line in enumerate(lines):
        if not is_stock_code(line):
            continue

        code = line
        stock_name = lines[i + 1] if i + 1 < len(lines) else ""
        numeric_next = [safe_float(x) for x in lines[i + 2:i + 10] if looks_like_number(x)]
        if not numeric_next:
            continue

        shares = max([x for x in numeric_next if x > 1000] or [0])
        weight_candidates = [x for x in numeric_next if 0 < x <= 100]
        weight = weight_candidates[0] if weight_candidates else 0.0

        if shares <= 0:
            continue

        rows.append(
            {
                "stock_code": code,
                "stock_name": stock_name,
                "shares": round(shares, 4),
                "weight_pct": round(weight, 6),
                "close": 0.0,
                "market_value": 0.0,
                "source_table": "text_fallback",
            }
        )

    return rows


def dedupe_holdings(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        code = clean_text(row.get("stock_code"))
        if is_stock_code(code):
            grouped[code].append(row)

    output = []
    for code, items in grouped.items():
        # Choose row with best data quality.
        def score(x: Dict[str, Any]) -> float:
            return (
                (1 if x.get("stock_name") else 0)
                + (2 if safe_float(x.get("shares")) > 0 else 0)
                + (2 if safe_float(x.get("market_value")) > 0 else 0)
                + (1 if safe_float(x.get("weight_pct")) > 0 else 0)
                + (1 if safe_float(x.get("close")) > 0 else 0)
            )

        best = max(items, key=score)
        output.append(best)

    return sorted(output, key=lambda x: x["stock_code"])


def save_holding_snapshot(report_date: str, etf_code: str, payload: Dict[str, Any]) -> None:
    atomic_write_json(RAW_HOLDINGS_DIR / report_date / f"{etf_code}.json", payload)


# ============================================================
# Existing holdings snapshots and diffs
# ============================================================
def load_holdings_snapshot(trade_date: str) -> Dict[str, Dict[str, Any]]:
    folder = RAW_HOLDINGS_DIR / trade_date
    if not folder.exists():
        return {}

    output: Dict[str, Dict[str, Any]] = {}
    for path in sorted(folder.glob("*.json")):
        try:
            payload = read_json(path)
            code = normalize_code(payload.get("etf_code", path.stem))
            output[code] = payload
        except Exception as exc:
            logger.warning("Failed to read holdings snapshot %s: %s", path, exc)
    return output


def previous_available_holding_date(report_date: str, max_lookback_days: int = 10) -> Optional[str]:
    current = datetime.strptime(report_date, "%Y-%m-%d").date()
    for i in range(1, max_lookback_days + 1):
        candidate = (current - timedelta(days=i)).strftime("%Y-%m-%d")
        folder = RAW_HOLDINGS_DIR / candidate
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

            today_mv = safe_float(today_row.get("market_value", 0))
            prev_mv = safe_float(prev_row.get("market_value", 0))
            price = safe_float(today_row.get("close", 0)) or safe_float(prev_row.get("close", 0))

            if today_mv and prev_mv:
                delta_value_yi = (today_mv - prev_mv) / 100_000_000
            else:
                delta_value_yi = delta_shares * price / 100_000_000

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
                    "delta_value_yi": round(delta_value_yi, 4),
                    "weight_delta_pct": round(today_weight - prev_weight, 4),
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


def aggregate_stock_changes(diffs: List[Dict[str, Any]], top_n: Optional[int] = None) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in diffs:
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

    rankings: List[Dict[str, Any]] = []
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


def build_kpis(all_changes: List[Dict[str, Any]], top_changes: List[Dict[str, Any]]) -> Dict[str, Any]:
    net = sum(safe_float(x["delta_value_yi"]) for x in all_changes)
    consensus_buy = sum(1 for x in all_changes if safe_float(x["delta_value_yi"]) > 0 and safe_int(x.get("etf_count")) >= 2)
    consensus_sell = sum(1 for x in all_changes if safe_float(x["delta_value_yi"]) < 0 and safe_int(x.get("etf_count")) >= 2)

    total_abs = sum(abs(safe_float(x["delta_value_yi"])) for x in all_changes) or 1.0
    top3_abs = sum(abs(safe_float(x["delta_value_yi"])) for x in top_changes[:3])
    concentration_score = round(top3_abs / total_abs * 100) if all_changes else 0

    return {
        "net_change_value_yi": round(net, 1),
        "consensus_buy_count": consensus_buy,
        "consensus_sell_count": consensus_sell,
        "concentration_score": concentration_score,
    }


def build_stock_radar(top_changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []
    for item in top_changes:
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


def holdings_quality(master_count: int, today_holdings_count: int) -> Dict[str, Any]:
    ratio = today_holdings_count / master_count if master_count else 0.0
    return {
        "tracked_etfs": master_count,
        "covered_etfs": today_holdings_count,
        "coverage_ratio": round(ratio, 4),
        "min_required_coverage_ratio": MIN_PCF_COVERAGE_RATIO,
        "is_ready": ratio >= MIN_PCF_COVERAGE_RATIO,
    }


def build_events(top_changes: List[Dict[str, Any]], quality: Dict[str, Any], master_count: int, pcf_statuses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

    ok_count = sum(1 for x in pcf_statuses if x.get("status") == "ok")
    events.append(
        {
            "time": "資料檢查",
            "title": "TWSE ETF Master 已成功更新",
            "desc": f"本次從 TWSE ISIN 官方資料解析並驗證 {master_count} 檔上市 ETF master。",
        }
    )
    events.append(
        {
            "time": "資料檢查",
            "title": "PCF 持股快照覆蓋率",
            "desc": f"今日已成功解析 {ok_count}/{quality['tracked_etfs']} 檔 ETF PCF，覆蓋率 {quality['coverage_ratio']:.1%}。",
        }
    )
    return events


def build_ai_report(top_changes: List[Dict[str, Any]], kpis: Dict[str, Any], master_count: int, quality: Dict[str, Any]) -> Dict[str, Any]:
    if not top_changes:
        return {
            "headline": f"TWSE ETF master 已更新，今日成功解析 PCF 覆蓋率 {quality['coverage_ratio']:.1%}。",
            "summary": (
                "目前系統已接上真實 PCF crawler，不再使用 sample holdings。"
                "若前十大個股變動仍為空，常見原因是：第一天尚無前一交易日 raw/holdings 快照可比較，"
                "或目前成功解析的 PCF ETF 數量不足。請等下一個交易日或補齊 data/pcf_source_registry.json。"
            ),
            "watchlist": [],
            "risk": "PCF 來自各投信網站，格式可能調整；請查看 data/pcf_fetch_status.json 確認覆蓋率與錯誤原因。",
        }

    net = safe_float(kpis["net_change_value_yi"])
    bias = "偏多" if net > 0 else "偏空" if net < 0 else "中性"

    watchlist = [
        f"{x['stock_code']} {x['stock_name']}：ETF {'加碼' if safe_float(x['delta_value_yi']) > 0 else '減碼'} {abs(safe_float(x['delta_value_yi'])):.2f} 億，參與 ETF {x['etf_count']} 檔"
        for x in top_changes[:5]
    ]

    return {
        "headline": f"今日全市場 ETF 籌碼{bias}，前十大變動淨額 {net:.1f} 億元。",
        "summary": (
            f"本報告以 TWSE ETF master 與各投信 PCF 持股快照計算。"
            f"今日 PCF 覆蓋率為 {quality['coverage_ratio']:.1%}，"
            f"前十大變動集中度為 {kpis['concentration_score']}%。"
        ),
        "watchlist": watchlist,
        "risk": "若 PCF 覆蓋率未達 100%，請避免將報告視為完整市場結論。",
    }


def build_data_sources() -> List[Dict[str, Any]]:
    return [
        {
            "name": "TWSE ISIN 證券編碼分類查詢",
            "type": "ETF master",
            "update_freq": "依 TWSE 官方資料更新",
            "status": "ready",
            "fields": ["isin", "etf_code", "etf_name", "market", "security_type", "listing_date", "cfi_code"],
        },
        {
            "name": "TWSE ETF 商品資訊頁",
            "type": "PCF link discovery",
            "update_freq": "依 TWSE 官方頁面更新",
            "status": "ready",
            "fields": ["etf_code", "issuer_pcf_url"],
        },
        {
            "name": "各投信 PCF / 每日持股揭露",
            "type": "每日持股核心資料",
            "update_freq": "每日盤前或盤後",
            "status": "ready",
            "fields": ["stock_code", "stock_name", "shares", "weight_pct", "market_value"],
        },
        {
            "name": "raw/holdings snapshot",
            "type": "本系統標準化後持股快照",
            "update_freq": "每日",
            "status": "ready",
            "fields": ["trade_date", "etf_code", "stock_code", "shares", "weight_pct", "close", "market_value"],
        },
    ]


# ============================================================
# Report
# ============================================================
def save_master(report_date: str, master: List[Dict[str, Any]]) -> None:
    atomic_write_json(DATA_DIR / "etf_master_latest.json", master)
    atomic_write_json(RAW_MASTER_DIR / report_date / "twse_etf_master.json", master)


def build_report() -> Dict[str, Any]:
    now = now_taipei()
    report_date = date_str(now.date())

    master = fetch_twse_etf_master(report_date)
    save_master(report_date, master)

    today_holdings, pcf_statuses = fetch_all_pcf_holdings(master, report_date)

    quality = holdings_quality(len(master), len(today_holdings))
    if STRICT_PCF_COVERAGE and not quality["is_ready"]:
        raise RuntimeError(
            f"PCF coverage too low: {quality['coverage_ratio']:.1%}, "
            f"min_required={MIN_PCF_COVERAGE_RATIO:.1%}. "
            "See data/pcf_fetch_status.json."
        )

    prev_date = previous_available_holding_date(report_date)
    prev_holdings = load_holdings_snapshot(prev_date) if prev_date else {}

    diffs = calculate_etf_stock_diffs(today_holdings, prev_holdings) if today_holdings and prev_holdings else []
    all_stock_changes = aggregate_stock_changes(diffs, top_n=None)
    top_stock_changes = all_stock_changes[:TOP_N_STOCKS]
    kpis = build_kpis(all_stock_changes, top_stock_changes)

    net = safe_float(kpis["net_change_value_yi"])
    if top_stock_changes:
        market_bias = "偏多" if net > 0 else "偏空" if net < 0 else "中性"
    else:
        market_bias = "PCF 已更新"

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
            "snapshot_mode": "production_issuer_pcf",
            "previous_snapshot_date": prev_date or "",
            "sample_mode": False,
        },
        "data_quality": quality,
        "kpis": kpis,
        "top_stock_changes": top_stock_changes,
        "stock_radar": build_stock_radar(top_stock_changes),
        "etf_rankings": build_etf_rankings(master, diffs),
        "events": build_events(top_stock_changes, quality, len(master), pcf_statuses),
        "data_sources": build_data_sources(),
        "ai_report": build_ai_report(top_stock_changes, kpis, len(master), quality),
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
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
