#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股法人籌碼 — 資料抓取與看板產生
====================================
由 GitHub Actions 每個交易日盤後自動執行，也可在本機手動執行。

產出檔案（全部放在 docs/data/，由 GitHub Pages 直接對外提供）：
  days/YYYY-MM-DD.json   每個交易日的完整資料（壓縮陣列格式）
  board/YYYY-MM-DD.json  該日的排行榜 + 連續天數 + 近 20 日走勢
  index.json             可用日期清單與更新時間

資料來源（官方免費公開 API，無需金鑰）：
  上市 TWSE https://www.twse.com.tw/rwd/zh/fund/T86
  上櫃 TPEX https://www.tpex.org.tw/openapi/

只使用 Python 標準函式庫。

用法：
  python scripts/fetch_flows.py update              # 補齊近 14 天缺漏（每日排程用）
  python scripts/fetch_flows.py backfill --days 130 # 首次回補歷史
  python scripts/fetch_flows.py rebuild             # 不連網，只用現有資料重算看板
  python scripts/fetch_flows.py discover-tpex       # 列出櫃買可用端點（除錯用）
"""

import argparse
import datetime as dt
import glob
import gzip
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------- 設定

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "docs", "data")
DAYS_DIR = os.path.join(DATA_DIR, "days")
BOARD_DIR = os.path.join(DATA_DIR, "board")
INDEX_PATH = os.path.join(DATA_DIR, "index.json")

TOP_N = 10            # 排行榜取前幾名
STREAK_LOOKBACK = 250 # 計算連續天數時最多往回看幾個交易日
SPARK_DAYS = 20       # 走勢圖天數
BOARD_REBUILD = 30    # 每次執行重算最近幾天的看板
REQUEST_PAUSE = 1.2   # 請求間隔秒數，避免被官方限流

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# 櫃買中心近年改版數次，放多組候選端點依序嘗試。
TPEX_CANDIDATES = [
    ("https://www.tpex.org.tw/openapi/v1/tpex_3itrade_hedge", "openapi"),
    ("https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading", "openapi"),
    ("https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
     "?type=Daily&sect=EW&date={ymd_slash}&response=json", "tabular"),
    ("https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
     "?l=zh-tw&se=EW&t=D&d={roc_slash}&o=json", "tabular"),
]


# ---------------------------------------------------------------- 工具

def log(msg):
    print("[%s] %s" % (dt.datetime.now().strftime("%H:%M:%S"), msg), flush=True)


def to_int(s):
    if s is None:
        return 0
    s = str(s).strip().replace(",", "").replace(" ", "")
    if s in ("", "--", "-", "N/A"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


RETRY_CODES = (429, 500, 502, 503, 504, 520, 521, 522, 524)


def http_get_json(url, timeout=40, retries=2):
    """
    取得 JSON。遇到暫時性錯誤（Cloudflare 520 之類）會自動重試。
    櫃買中心的 520 相當常見，不重試的話大約 1 成的日期會漏掉上櫃資料。
    """
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9",
        "Accept-Encoding": "gzip",
        "Referer": "https://www.twse.com.tw/" if "twse" in url else "https://www.tpex.org.tw/",
    })
    last = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
            text = raw.decode("utf-8-sig", errors="replace").strip()
            if not text:
                raise ValueError("回應為空")
            return json.loads(text)
        except urllib.error.HTTPError as e:
            last = e
            if e.code not in RETRY_CODES or attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            last = e
            if attempt == retries:
                raise
        time.sleep(2.0 * (attempt + 1))    # 2 秒、4 秒後再試
    raise last


def is_common_stock(code):
    """排除 ETF / ETN / 權證 / 特別股。普通股一律 4 碼數字，ETF 以 00 開頭。"""
    code = (code or "").strip()
    return len(code) == 4 and code.isdigit() and not code.startswith("00")


def normalize_any_date(s):
    """2025-06-12 / 2025/06/12 / 20250612 / 114/06/12 / 1140612 -> YYYY-MM-DD"""
    s = str(s).strip()
    m = re.match(r"^(\d{4})[-/]?(\d{2})[-/]?(\d{2})$", s)
    if m:
        return "%s-%s-%s" % m.groups()
    m = re.match(r"^(\d{2,3})[-/]?(\d{2})[-/]?(\d{2})$", s)
    if m:
        return "%04d-%s-%s" % (int(m.group(1)) + 1911, m.group(2), m.group(3))
    return None


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def read_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


# ---------------------------------------------------------------- 抓取：上市

def fetch_twse(date_str):
    """
    回傳 (rows, ok)。ok=False 代表當天非交易日或尚未公布。
    每列格式：[代號, 名稱, 市場, 投信淨額, 外資淨額]  單位為股。
    """
    ymd = date_str.replace("-", "")
    url = ("https://www.twse.com.tw/rwd/zh/fund/T86"
           "?date=%s&selectType=ALL&response=json" % ymd)
    data = http_get_json(url)
    if data.get("stat") != "OK" or not data.get("data"):
        return [], False

    fields = data.get("fields", [])

    def idx(*kws):
        for i, f in enumerate(fields):
            if all(k in f for k in kws):
                return i
        return None

    i_frgn = idx("外陸資買賣超")
    i_frgn_dlr = idx("外資自營商買賣超")
    i_trust = idx("投信買賣超")
    if i_frgn is None or i_trust is None:
        raise ValueError("TWSE 欄位格式已變更：%s" % fields)

    rows = []
    for rec in data["data"]:
        code = rec[0].strip()
        if not is_common_stock(code):
            continue
        frgn = to_int(rec[i_frgn]) + (to_int(rec[i_frgn_dlr]) if i_frgn_dlr is not None else 0)
        rows.append([code, rec[1].strip(), "T", to_int(rec[i_trust]), frgn])
    return rows, True


# ---------------------------------------------------------------- 抓取：上櫃

def _pick(d, *kws):
    for k, v in d.items():
        if all(kw in k for kw in kws):
            return v
    return None


def parse_tpex_openapi(payload, date_str):
    if not isinstance(payload, list) or not payload:
        return [], False
    sample = payload[0]
    date_val = _pick(sample, "日期") or sample.get("Date") or sample.get("date")
    if date_val:
        norm = normalize_any_date(str(date_val))
        if norm and norm != date_str:
            return [], False  # OpenAPI 只提供最新交易日，日期不符就跳過

    rows = []
    for rec in payload:
        code = str(_pick(rec, "代號") or rec.get("SecuritiesCompanyCode")
                   or rec.get("Code") or "").strip()
        if not is_common_stock(code):
            continue
        name = str(_pick(rec, "名稱") or rec.get("CompanyName") or "").strip()
        frgn = to_int(_pick(rec, "外資", "買賣超") or rec.get("ForeignInvestorsNet") or 0)
        hedge = _pick(rec, "外資自營商", "買賣超")
        if hedge is not None:
            frgn += to_int(hedge)
        trust = to_int(_pick(rec, "投信", "買賣超")
                       or rec.get("SecuritiesInvestmentTrustNet") or 0)
        rows.append([code, name, "O", trust, frgn])
    return rows, bool(rows)


def parse_tpex_tabular(payload, date_str):
    table = None
    if isinstance(payload, dict):
        if payload.get("tables"):
            table = payload["tables"][0]
        elif payload.get("aaData"):
            table = {"fields": [], "data": payload["aaData"]}
        elif payload.get("data"):
            table = {"fields": payload.get("fields", []), "data": payload["data"]}
    if not table or not table.get("data"):
        return [], False

    fields = table.get("fields") or []

    def idx(*kws):
        for i, f in enumerate(fields):
            if all(k in str(f) for k in kws):
                return i
        return None

    i_frgn, i_trust = idx("外資", "買賣超"), idx("投信", "買賣超")
    if i_frgn is None or i_trust is None:
        i_frgn, i_trust = 10, 13   # 舊版固定欄位位置

    rows = []
    for rec in table["data"]:
        code = str(rec[0]).strip()
        if not is_common_stock(code):
            continue
        try:
            rows.append([code, str(rec[1]).strip(), "O",
                         to_int(rec[i_trust]), to_int(rec[i_frgn])])
        except IndexError:
            continue
    return rows, bool(rows)


def fetch_tpex(date_str):
    d = dt.datetime.strptime(date_str, "%Y-%m-%d")
    subs = {
        "ymd_slash": d.strftime("%Y/%m/%d"),
        "roc_slash": "%d/%02d/%02d" % (d.year - 1911, d.month, d.day),
    }
    last_err = None
    for tmpl, kind in TPEX_CANDIDATES:
        url = tmpl.format(**subs)
        try:
            payload = http_get_json(url)
            rows, ok = (parse_tpex_openapi if kind == "openapi"
                        else parse_tpex_tabular)(payload, date_str)
        except Exception as e:                        # noqa: BLE001
            last_err = "%s -> %s" % (url.split("?")[0], e)
            continue
        if ok:
            return rows, True
    if last_err:
        log("  ! 上櫃取得失敗（%s）；本日僅收錄上市" % last_err)
    return [], False


# ---------------------------------------------------------------- 每日資料

def day_path(date_str):
    return os.path.join(DAYS_DIR, date_str + ".json")


def have_day(date_str):
    return os.path.exists(day_path(date_str))


def fetch_day(date_str):
    """抓一天並寫檔。回傳寫入筆數，0 代表非交易日或無資料。"""
    if have_day(date_str):
        return 0
    try:
        tw_rows, tw_ok = fetch_twse(date_str)
    except Exception as e:                            # noqa: BLE001
        log("  ! %s 上市抓取錯誤：%s" % (date_str, e))
        return 0
    time.sleep(REQUEST_PAUSE)
    if not tw_ok:
        return 0                                       # 非交易日，不留檔

    tp_rows, _ = fetch_tpex(date_str)
    time.sleep(REQUEST_PAUSE)

    rows = tw_rows + tp_rows
    write_json(day_path(date_str), {"date": date_str, "rows": rows})
    return len(rows)


def weekday_dates(days_back, end=None):
    end = end or dt.date.today()
    out = []
    for i in range(days_back):
        d = end - dt.timedelta(days=i)
        if d.weekday() < 5:
            out.append(d.isoformat())
    return out


def available_dates():
    files = glob.glob(os.path.join(DAYS_DIR, "*.json"))
    return sorted(os.path.basename(f)[:-5] for f in files)


# ---------------------------------------------------------------- 連續天數與看板

def load_days(dates):
    """回傳 {date: {code: (name, market, trust, foreign)}}"""
    out = {}
    for ds in dates:
        payload = read_json(day_path(ds))
        if not payload:
            continue
        out[ds] = {r[0]: (r[1], r[2], r[3], r[4]) for r in payload.get("rows", [])}
    return out


def build_board(target_date, all_dates):
    """
    產生某一天的看板。all_dates 為由舊到新排序的完整交易日清單。
    連續天數只往回看（不看未來），因此歷史日期的看板與當時看到的一致。
    """
    pos = all_dates.index(target_date)
    window = all_dates[max(0, pos - STREAK_LOOKBACK + 1): pos + 1]
    window_desc = list(reversed(window))               # 由新到舊
    loaded = load_days(window_desc)

    today = loaded.get(target_date)
    if not today:
        return None

    def streak(code, slot):
        """slot: 2=投信 3=外資。正數為連續買超天數，負數為連續賣超。"""
        first = today.get(code)
        if not first or first[slot] == 0:
            return 0
        sign = 1 if first[slot] > 0 else -1
        n = 0
        for ds in window_desc:
            rec = loaded.get(ds, {}).get(code)
            v = rec[slot] if rec else 0
            if v == 0 or (1 if v > 0 else -1) != sign:
                break
            n += 1
        return n * sign

    records = []
    for code, (name, market, trust, frgn) in today.items():
        records.append({
            "code": code, "name": name,
            "market": "上市" if market == "T" else "上櫃",
            "trust": trust, "foreign": frgn,
            "trustStreak": streak(code, 2),
            "foreignStreak": streak(code, 3),
        })

    def top(key, buy_side):
        """買超榜只收正值、賣超榜只收負值；不足 TOP_N 就少列，不補反向的股票。"""
        pool = [r for r in records if (r[key] > 0 if buy_side else r[key] < 0)]
        pool.sort(key=lambda r: r[key], reverse=buy_side)
        return pool[:TOP_N]

    boards = {
        "trustBuy": top("trust", True),
        "trustSell": top("trust", False),
        "foreignBuy": top("foreign", True),
        "foreignSell": top("foreign", False),
    }

    # 榜上股票的近 20 日走勢
    board_codes = {r["code"] for b in boards.values() for r in b}
    spark_dates = window[-SPARK_DAYS:]
    history = {}
    for code in board_codes:
        series = []
        for ds in spark_dates:
            rec = loaded.get(ds, {}).get(code)
            series.append({"d": ds[5:], "t": rec[2] if rec else 0,
                           "f": rec[3] if rec else 0})
        history[code] = series

    return {
        "date": target_date,
        "tradingDays": len(window),
        "generatedAt": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "boards": boards,
        "history": history,
    }


def rebuild_boards(limit=BOARD_REBUILD):
    dates = available_dates()
    if not dates:
        log("尚無任何資料，請先執行 backfill")
        return False

    targets = dates[-limit:] if limit else dates
    built = 0
    for ds in targets:
        board = build_board(ds, dates)
        if board:
            write_json(os.path.join(BOARD_DIR, ds + ".json"), board)
            built += 1

    # index.json 只列出已有看板的日期，避免前端載到 404
    have_board = sorted(
        os.path.basename(f)[:-5]
        for f in glob.glob(os.path.join(BOARD_DIR, "*.json"))
    )
    write_json(INDEX_PATH, {
        "dates": have_board,
        "latest": have_board[-1] if have_board else None,
        "totalDays": len(dates),
        "updated": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    })
    log("看板已重算 %d 天；資料庫共 %d 個交易日；最新 %s"
        % (built, len(dates), have_board[-1] if have_board else "無"))
    return True


# ---------------------------------------------------------------- 指令

def cmd_backfill(days):
    dates = weekday_dates(days)
    log("開始回補：%s ~ %s（%d 個平日）" % (dates[-1], dates[0], len(dates)))
    got = 0
    for ds in reversed(dates):
        n = fetch_day(ds)
        if n:
            got += 1
            log("  %s 收錄 %d 檔" % (ds, n))
    log("回補完成，新增 %d 個交易日" % got)


def cmd_update():
    """補齊近 14 天缺漏，涵蓋連假與漏跑的情況。"""
    got = 0
    for ds in reversed(weekday_dates(14)):
        n = fetch_day(ds)
        if n:
            got += 1
            log("%s 收錄 %d 檔" % (ds, n))
    if not got:
        log("沒有新資料（可能尚未收盤或今日非交易日）")
    return got


def cmd_discover_tpex():
    try:
        spec = http_get_json("https://www.tpex.org.tw/openapi/swagger.json", timeout=90)
    except Exception as e:                            # noqa: BLE001
        log("無法取得 swagger.json：%s" % e)
        return
    hits = []
    for path, ops in (spec.get("paths") or {}).items():
        blob = json.dumps(ops, ensure_ascii=False)
        if any(k in blob for k in ("三大法人", "投信", "外資")) or "insti" in path.lower():
            summary = next((op.get("summary", "") for op in ops.values()
                            if isinstance(op, dict) and op.get("summary")), "")
            hits.append((path, summary))
    if not hits:
        log("沒找到相關資料集")
    for p, s in sorted(hits):
        log("  https://www.tpex.org.tw%s  %s" % (p, s))


def main():
    ap = argparse.ArgumentParser(description="台股法人籌碼資料更新")
    ap.add_argument("command", choices=["update", "backfill", "rebuild", "discover-tpex"])
    ap.add_argument("--days", type=int, default=130, help="backfill 往回幾個日曆日")
    args = ap.parse_args()

    os.makedirs(DAYS_DIR, exist_ok=True)
    os.makedirs(BOARD_DIR, exist_ok=True)

    if args.command == "discover-tpex":
        cmd_discover_tpex()
    elif args.command == "backfill":
        cmd_backfill(args.days)
        rebuild_boards(limit=None)
    elif args.command == "update":
        cmd_update()
        rebuild_boards()
    elif args.command == "rebuild":
        rebuild_boards(limit=None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
