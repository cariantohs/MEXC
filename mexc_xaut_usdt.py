# ============================================================
# MEXC Futures XAUT_USDT - Full Data Downloader + Live Stream (Optional)
# Siap tempel ke Google Colab - 1 sel saja.
# ============================================================

# -- install deps (jalan dalam Python, cocok di Colab) --
import sys, subprocess, os, json, time, math, threading
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "requests", "pandas", "websocket-client"])

import typing as t
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from websocket import WebSocketApp

# ----------------------------
# Konfigurasi
# ----------------------------
BASE = "https://contract.mexc.com"
SYMBOL = "XAUT_USDT"
OUTDIR = "mexc_xaut_usdt"
os.makedirs(OUTDIR, exist_ok=True)

# Histori K-line:
FULL_HISTORY = True            # True = ambil penuh dari awal listing hingga terbaru
FROM_DAYS = 30                 # Dipakai jika FULL_HISTORY=False
KLINE_INTERVALS = ["Min1", "Min5", "Min15", "Min30", "Min60", "Hour4", "Day1"]

# K-line Index & Fair (opsional):
INDEX_FAIR_INTERVALS = ["Min1", "Min5", "Min60", "Day1"]

# Rate limit protection (lihat docs rate limit market endpoints)
REQ_SLEEP = 0.15
KLINE_MAX = 2000  # batas per permintaan (lihat dokumen)

# Streaming WS (opsional)
WS_ENABLE = True           # True untuk merekam stream real-time
WS_RUN_SECONDS = 180       # lama rekam (detik). Ubah sesuai kebutuhan.
WS_DEPTH_LIMIT = 20        # untuk sub.depth.full (5/10/20, default 20)

# ----------------------------
# Utilitas umum
# ----------------------------
def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def to_epoch_s(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def req_get(path: str, params: dict | None = None) -> dict | list:
    """GET helper: normalisasi response MEXC (kadang ber-bungkus success/data)."""
    url = f"{BASE}{path}"
    for attempt in range(5):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "data" in data and ("success" in data or "code" in data):
                return data["data"]
            return data
        except Exception as e:
            if attempt == 4:
                raise
            time.sleep(0.6 * (attempt + 1))
    return {}

def save_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(OUTDIR, filename)
    df.to_csv(path, index=False)
    print(f"✔ Saved: {path}")

# ----------------------------
# Endpoints (Market / Public)
# (sesuai: https://www.mexc.com/api-docs/futures/market-endpoints)
# ----------------------------
def get_server_time() -> int:
    data = req_get("/api/v1/contract/ping")
    # contoh response: {"success":true,"data":1587442022003}
    if isinstance(data, int):
        return data
    if isinstance(data, dict) and "data" in data:
        return int(data["data"])
    return int(data or 0)

def get_contract_detail(symbol: str | None = None) -> list[dict]:
    params = {"symbol": symbol} if symbol else None
    data = req_get("/api/v1/contract/detail", params=params)
    return data if isinstance(data, list) else []

def get_ticker(symbol: str | None = None) -> dict:
    params = {"symbol": symbol} if symbol else None
    return req_get("/api/v1/contract/ticker", params=params)

def get_deals(symbol: str, limit: int = 100) -> list[dict]:
    return req_get(f"/api/v1/contract/deals/{symbol}", params={"limit": limit})

def get_depth(symbol: str, limit: int | None = None) -> dict:
    params = {"limit": limit} if limit else None
    return req_get(f"/api/v1/contract/depth/{symbol}", params=params)

def get_depth_commits(symbol: str, n: int = 20) -> dict:
    return req_get(f"/api/v1/contract/depth_commits/{symbol}/{n}")

def get_index_price(symbol: str) -> dict:
    return req_get(f"/api/v1/contract/index_price/{symbol}")

def get_fair_price(symbol: str) -> dict:
    return req_get(f"/api/v1/contract/fair_price/{symbol}")

def get_funding_rate(symbol: str) -> dict:
    return req_get(f"/api/v1/contract/funding_rate/{symbol}")

def get_funding_history(symbol: str, page_size: int = 1000) -> pd.DataFrame:
    rows = []
    page = 1
    while True:
        data = req_get("/api/v1/contract/funding_rate/history",
                       params={"symbol": symbol, "page_num": page, "page_size": page_size})
        if not isinstance(data, dict) or "resultList" not in data:
            break
        for x in data["resultList"]:
            rows.append({
                "symbol": x.get("symbol"),
                "fundingRate": x.get("fundingRate"),
                "settleTime": x.get("settleTime"),
                "settleTime_iso": datetime.utcfromtimestamp(x.get("settleTime", 0)/1000).isoformat()
            })
        total_page = int(data.get("totalPage", page))
        print(f"Funding history page {page}/{total_page} fetched ({len(rows)} rows)")
        if page >= total_page:
            break
        page += 1
        time.sleep(REQ_SLEEP)
    return pd.DataFrame(rows)

# ----------------------------
# K-line helpers (OHLCV)
# ----------------------------
INTERVAL_SECONDS = {
    "Min1": 60, "Min5": 300, "Min15": 900, "Min30": 1800, "Min60": 3600,
    "Hour4": 14400, "Hour8": 28800, "Day1": 86400, "Week1": 604800, "Month1": 2592000
}

def _kline_payload_to_df(payload: dict, interval: str) -> pd.DataFrame:
    if not isinstance(payload, dict) or "time" not in payload or not payload["time"]:
        return pd.DataFrame(columns=["t","t_iso","interval","open","high","low","close","vol","amount"])
    n = len(payload["time"])
    def col(name): return payload.get(name, [None]*n)
    df = pd.DataFrame({
        "t": col("time"),
        "open": col("open"),
        "high": col("high"),
        "low": col("low"),
        "close": col("close"),
        "vol": col("vol"),
        "amount": col("amount"),
    })
    df["t_iso"] = pd.to_datetime(df["t"], unit="s", utc=True).dt.tz_convert("UTC").astype(str)
    df["interval"] = interval
    return df[["t","t_iso","interval","open","high","low","close","vol","amount"]]

def fetch_kline_range(symbol: str, interval: str, start_s: int, end_s: int) -> pd.DataFrame:
    """Range forward (pakai start/end)."""
    step = INTERVAL_SECONDS.get(interval, 60) * KLINE_MAX
    cur_start = start_s
    all_df = []
    while cur_start <= end_s:
        cur_end = min(end_s, cur_start + step - 1)
        data = req_get(f"/api/v1/contract/kline/{symbol}",
                       params={"interval": interval, "start": cur_start, "end": cur_end})
        df = _kline_payload_to_df(data, interval)
        all_df.append(df)
        print(f"Kline {interval}: +{len(df)} rows for {datetime.utcfromtimestamp(cur_start)} .. {datetime.utcfromtimestamp(cur_end)}")
        cur_start = cur_end + 1
        time.sleep(REQ_SLEEP)
    if all_df:
        out = pd.concat(all_df, ignore_index=True)
        out = out.drop_duplicates(subset=["t","interval"]).sort_values(["t"]).reset_index(drop=True)
        return out
    return pd.DataFrame(columns=["t","t_iso","interval","open","high","low","close","vol","amount"])

def fetch_kline_full_history(symbol: str, interval: str, end_s: int | None = None) -> pd.DataFrame:
    """Mundur (paging by 'end'), kumpulkan hingga mentok (<=2000 bar)."""
    if end_s is None:
        end_s = to_epoch_s(now_utc())
    all_df = []
    last_min_t = None
    while True:
        data = req_get(f"/api/v1/contract/kline/{symbol}",
                       params={"interval": interval, "end": end_s})
        df = _kline_payload_to_df(data, interval)
        if df.empty:
            break
        all_df.append(df)
        earliest = int(df["t"].min())
        # Jika data < 2000 bar, umumnya sudah mentok ke awal
        if len(df) < KLINE_MAX:
            break
        # Hindari loop tak berujung
        if last_min_t is not None and earliest >= last_min_t:
            break
        last_min_t = earliest
        end_s = earliest - 1
        print(f"Kline {interval}: batch {len(df)} rows, earliest={datetime.utcfromtimestamp(earliest)}")
        time.sleep(REQ_SLEEP)
    if not all_df:
        return pd.DataFrame(columns=["t","t_iso","interval","open","high","low","close","vol","amount"])
    out = pd.concat(all_df, ignore_index=True)
    out = out.drop_duplicates(subset=["t","interval"]).sort_values(["t"]).reset_index(drop=True)
    return out

def fetch_index_kline_full(symbol: str, interval: str) -> pd.DataFrame:
    all_df, end_s = [], to_epoch_s(now_utc())
    last_min_t = None
    while True:
        data = req_get(f"/api/v1/contract/kline/index_price/{symbol}",
                       params={"interval": interval, "end": end_s})
        df = _kline_payload_to_df(data, interval)
        if df.empty:
            break
        all_df.append(df)
        earliest = int(df["t"].min())
        if len(df) < KLINE_MAX: break
        if last_min_t is not None and earliest >= last_min_t: break
        last_min_t = earliest
        end_s = earliest - 1
        print(f"Index {interval}: +{len(df)} rows; earliest={datetime.utcfromtimestamp(earliest)}")
        time.sleep(REQ_SLEEP)
    return pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()

def fetch_fair_kline_full(symbol: str, interval: str) -> pd.DataFrame:
    all_df, end_s = [], to_epoch_s(now_utc())
    last_min_t = None
    while True:
        data = req_get(f"/api/v1/contract/kline/fair_price/{symbol}",
                       params={"interval": interval, "end": end_s})
        df = _kline_payload_to_df(data, interval)
        if df.empty:
            break
        all_df.append(df)
        earliest = int(df["t"].min())
        if len(df) < KLINE_MAX: break
        if last_min_t is not None and earliest >= last_min_t: break
        last_min_t = earliest
        end_s = earliest - 1
        print(f"Fair {interval}: +{len(df)} rows; earliest={datetime.utcfromtimestamp(earliest)}")
        time.sleep(REQ_SLEEP)
    return pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()

# ----------------------------
# Streaming (WebSocket)
# (sesuai: https://www.mexc.com/api-docs/futures/websocket-api)
# ----------------------------
class MexcWSRecorder:
    def __init__(self, symbol: str, outdir: str, run_seconds: int = 120, depth_limit: int = 20):
        self.symbol = symbol
        self.outdir = outdir
        self.run_seconds = max(10, run_seconds)
        self.depth_limit = depth_limit
        self.ws_url = "wss://contract.mexc.com/edge"
        self._stop = threading.Event()
        self._ws = None
        self._ping_thread = None

        # file path
        self.files = {
            "deal": os.path.join(outdir, "stream_deal.csv"),
            "depth": os.path.join(outdir, "stream_depth_full.csv"),
            "ticker": os.path.join(outdir, "stream_ticker.csv"),
            "index": os.path.join(outdir, "stream_index_price.csv"),
            "fair": os.path.join(outdir, "stream_fair_price.csv"),
            "funding": os.path.join(outdir, "stream_funding_rate.csv"),
        }

        # header init if not exists
        for k, path in self.files.items():
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    if k == "deal":
                        f.write("ts,symbol,p,v,T,O,M,t\n")
                    elif k == "depth":
                        f.write("ts,symbol,limit,version,asks_json,bids_json\n")
                    elif k == "ticker":
                        f.write("ts,symbol,lastPrice,bid1,ask1,volume24,holdVol,indexPrice,fairPrice,fundingRate\n")
                    elif k == "index":
                        f.write("ts,symbol,price\n")
                    elif k == "fair":
                        f.write("ts,symbol,price\n")
                    elif k == "funding":
                        f.write("ts,symbol,rate\n")

    def _send(self, ws, data: dict):
        ws.send(json.dumps(data))

    def _on_open(self, ws):
        # subscribe channels
        self._send(ws, {"method": "sub.ticker", "param": {"symbol": self.symbol}})
        self._send(ws, {"method": "sub.deal", "param": {"symbol": self.symbol}})
        self._send(ws, {"method": "sub.depth.full", "param": {"symbol": self.symbol, "limit": self.depth_limit}})
        self._send(ws, {"method": "sub.index.price", "param": {"symbol": self.symbol}})
        self._send(ws, {"method": "sub.fair.price", "param": {"symbol": self.symbol}})
        self._send(ws, {"method": "sub.funding.rate", "param": {"symbol": self.symbol}})

        # start ping thread (10–20s)
        def _ping_loop():
            while not self._stop.is_set():
                try:
                    ws.send(json.dumps({"method": "ping"}))
                except Exception:
                    pass
                time.sleep(15)
        self._ping_thread = threading.Thread(target=_ping_loop, daemon=True)
        self._ping_thread.start()

        # stop after run_seconds
        def _stopper():
            time.sleep(self.run_seconds)
            self._stop.set()
            try:
                ws.close()
            except Exception:
                pass
        threading.Thread(target=_stopper, daemon=True).start()

    def _on_message(self, ws, message: str):
        try:
            obj = json.loads(message)
        except Exception:
            return
        ch = obj.get("channel") or ""
        ts = obj.get("ts") or int(time.time() * 1000)
        sym = obj.get("symbol") or self.symbol
        data = obj.get("data", {})

        if ch == "push.deal":
            row = [
                str(ts), sym,
                str(data.get("p","")), str(data.get("v","")), str(data.get("T","")),
                str(data.get("O","")), str(data.get("M","")), str(data.get("t",""))
            ]
            with open(self.files["deal"], "a", encoding="utf-8") as f:
                f.write(",".join(row) + "\n")

        elif ch == "push.depth":
            # data: asks[[price,vol,order_count],...], bids[[...]], version
            asks_json = json.dumps(data.get("asks", []), ensure_ascii=False)
            bids_json = json.dumps(data.get("bids", []), ensure_ascii=False)
            version = data.get("version", "")
            row = [str(ts), sym, str(self.depth_limit), str(version), json.dumps(asks_json), json.dumps(bids_json)]
            with open(self.files["depth"], "a", encoding="utf-8") as f:
                f.write(",".join(row) + "\n")

        elif ch == "push.ticker":
            d = data or {}
            row = [
                str(ts), sym,
                str(d.get("lastPrice","")), str(d.get("bid1","")), str(d.get("ask1","")),
                str(d.get("volume24","")), str(d.get("holdVol","")),
                str(d.get("indexPrice","")), str(d.get("fairPrice","")), str(d.get("fundingRate",""))
            ]
            with open(self.files["ticker"], "a", encoding="utf-8") as f:
                f.write(",".join(row) + "\n")

        elif ch == "push.index.price":
            price = data.get("price","")
            with open(self.files["index"], "a", encoding="utf-8") as f:
                f.write(f"{ts},{sym},{price}\n")

        elif ch == "push.fair.price":
            price = data.get("price","")
            with open(self.files["fair"], "a", encoding="utf-8") as f:
                f.write(f"{ts},{sym},{price}\n")

        elif ch == "push.funding.rate":
            rate = data.get("rate","")
            with open(self.files["funding"], "a", encoding="utf-8") as f:
                f.write(f"{ts},{sym},{rate}\n")

    def _on_error(self, ws, error):
        print("WS error:", error)

    def _on_close(self, ws, code, msg):
        print("WS closed:", code, msg)
        self._stop.set()

    def run(self):
        print(f"Starting WS recorder for {self.symbol} for {self.run_seconds}s ...")
        self._stop.clear()
        self._ws = WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self._ws.run_forever()
        print("WS recorder finished. Files saved in:", self.outdir)

# ----------------------------
# Main workflow
# ----------------------------
def main():
    print("== MEXC Futures XAUT_USDT - Downloader ==")
    try:
        server_ms = get_server_time()
        print("Server time (ms):", server_ms)
    except Exception as e:
        print("⚠ get_server_time error:", e)

    # --- Contract detail ---
    try:
        details = get_contract_detail(SYMBOL)
        df_detail = pd.DataFrame(details)
        if df_detail.empty:
            # fallback: semua lalu filter manual
            all_details = get_contract_detail()
            dfa = pd.DataFrame(all_details)
            if not dfa.empty:
                dfa_sym = dfa[dfa["symbol"] == SYMBOL]
                save_csv(dfa_sym if not dfa_sym.empty else dfa, "contract_detail.csv")
        else:
            save_csv(df_detail, "contract_detail.csv")
    except Exception as e:
        print("❌ contract_detail:", e)

    # --- Ticker ---
    try:
        tk = get_ticker(SYMBOL)
        df_tk = pd.json_normalize(tk)
        save_csv(df_tk, "ticker.csv")
    except Exception as e:
        print("❌ ticker:", e)

    # --- Deals (recent up to 100) ---
    try:
        deals = get_deals(SYMBOL, 100)
        df_deals = pd.DataFrame(deals)
        if "t" in df_deals.columns:
            df_deals["t_iso"] = pd.to_datetime(df_deals["t"], unit="ms", utc=True).dt.tz_convert("UTC").astype(str)
        save_csv(df_deals, "deals_recent.csv")
    except Exception as e:
        print("❌ deals:", e)

    # --- Depth snapshot & commits ---
    try:
        depth = get_depth(SYMBOL, limit=50)
        def _depth_side_to_df(side: list[list[float]] | None, side_name: str) -> pd.DataFrame:
            rows, side = [], (side or [])
            for x in side:
                price = x[0] if len(x)>0 else None
                vol   = x[1] if len(x)>1 else None
                n     = x[2] if len(x)>2 else None
                rows.append({"side": side_name, "price": price, "volume": vol, "order_count": n})
            return pd.DataFrame(rows)
        df_asks = _depth_side_to_df(depth.get("asks"), "ask")
        df_bids = _depth_side_to_df(depth.get("bids"), "bid")
        if not df_asks.empty: save_csv(df_asks, "depth_asks.csv")
        if not df_bids.empty: save_csv(df_bids, "depth_bids.csv")

        dc = get_depth_commits(SYMBOL, n=20)
        # simpan mentah sebagai jsonlines (csv satu kolom)
        path = os.path.join(OUTDIR, "depth_commits_latestN.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for item in dc if isinstance(dc, list) else dc.get("data", []):
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        print("✔ Saved:", path)
    except Exception as e:
        print("❌ depth/depth_commits:", e)

    # --- Index & Fair (current) ---
    try:
        idx = get_index_price(SYMBOL)
        fair = get_fair_price(SYMBOL)
        save_csv(pd.json_normalize(idx), "index_price_now.csv")
        save_csv(pd.json_normalize(fair), "fair_price_now.csv")
    except Exception as e:
        print("❌ index/fair:", e)

    # --- Funding (now + history) ---
    try:
        fr_now = get_funding_rate(SYMBOL)
        save_csv(pd.json_normalize(fr_now), "funding_rate_now.csv")
        df_fr_hist = get_funding_history(SYMBOL, page_size=1000)
        if not df_fr_hist.empty:
            save_csv(df_fr_hist, "funding_rate_history.csv")
    except Exception as e:
        print("❌ funding:", e)

    # --- K-line (OHLCV) ---
    utc_now = now_utc()
    start_s = to_epoch_s(utc_now - timedelta(days=FROM_DAYS))
    end_s   = to_epoch_s(utc_now)

    for itv in KLINE_INTERVALS:
        try:
            if FULL_HISTORY:
                df = fetch_kline_full_history(SYMBOL, itv)
            else:
                df = fetch_kline_range(SYMBOL, itv, start_s, end_s)
            if not df.empty:
                save_csv(df, f"kline_{itv}.csv")
        except Exception as e:
            print(f"❌ kline {itv}:", e)

    # --- Index/Fair K-line (opsional) ---
    for itv in INDEX_FAIR_INTERVALS:
        try:
            dfi = fetch_index_kline_full(SYMBOL, itv) if FULL_HISTORY else fetch_index_kline_full(SYMBOL, itv)
            if not dfi.empty:
                save_csv(dfi, f"index_price_kline_{itv}.csv")
        except Exception as e:
            print(f"❌ index kline {itv}:", e)
        try:
            dff = fetch_fair_kline_full(SYMBOL, itv) if FULL_HISTORY else fetch_fair_kline_full(SYMBOL, itv)
            if not dff.empty:
                save_csv(dff, f"fair_price_kline_{itv}.csv")
        except Exception as e:
            print(f"❌ fair kline {itv}:", e)

    print("\n✅ Unduhan histori selesai. Folder:", OUTDIR)

    # --- Streaming (opsional) ---
    if WS_ENABLE:
        try:
            rec = MexcWSRecorder(SYMBOL, OUTDIR, run_seconds=WS_RUN_SECONDS, depth_limit=WS_DEPTH_LIMIT)
            rec.run()
        except Exception as e:
            print("❌ WS stream:", e)

if __name__ == "__main__":
    main()
