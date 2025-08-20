import requests
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime, timezone, timedelta

# ---------- Konfigurasi ----------

# Rentang waktu
START_UTC = pd.Timestamp("2025-02-03 00:00:00", tz="UTC")
END_UTC = pd.Timestamp("2025-08-18 23:59:59", tz="UTC")

# API URL untuk data pasar XAUT/USDT di MEXC
MEXC_URL = "https://www.mexc.com/api/v2/market/kline"

# Kandidat simbol untuk data TradingView
cariantohs02 = None  # Isi dengan username TradingView jika perlu
@Hottua140296 = None  # Isi dengan password TradingView jika perlu

# Data yang akan digunakan
XAUT_CANDIDATES = [("BYBIT", "XAUT_USDT"), ("MEXC", "XAUT_USDT"), ("OKX", "XAUT-USDT")]
GOLD_PRIMARY = ("COMEX", "GC1!")
DXY_PRIMARY = ("TVC", "DXY")
REAL_YIELD = ("FRED", "DFII10")

M5 = Interval.in_5_minute  # Interval 5 menit

# ---------- Helper Functions ----------

def tv_login(user=None, pwd=None):
    if user and pwd:
        return TvDatafeed(user, pwd)
    return TvDatafeed()

def fetch_hist(tv, symbol, exchange, interval, n_bars=100000, futures=False):
    try:
        df = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars, fut=futures)
        if df is not None and not df.empty:
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
            return df.loc[(df.index >= START_UTC) & (df.index <= END_UTC)].copy()
    except Exception as e:
        pass
    return None

def fetch_mexc_data(symbol="XAUT_USDT", interval="5m"):
    params = {
        "symbol": symbol,
        "interval": interval,
        "start_time": int((START_UTC - datetime(1970, 1, 1)).total_seconds() * 1000),
        "end_time": int((END_UTC - datetime(1970, 1, 1)).total_seconds() * 1000),
        "limit": 1000,
    }
    response = requests.get(MEXC_URL, params=params)
    data = response.json()
    if 'data' in data:
        df = pd.DataFrame(data['data'], columns=["timestamp", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        return df
    return None

def try_candidates(tv, candidates, interval, futures=False):
    for ex, sym in candidates:
        df = fetch_hist(tv, sym, ex, interval, n_bars=120000, futures=futures)
        if df is not None and len(df) > 0:
            return (ex, sym, df)
    return (None, None, None)

def save_csv(df, path):
    df = df.sort_index()
    df.to_csv(path, index_label="time")

# ---------- Main Process ----------

# Login to TradingView
tv = tv_login(TV_USERNAME, TV_PASSWORD)

# 1) XAUT/USDT dari MEXC (futures)
df_xaut = fetch_mexc_data("XAUT_USDT", "5m")
if df_xaut is None:
    raise RuntimeError("Gagal mengambil XAUT/USDT dari MEXC API.")

# 2) Gold (GC1! futures)
ex, sym, df_gold = try_candidates(tv, [GOLD_PRIMARY], M5, futures=True)
if df_gold is None:
    raise RuntimeError("Gagal mengambil data emas (GC1!).")

# 3) DXY (US Dollar Index)
ex, sym, df_dxy = try_candidates(tv, [DXY_PRIMARY], M5, futures=False)
if df_dxy is None:
    raise RuntimeError("Gagal mengambil DXY.")

# 4) Real Yield (DFII10)
df_real_daily = fetch_hist(tv, REAL_YIELD[1], REAL_YIELD[0], Interval.in_daily, n_bars=1500)
if df_real_daily is None or df_real_daily.empty:
    raise RuntimeError("Gagal mengambil DFII10.")
s_daily = df_real_daily["close"].copy()
rng_5m = pd.date_range(start=START_UTC, end=END_UTC, freq="5T", tz="UTC")
s_5m = s_daily.reindex(s_daily.index.union(rng_5m)).sort_index().ffill().reindex(rng_5m)
df_real_5m = pd.DataFrame({"open": s_5m, "high": s_5m, "low": s_5m, "close": s_5m, "volume": 0.0}, index=s_5m.index)

# ---------- Merge Data ----------

# Gabungkan semua DataFrame dalam satu CSV
df_combined = df_xaut.join(df_gold[['close']], rsuffix='_gold')
df_combined = df_combined.join(df_dxy[['close']], rsuffix='_dxy')
df_combined = df_combined.join(df_real_5m[['close']], rsuffix='_real_yield')

# Simpan hasilnya ke CSV
save_csv(df_combined, "combined_XAUT_USDT_GOLD_DXY_REAL_YIELD.csv")
print("Data berhasil disimpan ke combined_XAUT_USDT_GOLD_DXY_REAL_YIELD.csv")
