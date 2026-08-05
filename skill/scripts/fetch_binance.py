#!/usr/bin/env python3
"""Загрузка исторических баров Binance из публичных дампов.

Примеры:
    python fetch_binance.py BTCUSDT 1h 2022-01 2026-07 -o btc_1h.csv
    python fetch_binance.py BTCUSDT,ETHUSDT,SOLUSDT 5m 2024-01 2026-07 -o panel.csv
    python fetch_binance.py --list-all          # все пары, включая делистнутые

Без ключей API. Данные помесячные, склеиваются и сохраняются в один CSV
с колонками: symbol, ts, open, high, low, close, volume, quote_volume, trades
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

DUMP = ("https://data.binance.vision/data/futures/um/monthly/klines/"
        "{s}/{tf}/{s}-{tf}-{ym}.zip")
S3 = ("https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
      "?delimiter=/&prefix=data/futures/um/monthly/klines/")
LIVE = "https://fapi.binance.com/fapi/v1/exchangeInfo"

COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
        "quote_volume", "trades", "tb_vol", "tb_quote", "ignore"]


def months(a: str, b: str) -> list[str]:
    out, y, m = [], *map(int, a.split("-"))
    ey, em = map(int, b.split("-"))
    while (y, m) <= (ey, em):
        out.append(f"{y}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def all_symbols() -> tuple[list[str], list[str]]:
    """Возвращает (все когда-либо торговавшиеся, живые сейчас)."""
    import json
    xml = urllib.request.urlopen(S3 + "&max-keys=5000", timeout=90).read().decode()
    ever = [s for s in re.findall(
        r"<Prefix>data/futures/um/monthly/klines/([^<]+)/</Prefix>", xml)
        if s.endswith("USDT")]
    info = json.loads(urllib.request.urlopen(LIVE, timeout=60).read())
    live = [x["symbol"] for x in info["symbols"]
            if x["contractType"] == "PERPETUAL" and x["quoteAsset"] == "USDT"
            and x["status"] == "TRADING"]
    return sorted(ever), sorted(live)


def one(args) -> pd.DataFrame | None:
    sym, tf, ym = args
    try:
        raw = urllib.request.urlopen(DUMP.format(s=sym, tf=tf, ym=ym), timeout=180).read()
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            txt = z.read(z.namelist()[0]).decode()
    except Exception:
        return None                      # месяца нет — инструмент ещё/уже не торговался
    head = txt.split("\n", 1)[0]
    df = (pd.read_csv(io.StringIO(txt)) if head.startswith("open_time")
          else pd.read_csv(io.StringIO(txt), header=None, names=COLS))
    df.columns = [c.strip() for c in df.columns]
    # в файлах с заголовком колонка сделок называется count
    df = df.rename(columns={"count": "trades", "number_of_trades": "trades"})
    ot = pd.to_numeric(df["open_time"], errors="coerce")
    unit = "us" if ot.max() > 1e15 else "ms"     # бывает и в микросекундах
    df["ts"] = pd.to_datetime(ot, unit=unit, utc=True)
    df["symbol"] = sym
    keep = ["symbol", "ts", "open", "high", "low", "close", "volume",
            "quote_volume", "trades"]
    for c in keep[2:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[keep]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("symbols", nargs="?", help="через запятую, напр. BTCUSDT,ETHUSDT")
    p.add_argument("timeframe", nargs="?", help="1m 5m 15m 1h 4h 1d")
    p.add_argument("start", nargs="?", help="YYYY-MM")
    p.add_argument("end", nargs="?", help="YYYY-MM")
    p.add_argument("-o", "--out", default="data.csv")
    p.add_argument("--list-all", action="store_true",
                   help="показать все пары, включая делистнутые")
    p.add_argument("--workers", type=int, default=8)
    a = p.parse_args()

    if a.list_all:
        ever, live = all_symbols()
        dead = sorted(set(ever) - set(live))
        print(f"всего пар USDT за всю историю: {len(ever)}")
        print(f"торгуются сейчас: {len(live)}")
        print(f"делистнуто: {len(dead)}  <- их обязательно включать в тесты,")
        print("   иначе получится ошибка выжившего (умершие проекты исчезнут)")
        print("\nделистнутые:", ", ".join(dead))
        return

    if not all([a.symbols, a.timeframe, a.start, a.end]):
        p.error("нужны symbols, timeframe, start, end (или --list-all)")

    syms = [s.strip().upper() for s in a.symbols.split(",")]
    jobs = [(s, a.timeframe, ym) for s in syms for ym in months(a.start, a.end)]
    print(f"качаю {len(jobs)} файлов ({len(syms)} инстр. × "
          f"{len(months(a.start, a.end))} мес.)", file=sys.stderr)

    frames = []
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        for i, df in enumerate(ex.map(one, jobs), 1):
            if df is not None and len(df):
                frames.append(df)
            if i % 50 == 0:
                print(f"  {i}/{len(jobs)}", file=sys.stderr)

    if not frames:
        sys.exit("данных не получено — проверь символ и период")
    out = pd.concat(frames, ignore_index=True).sort_values(["symbol", "ts"])
    out = out.drop_duplicates(["symbol", "ts"])
    out.to_csv(a.out, index=False)
    print(f"\nготово: {len(out)} строк, {out.symbol.nunique()} инстр., "
          f"{out.ts.min().date()} .. {out.ts.max().date()} -> {a.out}")


if __name__ == "__main__":
    main()
