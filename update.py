#!/usr/bin/env python3
"""Cloud updater. Runs on GitHub Actions every 5 minutes (no computer needed).

Reads oi.json (the open-interest file your PC saved, uploaded once a day), fetches free
Yahoo prices (15 min delayed) for the 7 stocks, the ASX200 index and candles, works out the
levels, and writes index.html + data.json for GitHub Pages.

  python update.py --demo     fake data, to test the page
"""
import json
import os
import sys
import time
import types
import urllib.parse

import aus200_gex as g
import aus200_live as L

NO_DATA = {"msg": "No open interest yet. Run aus200_live.py on your PC, then upload the saved aus200_oi_<date>.json "
                  "to this repo as oi.json.", "levels": [], "index": None}


def yahoo_price(sym, tries=3):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(sym) + "?interval=1m&range=1d"
    err = None
    for _ in range(tries):
        try:
            return float(L.http_json(url, 15)["chart"]["result"][0]["meta"]["regularMarketPrice"])
        except Exception as e:
            err = e
            time.sleep(2)
    raise RuntimeError(f"{sym}: {err}")


def candles(demo):
    out = {}
    for tf in ("1m", "5m", "15m", "1h", "4h"):
        for _ in range(3):
            try:
                cs = L.fetch_candles(tf, demo)
                out[tf] = [{"time": c["time"], "open": round(c["open"], 2), "high": round(c["high"], 2),
                            "low": round(c["low"], 2), "close": round(c["close"], 2)} for c in cs[-600:]]
                break
            except Exception as e:
                print(f"candles {tf}: {e}")
                time.sleep(2)
    return out


def write(data):
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(L.PAGE.replace("window.STATIC = false;", "window.STATIC = true;"))
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))


def main():
    demo = "--demo" in sys.argv
    if demo:
        snap = L.demo_snapshot()
    elif os.path.exists("oi.json"):
        snap = json.load(open("oi.json"))
    else:
        write(dict(NO_DATA)); print("no oi.json"); return
    if not snap.get("stocks"):
        write(dict(NO_DATA)); return
    expiry = next(iter(snap["stocks"].values()))["expiry"]
    if not demo and time.strftime("%Y%m%d") > expiry:
        d = dict(NO_DATA); d["msg"] = f"Open interest expired on {expiry}. Pull a new one on your PC and upload it as oi.json."
        write(d); print("expired"); return

    args = types.SimpleNamespace(nodes=8, merge_pct=0.05, top_index=1)
    weights = dict(g.WEIGHTS)
    spots, live = {}, 0
    for sym in snap["stocks"]:
        try:
            spots[sym] = 100.0 if demo else yahoo_price(sym + ".AX")
            if demo:
                spots[sym] = L.DEMO_SPOTS[sym]
            live += 1
        except Exception as e:
            print(e)
            spots[sym] = snap["stocks"][sym]["spot"]
    try:
        raw = L.DEMO["index"] if demo else yahoo_price("^AXJO")
    except Exception as e:
        print("index price failed, keeping the previous data.json:", e)
        return
    st = L.make_state(snap, spots, raw, "demo" if demo else "Yahoo ^AXJO", weights, args, live, 0.0)
    if st is None:
        write(dict(NO_DATA)); return
    st["tfs"] = ["1m", "5m", "15m", "1h", "4h"]
    st["candles"] = candles(demo)
    if not st["candles"]:
        print("no candles, keeping the previous data.json"); return
    write(st)
    print(f"ok: price {st['index']['price']}, net GEX {st['net']}m, {len(st['levels'])} levels, {live}/{len(spots)} stock prices")


if __name__ == "__main__":
    main()
