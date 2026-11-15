#!/usr/bin/env python3
"""Cloud updater for the AUS200 GEX page.

GitHub Actions starts this and it keeps running for about 55 minutes, refreshing every minute
(GitHub's own 5-minute timer is unreliable). Each refresh reads oi.json (the open-interest file
saved by aus200_live.py on your PC), gets free Yahoo prices and candles, works out the levels,
and publishes data.json to the `data` branch. The web page reads that file.

  python update.py --demo --out out        one demo run, fake data, no internet
  python update.py --loop 3300 --every 60 --publish      what GitHub runs
  python update.py --page                  rewrite index.html
"""
import json
import os
import subprocess
import sys
import time
import types
import urllib.parse

import aus200_gex as g
import aus200_live as L

REPO = os.environ.get("GITHUB_REPOSITORY", "LilAdelaide/aus200-gex")
DATA_URL = f"https://raw.githubusercontent.com/{REPO}/data/data.json"
TFS = ("1m", "5m", "15m", "1h", "4h")
NO_DATA = {"msg": "No open interest yet. Run aus200_live.py on your PC, then upload the saved aus200_oi_<date>.json "
                  "to this repo as oi.json.", "levels": [], "index": None}


def arg(name, default=None):
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default


def yahoo_price(sym, tries=3):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(sym) + "?interval=1m&range=1d"
    err = None
    for _ in range(tries):
        try:
            return float(L.http_json(url, 15)["chart"]["result"][0]["meta"]["regularMarketPrice"])
        except Exception as e:
            err = e
            time.sleep(1.5)
    raise RuntimeError(f"{sym}: {err}")


def candles(demo, prev):
    out = {}
    for tf in TFS:
        for _ in range(2):
            try:
                cs = L.fetch_candles(tf, demo)
                out[tf] = [{"time": c["time"], "open": round(c["open"], 2), "high": round(c["high"], 2),
                            "low": round(c["low"], 2), "close": round(c["close"], 2)} for c in cs[-600:]]
                break
            except Exception as e:
                print(f"candles {tf}: {e}")
                time.sleep(1.5)
        if tf not in out and prev.get(tf):
            out[tf] = prev[tf]
    return out


def build(demo, prev_candles):
    """One refresh. Returns the data dict, or None to keep the last published one."""
    if demo:
        snap = L.demo_snapshot()
    elif os.path.exists("oi.json"):
        snap = json.load(open("oi.json"))
    else:
        return dict(NO_DATA)
    if not snap.get("stocks"):
        return dict(NO_DATA)
    expiry = next(iter(snap["stocks"].values()))["expiry"]
    if not demo and time.strftime("%Y%m%d") > expiry:
        d = dict(NO_DATA)
        d["msg"] = f"Open interest expired on {expiry}. Pull a new one on your PC and upload it as oi.json."
        return d
    args = types.SimpleNamespace(nodes=8, merge_pct=0.05, top_index=1)
    spots, live = {}, 0
    for sym in snap["stocks"]:
        try:
            spots[sym] = L.DEMO_SPOTS[sym] if demo else yahoo_price(sym + ".AX")
            live += 1
        except Exception as e:
            print(e)
            spots[sym] = snap["stocks"][sym]["spot"]
    try:
        raw = L.DEMO["index"] if demo else yahoo_price("^AXJO")
    except Exception as e:
        print("index price failed, keeping the last data:", e)
        return None
    st = L.make_state(snap, spots, raw, "demo" if demo else "Yahoo ^AXJO", dict(g.WEIGHTS), args, live, 0.0)
    if st is None:
        return dict(NO_DATA)
    st["tfs"] = list(TFS)
    st["candles"] = candles(demo, prev_candles)
    if not st["candles"]:
        print("no candles, keeping the last data")
        return None
    return st


def write_page():
    html = L.PAGE.replace("window.STATIC = false;", 'window.STATIC = true; window.DATA_URL = "' + DATA_URL + '";')
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)


def refresh_oi():
    """Pick up a newly uploaded oi.json without restarting."""
    try:
        subprocess.run(["git", "fetch", "-q", "--depth=1", "origin", "main"], check=True, capture_output=True, timeout=60)
        out = subprocess.run(["git", "show", "FETCH_HEAD:oi.json"], check=True, capture_output=True, text=True, timeout=30).stdout
        if out.strip():
            open("oi.json", "w", encoding="utf-8").write(out)
    except Exception as e:
        print("oi refresh skipped:", e)


def publish(data):
    tok = os.environ["GITHUB_TOKEN"]
    d = "dist"

    def git(*a):
        r = subprocess.run(["git", "-C", d, *a], capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError((r.stderr or r.stdout).replace(tok, "***"))
        return r
    if not os.path.isdir(d + "/.git"):
        os.makedirs(d, exist_ok=True)
        git("init", "-q"); git("checkout", "-q", "-b", "data")
        git("config", "user.name", "github-actions"); git("config", "user.email", "actions@users.noreply.github.com")
        git("remote", "add", "origin", f"https://x-access-token:{tok}@github.com/{REPO}.git")
    with open(d + "/data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    git("add", "data.json")
    has = subprocess.run(["git", "-C", d, "rev-parse", "HEAD"], capture_output=True).returncode == 0
    git("commit", "-q", *(["--amend"] if has else []), "-m", "data " + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    git("push", "-q", "-f", "origin", "data")


def loop(secs, every):
    end, n, last = time.time() + secs, 0, {}
    while True:
        t0 = time.time()
        if n % 5 == 0:
            refresh_oi()
        try:
            data = build(False, last.get("candles") or {})
            if data is not None:
                last = data
                publish(data)
                print(time.strftime("%H:%M:%S"), "published", data.get("index", {}) and data["index"]["price"], flush=True)
        except Exception as e:
            print("refresh failed:", e, flush=True)
        n += 1
        wait = max(1.0, every - (time.time() - t0))
        if time.time() + wait > end:
            break
        time.sleep(wait)


def main():
    if "--page" in sys.argv:
        write_page(); print("index.html written"); return
    if "--loop" in sys.argv:
        loop(float(arg("--loop")), float(arg("--every", "60"))); return
    demo = "--demo" in sys.argv
    out = arg("--out", ".")
    os.makedirs(out, exist_ok=True)
    data = build(demo, {})
    if data is None:
        print("nothing to write"); return
    with open(os.path.join(out, "data.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    if data.get("index"):
        print(f"ok: price {data['index']['price']}, net GEX {data['net']}m, {len(data['levels'])} levels")
    else:
        print(data.get("msg"))


if __name__ == "__main__":
    main()
