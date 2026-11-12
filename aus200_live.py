#!/usr/bin/env python3
"""
AUS200 live GEX dashboard.

What it does
  1. Pulls open interest for the 7 ASX stocks from TWS ONCE per day (about 10 minutes) and saves it
     to aus200_oi_YYYYMMDD.json. Later starts the same day load that file in seconds.
  2. Streams the (15-minute delayed) stock prices from TWS and recalculates every wall and node
     once a second from that saved open interest.
  3. Serves a live web page on your computer (default http://127.0.0.1:8765) with an AUS200
     candle chart, the call/put walls as lines, the positive/negative nodes as boxes, and a
     level table. Open it in a browser next to TradingView.

Setup
  Keep this file in the SAME folder as aus200_gex.py (it reuses its maths).
  pip install ib_async     (already installed if aus200_gex.py works)
  TWS must be open, logged in, with the API enabled (same as aus200_gex.py).

Usage
  python aus200_live.py                    # normal: pull OI if needed, then go live
  python aus200_live.py --refresh-oi       # force a fresh open-interest pull
  python aus200_live.py --demo             # fake data, no TWS needed (to see the page)
  python aus200_live.py --offset 12        # AUS200 CFD trades 12 points above the XJO

Index price (the chart and the level conversion need it)
  0. BEST: your own Pepperstone AUS200 via MetaTrader 5 (add --mt5). Live CFD price and candles
     (1m to 4h) straight from your broker, so the chart matches TradingView and no offset is needed.
     Needs Windows, the MT5 terminal open and logged in, and:  pip install MetaTrader5 tzdata
  1. IBKR's XJO if your account returns it, else
  2. Yahoo Finance ^AXJO (free, also delayed), else
  3. the price you type into the page ("Match my chart price").
  Candles are the XJO. If your AUS200 CFD sits above or below it, type your chart price into the
  page and press Match: it sets the offset so candles and levels line up with your chart.

Caveats
  - Open interest only changes overnight. Second-by-second updates move the levels with price,
    they do not add new information.
  - Gamma is recalculated here from the saved implied vols, so numbers can differ slightly from
    the morning aus200_gex.py run.
  - Levels are a rough proxy built from 7 stocks, not true XJO gamma.
  - General information, not financial advice.
"""
import argparse
import datetime as dt
import json
import math
import os
import random
import threading
import time
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import aus200_gex as g

LOCK = threading.Lock()
MT5_LOCK = threading.Lock()
MT5 = {"on": False, "symbol": "AUS200", "utc_offset": None}
MT5_TF = {"1m": "TIMEFRAME_M1", "5m": "TIMEFRAME_M5", "15m": "TIMEFRAME_M15", "1h": "TIMEFRAME_H1", "4h": "TIMEFRAME_H4"}
STATE = {"msg": "Starting...", "levels": [], "index": None}
CFG = {"offset": 0.0, "manual_price": None}
YAHOO = {"price": None, "ts": 0.0}
CANDLE_CACHE = {}

DEMO_SPOTS = {"CBA": 160.0, "BHP": 58.0, "NAB": 40.0, "WBC": 34.0, "ANZ": 37.0, "RIO": 160.0, "FMG": 15.0}
DEMO_STEP = {"CBA": 2.0, "BHP": 1.0, "NAB": 1.0, "WBC": 1.0, "ANZ": 1.0, "RIO": 2.0, "FMG": 0.5}
DEMO = {"index": 9100.0}

TF = {  # page timeframe -> (yahoo interval, yahoo range, seconds)
    "1m": ("1m", "5d", 60),
    "5m": ("5m", "5d", 300),
    "15m": ("15m", "1mo", 900),
    "1h": ("60m", "3mo", 3600),
    "4h": (None, None, 14400),  # MT5 only
}


# ----------------------------------------------------------------------------
# Level maths (pure, no IBKR needed)
# ----------------------------------------------------------------------------
def merge_levels(items, tol):
    """items: [(level, gex, name)]. Levels within tol of the first level in a cluster are merged."""
    items = sorted(items, key=lambda x: x[0])
    out, cur = [], None
    for lvl, gex, name in items:
        w = gex if gex > 0 else 1.0
        if cur is not None and lvl - cur["anchor"] <= tol:
            cur["wsum"] += w
            cur["lsum"] += lvl * w
            cur["gex"] += gex
            if name and name not in cur["names"]:
                cur["names"].append(name)
        else:
            if cur:
                out.append(cur)
            cur = {"anchor": lvl, "wsum": w, "lsum": lvl * w, "gex": gex, "names": [name] if name else []}
    if cur:
        out.append(cur)
    return [{"level": c["lsum"] / c["wsum"], "gex": c["gex"], "stocks": "+".join(c["names"])} for c in out]


def days_to_expiry_T(expiry):
    exp_date = dt.datetime.strptime(expiry, "%Y%m%d").date()
    days = max((exp_date - dt.date.today()).days, 0.5)
    return days / 365.0


def analyse_live(sym, snap, S, top_index):
    T = days_to_expiry_T(snap["expiry"])
    rows = []
    for r in snap["rows"]:
        gam = g.bs_gamma(S, r["strike"], T, r["iv"]) if r.get("iv") else 0.0
        rows.append({"strike": r["strike"], "right": r["right"], "oi": r["oi"], "gamma": gam, "iv": r.get("iv")})
    prof = g.build_profile(rows, S)
    cw, pw = g.walls(prof, S, top_index)
    return {"sym": sym, "spot": S, "prof": prof, "net": g.net_gex(prof), "call_walls": cw, "put_walls": pw}


def compute_levels(snaps, spots, index_raw, offset, weights, nodes_top=8, merge_pct=0.05, top_index=1):
    results = [analyse_live(sym, snaps[sym], spots[sym], top_index) for sym in snaps if spots.get(sym)]
    if not results:
        return None
    calls, puts = [], []
    for a in results:
        w = weights.get(a["sym"], 0.0)
        for k, gx in a["call_walls"]:
            calls.append((g.index_level(index_raw, w, k, a["spot"], offset), gx / 1e6, a["sym"]))
        for k, gx in a["put_walls"]:
            puts.append((g.index_level(index_raw, w, k, a["spot"], offset), gx / 1e6, a["sym"]))
    tol = index_raw * merge_pct / 100.0
    cm, pm = merge_levels(calls, tol), merge_levels(puts, tol)
    nodes = g.node_levels(results, weights, index_raw, offset, nodes_top) if nodes_top > 0 else []

    max_w = max([x["gex"] for x in cm + pm] or [1.0]) or 1.0
    max_n = max([abs(gx) / 1e6 for _, gx in nodes] or [1.0]) or 1.0
    levels = []
    for x in cm:
        levels.append({"type": "call", "level": round(x["level"], 1), "gex": round(x["gex"], 2), "stocks": x["stocks"], "strength": round(min(x["gex"] / max_w, 1.0), 3)})
    for x in pm:
        levels.append({"type": "put", "level": round(x["level"], 1), "gex": round(x["gex"], 2), "stocks": x["stocks"], "strength": round(min(x["gex"] / max_w, 1.0), 3)})
    for lvl, gx in nodes:
        m = gx / 1e6
        levels.append({"type": "pos" if m >= 0 else "neg", "level": round(lvl, 1), "gex": round(m, 2), "stocks": "", "strength": round(abs(m) / max_n, 3)})
    levels.sort(key=lambda x: x["level"])

    by_sym = {a["sym"]: a for a in results}
    groups = {}
    for name, members in g.GROUPS.items():
        groups[name] = sum(by_sym[m]["net"] for m in members if m in by_sym) / 1e6
    total = sum(a["net"] for a in results) / 1e6
    return {"levels": levels, "net": round(total, 2), "banks": round(groups.get("Banks", 0.0), 2),
            "miners": round(groups.get("Miners", 0.0), 2),
            "stocks": [{"sym": a["sym"], "spot": round(a["spot"], 3), "net": round(a["net"] / 1e6, 2)} for a in results]}


# ----------------------------------------------------------------------------
# Open-interest snapshot (pulled once a day)
# ----------------------------------------------------------------------------
def snap_path():
    return f"aus200_oi_{dt.date.today():%Y%m%d}.json"


def load_snapshot():
    p = snap_path()
    if not os.path.exists(p):
        return None
    try:
        with open(p) as f:
            snap = json.load(f)
        if not snap.get("stocks"):
            return None
        for s in snap["stocks"].values():
            if dt.datetime.strptime(s["expiry"], "%Y%m%d").date() < dt.date.today():
                return None  # expired contracts, pull again
        return snap
    except Exception:
        return None


def pull_snapshot(ib, args, set_msg):
    syms = [s.strip().upper() for s in args.tickers.split(",") if s.strip()]
    stocks = {}
    for i, sym in enumerate(syms, 1):
        set_msg(f"Pulling open interest: {sym} ({i}/{len(syms)}). This takes about 10 minutes, once a day.")
        data = None
        for attempt in range(3):
            data = g.fetch_stock(ib, sym, args)
            if data:
                break
            print(f"{sym}: attempt {attempt + 1} failed, retrying...")
            ib.sleep(3)
        if data:
            stocks[sym] = {
                "spot": data["spot"], "expiry": data["expiry"],
                "rows": [{"strike": r["strike"], "right": r["right"], "oi": r["oi"], "iv": r["iv"]} for r in data["rows"]],
            }
    if not stocks:
        return None
    snap = {"date": dt.date.today().isoformat(), "stocks": stocks}
    with open(snap_path(), "w") as f:
        json.dump(snap, f)
    print(f"Saved open interest to {snap_path()}")
    return snap


def demo_snapshot():
    random.seed(7)
    expiry = "20261119"
    stocks = {}
    for sym, spot in DEMO_SPOTS.items():
        step = DEMO_STEP[sym]
        rows = []
        k = spot * 0.85
        while k <= spot * 1.15:
            strike = round(round(k / step) * step, 2)
            dist = abs(strike / spot - 1)
            for right in ("C", "P"):
                oi = random.randint(50, 600) * (1 + 14 * math.exp(-((dist * 14) ** 2)) * random.random())
                rows.append({"strike": strike, "right": right, "oi": float(int(oi)), "iv": 0.28 + random.random() * 0.06})
            k += step
        stocks[sym] = {"spot": spot, "expiry": expiry, "rows": rows}
    return {"date": dt.date.today().isoformat(), "stocks": stocks}


# ----------------------------------------------------------------------------
# Price and candle sources
# ----------------------------------------------------------------------------
def http_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def yahoo_chart(interval, rng):
    j = http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/%5EAXJO?interval={interval}&range={rng}")
    res = j["chart"]["result"][0]
    return res


def yahoo_loop():
    while True:
        try:
            res = yahoo_chart("1m", "1d")
            p = res["meta"].get("regularMarketPrice")
            if p:
                YAHOO["price"], YAHOO["ts"] = float(p), time.time()
        except Exception:
            pass
        time.sleep(5)


def fetch_candles(tf, demo=False):
    key = tf
    now = time.time()
    hit = CANDLE_CACHE.get(key)
    if hit and now - hit[0] < (2 if MT5["on"] else 10):
        return hit[1]
    secs = TF[tf][2]
    out = []
    if MT5["on"] and not demo:
        out = mt5_candles(tf)
    elif demo:
        rnd = random.Random(11)
        t = int(now // secs) * secs - 300 * secs
        price = DEMO["index"] * 0.99
        for _ in range(300):
            o = price
            c = o + rnd.gauss(0, 4 * math.sqrt(secs / 60) ** 0.8)
            h = max(o, c) + abs(rnd.gauss(0, 2))
            lo = min(o, c) - abs(rnd.gauss(0, 2))
            out.append({"time": t, "open": o, "high": h, "low": lo, "close": c})
            price = c
            t += secs
        shift = DEMO["index"] - out[-1]["close"]
        for c in out:
            for k in ("open", "high", "low", "close"):
                c[k] += shift
    else:
        if TF[tf][0] is None:  # 4h: built from the 1h candles (Yahoo has no 4h)
            out = []
            for c in fetch_candles("1h"):
                b = int(c["time"] // 14400) * 14400
                if out and out[-1]["time"] == b:
                    o = out[-1]
                    o["high"] = max(o["high"], c["high"]); o["low"] = min(o["low"], c["low"]); o["close"] = c["close"]
                else:
                    out.append({"time": b, "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"]})
            CANDLE_CACHE[key] = (now, out)
            return out
        res = yahoo_chart(TF[tf][0], TF[tf][1])
        ts = res.get("timestamp") or []
        q = res["indicators"]["quote"][0]
        for i, t in enumerate(ts):
            o, h, lo, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
            if None in (o, h, lo, c):
                continue
            out.append({"time": int(t), "open": o, "high": h, "low": lo, "close": c})
    CANDLE_CACHE[key] = (now, out)
    return out


def mt5_server_offset_secs():
    """MT5 stamps bars in broker server time. Most brokers use GMT+2 (GMT+3 in US summer time)."""
    if MT5["utc_offset"] is not None:
        return int(MT5["utc_offset"] * 3600)
    try:
        from zoneinfo import ZoneInfo
        ny = dt.datetime.now(ZoneInfo("America/New_York"))
        return int((2 + (1 if ny.dst() else 0)) * 3600)
    except Exception:
        return 2 * 3600


def mt5_start(symbol, utc_offset=None):
    try:
        import MetaTrader5 as mt5
    except ImportError:
        print("MT5: the MetaTrader5 package is missing. Run:  pip install MetaTrader5 tzdata")
        return False
    with MT5_LOCK:
        if not mt5.initialize():
            print("MT5: could not connect to the MetaTrader 5 terminal:", mt5.last_error(), "(open MT5 and log in first)")
            return False
        if not mt5.symbol_select(symbol, True):
            print(f"MT5: no symbol called '{symbol}'. Check the exact name in MT5 Market Watch (e.g. AUS200).")
            return False
    MT5.update(on=True, symbol=symbol, utc_offset=utc_offset)
    print(f"MT5: connected, using {symbol}")
    return True


def mt5_price():
    """(bid, tick_age_seconds) or (None, None)."""
    import MetaTrader5 as mt5
    with MT5_LOCK:
        t = mt5.symbol_info_tick(MT5["symbol"])
    if t is None or not getattr(t, "bid", 0):
        return None, None
    age = max(0.0, time.time() - (t.time - mt5_server_offset_secs()))
    return float(t.bid), age


def mt5_candles(tf, n=500):
    import MetaTrader5 as mt5
    with MT5_LOCK:
        rates = mt5.copy_rates_from_pos(MT5["symbol"], getattr(mt5, MT5_TF[tf]), 0, n)
    if rates is None:
        return []
    off = mt5_server_offset_secs()
    return [{"time": int(r["time"]) - off, "open": float(r["open"]), "high": float(r["high"]),
             "low": float(r["low"]), "close": float(r["close"])} for r in rates]


def setup_mt5(args):
    """Shared by the dashboard and the publisher. Returns True if MT5 is the price source."""
    if not getattr(args, "mt5", False):
        return False
    ok = mt5_start(args.mt5_symbol, args.mt5_utc_offset)
    if ok:
        CFG["offset"] = 0.0  # levels are built straight from the CFD price
    else:
        print("MT5 failed, falling back to IBKR / Yahoo for the price.")
    return ok


def available_tfs():
    return ["1m", "5m", "15m", "1h", "4h"]


def live_price(t):
    for v in (t.marketPrice(), getattr(t, "last", None), getattr(t, "delayedLast", None)):
        if g.valid(v):
            return float(v)
    return None


# ----------------------------------------------------------------------------
# Web server
# ----------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    demo = False

    def log_message(self, *a):
        pass

    def _send(self, body, ctype="application/json"):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(200)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/":
                self._send(PAGE, "text/html")
            elif u.path == "/api/state":
                with LOCK:
                    self._send(json.dumps(STATE))
            elif u.path == "/api/candles":
                tf = q.get("tf", ["15m"])[0]
                tf = tf if tf in TF else "15m"
                try:
                    candles = fetch_candles(tf, self.demo)
                    with LOCK:
                        off = CFG["offset"]
                    shifted = [{"time": c["time"], "open": c["open"] + off, "high": c["high"] + off,
                                "low": c["low"] + off, "close": c["close"] + off} for c in candles]
                    self._send(json.dumps({"candles": shifted}))
                except Exception as e:
                    self._send(json.dumps({"candles": [], "error": str(e)}))
            elif u.path == "/api/set":
                with LOCK:
                    if not MT5["on"]:
                        CFG["offset"] = float(q.get("offset", ["0"])[0])
                self._send(json.dumps({"ok": True}))
            elif u.path == "/api/calibrate":
                price = float(q.get("price", ["0"])[0])
                with LOCK:
                    raw = STATE.get("raw_index")
                    if MT5["on"]:
                        pass  # price comes straight from your broker, nothing to match
                    elif raw and price > 0:
                        CFG["offset"] = price - raw
                        CFG["manual_price"] = None
                    elif price > 0:
                        CFG["manual_price"] = price  # no feed: use the typed price directly
                self._send(json.dumps({"ok": True}))
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            try:
                self._send(json.dumps({"error": str(e)}))
            except Exception:
                pass


def lan_ip():
    import socket
    try:
        k = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); k.connect(("8.8.8.8", 80)); ip = k.getsockname()[0]; k.close()
        return ip
    except Exception:
        return None


def start_server(port, demo, share=False):
    Handler.demo = demo
    srv = ThreadingHTTPServer(("0.0.0.0" if share else "127.0.0.1", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def set_msg(msg):
    with LOCK:
        STATE["msg"] = msg
    print(msg)


# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------
def make_state(snap, spots, raw_index, source, weights, args, spots_live, offset):
    """Pure: returns the state dict the page draws (or None if nothing could be computed)."""
    res = compute_levels(snap["stocks"], spots, raw_index, offset, weights, args.nodes, args.merge_pct, args.top_index)
    if res is None:
        return None
    net = res["net"]
    return {
        "msg": "Live",
        "ts": time.time(),
        "raw_index": raw_index,
        "offset": offset,
        "index": {"price": round(raw_index + offset, 2), "source": source},
        "oi_date": snap.get("date"),
        "expiry": next(iter(snap["stocks"].values()))["expiry"],
        "net": net, "banks": res["banks"], "miners": res["miners"],
        "regime": "dampening (range / mean-revert)" if net > 0 else "accelerating (trend risk)",
        "stocks": res["stocks"],
        "stock_prices_live": spots_live,
        "levels": res["levels"],
        "tfs": available_tfs(),
    }


def build_state(snap, spots, raw_index, source, weights, args, spots_live):
    with LOCK:
        offset = CFG["offset"]
    st = make_state(snap, spots, raw_index, source, weights, args, spots_live, offset)
    if st is None:
        return
    with LOCK:
        STATE.clear()
        STATE.update(st)


def pick_index(ib_ticker, args):
    """Returns (raw_index_price, source) or (None, None)."""
    if MT5["on"]:
        try:
            p, age = mt5_price()
            if p:
                return p, f"MT5 {MT5['symbol']} (tick {age:.0f}s ago)"
        except Exception:
            pass
    if ib_ticker is not None:
        p = live_price(ib_ticker)
        if p:
            return p, "IBKR XJO"
    if YAHOO["price"] and time.time() - YAHOO["ts"] < 60:
        return YAHOO["price"], "Yahoo ^AXJO"
    with LOCK:
        man, off = CFG["manual_price"], CFG["offset"]
    if man:
        return man - off, "manual"
    if args.index_spot:
        return args.index_spot - off, "manual (--index-spot)"
    return None, None


def run_live(args, weights):
    from ib_async import IB, Index, Stock

    ib = IB()
    set_msg("Connecting to TWS...")
    ib.connect(args.host, args.port, clientId=args.client_id)
    ib.reqMarketDataType(args.mdt)

    snap = None if args.refresh_oi else load_snapshot()
    if snap:
        set_msg(f"Loaded saved open interest from {snap['date']}.")
    else:
        snap = pull_snapshot(ib, args, set_msg)
        if not snap:
            set_msg("No open interest returned. Check TWS is logged in and the ASX market data is on, then restart.")
            ib.disconnect()
            return

    tickers = {}
    for sym in snap["stocks"]:
        stk = Stock(sym, "ASX", "AUD")
        ib.qualifyContracts(stk)
        tickers[sym] = ib.reqMktData(stk, "", False, False)

    ix_ticker = None
    try:
        ix = Index("XJO", "ASX", "AUD")
        ib.qualifyContracts(ix)
        ix_ticker = ib.reqMktData(ix, "", False, False)
    except Exception:
        ix_ticker = None

    set_msg("Live. Open the page in your browser.")
    try:
        while True:
            ib.sleep(args.interval)
            spots, live_n = {}, 0
            for sym, t in tickers.items():
                p = live_price(t)
                if p:
                    live_n += 1
                spots[sym] = p or snap["stocks"][sym]["spot"]
            raw, source = pick_index(ix_ticker, args)
            if raw is None:
                set_msg("No index price. Type your AUS200 chart price into the page and press Match.")
                continue
            build_state(snap, spots, raw, source, weights, args, live_n)
    except KeyboardInterrupt:
        pass
    finally:
        ib.disconnect()


def run_demo(args, weights):
    snap = demo_snapshot()
    base = dict(DEMO_SPOTS)
    set_msg("DEMO MODE: fake data, no TWS. Open the page in your browser.")
    rnd = random.Random(3)
    idx0 = DEMO["index"]
    try:
        while True:
            time.sleep(args.interval)
            DEMO["index"] = max(8000.0, DEMO["index"] + rnd.gauss(0, 1.2))
            f = DEMO["index"] / idx0
            spots = {s: p * f for s, p in base.items()}
            build_state(snap, spots, DEMO["index"], "demo", weights, args, len(spots))
    except KeyboardInterrupt:
        pass


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7496, help="TWS API port")
    ap.add_argument("--client-id", type=int, default=79)
    ap.add_argument("--web-port", type=int, default=8765)
    ap.add_argument("--tickers", default="BHP,FMG,RIO,CBA,ANZ,WBC,NAB")
    ap.add_argument("--expiry", default=None, help="YYYYMMDD (default: nearest monthly)")
    ap.add_argument("--band", type=float, default=0.20)
    ap.add_argument("--wait", type=float, default=8.0)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--mdt", type=int, default=4, help="1 live, 2 frozen, 3 delayed, 4 delayed-frozen")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between recalculations")
    ap.add_argument("--nodes", type=int, default=8)
    ap.add_argument("--merge-pct", type=float, default=0.05, help="merge walls closer than this %% of price")
    ap.add_argument("--top-index", type=int, default=1)
    ap.add_argument("--offset", type=float, default=0.0, help="AUS200 CFD minus XJO, in points")
    ap.add_argument("--index-spot", type=float, default=None, help="fixed AUS200 price if no feed works")
    ap.add_argument("--weights", default=None, help="override, e.g. CBA=10.5,BHP=8.8")
    ap.add_argument("--refresh-oi", action="store_true", help="pull open interest again even if saved today")
    ap.add_argument("--demo", action="store_true", help="fake data, no TWS")
    ap.add_argument("--mt5", action="store_true", help="take price and candles from your MetaTrader 5 (Pepperstone)")
    ap.add_argument("--mt5-symbol", default="AUS200", help="exact symbol name in MT5 Market Watch")
    ap.add_argument("--mt5-utc-offset", type=float, default=None, help="broker server time vs UTC in hours (default: GMT+2, GMT+3 in US summer)")
    ap.add_argument("--share", action="store_true", help="let phones/tablets on your Wi-Fi open the page")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    weights = dict(g.WEIGHTS)
    if args.weights:
        for kv in args.weights.split(","):
            k, v = kv.split("=")
            weights[k.strip().upper()] = float(v)
    CFG["offset"] = args.offset
    setup_mt5(args)

    start_server(args.web_port, args.demo, args.share)
    url = f"http://127.0.0.1:{args.web_port}"
    print(f"Dashboard: {url}")
    if args.share:
        ip = lan_ip()
        print(f"On your phone/tablet (same Wi-Fi) open: http://{ip}:{args.web_port}" if ip else "Could not find this PC's Wi-Fi address; run ipconfig and use the IPv4 address.")
    if not args.demo:
        threading.Thread(target=yahoo_loop, daemon=True).start()
    if not args.no_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        if args.demo:
            run_demo(args, weights)
        else:
            run_live(args, weights)
    except KeyboardInterrupt:
        pass


# ----------------------------------------------------------------------------
# The page
# ----------------------------------------------------------------------------
PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>AUS200 GEX</title>
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0f1115">
<style>
  :root { --bg:#131722; --panel:#1e222d; --line:#2a2e39; --txt:#d1d4dc; --dim:#787b86; --grid:#1c202b; --acc:#2962ff; --c-call:#ff3df5; --c-put:#3d7bff; --c-pos:#2f6bff; --c-neg:#ff9d2e; --hi:#ffd23f; color-scheme:dark; }
  * { box-sizing:border-box; -webkit-tap-highlight-color:transparent; }
  html, body { margin:0; height:100%; background:var(--bg); color:var(--txt); font:13px/1.35 -apple-system, BlinkMacSystemFont, "Trebuchet MS", Roboto, "Segoe UI", sans-serif; overscroll-behavior:none; }
  #app { display:flex; flex-direction:column; height:100%; height:100dvh; padding-left:env(safe-area-inset-left,0px); padding-right:env(safe-area-inset-right,0px); }
  #top { display:flex; flex-wrap:wrap; gap:6px 8px; align-items:center; padding:7px 10px; background:var(--panel); border-bottom:1px solid var(--line); }
  .brand { font-size:14px; letter-spacing:.02em; white-space:nowrap; } .brand span { color:var(--hi); }
  .grow { flex:1; }
  .chip { padding:3px 9px; border-radius:11px; background:var(--bg); border:1px solid var(--line); white-space:nowrap; max-width:100%; overflow:hidden; text-overflow:ellipsis; }
  .chip.ok { border-color:#2e7d5b; } .chip.warn { border-color:#b9770e; } .chip.bad { border-color:#a33; }
  #chipSlot, #chips { display:flex; flex-wrap:wrap; gap:6px 8px; align-items:center; }
  button, select, input[type=number] { font:inherit; color:var(--txt); background:var(--bg); border:1px solid var(--line); border-radius:7px; padding:4px 10px; min-height:28px; }
  button { cursor:pointer; }
  #tfs { display:inline-flex; gap:4px; } #tfSel { padding:4px 6px; max-width:64px; }
  #keys { display:flex; gap:6px; padding:6px 10px; overflow-x:auto; border-bottom:1px solid var(--line); background:var(--bg); scrollbar-width:none; }
  #keys::-webkit-scrollbar { display:none; }
  #keys .k { flex:0 0 auto; display:flex; align-items:center; gap:6px; padding:3px 10px; border-radius:999px; border:1px solid var(--hi); background:var(--panel); white-space:nowrap; font-variant-numeric:tabular-nums; }
  #keys .k i { width:9px; height:9px; border-radius:50%; display:inline-block; }
  #keys .k small { color:var(--dim); }
  #main { flex:1; display:flex; min-height:0; position:relative; }
  #chartwrap { flex:1; position:relative; min-width:0; }
  #chart { position:absolute; inset:0; z-index:1; }
  #ov { position:absolute; inset:0; pointer-events:none; z-index:5; }
  #side { width:330px; overflow:auto; border-left:1px solid var(--line); background:var(--panel); }
  #side.off { display:none; }
  table { width:100%; border-collapse:collapse; font-variant-numeric:tabular-nums; }
  th, td { padding:4px 6px; text-align:right; border-bottom:1px solid var(--line); }
  th { position:sticky; top:0; background:var(--panel); color:var(--dim); font-weight:600; }
  td:first-child, th:first-child { text-align:left; }
  tr.price td { background:#1d2b4d; color:#fff; font-weight:700; }
  tr.key td { background:rgba(255,210,63,.10); font-weight:700; }
  .t-call { color:var(--c-call); } .t-put { color:var(--c-put); } .t-pos { color:var(--c-pos); } .t-neg { color:var(--c-neg); }
  .bar { display:inline-block; height:7px; border-radius:2px; background:currentColor; opacity:.8; vertical-align:middle; }
  #msg { padding:10px 12px; color:var(--dim); }
  #timer { display:flex; justify-content:space-between; gap:10px; padding:5px 10px 0; font-size:12px; color:var(--dim); font-variant-numeric:tabular-nums; background:var(--bg); white-space:nowrap; }
  #timer b { color:var(--txt); } #timer span { overflow:hidden; text-overflow:ellipsis; }
  #tbar { position:relative; height:20px; margin:6px 10px 0; border-radius:10px; background:var(--line); overflow:hidden; }
  #tbar i { position:absolute; left:0; top:0; bottom:0; width:0; background:#e5484d; transition:width .3s linear, background-color .3s linear; }
  #tbar em { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-style:normal; font-weight:700; font-size:13px; color:#fff; text-shadow:0 1px 2px rgba(0,0,0,.75); font-variant-numeric:tabular-nums; }
  /* TradingView-style chrome */
  #top { border-bottom:1px solid var(--line); }
  button { border-color:transparent; background:transparent; } button:hover { background:var(--line); }
  button.on, button.on:hover { background:var(--acc); border-color:var(--acc); color:#fff; }
  .chip { background:transparent; }
  #tools { width:44px; flex:0 0 44px; display:flex; flex-direction:column; align-items:center; gap:2px; padding:6px 0; background:var(--panel); border-right:1px solid var(--line); overflow-y:auto; scrollbar-width:none; z-index:12; }
  #tools::-webkit-scrollbar { display:none; }
  #tools button { width:36px; height:36px; min-height:36px; padding:0; border:0; border-radius:6px; display:flex; align-items:center; justify-content:center; color:var(--dim); flex:0 0 auto; }
  #tools button:hover { background:var(--line); color:var(--txt); }
  #tools button.on { background:var(--acc); color:#fff; } #tools button.warn { background:#a33; color:#fff; }
  #tools hr { width:26px; border:0; border-top:1px solid var(--line); margin:4px 0; flex:0 0 auto; }
  #tools svg { width:20px; height:20px; stroke:currentColor; fill:none; stroke-width:1.6; stroke-linecap:round; stroke-linejoin:round; pointer-events:none; }
  #legend { position:absolute; top:6px; left:8px; z-index:6; pointer-events:none; font-size:12px; line-height:1.4; font-variant-numeric:tabular-nums; text-shadow:0 0 4px var(--bg), 0 0 4px var(--bg); }
  #legend .dim { color:var(--dim); } #legend b { font-weight:600; }
  #propbar { display:none; position:absolute; bottom:40px; left:50%; transform:translateX(-50%); z-index:9; gap:8px; align-items:center; padding:5px 8px; background:var(--panel); border:1px solid var(--line); border-radius:8px; box-shadow:0 4px 16px rgba(0,0,0,.4); }
  #propbar.show { display:flex; }
  #propbar input[type=color] { width:34px; height:28px; padding:0; border:1px solid var(--line); background:none; border-radius:6px; }
  #propbar select { width:auto; }
  #textin { display:none; position:absolute; z-index:9; width:160px; background:var(--panel); color:var(--txt); border:1px solid var(--acc); border-radius:6px; padding:5px 8px; font:inherit; }
  #btnDraw { display:none; }
  @media (max-width:760px), (max-height:520px) {
    #btnDraw { display:inline-block; }
    #tools { display:none; position:absolute; left:0; top:0; bottom:0; }
    body.tools-open #tools { display:flex; }
    body.tools-open #legend { left:52px; }
  }
  @media (max-height:520px) {
    .brand, #cReg, #keys { display:none; }
    #top { padding:3px 8px; gap:4px 6px; } #timer { padding-top:2px; } #tbar { height:16px; margin-top:3px; } #legend { font-size:11px; }
  }
  /* menu drawer */
  #set { display:none; position:absolute; top:0; right:0; bottom:0; width:min(340px,100%); overflow:auto; z-index:20; background:var(--panel); border-left:1px solid var(--line); box-shadow:-8px 0 28px rgba(0,0,0,.45); padding-bottom:20px; -webkit-overflow-scrolling:touch; }
  #set.open { display:block; }
  .sethead { position:sticky; top:0; z-index:2; display:flex; justify-content:space-between; align-items:center; padding:8px 12px; background:var(--panel); border-bottom:1px solid var(--line); }
  #set details { border-bottom:1px solid var(--line); }
  #set summary { cursor:pointer; padding:11px 12px; font-weight:600; list-style:none; display:flex; justify-content:space-between; align-items:center; min-height:42px; }
  #set summary::-webkit-details-marker { display:none; }
  #set summary::after { content:"\25BE"; color:var(--dim); transition:transform .15s; }
  #set details[open] > summary::after { transform:rotate(180deg); }
  #set .pad { padding:2px 12px 12px; display:flex; flex-direction:column; gap:8px; }
  #set .r { display:flex; align-items:center; justify-content:space-between; gap:10px; min-height:32px; }
  #set .r > span { flex:1; }
  #set select, #set input[type=number] { width:116px; }
  #set input[type=color] { width:48px; height:30px; padding:0; border:1px solid var(--line); background:none; border-radius:6px; }
  #set input[type=checkbox] { width:20px; height:20px; accent-color:#3d63b8; }
  #set .btns { display:flex; gap:8px; padding:12px; }
  .star { background:none; border:0; font-size:20px; color:var(--dim); padding:0 6px; min-height:32px; } .star.on { color:var(--hi); background:none; }
  /* compact layouts: phones, and landscape phones */
  @media (max-width:760px), (max-height:520px) {
    button, select, input[type=number] { min-height:34px; }
    #chips { display:none; }
    #side { position:absolute; z-index:15; display:none; border-left:0; }
    body.side-open #side { display:block; }
  }
  @media (max-width:760px) and (orientation:portrait), (max-height:520px) and (orientation:portrait) {
    #side { left:0; right:0; bottom:0; width:auto; height:46%; border-top:1px solid var(--line); box-shadow:0 -8px 24px rgba(0,0,0,.4); }
  }
  @media (max-width:760px) and (orientation:landscape), (max-height:520px) and (orientation:landscape) {
    #side { top:0; right:0; bottom:0; width:min(320px,55%); border-left:1px solid var(--line); box-shadow:-8px 0 24px rgba(0,0,0,.4); }
    #top { padding:4px 8px; } #keys { padding:3px 8px; }
  }
  /* intro: logo fades in, then the whole overlay fades out */
  #intro { position:fixed; inset:0; z-index:100; display:flex; flex-direction:column; align-items:center; justify-content:center; gap:6px; background:var(--bg); pointer-events:none; animation:introOut 2.4s ease forwards; }
  #intro .mark { font-size:clamp(26px,7vw,44px); font-weight:700; letter-spacing:.04em; animation:introIn 1.1s ease both; } #intro .mark b { color:var(--hi); }
  #intro .sub { color:var(--dim); letter-spacing:.14em; text-transform:uppercase; font-size:11px; animation:introIn 1.1s .25s ease both; }
  @keyframes introIn { from { opacity:0; } to { opacity:1; } }
  @keyframes introOut { 0%,68% { opacity:1; } 100% { opacity:0; visibility:hidden; } }
  @media (prefers-reduced-motion: reduce) { #intro { display:none; } }
</style></head>
<body>
<div id="intro"><div class="mark">AUS200 <b>GEX</b></div><div class="sub">gamma exposure levels</div></div>
<div id="app">
  <header id="top">
    <b class="brand">AUS200 <span>GEX</span></b>
    <span class="chip" id="cPrice">price -</span>
    <span class="chip" id="cReg">GEX -</span>
    <span id="chipSlot"><span id="chips">
      <span class="chip" id="cRegL">-</span>
      <span class="chip" id="cSrc">source -</span>
      <span class="chip" id="cGrp">banks - | miners -</span>
      <span class="chip" id="cOi">OI -</span>
      <span class="chip" id="cAge">update -</span>
    </span></span>
    <span class="grow"></span>
    <span id="tfs"></span><select id="tfSel" aria-label="More timeframes"></select>
    <button id="btnDraw" title="Drawing tools">Draw</button>
    <button id="btnLevels" title="Show or hide the level list">Levels</button>
    <button id="btnMenu" title="Menu and settings">Menu</button>
  </header>
  <div id="timer"><span id="tBar">bar timer: waiting for candles</span><span id="tData">-</span></div>
  <div id="tbar"><i id="tfill"></i><em id="ttxt">-</em></div>
  <div id="keys"></div>
  <div id="main">
    <div id="tools"></div>
    <div id="chartwrap"><div id="chart"></div><canvas id="ov"></canvas><div id="legend"></div>
      <div id="propbar"><input id="pcol" type="color" value="#2962ff" aria-label="Colour"><select id="pw" aria-label="Width"><option>1</option><option>2</option><option>3</option><option>4</option></select><select id="ps" aria-label="Style"><option>Solid</option><option>Dashed</option><option>Dotted</option></select><button id="pdel" title="Delete drawing">Delete</button></div>
      <input id="textin" type="text" placeholder="Type text, press Enter"></div>
    <div id="side"><div id="msg">Waiting for data...</div><table id="tbl" style="display:none">
      <thead><tr><th>Level</th><th>GEX m</th><th>Dist</th><th>Str</th></tr></thead><tbody id="tb"></tbody></table></div>
    <div id="set">
      <div class="sethead"><b>Menu</b><button id="btnCloseSet">Close</button></div>
      <details open><summary>Status</summary><div class="pad" id="menuStatus"></div></details>
      <details open><summary>Price match and export</summary><div class="pad">
        <div class="r"><span>My chart price</span><input id="inPrice" type="number" step="0.1" placeholder="e.g. 9120"></div>
        <div class="r"><span>Offset (points)</span><input id="inOff" type="number" step="0.5" value="0"></div>
        <div class="r"><button id="btnMatch" title="Sets the offset so candles and levels match your AUS200 chart">Match price</button>
          <button id="btnCopy" title="Copies the current levels as one line to paste into your TradingView indicator">Copy for TradingView</button></div>
      </div></details>
      <details><summary>Timeframes (tap a star to favourite)</summary><div class="pad" id="tfFav"></div></details>
      <div id="setGroups"></div>
      <div class="btns"><button id="btnReset">Reset to defaults</button></div>
    </div>
  </div>
</div>
<script>window.STATIC = false;</script>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
const $ = id => document.getElementById(id);
if (typeof LightweightCharts === 'undefined') {
  $('msg').textContent = 'The chart library did not load. This page needs internet access (unpkg.com).';
}
const NAME = { call:'CALL', put:'PUT', pos:'POS', neg:'NEG' };
const KEYNAME = { call:'Call wall', put:'Put wall', pos:'+ node', neg:'- node' };
const ALLTF = ['1m', '5m', '15m', '1h', '4h'];
const THEMES = {
  Dark:  { bg:'#131722', panel:'#1e222d', line:'#2a2e39', txt:'#d1d4dc', dim:'#787b86', grid:'#1c202b', lab:'19,23,34', labTxt:'#e6e9f0' },
  Black: { bg:'#000000', panel:'#0b0b0c', line:'#1f2023', txt:'#e8e8ea', dim:'#8b8d93', grid:'#101012', lab:'0,0,0', labTxt:'#f0f0f2' },
  Slate: { bg:'#101722', panel:'#162030', line:'#25344a', txt:'#d9e2f0', dim:'#8fa1b8', grid:'#172335', lab:'16,23,34', labTxt:'#e8eef8' },
  Light: { bg:'#f6f7f9', panel:'#ffffff', line:'#d9dde5', txt:'#1b2230', dim:'#5d6b82', grid:'#e6e9ef', lab:'255,255,255', labTxt:'#1b2230' }
};
const tzShift = -new Date().getTimezoneOffset() * 60;
const STATIC = !!window.STATIC;

// ---- settings (saved in this browser)
const SCHEMA = [
  ['Show', [
    { k:'walls', t:'bool', l:'Call / put walls', d:true },
    { k:'nodes', t:'bool', l:'Net GEX nodes', d:true },
    { k:'posOn', t:'bool', l:'Positive nodes', d:true },
    { k:'negOn', t:'bool', l:'Negative nodes', d:true },
    { k:'hideStale', t:'bool', l:'Hide levels after expiry', d:true },
    { k:'scale', t:'bool', l:'Scale chart to levels', d:true } ]],
  ['Highlight biggest', [
    { k:'hiOn',    t:'bool',  l:'Highlight biggest of each type', d:true },
    { k:'hiCol',   t:'color', l:'Highlight colour', d:'#ffd23f' },
    { k:'hiWidth', t:'num',   l:'Glow thickness', d:5, min:0, max:14, step:1 },
    { k:'keyBar',  t:'bool',  l:'Show key level bar', d:true } ]],
  ['Chart', [
    { k:'theme',  t:'sel',  l:'Theme', d:'Dark', o:['Dark','Black','Slate','Light'] },
    { k:'candleStyle', t:'sel', l:'Chart type', d:'Candles', o:['Candles','Hollow candles','Bars (OHLC)','Line','Area'] },
    { k:'upCol',  t:'color', l:'Up candle', d:'#26a69a' },
    { k:'downCol', t:'color', l:'Down candle', d:'#ef5350' },
    { k:'gridOn', t:'bool',  l:'Grid lines', d:true },
    { k:'priceLine', t:'bool', l:'Last price line', d:true },
    { k:'bars',   t:'num',   l:'Bars shown on open', d:130, min:30, max:400, step:10 } ]],
  ['Level colours', [
    { k:'call', t:'color', l:'Call wall', d:'#ff3df5' },
    { k:'put',  t:'color', l:'Put wall', d:'#3d7bff' },
    { k:'pos',  t:'color', l:'Positive node', d:'#2f6bff' },
    { k:'neg',  t:'color', l:'Negative node', d:'#ff9d2e' } ]],
  ['Drawn as', [
    { k:'callMode', t:'sel', l:'Call walls', d:'Line', o:['Line','Box'] },
    { k:'putMode',  t:'sel', l:'Put walls', d:'Line', o:['Line','Box'] },
    { k:'posMode',  t:'sel', l:'Positive nodes', d:'Box', o:['Line','Box'] },
    { k:'negMode',  t:'sel', l:'Negative nodes', d:'Box', o:['Line','Box'] } ]],
  ['Lines', [
    { k:'wallStyle', t:'sel', l:'Wall line style', d:'Dashed', o:['Solid','Dashed','Dotted'] },
    { k:'nodeStyle', t:'sel', l:'Node line style', d:'Solid', o:['Solid','Dashed','Dotted'] },
    { k:'widthMode', t:'sel', l:'Line thickness', d:'By strength', o:['By strength','Fixed'] },
    { k:'maxW',   t:'num', l:'Max width (by strength)', d:4, min:1, max:8, step:1 },
    { k:'fixedW', t:'num', l:'Width (fixed)', d:2, min:1, max:8, step:1 },
    { k:'lineTr', t:'num', l:'Transparency (0-95)', d:0, min:0, max:95, step:5 },
    { k:'extent', t:'sel', l:'Line length', d:'Full width', o:['Full width','Bars'] },
    { k:'back',   t:'num', l:'Start (bars back)', d:50, min:0, max:5000, step:5 },
    { k:'fwd',    t:'num', l:'End (bars forward)', d:100, min:1, max:5000, step:5 } ]],
  ['Boxes / zones', [
    { k:'zoneW',  t:'num', l:'Zone half-width (% of price)', d:0.03, min:0, max:1, step:0.01 },
    { k:'zoneMid', t:'bool', l:'Thin centre line in boxes', d:true },
    { k:'boxStrong', t:'num', l:'Opacity at strongest (%)', d:55, min:5, max:100, step:5 },
    { k:'boxWeak',   t:'num', l:'Opacity at weakest (%)', d:12, min:1, max:100, step:1 } ]],
  ['Labels', [
    { k:'showLab', t:'bool', l:'Show labels', d:true },
    { k:'hPos',    t:'sel', l:'Horizontal', d:'Right', o:['Left','Centre','Right'] },
    { k:'labSize', t:'sel', l:'Size', d:'Small', o:['Tiny','Small','Normal'] },
    { k:'labTxt',  t:'sel', l:'Text colour', d:'Theme', o:['Theme','Level colour'] },
    { k:'labBgTr', t:'num', l:'Background transparency', d:18, min:0, max:95, step:5 },
    { k:'labGap',  t:'num', l:'Min spacing (% of price)', d:0.02, min:0, max:1, step:0.005 },
    { k:'labStocks', t:'bool', l:'Show stock names', d:true },
    { k:'labGex',  t:'bool', l:'Show GEX value', d:true },
    { k:'labBars', t:'bool', l:'Show strength bars', d:true } ]],
  ['Merge', [
    { k:'merge',    t:'bool', l:'Merge stacked levels', d:true },
    { k:'mergePct', t:'num', l:'Merge within (% of price)', d:0.05, min:0, max:1, step:0.01 } ]],
  ['App', [
    { k:'intro', t:'bool', l:'Fade-in intro on open', d:true },
    { k:'table', t:'bool', l:'Level list (desktop)', d:true } ]]
];
const S = { fav:['1m','5m','15m','1h','4h'], tf:'15m', drawCol:'#2962ff', drawW:2, drawStyle:'Solid', magnet:false };
for (const g of SCHEMA) for (const f of g[1]) S[f.k] = f.d;
try { Object.assign(S, JSON.parse(localStorage.getItem('aus_set') || '{}')); } catch (e) {}
if (!Array.isArray(S.fav)) S.fav = ['1m','5m','15m','1h','4h'];
if (S.labTxt === 'White') S.labTxt = 'Theme';
function saveSet() { try { localStorage.setItem('aus_set', JSON.stringify(S)); } catch (e) {} }
function T() { return THEMES[S.theme] || THEMES.Dark; }

let tf = ALLTF.includes(S.tf) ? S.tf : '15m', state = null, base = null, cache = {}, lastCandle = null, lastT = 0, nBars = 0, inflight = false;
let curCs = [], availTf = ALLTF.slice(), series = null;
let off = 0;
if (STATIC) { try { off = parseFloat(localStorage.getItem('aus_off') || '0') || 0; } catch (e) {} }
function saveOff() { try { localStorage.setItem('aus_off', String(off)); } catch (e) {} }
function adjust() {
  if (!base || !base.levels || !base.index) { state = base; return; }
  state = Object.assign({}, base, {
    levels: base.levels.map(L => Object.assign({}, L, { level: Math.round((L.level + off) * 10) / 10 })),
    index: { price: base.index.price + off, source: base.index.source },
    offset: (base.offset || 0) + off
  });
}
function rgba(hex, a) { const n = parseInt(hex.slice(1), 16); return 'rgba(' + (n >> 16 & 255) + ',' + (n >> 8 & 255) + ',' + (n & 255) + ',' + a + ')'; }

// ---- level selection, merge, and the biggest of each type
function mergeLevels(levels, p) {
  if (!S.merge) return levels;
  const tol = p * S.mergePct / 100, out = [];
  for (const type of ['call', 'put', 'pos', 'neg']) {
    const g = levels.filter(L => L.type === type).sort((a, b) => a.level - b.level);
    let cur = null;
    for (const L of g) {
      if (cur && L.level - cur.hi <= tol) { cur.items.push(L); cur.hi = L.level; }
      else { cur = { items:[L], hi:L.level }; out.push(cur); }
    }
  }
  return out.map(c => {
    const it = c.items; if (it.length === 1) return Object.assign({}, it[0]);
    const top = it.reduce((a, b) => Math.abs(b.gex) > Math.abs(a.gex) ? b : a);
    const stocks = [...new Set(it.flatMap(L => String(L.stocks || '').split(/[^A-Za-z0-9]+/).filter(Boolean)))].join('+');
    return Object.assign({}, top, { gex: it.reduce((s, L) => s + L.gex, 0), stocks, strength: Math.max(...it.map(L => L.strength)) });
  });
}
function visLevels() {
  if (!state || !state.levels || !state.index) return [];
  if (S.hideStale && state.expiry) {
    const d = new Date(), today = d.getFullYear() * 10000 + (d.getMonth() + 1) * 100 + d.getDate();
    if (today > parseInt(state.expiry, 10)) return [];
  }
  const ok = L => (L.type === 'call' || L.type === 'put') ? S.walls : (S.nodes && (L.type === 'pos' ? S.posOn : S.negOn));
  const lv = mergeLevels(state.levels.filter(ok), state.index.price);
  for (const type of ['call', 'put', 'pos', 'neg']) {
    let best = null;
    for (const L of lv) if (L.type === type && (!best || Math.abs(L.gex) > Math.abs(best.gex))) best = L;
    for (const L of lv) if (L.type === type) L._key = (L === best);
  }
  return lv;
}

// ---- chart
const chart = LightweightCharts.createChart($('chart'), {
  layout:{ background:{ color:'#0f1115' }, textColor:'#c8ccd4' },
  grid:{ vertLines:{ color:'#171a22' }, horzLines:{ color:'#171a22' } },
  timeScale:{ timeVisible:true, secondsVisible:false, rightOffset:12 },
  rightPriceScale:{ borderColor:'#2a2e38' }, crosshair:{ mode:0 }
});
function autoscale(orig) {
  const r = orig();
  if (!S.scale || !r || !state || !state.levels || !state.index) return r;
  const p = state.index.price; let lo = r.priceRange.minValue, hi = r.priceRange.maxValue;
  for (const L of state.levels) if (Math.abs(L.level / p - 1) < 0.012) { lo = Math.min(lo, L.level); hi = Math.max(hi, L.level); }
  return { priceRange:{ minValue:lo, maxValue:hi }, margins:r.margins };
}
const isPoint = () => S.candleStyle === 'Line' || S.candleStyle === 'Area';
const pt = c => isPoint() ? { time:c.time, value:c.close } : c;
function makeSeries() {
  if (series) { try { chart.removeSeries(series); } catch (e) {} }
  const common = { autoscaleInfoProvider:autoscale, priceLineVisible:S.priceLine, lastValueVisible:true };
  const st = S.candleStyle;
  if (st === 'Line') series = chart.addLineSeries(Object.assign({ color:S.upCol, lineWidth:2 }, common));
  else if (st === 'Area') series = chart.addAreaSeries(Object.assign({ lineColor:S.upCol, topColor:rgba(S.upCol, 0.35), bottomColor:rgba(S.upCol, 0), lineWidth:2 }, common));
  else if (st === 'Bars (OHLC)') series = chart.addBarSeries(Object.assign({ upColor:S.upCol, downColor:S.downCol }, common));
  else {
    const hollow = st === 'Hollow candles';
    series = chart.addCandlestickSeries(Object.assign({
      upColor: hollow ? 'rgba(0,0,0,0)' : S.upCol, downColor:S.downCol, borderVisible:hollow,
      borderUpColor:S.upCol, borderDownColor:S.downCol, wickUpColor:S.upCol, wickDownColor:S.downCol }, common));
  }
  if (curCs.length) series.setData(curCs.map(pt));
}
function applyChartLook() {
  const t = T();
  chart.applyOptions({
    layout:{ background:{ color:t.bg }, textColor:t.dim },
    grid:{ vertLines:{ color:t.grid, visible:S.gridOn }, horzLines:{ color:t.grid, visible:S.gridOn } },
    rightPriceScale:{ borderColor:t.line }, timeScale:{ borderColor:t.line }
  });
}
function applyTheme() {
  const t = T(), r = document.documentElement.style;
  for (const k of ['bg', 'panel', 'line', 'txt', 'dim', 'grid']) r.setProperty('--' + k, t[k]);
  for (const k of ['call', 'put', 'pos', 'neg']) r.setProperty('--c-' + k, S[k]);
  r.setProperty('--hi', S.hiCol); r.colorScheme = S.theme === 'Light' ? 'light' : 'dark';
  const m = document.querySelector('meta[name=theme-color]'); if (m) m.content = t.panel;
}

// ---- layout (phone / tablet / desktop, portrait / landscape)
const mq = window.matchMedia('(max-width:760px), (max-height:520px)');
function isCompact() { return mq.matches; }
function layout() {
  const compact = isCompact();
  document.body.classList.toggle('compact', compact);
  (compact ? $('menuStatus') : $('chipSlot')).appendChild($('chips'));
  $('chips').style.display = '';
  if (compact) $('chips').style.display = 'flex';
  $('side').classList.toggle('off', !compact && !S.table);
  $('keys').style.display = S.keyBar ? '' : 'none';
  $('btnDraw').classList.toggle('on', document.body.classList.contains('tools-open'));
  $('btnLevels').classList.toggle('on', compact ? document.body.classList.contains('side-open') : !!S.table);
  setTimeout(sizeChart, 0);
}
function sizeChart() { chart.resize($('chart').clientWidth, $('chart').clientHeight); }
if (mq.addEventListener) mq.addEventListener('change', layout); else mq.addListener(layout);
window.addEventListener('orientationchange', () => setTimeout(() => { layout(); sizeChart(); }, 250));
window.addEventListener('resize', () => setTimeout(sizeChart, 0));

// ---- timeframes
function buildTfs() {
  const list = availTf;
  $('tfs').innerHTML = '';
  for (const t of S.fav) {
    if (!list.includes(t)) continue;
    const b = document.createElement('button'); b.textContent = t; b.dataset.tf = t; b.onclick = () => setTf(t);
    $('tfs').appendChild(b);
  }
  const sel = $('tfSel'); sel.innerHTML = '';
  const first = document.createElement('option'); first.textContent = 'TF'; first.value = ''; sel.appendChild(first);
  for (const t of list) { const o = document.createElement('option'); o.textContent = t; o.value = t; sel.appendChild(o); }
  sel.value = '';
  const fav = $('tfFav'); fav.innerHTML = '';
  for (const t of ALLTF) {
    const row = document.createElement('div'); row.className = 'r';
    const lab = document.createElement('span'); lab.textContent = t + (list.includes(t) ? '' : ' (no data)'); row.appendChild(lab);
    const st = document.createElement('button'); st.className = 'star' + (S.fav.includes(t) ? ' on' : ''); st.textContent = S.fav.includes(t) ? '★' : '☆';
    st.setAttribute('aria-label', 'Favourite ' + t);
    st.onclick = () => { S.fav = S.fav.includes(t) ? S.fav.filter(x => x !== t) : ALLTF.filter(x => x === t || S.fav.includes(x)); saveSet(); buildTfs(); };
    row.appendChild(st); fav.appendChild(row);
  }
  markTf();
}
function markTf() { for (const b of $('tfs').children) b.classList.toggle('on', b.dataset.tf === tf); }
function setTf(t) { tf = t; S.tf = t; saveSet(); markTf(); lastT = 0; loadCandles(true); }
$('tfSel').onchange = () => { const v = $('tfSel').value; $('tfSel').value = ''; if (v) setTf(v); };

// ---- menu
function buildSet() {
  const box = $('setGroups'); box.innerHTML = '';
  for (const [name, fields] of SCHEMA) {
    const det = document.createElement('details'); const sum = document.createElement('summary'); sum.textContent = name; det.appendChild(sum);
    const pad = document.createElement('div'); pad.className = 'pad'; det.appendChild(pad);
    for (const f of fields) {
      const row = document.createElement('div'); row.className = 'r';
      const lab = document.createElement('span'); lab.textContent = f.l; row.appendChild(lab);
      let el;
      if (f.t === 'sel') {
        el = document.createElement('select');
        for (const o of f.o) { const op = document.createElement('option'); op.textContent = o; el.appendChild(op); }
        el.value = S[f.k];
      } else {
        el = document.createElement('input');
        el.type = f.t === 'bool' ? 'checkbox' : f.t === 'color' ? 'color' : 'number';
        if (f.t === 'num') { el.min = f.min; el.max = f.max; el.step = f.step; el.inputMode = 'decimal'; }
        if (f.t === 'bool') el.checked = !!S[f.k]; else el.value = S[f.k];
      }
      el.onchange = () => {
        S[f.k] = f.t === 'bool' ? el.checked : f.t === 'num' ? (isNaN(parseFloat(el.value)) ? f.d : parseFloat(el.value)) : el.value;
        saveSet(); applySet(f.k);
      };
      row.appendChild(el); pad.appendChild(row);
    }
    box.appendChild(det);
  }
}
function applySet(k) {
  applyTheme(); applyChartLook();
  if (k === 'candleStyle' || k === 'upCol' || k === 'downCol' || k === 'priceLine') makeSeries();
  layout(); render();
}
$('btnReset').onclick = () => { const keep = { fav:S.fav, tf:S.tf }; for (const g of SCHEMA) for (const f of g[1]) S[f.k] = f.d; Object.assign(S, keep); saveSet(); buildSet(); makeSeries(); applySet(); };
function openMenu(open) {
  $('set').classList.toggle('open', open); $('btnMenu').classList.toggle('on', open);
  if (open) document.body.classList.remove('side-open');
  layout();
}
$('btnMenu').onclick = () => openMenu(!$('set').classList.contains('open'));
$('btnCloseSet').onclick = () => openMenu(false);
$('btnLevels').onclick = () => {
  if (isCompact()) { document.body.classList.toggle('side-open'); if (document.body.classList.contains('side-open')) $('set').classList.remove('open'); }
  else { S.table = !S.table; saveSet(); }
  layout();
};

// ---- price match / export
$('btnMatch').onclick = async () => {
  const v = parseFloat($('inPrice').value);
  if (!(v > 0)) return;
  if (STATIC) { if (base && base.index) { off = v - base.index.price; saveOff(); adjust(); render(); loadCandles(true); } }
  else { await fetch('/api/calibrate?price=' + v); loadCandles(true); }
};
$('inOff').onchange = async () => {
  const v = parseFloat($('inOff').value) || 0;
  if (STATIC) { off = v; saveOff(); adjust(); render(); loadCandles(true); }
  else { await fetch('/api/set?offset=' + v); loadCandles(true); }
};
function tvLine() {
  if (!state || !state.levels || !state.levels.length) return '';
  const e = (L) => L.level.toFixed(1) + ':' + Math.abs(L.gex).toFixed(1) + ':' + (L.stocks || 'X');
  const calls = state.levels.filter(L => L.type === 'call').map(e);
  const puts = state.levels.filter(L => L.type === 'put').map(e);
  const nodes = state.levels.filter(L => L.type === 'pos' || L.type === 'neg').map(L => L.level.toFixed(1) + ':' + (L.gex >= 0 ? '+' : '-') + Math.abs(L.gex).toFixed(1));
  const x = String(state.expiry || '');
  const exp = x.length === 8 ? x.slice(0, 4) + '-' + x.slice(4, 6) + '-' + x.slice(6, 8) : '';
  const d = new Date(), pad = n => String(n).padStart(2, '0');
  const upd = d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
  return ['AUS200,XJO', 'C ' + calls.join(', '), 'P ' + puts.join(', '), 'N ' + nodes.join(', '), 'NET ' + state.net.toFixed(1),
          'BANKS ' + state.banks.toFixed(1), 'MINERS ' + state.miners.toFixed(1), exp ? 'EXP ' + exp : '', 'UPD ' + upd].filter(Boolean).join(' | ');
}
$('btnCopy').onclick = async () => {
  const t = tvLine(), b = $('btnCopy');
  if (!t) return;
  try { await navigator.clipboard.writeText(t); b.textContent = 'Copied!'; }
  catch (e) { b.textContent = 'Copy failed'; }
  setTimeout(() => { b.textContent = 'Copy for TradingView'; }, 2000);
};

// ---- candles
async function loadCandles(reset) {
  try {
    let raw;
    if (STATIC) raw = (cache[tf] || []).map(c => ({ time:c.time, open:c.open + off, high:c.high + off, low:c.low + off, close:c.close + off }));
    else raw = (await (await fetch('/api/candles?tf=' + tf)).json()).candles || [];
    const cs = raw.map(c => ({ time: c.time + tzShift, open:c.open, high:c.high, low:c.low, close:c.close }));
    if (!cs.length) return;
    if (reset || !lastT) {
      curCs = cs; series.setData(curCs.map(pt)); nBars = cs.length;
      try { chart.timeScale().setVisibleLogicalRange({ from: Math.max(0, cs.length - S.bars), to: cs.length + 12 }); } catch (e) {}
    } else {
      for (const c of cs) if (c.time >= lastT) {
        if (c.time > lastT) { nBars++; curCs.push(c); } else curCs[curCs.length - 1] = c;
        series.update(pt(c));
      }
    }
    lastCandle = Object.assign({}, curCs[curCs.length - 1]); lastT = lastCandle.time;
  } catch (e) {}
}
function applyLive() {
  if (!lastCandle || !state || !state.index) return;
  const p = state.index.price;
  lastCandle.close = p; lastCandle.high = Math.max(lastCandle.high, p); lastCandle.low = Math.min(lastCandle.low, p);
  if (curCs.length) curCs[curCs.length - 1] = Object.assign({}, lastCandle);
  series.update(pt(lastCandle));
}

// ---- bar countdown and refresh timer
let lastPollAt = 0;
const TFSEC = { '1m':60, '5m':300, '15m':900, '1h':3600, '4h':14400 };
function fmtT(s) { s = Math.max(0, Math.floor(s)); const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), x = s % 60, p = n => String(n).padStart(2, '0'); return h ? h + ':' + p(m) + ':' + p(x) : m + ':' + p(x); }
function tick() {
  const secs = TFSEC[tf], now = Date.now() / 1000;
  if (lastT && secs) {
    const rem = (lastT - tzShift) + secs - now;
    if (rem > 0 && rem <= secs) {
      const f = (secs - rem) / secs;  // 0 at bar open, 1 at bar close: red, then yellow, then green
      $('tBar').textContent = tf + ' bar closes in'; $('ttxt').textContent = fmtT(rem);
      $('tfill').style.width = (f * 100) + '%'; $('tfill').style.backgroundColor = 'hsl(' + Math.round(f * 125) + ',75%,42%)';
    } else { $('tBar').textContent = tf + ' bar'; $('ttxt').textContent = 'waiting for next candle'; $('tfill').style.width = '0%'; }
  }
  if (state && state.ts) {
    const age = now - state.ts, ageT = age >= 120 ? Math.round(age / 60) + ' min old' : Math.round(age) + 's old';
    if (window.SNAP) $('tData').textContent = 'snapshot ' + ageT;
    else if (STATIC) $('tData').innerHTML = 'data ' + ageT + ' | page checks in <b>' + fmtT(Math.max(0, 30 - (now - lastPollAt))) + '</b>';
    else $('tData').innerHTML = 'data ' + ageT + ' | <b>live</b>, every 1s';
  }
}
setInterval(tick, 250);

// ---- page text
function chip(id, text, cls) { const e = $(id); e.textContent = text; e.className = 'chip' + (cls ? ' ' + cls : ''); }
function render() {
  if (!state) return;
  if (!state.levels || !state.levels.length || !state.index) {
    $('msg').style.display = ''; $('msg').textContent = state.msg || 'Waiting...'; $('tbl').style.display = 'none'; $('keys').innerHTML = ''; return;
  }
  $('msg').style.display = 'none'; $('tbl').style.display = '';
  const p = state.index.price;
  const want = state.tfs ? ALLTF.filter(t => state.tfs.includes(t)) : ALLTF.slice();
  const avail = STATIC ? ALLTF.filter(t => cache[t] && cache[t].length) : want;
  if (avail.join() !== availTf.join() && avail.length) { availTf = avail; buildTfs(); if (!avail.includes(tf)) { tf = avail.includes('15m') ? '15m' : avail[0]; markTf(); lastT = 0; loadCandles(true); } }
  chip('cPrice', 'price ' + p.toFixed(1));
  chip('cReg', 'GEX ' + (state.net > 0 ? '+' : '') + state.net.toFixed(1) + 'm ' + (state.net > 0 ? 'dampening' : 'accelerating'), state.net > 0 ? 'ok' : 'warn');
  chip('cRegL', 'net GEX ' + (state.net > 0 ? '+' : '') + state.net.toFixed(1) + 'm: ' + state.regime, state.net > 0 ? 'ok' : 'warn');
  chip('cSrc', state.index.source + (state.offset ? ' (offset ' + (state.offset > 0 ? '+' : '') + state.offset.toFixed(1) + ')' : ''), state.index.source.indexOf('manual') >= 0 ? 'warn' : 'ok');
  chip('cGrp', 'banks ' + state.banks.toFixed(1) + 'm | miners ' + state.miners.toFixed(1) + 'm');
  chip('cOi', 'OI ' + state.oi_date + ' | exp ' + state.expiry);
  if (window.SNAP) chip('cAge', 'snapshot ' + new Date(state.ts * 1000).toLocaleString([], { weekday:'short', hour:'2-digit', minute:'2-digit' }) + ' | stocks ' + state.stock_prices_live + '/' + state.stocks.length, 'ok');
  else {
    const age = Math.max(0, Date.now() / 1000 - state.ts);
    const ageTxt = age > 120 ? Math.round(age / 60) + ' min ago' : age.toFixed(0) + 's ago';
    chip('cAge', 'data ' + ageTxt + ' | stocks live ' + state.stock_prices_live + '/' + state.stocks.length, age > (STATIC ? 1500 : 15) ? 'bad' : 'ok');
  }
  if (document.activeElement !== $('inOff')) $('inOff').value = (STATIC ? off : (state.offset || 0)).toFixed(1);

  const rows = visLevels().sort((a, b) => b.level - a.level);
  // key level bar: the biggest call wall, put wall, positive node, negative node
  let kh = '';
  for (const type of ['call', 'put', 'pos', 'neg']) {
    const L = rows.find(x => x.type === type && x._key);
    if (!L) continue;
    const d = (L.level / p - 1) * 100;
    kh += '<span class="k"><i style="background:' + S[type] + '"></i><b>★ ' + KEYNAME[type] + ' ' + L.level.toFixed(1) + '</b><small>' +
      (L.type === 'pos' ? '+' : '') + L.gex.toFixed(1) + 'm | ' + (d > 0 ? '+' : '') + d.toFixed(2) + '%</small></span>';
  }
  $('keys').innerHTML = kh;
  let html = '', placed = false;
  const row = (L) => {
    const d = (L.level / p - 1) * 100, bars = Math.max(1, Math.round(L.strength * 5));
    const lab = (S.hiOn && L._key ? '★ ' : '') + NAME[L.type] + (L.stocks ? ' ' + L.stocks : '');
    return '<tr class="t-' + L.type + (S.hiOn && L._key ? ' key' : '') + '"><td>' + lab + ' ' + L.level.toFixed(1) + '</td><td>' + (L.gex > 0 && (L.type === 'pos') ? '+' : '') + L.gex.toFixed(1) +
      '</td><td>' + (d > 0 ? '+' : '') + d.toFixed(2) + '%</td><td><span class="bar" style="width:' + (bars * 8) + 'px"></span></td></tr>';
  };
  for (const L of rows) {
    if (!placed && L.level < p) { html += '<tr class="price"><td>PRICE ' + p.toFixed(1) + '</td><td></td><td></td><td></td></tr>'; placed = true; }
    html += row(L);
  }
  if (!placed) html += '<tr class="price"><td>PRICE ' + p.toFixed(1) + '</td><td></td><td></td><td></td></tr>';
  $('tb').innerHTML = html;
}

async function poll() {
  if (inflight) return; inflight = true;
  try {
    if (STATIC) {
      if (window.DATA) base = JSON.parse(JSON.stringify(window.DATA));
      else base = await (await fetch('data.json?t=' + Date.now(), { cache:'no-store' })).json();
      cache = base.candles || {};
      adjust(); loadCandles(!curCs.length);
    } else {
      state = await (await fetch('/api/state')).json();
    }
    applyLive(); render();
  } catch (e) { chip('cAge', STATIC ? 'no data.json yet' : 'server offline', 'bad'); }
  inflight = false; lastPollAt = Date.now() / 1000;
}

// ---- overlay: lines / boxes / labels, with the biggest level of each type highlighted
const cv = $('ov'), ctx = cv.getContext('2d');
const DASH = { Solid:[], Dashed:[7,5], Dotted:[2,3] };
function drawLevels() {
  const w = $('chart').clientWidth, h = $('chart').clientHeight, dpr = window.devicePixelRatio || 1;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) {
    cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); cv.style.width = w + 'px'; cv.style.height = h + 'px';
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h);
  const lv = visLevels();
  if (!lv.length || !series) return;
  const th = T();
  let axisW = 60; try { axisW = chart.priceScale('right').width(); } catch (e) {}
  const plotW = w - axisW, p = state.index.price, half = p * S.zoneW / 100, small = plotW < 520;
  let x0 = 0, x1 = plotW;
  if (S.extent === 'Bars' && nBars > 0) {
    const ts = chart.timeScale(), a = ts.logicalToCoordinate(nBars - 1 - S.back), b = ts.logicalToCoordinate(nBars - 1 + S.fwd);
    if (a !== null) x0 = Math.max(0, a); if (b !== null) x1 = Math.min(plotW, b);
  }
  if (x1 <= x0) return;
  const lineA = 1 - S.lineTr / 100, labels = [];
  for (const L of lv) {
    const y = series.priceToCoordinate(L.level);
    if (y === null || y < -20 || y > h + 20) continue;
    const node = L.type === 'pos' || L.type === 'neg', c = S[L.type], hi = S.hiOn && L._key;
    const lw = S.widthMode === 'Fixed' ? S.fixedW : 1 + (S.maxW - 1) * L.strength;
    if (S[L.type + 'Mode'] === 'Box') {
      const y1 = series.priceToCoordinate(L.level + half), y2 = series.priceToCoordinate(L.level - half);
      if (y1 !== null && y2 !== null) {
        const top = Math.min(y1, y2), hgt = Math.max(2, Math.abs(y2 - y1));
        ctx.fillStyle = rgba(c, (S.boxWeak + (S.boxStrong - S.boxWeak) * L.strength) / 100);
        ctx.fillRect(x0, top, x1 - x0, hgt);
        if (S.zoneMid) { ctx.strokeStyle = rgba(c, 0.55); ctx.lineWidth = 1; ctx.setLineDash([]); ctx.beginPath(); ctx.moveTo(x0, y); ctx.lineTo(x1, y); ctx.stroke(); }
        if (hi) { ctx.strokeStyle = rgba(S.hiCol, 0.9); ctx.lineWidth = 2; ctx.setLineDash([]); ctx.strokeRect(x0, top, x1 - x0, hgt); }
      }
    } else {
      if (hi && S.hiWidth > 0) { ctx.strokeStyle = rgba(S.hiCol, 0.3); ctx.lineWidth = lw + S.hiWidth; ctx.setLineDash([]); ctx.beginPath(); ctx.moveTo(x0, y); ctx.lineTo(x1, y); ctx.stroke(); }
      ctx.strokeStyle = rgba(c, lineA);
      ctx.lineWidth = hi ? Math.max(lw, 2.5) : lw;
      ctx.setLineDash(DASH[node ? S.nodeStyle : S.wallStyle] || []);
      ctx.beginPath(); ctx.moveTo(x0, y); ctx.lineTo(x1, y); ctx.stroke(); ctx.setLineDash([]);
    }
    const parts = [(hi ? '★ ' : '') + NAME[L.type] + ' ' + L.level.toFixed(1)];
    if (S.labStocks && L.stocks && !small) parts.push(L.stocks);
    if (S.labGex) parts.push((L.type === 'pos' ? '+' : '') + L.gex.toFixed(1) + 'm');
    if (S.labBars && !small) parts.push('|'.repeat(Math.max(1, Math.round(L.strength * 5))));
    labels.push({ y, c, hi, text: parts.join(' ') });
  }
  if (!S.showLab) return;
  const fsz = { Tiny:9, Small:11, Normal:13 }[S.labSize] || 11, lh = fsz + 3;
  const yA = series.priceToCoordinate(p), yB = series.priceToCoordinate(p * (1 + S.labGap / 100));
  const gap = Math.max(lh, yA !== null && yB !== null ? Math.abs(yA - yB) : 0);
  labels.sort((a, b) => a.y - b.y);
  let prev = -1e9;
  for (const l of labels) { l.ly = Math.max(l.y, prev + gap); prev = l.ly; }
  ctx.textBaseline = 'middle';
  for (const l of labels) {
    ctx.font = (l.hi ? 'bold ' : '') + fsz + 'px sans-serif';
    const tw = ctx.measureText(l.text).width + 10;
    const x = S.hPos === 'Left' ? x0 + 4 : S.hPos === 'Centre' ? (x0 + x1) / 2 - tw / 2 : x1 - tw - 4;
    const ax = S.hPos === 'Left' ? x0 : S.hPos === 'Centre' ? x + tw / 2 : x1, ex = S.hPos === 'Left' ? x : S.hPos === 'Centre' ? x + tw / 2 : x + tw;
    ctx.fillStyle = 'rgba(' + th.lab + ',' + (1 - S.labBgTr / 100) + ')'; ctx.fillRect(x, l.ly - lh / 2, tw, lh);
    ctx.strokeStyle = l.hi ? S.hiCol : rgba(l.c, 0.9); ctx.lineWidth = l.hi ? 2 : 1; ctx.setLineDash([]); ctx.strokeRect(x, l.ly - lh / 2, tw, lh);
    ctx.lineWidth = 1;
    if (Math.abs(l.ly - l.y) > 1) { ctx.beginPath(); ctx.moveTo(ex, l.ly); ctx.lineTo(ax, l.y); ctx.stroke(); }
    ctx.fillStyle = S.labTxt === 'Level colour' ? l.c : th.labTxt; ctx.fillText(l.text, x + 5, l.ly);
  }
}

// ---- drawing tools (trend line, rays, lines, channel, fib, rectangle, text, measure, brush)
const DRAW_KEY = 'aus_draw';
const FIB = [[0, '#787b86'], [0.236, '#f23645'], [0.382, '#ff9800'], [0.5, '#4caf50'], [0.618, '#089981'], [0.786, '#00bcd4'], [1, '#787b86']];
const ICON = {
  cursor: '<path d="M5 3l12 7-5 1.5L9.5 17z"/>',
  trend: '<circle cx="5" cy="17" r="1.8"/><circle cx="17" cy="5" r="1.8"/><path d="M6.4 15.6L15.6 6.4"/>',
  hray: '<circle cx="5" cy="11" r="1.8"/><path d="M7 11h13"/>',
  hline: '<path d="M2 11h18"/><circle cx="11" cy="11" r="1.5"/>',
  vline: '<path d="M11 2v18"/><circle cx="11" cy="11" r="1.5"/>',
  channel: '<path d="M3 14L15 4M7 19L19 9"/>',
  fib: '<path d="M3 4h16M3 8.5h16M3 13h16M3 18h16"/>',
  rect: '<rect x="4" y="6" width="14" height="10"/>',
  text: '<path d="M5 5h12M11 5v13"/>',
  measure: '<path d="M3 15L15 3l4 4L7 19z"/><path d="M8 11l2 2M11 8l2 2"/>',
  brush: '<path d="M4 18c3 0 4-2 4-3.5l8-8-2.5-2.5-8 8C4.5 12.5 4 15 4 18z"/>',
  eraser: '<path d="M8 18h10M5 14l7-7 5 5-5 6H8z"/>',
  magnet: '<path d="M6 4v7a5 5 0 0010 0V4h-3v7a2 2 0 01-4 0V4z"/>',
  eye: '<path d="M2 11s3.5-6 9-6 9 6 9 6-3.5 6-9 6-9-6-9-6z"/><circle cx="11" cy="11" r="2.5"/>',
  undo: '<path d="M8 6L4 10l4 4M4 10h8a5 5 0 010 10"/>',
  trash: '<path d="M5 7h12M9 7V4h4v3M7 7l1 11h6l1-11"/>',
  clear: '<path d="M5 5l12 12M17 5L5 17"/>'
};
const TOOLS = [
  ['cursor', 'Cursor (select and move)'], ['trend', 'Trend line'], ['hray', 'Horizontal ray'], ['hline', 'Horizontal line'], ['vline', 'Vertical line'],
  ['channel', 'Parallel channel'], ['fib', 'Fib retracement'], ['rect', 'Rectangle'], ['text', 'Text'], ['measure', 'Measure'], ['brush', 'Brush'], ['eraser', 'Eraser (tap a drawing)']
];
let D = [], hist = [], tool = 'cursor', drawing = null, sel = null, drag = null, hideDraw = false, crossActive = false;
function loadDraw() { try { D = JSON.parse(localStorage.getItem(DRAW_KEY) || '[]') || []; } catch (e) { D = []; } }
function saveDraw() { try { localStorage.setItem(DRAW_KEY, JSON.stringify(D)); } catch (e) {} }
function pushHist() { hist.push(JSON.stringify(D)); if (hist.length > 40) hist.shift(); }
function undo() { if (!hist.length) return; D = JSON.parse(hist.pop()); sel = null; drawing = null; drag = null; saveDraw(); syncProp(); redraw(); }
function redraw() { requestAnimationFrame(draw); }
const barSecs = () => TFSEC[tf] || 900;

// time <-> chart bar position (works across timeframes; stored time is real epoch seconds)
function t2l(t) {
  const a = curCs, n = a.length; if (!n) return null;
  const ct = t + tzShift, secs = barSecs();
  if (ct >= a[n - 1].time) return (n - 1) + (ct - a[n - 1].time) / secs;
  if (ct <= a[0].time) return (ct - a[0].time) / secs;
  let lo = 0, hi = n - 1;
  while (hi - lo > 1) { const m = (lo + hi) >> 1; if (a[m].time <= ct) lo = m; else hi = m; }
  return lo + (ct - a[lo].time) / (a[hi].time - a[lo].time);
}
function l2t(l) {
  const a = curCs, n = a.length, secs = barSecs();
  if (l >= n - 1) return a[n - 1].time + (l - (n - 1)) * secs - tzShift;
  if (l <= 0) return a[0].time + l * secs - tzShift;
  const i = Math.floor(l);
  return a[i].time + (l - i) * (a[i + 1].time - a[i].time) - tzShift;
}
function px(p) {
  const l = t2l(p.t); if (l === null) return null;
  const x = chart.timeScale().logicalToCoordinate(l), y = series.priceToCoordinate(p.p);
  return (x === null || y === null) ? null : { x, y };
}
function toPt(x, y, snap) {
  if (!curCs.length) return null;
  const l = chart.timeScale().coordinateToLogical(x), pr = series.coordinateToPrice(y);
  if (l === null || pr === null) return null;
  if (S.magnet && snap !== false && l > -0.5 && l < curCs.length - 0.5) {
    const c = curCs[Math.max(0, Math.min(curCs.length - 1, Math.round(l)))];
    let best = c.close, bd = 1e9;
    for (const v of [c.open, c.high, c.low, c.close]) { const yy = series.priceToCoordinate(v); if (yy !== null && Math.abs(yy - y) < bd) { bd = Math.abs(yy - y); best = v; } }
    return { t: c.time - tzShift, p: best };
  }
  return { t: l2t(l), p: pr };
}
function plotRect() {
  const w = $('chart').clientWidth, h = $('chart').clientHeight; let aw = 60, th = 26;
  try { aw = chart.priceScale('right').width(); } catch (e) {}
  try { const v = chart.timeScale().height(); if (v > 0) th = v; } catch (e) {}
  return { w: w - aw, h: h - th };
}
function segDist(x, y, a, b) {
  const dx = b.x - a.x, dy = b.y - a.y, L = dx * dx + dy * dy; let t = L ? ((x - a.x) * dx + (y - a.y) * dy) / L : 0;
  t = Math.max(0, Math.min(1, t)); return Math.hypot(x - (a.x + t * dx), y - (a.y + t * dy));
}
function chanShift(P) { const dx = P[1].x - P[0].x, m = dx ? (P[1].y - P[0].y) / dx : 0; return P[2].y - (P[0].y + m * (P[2].x - P[0].x)); }
function fibPrice(d, lvl) { return d.pts[1].p + (d.pts[0].p - d.pts[1].p) * lvl; }
function textSize(d) { return 12 + (d.w || 2) * 2; }
function dist(d, x, y) {
  const P = d.pts.map(px); if (P.some(q => !q)) return null;
  switch (d.type) {
    case 'trend': return segDist(x, y, P[0], P[1]);
    case 'hray': return x < P[0].x ? Math.hypot(x - P[0].x, y - P[0].y) : Math.abs(y - P[0].y);
    case 'hline': return Math.abs(y - P[0].y);
    case 'vline': return Math.abs(x - P[0].x);
    case 'rect': case 'measure': {
      const x1 = Math.min(P[0].x, P[1].x), x2 = Math.max(P[0].x, P[1].x), y1 = Math.min(P[0].y, P[1].y), y2 = Math.max(P[0].y, P[1].y);
      const dx = x < x1 ? x1 - x : x > x2 ? x - x2 : 0, dy = y < y1 ? y1 - y : y > y2 ? y - y2 : 0;
      if (dx || dy) return Math.hypot(dx, dy);
      return Math.min(x - x1, x2 - x, y - y1, y2 - y);
    }
    case 'fib': {
      const x1 = Math.min(P[0].x, P[1].x), x2 = Math.max(Math.max(P[0].x, P[1].x), x1 + 60);
      if (x < x1 - 8 || x > x2 + 8) return 1e9;
      let m = 1e9; for (const f of FIB) { const ly = series.priceToCoordinate(fibPrice(d, f[0])); if (ly !== null) m = Math.min(m, Math.abs(y - ly)); } return m;
    }
    case 'channel': {
      if (P.length < 3) return segDist(x, y, P[0], P[1]); const s = chanShift(P);
      return Math.min(segDist(x, y, P[0], P[1]), segDist(x, y, { x:P[0].x, y:P[0].y + s }, { x:P[1].x, y:P[1].y + s }));
    }
    case 'text': {
      ctx.font = 'bold ' + textSize(d) + 'px sans-serif'; const w = ctx.measureText(d.text || '').width, h = textSize(d);
      const dx = x < P[0].x ? P[0].x - x : x > P[0].x + w ? x - P[0].x - w : 0, dy = Math.abs(y - P[0].y) > h / 2 ? Math.abs(y - P[0].y) - h / 2 : 0;
      return Math.hypot(dx, dy);
    }
    case 'brush': { let m = 1e9; for (let i = 1; i < P.length; i++) m = Math.min(m, segDist(x, y, P[i - 1], P[i])); return m; }
  }
  return null;
}
function handlePos(d, P, i) {
  const R = plotRect();
  if (d.type === 'hline') return { x: Math.max(14, Math.min(R.w - 14, P[0].x)), y: P[0].y };
  if (d.type === 'vline') return { x: P[0].x, y: R.h / 2 };
  return P[i];
}
function hitTest(x, y) {
  const sd = D.find(d => d.id === sel);
  if (sd && sd.type !== 'brush') {
    const P = sd.pts.map(px);
    for (let i = 0; i < P.length; i++) if (P[i]) { const hp = handlePos(sd, P, i); if (Math.hypot(x - hp.x, y - hp.y) <= 14) return { d:sd, handle:i }; }
  }
  let best = null;
  for (let i = D.length - 1; i >= 0; i--) { const dd = dist(D[i], x, y); if (dd !== null && dd <= 10 && (!best || dd < best.dd)) best = { d:D[i], dd, handle:-1 }; }
  return best;
}

// ---- creating, selecting, moving
function pos(e) {
  const r = $('chartwrap').getBoundingClientRect(), t = (e.touches && e.touches[0]) || (e.changedTouches && e.changedTouches[0]) || e;
  return { x: t.clientX - r.left, y: t.clientY - r.top };
}
function setPoint(d, p) { d.pts[d.stage === 1 ? 2 : 1] = p; }
function advance() {
  const d = drawing; if (!d) return;
  if (d.type === 'channel' && !d.stage) { d.stage = 1; d.pts[2] = Object.assign({}, d.pts[1]); redraw(); return; }
  finish(d);
}
function finish(d) {
  drawing = null; sel = d.id; delete d.stage;
  if (tool !== 'cursor') setTool('cursor', true);
  saveDraw(); syncProp(); redraw();
}
function gDown(x, y) {
  if (tool === 'eraser') {
    const h = hitTest(x, y); if (h) { pushHist(); D = D.filter(q => q.id !== h.d.id); sel = null; saveDraw(); syncProp(); redraw(); } return true;
  }
  if (tool !== 'cursor') {
    const p = toPt(x, y); if (!p) return true;
    if (drawing) { setPoint(drawing, p); advance(); return true; }
    if (tool === 'text') { openText(x, y, p); return true; }
    pushHist();
    const d = { id: Date.now() + Math.random(), type: tool, pts: [p], col: S.drawCol, w: S.drawW, style: S.drawStyle };
    if (tool === 'hline' || tool === 'vline') { D.push(d); finish(d); return true; }
    if (tool !== 'brush') d.pts.push(Object.assign({}, p));
    D.push(d); drawing = d; drag = { mode:'create', sx:x, sy:y, moved:false }; sel = null; syncProp(); redraw(); return true;
  }
  if (hideDraw) return false;
  const h = hitTest(x, y);
  if (h) {
    sel = h.d.id; drag = { mode: h.handle >= 0 ? 'handle' : 'move', id:h.d.id, hi:h.handle, sx:x, sy:y, orig:h.d.pts.map(px), moved:false, hist:false };
    syncProp(); redraw(); return true;
  }
  if (sel) { sel = null; syncProp(); redraw(); }
  return false;
}
function gMove(x, y) {
  if (drawing && (!drag || drag.mode === 'create')) {
    const d = drawing;
    if (d.type === 'brush') {
      if (drag) { const p = toPt(x, y, false), q = d.pts.length ? px(d.pts[d.pts.length - 1]) : null; if (p && (!q || Math.hypot(q.x - x, q.y - y) > 3)) d.pts.push(p); }
    } else { const p = toPt(x, y); if (p) setPoint(d, p); }
    if (drag && Math.hypot(x - drag.sx, y - drag.sy) > 6) drag.moved = true;
    redraw(); return;
  }
  if (drag && (drag.mode === 'move' || drag.mode === 'handle')) {
    const d = D.find(q => q.id === drag.id); if (!d) return;
    const dx = x - drag.sx, dy = y - drag.sy;
    if (!drag.moved && Math.hypot(dx, dy) < 4) return;
    if (!drag.hist) { pushHist(); drag.hist = true; }
    drag.moved = true;
    if (drag.mode === 'handle') { const p = toPt(x, y); if (p) d.pts[drag.hi] = p; }
    else d.pts = d.pts.map((pt, i) => { const o = drag.orig[i]; if (!o) return pt; return toPt(o.x + dx, o.y + dy, false) || pt; });
    redraw();
  }
}
function gUp() {
  if (drag && drag.mode === 'create') {
    const d = drawing;
    if (d && d.type === 'brush') { if (d.pts.length < 2) { D = D.filter(q => q !== d); drawing = null; syncProp(); redraw(); } else finish(d); }
    else if (d && drag.moved) advance();
  } else if (drag && drag.moved) saveDraw();
  drag = null;
}
function openText(x, y, p) {
  const el = $('textin'); el.style.display = 'block'; el.style.left = Math.min(x, $('chart').clientWidth - 170) + 'px'; el.style.top = Math.max(4, y - 16) + 'px'; el.value = ''; el.focus();
  let done = false;
  const commit = (ok) => {
    if (done) return; done = true; const v = el.value.trim(); el.style.display = 'none';
    if (ok && v) { pushHist(); const d = { id: Date.now() + Math.random(), type:'text', pts:[p], col:S.drawCol, w:S.drawW, style:'Solid', text:v }; D.push(d); finish(d); }
  };
  el.onkeydown = (ev) => { if (ev.key === 'Enter') commit(true); else if (ev.key === 'Escape') commit(false); };
  el.onblur = () => commit(true);
}
function onDown(e) {
  if (e.button > 0) return;
  if (e.target.closest && e.target.closest('#propbar,#textin,#legend,#keys')) return;
  const { x, y } = pos(e), R = plotRect();
  if (x < 0 || y < 0 || x > R.w || y > R.h) return;
  if (gDown(x, y)) { e.preventDefault(); e.stopPropagation(); }
}
function onMove(e) {
  if (!drag && !drawing) return;
  const { x, y } = pos(e); gMove(x, y);
  if (drag && e.cancelable) e.preventDefault();
}
const cw = $('chartwrap');
cw.addEventListener('mousedown', onDown, true);
cw.addEventListener('touchstart', onDown, { capture:true, passive:false });
window.addEventListener('mousemove', onMove);
window.addEventListener('touchmove', onMove, { passive:false });
window.addEventListener('mouseup', gUp); window.addEventListener('touchend', gUp); window.addEventListener('touchcancel', gUp);
window.addEventListener('keydown', (e) => {
  const tg = e.target && e.target.tagName; if (tg === 'INPUT' || tg === 'SELECT' || tg === 'TEXTAREA') return;
  if ((e.key === 'Delete' || e.key === 'Backspace') && sel) { delSel(); e.preventDefault(); }
  else if (e.key === 'Escape') { setTool('cursor'); }
  else if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z') { undo(); e.preventDefault(); }
});
function delSel() { if (!sel) return; pushHist(); D = D.filter(d => d.id !== sel); sel = null; saveDraw(); syncProp(); redraw(); }
function setTool(t, keepSel) {
  if (drawing) { D = D.filter(d => d !== drawing); drawing = null; }
  drag = null; tool = t; if (!keepSel) sel = null;
  for (const b of $('tools').querySelectorAll('button[data-tool]')) b.classList.toggle('on', b.dataset.tool === t);
  cw.style.cursor = t === 'cursor' ? '' : 'crosshair';
  syncProp(); redraw();
}
function syncProp() {
  const d = D.find(q => q.id === sel), show = !!d || (tool !== 'cursor' && tool !== 'eraser');
  $('propbar').classList.toggle('show', show);
  const src = d || { col:S.drawCol, w:S.drawW, style:S.drawStyle };
  $('pcol').value = src.col; $('pw').value = String(src.w); $('ps').value = src.style || 'Solid';
  $('pdel').style.display = d ? '' : 'none';
}
function propChange() {
  S.drawCol = $('pcol').value; S.drawW = parseInt($('pw').value, 10) || 2; S.drawStyle = $('ps').value; saveSet();
  const d = D.find(q => q.id === sel);
  if (d) { pushHist(); d.col = S.drawCol; d.w = S.drawW; d.style = S.drawStyle; saveDraw(); redraw(); }
}
$('pcol').onchange = $('pw').onchange = $('ps').onchange = propChange; $('pdel').onclick = delSel;
function svg(k) { return '<svg viewBox="0 0 22 22">' + ICON[k] + '</svg>'; }
function buildTools() {
  const box = $('tools'); box.innerHTML = '';
  const add = (key, title, fn, attrs) => { const b = document.createElement('button'); b.innerHTML = svg(key); b.title = title; b.setAttribute('aria-label', title); if (attrs) Object.assign(b.dataset, attrs); b.onclick = fn; box.appendChild(b); return b; };
  for (const [k, t] of TOOLS) { add(k, t, () => setTool(tool === k && k !== 'cursor' ? 'cursor' : k), { tool:k }); if (k === 'cursor' || k === 'channel' || k === 'measure') box.appendChild(document.createElement('hr')); }
  box.appendChild(document.createElement('hr'));
  const mg = add('magnet', 'Magnet: snap to candle highs and lows', () => { S.magnet = !S.magnet; saveSet(); mg.classList.toggle('on', S.magnet); }); mg.classList.toggle('on', !!S.magnet);
  const ey = add('eye', 'Hide or show drawings', () => { hideDraw = !hideDraw; ey.classList.toggle('on', hideDraw); redraw(); });
  add('undo', 'Undo', undo);
  add('trash', 'Delete selected drawing', delSel);
  let armed = null;
  const cl = add('clear', 'Clear all drawings (tap twice)', () => {
    if (!armed) { cl.classList.add('warn'); armed = setTimeout(() => { armed = null; cl.classList.remove('warn'); }, 3000); return; }
    clearTimeout(armed); armed = null; cl.classList.remove('warn'); if (D.length) { pushHist(); D = []; sel = null; saveDraw(); syncProp(); redraw(); }
  });
}
$('btnDraw').onclick = () => { document.body.classList.toggle('tools-open'); layout(); };

// ---- drawing renderer
function arrow(a, b) {
  const ang = Math.atan2(b.y - a.y, b.x - a.x), s = 7;
  ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
  ctx.beginPath(); ctx.moveTo(b.x, b.y); ctx.lineTo(b.x - s * Math.cos(ang - 0.45), b.y - s * Math.sin(ang - 0.45)); ctx.moveTo(b.x, b.y); ctx.lineTo(b.x - s * Math.cos(ang + 0.45), b.y - s * Math.sin(ang + 0.45)); ctx.stroke();
}
function tag(text, x, y, col, th, R) {
  ctx.font = '11px sans-serif'; const w = ctx.measureText(text).width + 10, xx = Math.max(2, Math.min(R.w - w - 2, x - w / 2)), yy = Math.max(10, Math.min(R.h - 10, y));
  ctx.setLineDash([]); ctx.fillStyle = 'rgba(' + th.lab + ',0.92)'; ctx.fillRect(xx, yy - 9, w, 18); ctx.strokeStyle = col; ctx.lineWidth = 1; ctx.strokeRect(xx, yy - 9, w, 18);
  ctx.fillStyle = th.labTxt; ctx.textBaseline = 'middle'; ctx.fillText(text, xx + 5, yy);
}
function durTxt(s) { s = Math.abs(s); const d = Math.floor(s / 86400), h = Math.floor(s % 86400 / 3600), m = Math.floor(s % 3600 / 60); return ((d ? d + 'd ' : '') + (h ? h + 'h ' : '') + (m ? m + 'm' : '')).trim() || '0m'; }
function drawDrawings() {
  if (hideDraw || !D.length || !series || !curCs.length) return;
  const R = plotRect(), th = T();
  ctx.save(); ctx.beginPath(); ctx.rect(0, 0, R.w, R.h); ctx.clip();
  for (const d of D) {
    const P = d.pts.map(px); if (P.some(q => !q)) continue;
    const col = d.col || '#2962ff';
    ctx.strokeStyle = col; ctx.fillStyle = col; ctx.lineWidth = d.w || 2; ctx.setLineDash(DASH[d.style] || []); ctx.lineJoin = 'round'; ctx.lineCap = 'round'; ctx.textBaseline = 'middle';
    switch (d.type) {
      case 'trend': ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y); ctx.lineTo(P[1].x, P[1].y); ctx.stroke(); break;
      case 'hray': ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y); ctx.lineTo(R.w, P[0].y); ctx.stroke(); break;
      case 'hline': ctx.beginPath(); ctx.moveTo(0, P[0].y); ctx.lineTo(R.w, P[0].y); ctx.stroke(); tag(d.pts[0].p.toFixed(1), R.w - 34, P[0].y - 12, col, th, R); break;
      case 'vline': ctx.beginPath(); ctx.moveTo(P[0].x, 0); ctx.lineTo(P[0].x, R.h); ctx.stroke(); break;
      case 'rect': {
        const x1 = Math.min(P[0].x, P[1].x), y1 = Math.min(P[0].y, P[1].y);
        ctx.fillStyle = rgba(col.length === 7 ? col : '#2962ff', 0.14); ctx.fillRect(x1, y1, Math.abs(P[1].x - P[0].x), Math.abs(P[1].y - P[0].y));
        ctx.strokeRect(x1, y1, Math.abs(P[1].x - P[0].x), Math.abs(P[1].y - P[0].y)); break;
      }
      case 'channel': {
        ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y); ctx.lineTo(P[1].x, P[1].y); ctx.stroke();
        if (P.length >= 3) {
          const s = chanShift(P);
          ctx.fillStyle = rgba(col.length === 7 ? col : '#2962ff', 0.09); ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y); ctx.lineTo(P[1].x, P[1].y); ctx.lineTo(P[1].x, P[1].y + s); ctx.lineTo(P[0].x, P[0].y + s); ctx.closePath(); ctx.fill();
          ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y + s); ctx.lineTo(P[1].x, P[1].y + s); ctx.stroke();
          ctx.setLineDash([5, 5]); ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y + s / 2); ctx.lineTo(P[1].x, P[1].y + s / 2); ctx.stroke();
        }
        break;
      }
      case 'fib': {
        const x1 = Math.min(P[0].x, P[1].x), x2 = Math.max(Math.max(P[0].x, P[1].x), x1 + 60);
        const ys = FIB.map(f => series.priceToCoordinate(fibPrice(d, f[0])));
        if (ys.some(v => v === null)) break;
        for (let i = 1; i < FIB.length; i++) { ctx.fillStyle = rgba(FIB[i][1], 0.09); ctx.fillRect(x1, Math.min(ys[i - 1], ys[i]), x2 - x1, Math.abs(ys[i] - ys[i - 1])); }
        ctx.lineWidth = 1; ctx.setLineDash([]);
        FIB.forEach((f, i) => {
          ctx.strokeStyle = f[1]; ctx.beginPath(); ctx.moveTo(x1, ys[i]); ctx.lineTo(x2, ys[i]); ctx.stroke();
          ctx.fillStyle = f[1]; ctx.font = '11px sans-serif'; ctx.textBaseline = 'bottom'; ctx.fillText(f[0] + ' (' + fibPrice(d, f[0]).toFixed(1) + ')', x1 + 4, ys[i] - 2);
        });
        ctx.strokeStyle = '#787b86'; ctx.setLineDash([4, 4]); ctx.beginPath(); ctx.moveTo(P[0].x, P[0].y); ctx.lineTo(P[1].x, P[1].y); ctx.stroke();
        break;
      }
      case 'text': ctx.font = 'bold ' + textSize(d) + 'px sans-serif'; ctx.setLineDash([]); ctx.fillStyle = col; ctx.fillText(d.text || '', P[0].x, P[0].y); break;
      case 'measure': {
        const up = d.pts[1].p >= d.pts[0].p, c = up ? '#2962ff' : '#f23645', x1 = Math.min(P[0].x, P[1].x), x2 = Math.max(P[0].x, P[1].x), y1 = Math.min(P[0].y, P[1].y), y2 = Math.max(P[0].y, P[1].y);
        ctx.fillStyle = rgba(c, 0.15); ctx.fillRect(x1, y1, x2 - x1, y2 - y1); ctx.strokeStyle = c; ctx.lineWidth = 1; ctx.setLineDash([]);
        arrow({ x:(x1 + x2) / 2, y:P[0].y }, { x:(x1 + x2) / 2, y:P[1].y }); arrow({ x:P[0].x, y:(y1 + y2) / 2 }, { x:P[1].x, y:(y1 + y2) / 2 });
        const dp = d.pts[1].p - d.pts[0].p, pc = d.pts[0].p ? dp / d.pts[0].p * 100 : 0, bars = Math.round(Math.abs(t2l(d.pts[1].t) - t2l(d.pts[0].t)));
        tag((dp >= 0 ? '+' : '') + dp.toFixed(1) + ' (' + (pc >= 0 ? '+' : '') + pc.toFixed(2) + '%)  ' + bars + ' bars, ' + durTxt(d.pts[1].t - d.pts[0].t), (x1 + x2) / 2, up ? y1 - 16 : y2 + 16, c, th, R);
        break;
      }
      case 'brush': ctx.beginPath(); P.forEach((q, i) => i ? ctx.lineTo(q.x, q.y) : ctx.moveTo(q.x, q.y)); ctx.stroke(); break;
    }
    if (d.id === sel && d.type !== 'brush') {
      ctx.setLineDash([]); ctx.lineWidth = 2;
      P.forEach((q, i) => { const hp = handlePos(d, P, i); ctx.fillStyle = th.panel; ctx.strokeStyle = col; ctx.beginPath(); ctx.arc(hp.x, hp.y, 6, 0, 7); ctx.fill(); ctx.stroke(); });
    }
  }
  ctx.restore();
}
function draw() { drawLevels(); drawDrawings(); }

// ---- legend (OHLC readout, like TradingView)
function barAt(time) {
  const a = curCs; if (!a.length) return null;
  let lo = 0, hi = a.length - 1;
  while (lo < hi) { const m = (lo + hi) >> 1; if (a[m].time < time) lo = m + 1; else hi = m; }
  return lo;
}
function legend(time) {
  const el = $('legend'); if (!curCs.length) { el.innerHTML = ''; return; }
  const i = time ? barAt(time) : curCs.length - 1, c = curCs[i], prev = i > 0 ? curCs[i - 1].close : c.open;
  const ch = c.close - prev, pc = prev ? ch / prev * 100 : 0, col = ch >= 0 ? S.upCol : S.downCol, f = v => v.toFixed(1);
  const src = state && state.index ? state.index.source : '';
  el.innerHTML = '<b>AUS200</b> <span class="dim">&middot; ' + tf + (src ? ' &middot; ' + src : '') + '</span><br><span style="color:' + col + '">O ' + f(c.open) + ' H ' + f(c.high) + ' L ' + f(c.low) + ' C ' + f(c.close) + ' &nbsp;' + (ch >= 0 ? '+' : '') + f(ch) + ' (' + (pc >= 0 ? '+' : '') + pc.toFixed(2) + '%)</span>';
}
chart.subscribeCrosshairMove((p) => { crossActive = !!(p && p.time !== undefined && p.point); legend(crossActive ? p.time : null); });
chart.timeScale().subscribeVisibleLogicalRangeChange(() => redraw());
setInterval(() => { if (!crossActive) legend(null); }, 500);

// ---- start
loadDraw(); buildTools(); setTool('cursor');
new ResizeObserver(sizeChart).observe($('chart'));
applyTheme(); applyChartLook(); makeSeries(); buildSet(); buildTfs(); layout();
if (S.intro) { const el = $('intro'); el.addEventListener('animationend', (e) => { if (e.target === el) el.remove(); }); setTimeout(() => { if (el.parentNode) el.remove(); }, 3500); }
else $('intro').remove();
setInterval(poll, STATIC ? 30000 : 1000); if (!STATIC) setInterval(() => loadCandles(false), 5000); setInterval(draw, 100);
if (!STATIC) loadCandles(true); poll();
</script></body></html>
"""

if __name__ == "__main__":
    main()
