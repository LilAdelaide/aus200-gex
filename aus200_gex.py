#!/usr/bin/env python3
"""
AUS200 GEX proxy from the 7 ASX heavyweights (BHP, FMG, RIO, CBA, ANZ, WBC, NAB).

What it does
  1. Connects to TWS / IB Gateway and pulls each stock's option chain (open interest, IV, gamma).
  2. Merges the .E series into the main strikes (24.01 -> 24.00).
  3. Calculates dollar GEX per strike, net GEX, the gamma flip level, call/put walls and max pain.
  4. Prints paste-ready lines:
       - ONE AUS200 line for the standalone "AUS200 GEX" indicator (stock walls converted to
         index-equivalent levels, plus net GEX, banks, miners, expiry and data date).
         Also saved to aus200_gex_line.txt and copied to your clipboard.
       - optional per-stock lines for the Universal OI Levels indicator (skip with --index-only)

Setup (once)
  pip install ib_async pyperclip
  TWS: Global Configuration > API > Settings > tick "Enable ActiveX and Socket Clients"
  Ports: TWS live 7496, TWS paper 7497, Gateway live 4001, Gateway paper 4002

Usage
  python aus200_gex.py                          # nearest monthly expiry, delayed/frozen data
  python aus200_gex.py --index-spot 8650        # if IBKR won't give you the XJO price
  python aus200_gex.py --offset -12             # shift index levels by your CFD basis (points)
  python aus200_gex.py --expiry 20261119
  python aus200_gex.py --tickers CBA,BHP,WBC    # subset

Conventions and caveats
  - GEX = gamma x open interest x 100 x spot^2 x 1%  (dollars per 1% move), calls +, puts -.
    This assumes customers are long calls / short puts. ASX does not publish who is long or short.
  - Index weights below are rough placeholders. Update them from the S&P/ASX 200 factsheet.
  - Index-equivalent level = index x (1 + weight x (strike / spot - 1)). It is a rough proxy,
    not true XJO gamma.
"""
import argparse
import datetime as dt
import math
import statistics
from collections import defaultdict

# Rough index weights in % (EDIT from the S&P/ASX 200 factsheet)
WEIGHTS = {"CBA": 10.0, "BHP": 9.0, "NAB": 4.5, "WBC": 4.5, "ANZ": 3.0, "RIO": 2.5, "FMG": 1.5}
GROUPS = {"Banks": ["CBA", "NAB", "WBC", "ANZ"], "Miners": ["BHP", "RIO", "FMG"]}
CONTRACT_SIZE = 100
RATE = 0.04


# ----------------------------------------------------------------------------
# Pure maths (no IBKR needed, easy to test)
# ----------------------------------------------------------------------------
def merge_strike(strike):
    """Snap the .E series (24.01, 7.26, 61.01) onto the main strike (24.00, 7.25, 61.00)."""
    cents = int(round(strike * 100))
    if cents % 5 == 1:
        cents -= 1
    return cents / 100.0


def bs_gamma(S, K, T, iv, r=RATE):
    if S <= 0 or K <= 0 or T <= 0 or iv <= 0:
        return 0.0
    sq = iv * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * iv * iv) * T) / sq
    pdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    return pdf / (S * sq)


def dollar_gex(gamma, oi, S):
    """Dollars of delta-hedging per 1% move in the stock."""
    return gamma * oi * CONTRACT_SIZE * S * S * 0.01


def build_profile(rows, S):
    prof = defaultdict(lambda: {"call_oi": 0.0, "put_oi": 0.0, "call_gex": 0.0, "put_gex": 0.0})
    for r in rows:
        k = merge_strike(r["strike"])
        g = dollar_gex(r["gamma"], r["oi"], S)
        if r["right"] == "C":
            prof[k]["call_oi"] += r["oi"]
            prof[k]["call_gex"] += g
        else:
            prof[k]["put_oi"] += r["oi"]
            prof[k]["put_gex"] += g
    return dict(prof)


def net_gex(prof):
    return sum(v["call_gex"] - v["put_gex"] for v in prof.values())


def total_gex_at(spot, rows, T):
    tot = 0.0
    for r in rows:
        if r["iv"] <= 0:
            continue
        g = bs_gamma(spot, r["strike"], T, r["iv"])
        sign = 1.0 if r["right"] == "C" else -1.0
        tot += sign * dollar_gex(g, r["oi"], spot)
    return tot


def flip_level(rows, S, T, span=0.15, steps=121):
    """Spot price where total net GEX crosses zero, nearest to the current price."""
    best = None
    prev_s = prev_v = None
    for i in range(steps):
        s = S * (1 - span) + (2 * span * S) * i / (steps - 1)
        v = total_gex_at(s, rows, T)
        if prev_v is not None and prev_v * v < 0:
            x = prev_s + (s - prev_s) * (-prev_v) / (v - prev_v)
            if best is None or abs(x - S) < abs(best - S):
                best = x
        prev_s, prev_v = s, v
    return best


def max_pain(prof):
    if not prof:
        return None
    strikes = sorted(prof)

    def pay(K):
        return sum(v["call_oi"] * max(0.0, K - k) + v["put_oi"] * max(0.0, k - K) for k, v in prof.items())

    return min(strikes, key=pay)


def walls(prof, S, n=1):
    """Call wall = biggest call GEX at or above spot, put wall = biggest put GEX at or below spot."""
    calls = [(k, v["call_gex"]) for k, v in prof.items() if k >= S and v["call_gex"] > 0]
    puts = [(k, v["put_gex"]) for k, v in prof.items() if k <= S and v["put_gex"] > 0]
    if not calls:
        calls = [(k, v["call_gex"]) for k, v in prof.items() if v["call_gex"] > 0]
    if not puts:
        puts = [(k, v["put_gex"]) for k, v in prof.items() if v["put_gex"] > 0]
    calls.sort(key=lambda x: -x[1])
    puts.sort(key=lambda x: -x[1])
    return calls[:n], puts[:n]


def top_oi(prof, key, n):
    items = [(k, v[key]) for k, v in prof.items() if v[key] > 0]
    items.sort(key=lambda x: -x[1])
    return sorted(items[:n], key=lambda x: x[0])


def index_level(index_spot, weight_pct, K, S, offset=0.0):
    return index_spot * (1 + (weight_pct / 100.0) * (K / S - 1)) + offset


# ----------------------------------------------------------------------------
# Formatting
# ----------------------------------------------------------------------------
def fmt_k(k):
    return f"{k:g}"


def line_levels(items):
    return ", ".join(f"{fmt_k(k)}:{int(round(v))}" for k, v in items)


def regime_text(net):
    if net > 0:
        return "POSITIVE: dealer hedging tends to dampen moves"
    if net < 0:
        return "NEGATIVE: dealer hedging can accelerate moves"
    return "flat"


# ----------------------------------------------------------------------------
# IBKR
# ----------------------------------------------------------------------------
def valid(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x)) and x > 0


def pick_expiry(expiries, today, forced):
    if forced:
        return forced
    future = [e for e in expiries if e >= today]
    for e in future:
        d = dt.datetime.strptime(e, "%Y%m%d").date()
        if d.weekday() == 3 and 15 <= d.day <= 21:  # 3rd Thursday = monthly
            return e
    return future[0] if future else None


def get_spot(ib, contract):
    t = ib.reqMktData(contract, "", False, False)
    ib.sleep(3)
    s = t.marketPrice()
    if not valid(s):
        s = t.close
    ib.cancelMktData(contract)
    return s if valid(s) else None


def fetch_stock(ib, sym, args):
    from ib_async import Option, Stock

    stk = Stock(sym, "ASX", "AUD")
    ib.qualifyContracts(stk)
    spot = get_spot(ib, stk)
    if spot is None:
        print(f"{sym}: no price from IBKR, skipping")
        return None

    details = ib.reqContractDetails(Option(symbol=sym, exchange="ASX", currency="AUD"))
    contracts = [d.contract for d in details]
    if not contracts:
        print(f"{sym}: no option contracts returned (check ASX options data)")
        return None

    expiries = sorted({c.lastTradeDateOrContractMonth for c in contracts})
    today = dt.date.today().strftime("%Y%m%d")
    expiry = pick_expiry(expiries, today, args.expiry)
    if expiry is None:
        print(f"{sym}: no future expiry found")
        return None

    lo, hi = spot * (1 - args.band), spot * (1 + args.band)
    chosen = [c for c in contracts if c.lastTradeDateOrContractMonth == expiry and lo <= c.strike <= hi]
    print(f"{sym}: spot {spot:.2f}, expiry {expiry}, {len(chosen)} contracts")

    rows = []
    total_got, total_req = [0], [0]
    for i in range(0, len(chosen), 40):
        batch = chosen[i:i + 40]
        tickers = [ib.reqMktData(c, "101", False, False) for c in batch]
        ib.sleep(args.wait)
        def _oi(c, t):
            r = "C" if str(c.right).upper().startswith("C") else "P"
            return t.callOpenInterest if r == "C" else t.putOpenInterest
        for _ in range(args.retries):
            if all(valid(_oi(c, t)) for c, t in zip(batch, tickers)):
                break
            ib.sleep(5)
        got = sum(1 for c, t in zip(batch, tickers) if valid(_oi(c, t)))
        total_got[0] += got
        total_req[0] += len(batch)
        for c, t in zip(batch, tickers):
            right = "C" if str(c.right).upper().startswith("C") else "P"
            oi = t.callOpenInterest if right == "C" else t.putOpenInterest
            if not valid(oi):
                continue
            g = t.modelGreeks
            iv = g.impliedVol if (g and valid(g.impliedVol)) else None
            gam = g.gamma if (g and valid(g.gamma)) else None
            rows.append({"strike": float(c.strike), "right": right, "oi": float(oi), "gamma": gam, "iv": iv})
        for c in batch:
            ib.cancelMktData(c)

    print(f"{sym}: open interest received for {total_got[0]}/{total_req[0]} contracts")
    if not rows:
        print(f"{sym}: no open interest returned (market closed? try --mdt 4, or run on a trading day)")
        return None

    exp_date = dt.datetime.strptime(expiry, "%Y%m%d").date()
    days = max((exp_date - dt.date.today()).days, 0.5)
    T = days / 365.0

    ivs = [r["iv"] for r in rows if r["iv"]]
    fallback_iv = statistics.median(ivs) if ivs else 0.30
    atm = [r["iv"] for r in rows if r["iv"] and abs(r["strike"] / spot - 1) < 0.03]
    atm_iv = statistics.median(atm) if atm else fallback_iv
    for r in rows:
        if not r["iv"]:
            r["iv"] = fallback_iv
        if not r["gamma"]:
            r["gamma"] = bs_gamma(spot, r["strike"], T, r["iv"])

    return {"sym": sym, "spot": spot, "expiry": expiry, "T": T, "rows": rows, "atm_iv": atm_iv}


def get_index_spot(ib):
    from ib_async import Index

    try:
        ix = Index("XJO", "ASX", "AUD")
        ib.qualifyContracts(ix)
        return get_spot(ib, ix)
    except Exception:
        return None


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def analyse(data, args):
    S = data["spot"]
    prof = build_profile(data["rows"], S)
    net = net_gex(prof)
    flip = flip_level(data["rows"], S, data["T"])
    mp = max_pain(prof)
    cw, pw = walls(prof, S, args.top_index)
    return {
        "sym": data["sym"], "spot": S, "expiry": data["expiry"], "iv": data["atm_iv"] * 100,
        "prof": prof, "net": net, "flip": flip, "mp": mp, "call_walls": cw, "put_walls": pw,
    }


def stock_line(a, top):
    c = top_oi(a["prof"], "call_oi", top)
    p = top_oi(a["prof"], "put_oi", top)
    parts = [a["sym"], "C " + line_levels(c), "P " + line_levels(p)]
    if a["mp"] is not None:
        parts.append(f"MP {fmt_k(a['mp'])}")
    if a["flip"]:
        parts.append(f"FLIP {a['flip']:.2f}")
    parts.append(f"NET {a['net'] / 1e6:.1f}")
    parts.append(f"IV {a['iv']:.1f}")
    return " | ".join(parts)


def node_levels(results, weights, index_spot, offset, top=8, bucket_pct=0.05):
    """Net GEX per index level across ALL strikes (calls +, puts -), bucketed, strongest first."""
    width = index_spot * bucket_pct / 100.0
    buckets = {}
    for a in results:
        w = weights.get(a["sym"], 0.0)
        for k, v in a["prof"].items():
            net = v["call_gex"] - v["put_gex"]
            if net == 0:
                continue
            lvl = index_level(index_spot, w, k, a["spot"], offset)
            b = buckets.setdefault(round(lvl / width), {"net": 0.0, "wl": 0.0, "wa": 0.0})
            b["net"] += net
            b["wl"] += lvl * abs(net)
            b["wa"] += abs(net)
    nodes = [(b["wl"] / b["wa"], b["net"]) for b in buckets.values() if b["wa"] > 0]
    nodes.sort(key=lambda x: -abs(x[1]))
    return sorted(nodes[:top], key=lambda x: x[0])


def aus200_line(results, weights, index_spot, offset, names, group_nets=None, expiry=None, nodes_top=8):
    """One line for the standalone 'AUS200 GEX' Pine indicator."""
    calls, puts = [], []
    for a in results:
        w = weights.get(a["sym"], 0.0)
        for k, g in a["call_walls"]:
            calls.append(f"{index_level(index_spot, w, k, a['spot'], offset):.1f}:{g / 1e6:.1f}:{a['sym']}")
        for k, g in a["put_walls"]:
            puts.append(f"{index_level(index_spot, w, k, a['spot'], offset):.1f}:{g / 1e6:.1f}:{a['sym']}")
    total = sum(a["net"] for a in results) / 1e6
    parts = [names, "C " + ", ".join(calls), "P " + ", ".join(puts)]
    if nodes_top > 0:
        nodes = node_levels(results, weights, index_spot, offset, nodes_top)
        if nodes:
            parts.append("N " + ", ".join(f"{l:.1f}:{g / 1e6:+.1f}" for l, g in nodes))
    parts.append(f"NET {total:.1f}")
    if group_nets:
        parts.append(f"BANKS {group_nets.get('Banks', 0.0):.1f}")
        parts.append(f"MINERS {group_nets.get('Miners', 0.0):.1f}")
    if expiry:
        parts.append(f"EXP {expiry[:4]}-{expiry[4:6]}-{expiry[6:8]}")
    parts.append(f"UPD {dt.date.today().isoformat()}")
    return " | ".join(parts)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7496)
    ap.add_argument("--client-id", type=int, default=78)
    ap.add_argument("--tickers", default="BHP,FMG,RIO,CBA,ANZ,WBC,NAB")
    ap.add_argument("--expiry", default=None, help="YYYYMMDD (default: nearest monthly)")
    ap.add_argument("--band", type=float, default=0.20, help="strike range around spot (0.20 = +/-20%%)")
    ap.add_argument("--wait", type=float, default=8.0, help="seconds to wait for ticks per batch")
    ap.add_argument("--retries", type=int, default=4, help="extra 5s waits for contracts still missing open interest")
    ap.add_argument("--nodes", type=int, default=8, help="number of net-GEX nodes on the AUS200 line (0 = none)")
    ap.add_argument("--mdt", type=int, default=4, help="1 live, 2 frozen, 3 delayed, 4 delayed-frozen")
    ap.add_argument("--top", type=int, default=4, help="OI levels per side on each stock line")
    ap.add_argument("--top-index", type=int, default=1, help="walls per side per stock on the AUS200 line")
    ap.add_argument("--index-spot", type=float, default=None, help="XJO / AUS200 price if IBKR gives none")
    ap.add_argument("--offset", type=float, default=0.0, help="points added to index levels (CFD basis)")
    ap.add_argument("--index-names", default="AUS200,XJO", help="chart tickers for the AUS200 line")
    ap.add_argument("--weights", default=None, help="override, e.g. CBA=10.5,BHP=8.8")
    ap.add_argument("--index-only", action="store_true", help="only output the AUS200 line (skip per-stock lines)")
    args = ap.parse_args()

    weights = dict(WEIGHTS)
    if args.weights:
        for kv in args.weights.split(","):
            k, v = kv.split("=")
            weights[k.strip().upper()] = float(v)

    from ib_async import IB

    ib = IB()
    ib.connect(args.host, args.port, clientId=args.client_id)
    ib.reqMarketDataType(args.mdt)

    results = []
    try:
        for sym in [s.strip().upper() for s in args.tickers.split(",") if s.strip()]:
            data = None
            for attempt in range(3):
                data = fetch_stock(ib, sym, args)
                if data:
                    break
                print(f"{sym}: attempt {attempt + 1} failed, retrying...")
                ib.sleep(3)
            if data:
                results.append(analyse(data, args))
        index_spot = args.index_spot or get_index_spot(ib)
    finally:
        ib.disconnect()

    if not results:
        print("\nNo data returned. Nothing to output.")
        return

    # ---- summary table ----
    print("\n=== SUMMARY (GEX in $ millions per 1% move; calls +, puts -) ===")
    print(f"{'Stock':6}{'Spot':>9}{'IV%':>7}{'Net GEX':>10}{'Flip':>9}{'CallWall':>10}{'PutWall':>9}{'MaxPain':>9}")
    for a in results:
        cw = fmt_k(a["call_walls"][0][0]) if a["call_walls"] else "-"
        pw = fmt_k(a["put_walls"][0][0]) if a["put_walls"] else "-"
        flip = f"{a['flip']:.2f}" if a["flip"] else "-"
        mp = fmt_k(a["mp"]) if a["mp"] is not None else "-"
        print(f"{a['sym']:6}{a['spot']:9.2f}{a['iv']:7.1f}{a['net'] / 1e6:10.1f}{flip:>9}{cw:>10}{pw:>9}{mp:>9}")

    by_sym = {a["sym"]: a for a in results}
    print()
    group_nets = {}
    total = 0.0
    for g, members in GROUPS.items():
        v = sum(by_sym[m]["net"] for m in members if m in by_sym) / 1e6
        group_nets[g] = v
        total += v
        print(f"{g:7} net GEX {v:9.1f}m  ({regime_text(v)})")
    print(f"{'TOTAL':7} net GEX {total:9.1f}m  ({regime_text(total)})")

    # ---- paste lines ----
    expiry = results[0]["expiry"]
    out = []
    index_line = None
    if not index_spot:
        try:
            typed = input("\nNo index price from IBKR. Type the current AUS200 price (e.g. 9000) and press Enter: ").strip()
            index_spot = float(typed) if typed else None
        except (ValueError, EOFError):
            index_spot = None
    if index_spot:
        index_line = aus200_line(results, weights, index_spot, args.offset, args.index_names, group_nets, expiry, args.nodes)
        out.append("# AUS200 GEX line: paste into the 'AUS200 GEX' indicator (levels are index-equivalent, rough proxy)")
        out.append(index_line)
    else:
        out.append("# No XJO price from IBKR. Re-run with --index-spot <price> to get the AUS200 line.")
    if not args.index_only:
        out.append("# Stock lines: paste into the Universal OI Levels 'Paste all' box (use on each stock's chart)")
        out += [stock_line(a, args.top) for a in results]

    text = "\n".join(out) + "\n"
    print("\n=== PASTE THIS ===\n" + text)
    with open("aus200_gex_output.txt", "w") as f:
        f.write(text)
    print("Saved to aus200_gex_output.txt")
    clip = index_line if index_line else text
    if index_line:
        with open("aus200_gex_line.txt", "w") as f:
            f.write(index_line + "\n")
        print("AUS200 line saved to aus200_gex_line.txt")
    try:
        import pyperclip
        pyperclip.copy(clip)
        print("Copied to clipboard (the AUS200 line when available).")
    except Exception:
        pass


if __name__ == "__main__":
    main()
