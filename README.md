# AUS200 GEX

Live gamma-exposure levels for AUS200 (from 7 ASX stocks' options). Page updates itself on GitHub Pages.

**Every morning (about 7:15 am Perth):**
1. On the PC with TWS open: `python aus200_live.py` (about 10 min the first time each day). It saves `aus200_oi_<date>.json`.
2. On GitHub: Add file > Upload files, drop that file in, and **rename it `oi.json`** (replace the old one). Commit.
3. The page refreshes within a few minutes. Phone link: `https://<your-username>.github.io/aus200-gex/`

Prices and candles come from free Yahoo data (15 min delayed). Levels are a rough 7-stock proxy, not true index gamma.
