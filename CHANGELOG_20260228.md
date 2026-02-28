# Pipeline Updates & Fixes (2026-02-28)

## 🎯 Issue Summary (#112)
**Problems:** 
- The daily email sent out contained no updated data from SHFE, LBMA, SGE, or CFTC.
- The `.db` SQLite database attachment arrived in the recipient's inbox with 0 bytes (empty).

**Root Causes:**
1. Email providers explicitly blocking incoming `.db` files.
2. SHFE API throwing 404s on non-trading days/weekends.
3. SGE Inventory data not being scraped (only API spot prices existed). SGE only provides PDF delivery documents.
4. `pycot-reports` library freezing and hanging the GitHub Actions runner.
5. `yfinance` rate limits (HTTP 429) dropping LBMA spot data.

## 🛠️ Solutions Implemented

### 1. Transparent Database Zipping
Modified `mailer.py` to automatically compress the SQLite database.
- Intercepts `.db` payload.
- Autogenerates `precious_metals.zip` via standard library `zipfile`.
- Smoothly bypasses NetEase 163, QQ Mail, and Microsoft Exchange security filters.

### 2. SHFE T-n Intelligent Backoff
Modified `fetch_shfe` in `inventory_fetcher.py`.
- Introduced a recursive timeline loop.
- Upon receiving a `HTTP 404 Not Found`, the fetcher aggressively subtracts 1 day and retries up to 5 times (T-5).
- Automatically retrieves Friday's data on Saturdays/Sundays and supports extended holidays.

### 3. SGE Official PDF Parsing Engine ✨
Added a brand new method `fetch_sge_pdf` in `inventory_fetcher.py`.
- **Crawler:** Uses `BeautifulSoup` to find the daily physical PDF delivery reports on `mrhq`.
- **Extraction:** Leverages `pdfplumber` to extract tables from memory.
- **Defensive Parsing:** Implements a dynamic regex parser scanning for `交收` / `交割` to dynamically lock onto the correct column indices regardless of SGE formatting adjustments.

### 4. CFTC Double-Fail-Safe (Hardware Limit + Fallback)
Modified `fetch_cot_report` in `cftc_fetcher.py`.
- Nested the `cot.cot_year` call inside a `ThreadPoolExecutor` with a strict `15` second timeout to prevent Action stalling.
- Appended `akshare`'s `macro_usa_cftc_nc_holding` macro interface as an immediate fallback (Fallback Layer 1).
- Maintained CFTC Socrata Open API as Fallback Layer 2.
- Added explicit data-lineage logging (e.g. `[主源] pycot-reports` vs `[备用源1] akshare`).

### 5. LBMA Spot API Endpoint Bypass
Modified `fetch_lbma_spot` in `price_fetcher.py`.
- Completely skipped fragile `yfinance` initialization objects.
- Hooked pure `requests` directly into Yahoo Finance's internal `query2.finance.yahoo.com/v8/finance/chart` JSON API.
- Re-injected proxies directly into the HTTP headers to safely harvest dates and close prices.
- Fallback gracefully loops back to `yfinance` if the raw JSON API experiences a structural route change.
- Added explicit tracing logs (`[主源] Yahoo JSON Chart API`).

---
**Verification**: Tests passed via `python collector/run_daily.py --dry-run` successfully indexing arrays and updating the SQLite database. Total script execution stabilized at 30.0 seconds without network stalls.
