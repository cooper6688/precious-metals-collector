# Pipeline Updates & Fixes (2026-02-28)

## 🎯 Issue Summary (#112)
**Problems:** 
- The daily email sent out contained no updated data from SHFE, LBMA, SGE, or CFTC.
- The `.db` SQLite database attachment arrived in the recipient's inbox with 0 bytes (empty).

**Root Causes:**
1. Email providers explicitly blocking incoming `.db` files.
2. SHFE API throwing 404s on non-trading days/weekends, and the specific daily JSON endpoint `pmYYYYMMDD.dat` occasionally returning 404 regardless of Headers or TLS impersonation.
3. SGE Inventory data not being scraped (only API spot prices existed). SGE only provides PDF delivery documents, but the `mrhq` HTML page is dynamically rendered by Vue/React, causing pure `BeautifulSoup` to return empty sets even after WAF bypass.
4. `pycot-reports` library freezing and hanging the GitHub Actions runner.
5. `yfinance` rate limits (HTTP 429) dropping LBMA spot data. Moreover, `USE_PROXY=True` defaults triggered `Connection refused` on GitHub Actions since no local proxy exists on the Ubuntu runner. Furthermore, Yahoo heavily blocks Azure/GitHub Actions IPs with 404s/429s even when attempting to bypass with `curl_cffi` without a proxy.

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

### 5. 爬虫代理与反封禁增强 (Anti-Scraping Enhancements)
- **上期所 (SHFE) 仓单数据**:
  - **问题**: 原有 `requests` 请求被上期所强行拦截抛出 `Remote end closed connection without response`，确认为 TLS 指纹封锁。
  - **修复**: 引入 `curl_cffi` 库，并设置 `impersonate="chrome110"`。此方法成功伪造真实浏览器底层 TLS/JA3 指纹，安全绕过服务端拦截。
  - **备选方案尝试记录**: 曾尝试增补 Header (`User-Agent`, `Referer`) 均无济于事，仅 `curl_cffi` 能够穿透。
- **上海金交所 (SGE) 现货数据**:
  - **问题**: 官网行情页面 (`/sjzx/mrhq`) 改版，且部署了严格的 Web 应用防火墙 (WAF)。单纯的 `requests` 会返回 `您的访问请求可能对网站造成安全威胁` 的拦截页。
  - **修复**: 使用 `curl_cffi` 请求配合真实浏览器 Header 即可穿透该 WAF 获取到完整的静态 HTML。同时改进了 PDF 提取的正则逻辑，适配了动态表头的检测。
- **伦敦贵金属现货 (LBMA)**:
  - **问题**: Yahoo Finance 的原生和第三方 API (`yfinance`) 均加强了安全限制，直接请求返回 `404 Not Found` (未提供 Crumb)，并发调用立即触发强制限流 `429 Too Many Requests`。
  - **修复**: 将底层请求替换为 `curl_cffi`，重新伪造请求。曾尝试使用国内的 `akshare` 替代，但该库最新版本的 `spot_goods_sina` 及相关外盘接口不可用或返回空值，故最终方案妥协为增强版的 Yahoo API + `curl_cffi` 穿透。

### 6. LBMA Spot API Endpoint Bypass
Modified `fetch_lbma_spot` in `price_fetcher.py`.
- Completely skipped fragile `yfinance` initialization objects.
- Hooked pure `requests` directly into Yahoo Finance's internal `query2.finance.yahoo.com/v8/finance/chart` JSON API.
- Re-injected proxies directly into the HTTP headers to safely harvest dates and close prices.
- Fallback gracefully loops back to `yfinance` if the raw JSON API experiences a structural route change.
- Added explicit tracing logs (`[主源] Yahoo JSON Chart API`).

### 7. Scrapling Browser Engine Integration (New ✨)
- **SGE (Shanghai Gold Exchange)**: 
  - Switched to UI-driven scraping using `StealthyFetcher` with `page_action`. 
  - Simulates a real user clicking the "Search" button to trigger AJAX table loading, effectively bypassing WAF and dynamic rendering issues.
- **LBMA (London Bullion Market Association)**: 
  - Implemented `StealthyFetcher` with `solve_cloudflare=True` for direct price extraction.
  - Successfully handles Cloudflare Turnstile challenges that previously blocked `curl_cffi` and `requests`.
- **SHFE (Shanghai Futures Exchange)**: 
  - Upgraded inventory fetching to use `StealthyFetcher` for better session resilience.
  - Corrected the data URL pattern to `/data/tradedata/future/dailydata/` based on reverse-engineering the site's JS API.

---
**Verification**: 
- SGE Spot Price (UI Mode): **PASSED** (Validated via Scrapling `page_action`).
- SGE PDF Inventory: **PASSED** (Validated via `StealthyFetcher`).
- SHFE Inventory URL: **FIXED** (Path discovery completed).
- LBMA Cloudflare Bypass: **PASSED** (Engine successfully rendered the protected page).

## 🚀 [2026-03-01] 补充高级稳定性监控升级

### 1. LBMA 金库 XLSX 穿透下载路线切换
- **问题**: LBMA 二进制文件下载受阻，其所在的 CDN 和 AWS S3 服务器对爬虫流量进行了严苛封锁。
- **修复**: 组合使用 `Scrapling StealthyFetcher` 提取云端会话 Cookies 与 User-Agent，结合 `curl_cffi` 强力模拟 Chrome 浏览器指纹直接拉取二进制文件，完全越过防线。

### 2. SGE 动态页面网络防流氓轮询机制
- **问题**: `StealthyFetcher` 在抓取国内 SGE 页面时偶尔陷入网络超时，因国内站点存在长期挂起的心跳埋点，致使 `networkidle` 无限期阻塞。
- **修复**: 全面剥离 `networkidle` 强制等待，转移为基于 `wait_for_selector` 的轻量级 DOM 渲染事件感知。

### 3. GitHub Actions 环境隐蔽性增强 (Xvfb)
- **修复**: 彻底弃用 `headless=True`，转为在 Ubuntu 流水线底层安装配置 `Xvfb` (虚拟帧缓冲)，使 Python 程序能够在完全伪真切的显示器沙盒中以 `headless=False` 模式执行，隐蔽度拉满。

### 4. SHFE 高优路径嗅探熔断器
- **修复**: 识别最近上期所仓单接口路由由 `/data/tradedata/future/dailydata/` 取代旧版。引入长达 `3天` 容差期的 404 检测计数器；并为未来的路径调整铺设了直升管理员邮箱的紧急报警探针。
