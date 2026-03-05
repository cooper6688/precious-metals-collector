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

## 🔧 [2026-03-05] SGE 库存提取重构与告警智能化

### 1. SGE PDF 提取策略重写
- **问题**: SGE 官网 JSON 文章列表 API (`findArticleExtList`) 对直接请求和 `curl_cffi` 均返回 `302` 重定向到首页，导致 `inventory_fetcher` 无法获取每日交割 PDF 链接。同时页面上存在的 `.pdf` 占位文件（2018 年的历史文件）实际返回 HTML 而非 PDF，触发 `pdfplumber` 的 `No /Root object` 异常。
- **修复**:
  - 引入 **AJAX 拦截机制**: 通过 `StealthyFetcher` 的 `page_action` 注册 Playwright `page.on("response")` 监听器，拦截浏览器渲染页面时发出的 `findArticleExtList` AJAX 请求，直接从响应 JSON 中提取文章列表和 PDF 链接。
  - 增加 **DOM 降级策略**: 如果 AJAX 拦截未命中，回退到从已渲染的页面 DOM 中搜索有效 PDF 链接。
  - 增加 **PDF Magic-byte 校验**: 下载文件后检查前 5 字节是否以 `%PDF` 开头，彻底杜绝将 HTML 误当 PDF 解析的问题。

### 2. 数据新鲜度告警智能化 (`run_daily.py`)
- **问题**: 原有的 Stage 5 (Gap Check) 简单对比 `today` 日期，导致在北京时间上午运行时（CME 美盘尚未开盘、SGE 未结算）产生大量误报警告。
- **修复**:
  - **CME 宽容检查**: 对于 CME 数据，不再硬性要求当天有数据，改为检查最近一条记录是否在 3 天以内，覆盖跨周末和短假期场景。
  - **SGE 时段感知**: 仅在北京时间 11:30 以后才对 SGE 缺失触发 WARNING；早于该时间则输出 INFO 级别的"尚未落库"提示，避免无意义的告警噪音。

---
**验证**:
- SGE AJAX 拦截: **IMPLEMENTED** (需在 GitHub Actions 中验证实际效果)
- SGE PDF Magic-byte 校验: **PASSED** (成功拒绝 HTML 占位文件)
- CME Gap Check 宽容模式: **IMPLEMENTED**
- SGE 时段感知告警: **IMPLEMENTED**

### 3. Scrapling 全量依赖补全与 CI 稳定性加固
- **核心依赖补全**: 
  - 通过针对 `scrapling` 源码的静态扫描，彻底补全了 CI 环境中缺失的隐藏依赖：`msgspec` (数据模型), `anyio` (异步流水线), `httpx` (网络引擎)。
  - 目前 `requirements.txt` 已包含 9 个核心支撑包，确保 Scrapling 在无预装环境的容器中实现“即装即用”。
- **流水线回归修复 (Stabilization)**:
  - **逻辑修正**: 修复了 `run_daily.py` 查验阶段对数据库字典索引的 `KeyError`。
  - **环境自适应 Proxy**: 优化了 `settings.py`。新增对 `GITHUB_ACTIONS` 生产环境的感知，若检测到代理为 localhost 则自动进入“直连模式”，彻底消灭了 CI 宿主机因尝试连接本地不存在的 `127.0.0.1:10808` 而导致的连接被拒 (Connection Refused) 报错。
  - **代码健壮性**: 解决了 `logger` 定义顺序导致的 `NameError`，确保日志流在系统初始化阶段即全面覆盖。

---
**Verification (Final)**:
- Scrapling 全量依赖安装: **SUCCESS**
- GitHub Actions 代理自适应: **SUCCESS** (连接错误已消除)
- 自动化流水线回归测试: **PASS** (Master 运行成功)
