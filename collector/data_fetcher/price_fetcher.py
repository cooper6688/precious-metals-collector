"""
价格抓取器 - 获取期货与现货价格数据。

数据源：
- CME 期货: yfinance（GC=F / SI=F 连续合约）
- SHFE 期货: akshare 上期所日行情
- 上海金交所现货: akshare spot_hist_sge（Au99.99 / Ag(T+D)）
"""
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import pandas as pd
from scrapling import Fetcher, StealthyFetcher

from collector.database import DatabaseManager

logger = logging.getLogger(__name__)


class PriceFetcher:
    """价格数据抓取器。"""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    # --------------------------------------------------------
    # CME 期货（yfinance）
    # --------------------------------------------------------

    def fetch_cme_futures(
        self, metal: str = "gold", days: int = 5
    ) -> list[dict[str, Any]]:
        """
        通过 yfinance 获取 CME 黄金/白银期货最近数据。

        Args:
            metal: 'gold' 或 'silver'。
            days: 回溯天数。

        Returns:
            期货价格记录列表。
        """
        records: list[dict[str, Any]] = []
        if os.getenv("PM_SKIP_YFINANCE", "0") == "1":
            logger.info("CME %s: PM_SKIP_YFINANCE=1, 跳过 yfinance", metal)
            return records

        symbol = "GC=F" if metal == "gold" else "SI=F"
        try:
            import yfinance as yf

            def _fetch():
                ticker = yf.Ticker(symbol)
                return ticker.history(period=f"{days}d")

            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_fetch)
                df = future.result(timeout=15)  # 15 秒超时

            if df is None or df.empty:
                logger.warning("yfinance %s 数据为空", symbol)
                return records

            df = df.reset_index()
            date_col = df.columns[0]
            
            df_records = pd.DataFrame({
                "date": pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d"),
                "exchange": "CME",
                "metal": metal,
                "contract": "continuous",
                "close_price": df["Close"].astype(float),
                "open_interest": None,
                "volume": df["Volume"].astype(float),
                "currency": "USD",
                "source": "yfinance",
            })
            df_records["volume"] = df_records["volume"].where(df_records["volume"].notna(), None)
            
            records.extend(df_records.to_dict(orient="records"))

            logger.info("CME %s 获取 %d 条期货记录", metal, len(records))

        except FuturesTimeout:
            logger.warning("CME %s yfinance 超时 (curl_cffi SSL 问题/网络慢)", metal)
        except Exception:
            logger.exception("CME %s 期货抓取失败", metal)
        return records

    # --------------------------------------------------------
    # SHFE 期货（akshare）
    # --------------------------------------------------------

    def fetch_shfe_futures(self, metal: str = "gold", full_history: bool = False) -> list[dict[str, Any]]:
        """
        通过 akshare 获取上期所期货行情。
        
        Args:
            metal: 'gold' 或 'silver'。
            full_history: 如果为 True，返回所有历史数据；否则只返回最新 5 条。

        返回列映射（使用列名，拒绝位置索引）：
        - columns: ['日期', '开盘价', '最高价', '最低价', '收盘价', '成交量', '持仓量', '动态结算价']
        """
        records: list[dict[str, Any]] = []
        try:
            # akshare 的品种代码：au0=黄金主力连续, ag0=白银主力连续
            symbol = "au0" if metal == "gold" else "ag0"
            df = ak.futures_main_sina(symbol=symbol)
            if df is None or df.empty:
                logger.warning("SHFE %s 期货数据为空", metal)
                return records

            logger.debug("SHFE %s 列名: %s", metal, df.columns.tolist())

            if not full_history:
                df = df.tail(5).copy()

            df_records = pd.DataFrame({
                "date": df["日期"].astype(str).str[:10],
                "exchange": "SHFE",
                "metal": metal,
                "contract": symbol,
                "close_price": df["收盘价"].astype(float),
                "open_interest": df["持仓量"].astype(float) if "持仓量" in df.columns else None,
                "volume": df["成交量"].astype(float) if "成交量" in df.columns else None,
                "currency": "CNY",
                "source": "akshare_sina",
            })
            
            df_records["open_interest"] = df_records["open_interest"].where(df_records["open_interest"].notna(), None)
            df_records["volume"] = df_records["volume"].where(df_records["volume"].notna(), None)

            records.extend(df_records.to_dict(orient="records"))

            logger.info("SHFE %s 获取 %d 条期货记录", metal, len(records))
        except KeyError as e:
            logger.error("SHFE %s 列名不匹配: %s, 可用列: %s",
                         metal, e, df.columns.tolist() if df is not None else "N/A")
        except Exception:
            logger.exception("SHFE %s 期货抓取失败", metal)
        return records

    # --------------------------------------------------------
    # 上海金交所现货（akshare spot_hist_sge）
    # --------------------------------------------------------

    def fetch_sge_spot(self, metal: str = "gold", full_history: bool = False) -> list[dict[str, Any]]:
        """
        获取上海黄金交易所 (SGE) 现货价格。

        数据源: akshare spot_hist_sge
        - Au99.99: 黄金现货（元/克）
        - Ag(T+D): 白银延期（元/千克）

        Args:
            metal: 'gold' 或 'silver'。
            full_history: 如果为 True，返回所有历史数据；否则只返回最新 1 条。
        """
        results: list[dict[str, Any]] = []
        try:
            symbol = "Au99.99" if metal == "gold" else "Ag(T+D)"
            df = ak.spot_hist_sge(symbol=symbol)

            if df is None or df.empty:
                logger.warning("SGE %s 现货数据为空", metal)
                return results

            if not full_history:
                df = df.tail(1).copy()

            df_records = pd.DataFrame({
                "date": df["date"].astype(str).str[:10],
                "market": "SGE",
                "metal": metal,
                "price": df["close"].astype(float),
                "currency": "CNY",
                "source": "akshare_sge",
            })
            df_records["price"] = df_records["price"].where(df_records["price"].notna(), None)
            results.extend(df_records.to_dict(orient="records"))
            
            # 如果数据为空，尝试使用 Scrapling StealthyFetcher 作为 Fallback
            if not results:
                logger.info("SGE %s: akshare 数据为空，尝试使用 Scrapling Fallback", metal)
                results = self._fetch_sge_spot_scrapling(metal)
                
            return results
        except KeyError as e:
            logger.error("SGE %s 列名不匹配: %s", metal, e)
            # 同样尝试 Fallback
            return self._fetch_sge_spot_scrapling(metal)
        except Exception:
            logger.exception("SGE 现货 %s 抓取失败，尝试使用 Scrapling Fallback", metal)
            return self._fetch_sge_spot_scrapling(metal)

    def _fetch_sge_spot_scrapling(self, metal: str = "gold") -> list[dict[str, Any]]:
        """使用 Scrapling StealthyFetcher 模拟 UI 交互抓取 SGE 现货数据。"""
        results: list[dict[str, Any]] = []
        url = "https://www.sge.com.cn/sjzx/quotation_daily_new"
        
        def _action(page):
            try:
                search_btn = page.locator("a:has-text('查询'), button:has-text('查询'), .searchBtn")
                if search_btn.count() > 0:
                    search_btn.first.click()
                    # 显式等待表格内数据单元格出现（移除 networkidle 防止心跳埋点导致永远超时）
                    page.wait_for_selector("table tr td", state="visible", timeout=15000)
            except Exception as e:
                logger.debug("SGE Scrapling UI action warning: %s", e)

        try:
            # 移除固定的 wait 参数，完全依赖 _action 内的页面事件驱动；切换为 headed 模式以防检测
            resp = StealthyFetcher.fetch(url, headless=False, page_action=_action)
            if resp.status != 200:
                return results

            # 解析表格
            rows = resp.css('table tr')
            target_name = "Au99.99" if metal == "gold" else "Ag(T+D)"
            
            for row in rows:
                cells = [c.strip() for c in row.css('td::text, th::text').getall() if c.strip()]
                # SGE 表格通常结构: [日期, 品种, 开盘, 最高, 最低, 收盘, ...]
                if len(cells) >= 6 and target_name in cells[1]:
                    try:
                        price = float(cells[5].replace(",", ""))
                        date_str = cells[0]
                        results.append({
                            "date": date_str,
                            "market": "SGE",
                            "metal": metal,
                            "price": price,
                            "currency": "CNY",
                            "source": "scrapling_sge_ui",
                        })
                        break # 找到当日最新即可
                    except (ValueError, IndexError):
                        continue
            
            if results:
                logger.info("SGE %s: 通过 Scrapling 成功获取数据", metal)
        except Exception as e:
            logger.warning("SGE %s: Scrapling 抓取失败: %s", metal, e)
            
        return results

    # --------------------------------------------------------
    # 伦敦现货 (yfinance LBMA)
    # --------------------------------------------------------

    def fetch_lbma_spot(self, days: int = 5) -> list[dict[str, Any]]:
        """
        通过新浪行情接口获取伦敦现货黄金/白银 (XAU, XAG)。
        彻底解决 GitHub Actions 节点 IP 被 Yahoo 封杀的问题。
        """
        records: list[dict[str, Any]] = []
        
        # hf_XAU: 伦敦金现货, hf_XAG: 伦敦银现货
        url = "http://hq.sinajs.cn/list=hf_XAU,hf_XAG"
        headers = {
            "Referer": "https://finance.sina.com.cn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        
        try:
            import requests
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = 'gbk'
            
            if resp.status_code != 200:
                logger.warning("Sina LBMA 接口返回状态码: %d", resp.status_code)
                # 尝试走旧的 Yahoo/yfinance 逻辑作为 fallback
                return self._fetch_lbma_spot_yahoo_fallback(days)

            # 解析示例: var hq_str_hf_XAU="2700.50,5183.880,...";
            for line in resp.text.strip().split('\n'):
                if 'hq_str_hf_' not in line:
                    continue
                
                parts = line.split('"')
                if len(parts) < 2: continue
                
                symbol_part = line.split('=')[0].split('_')[-1] # XAU or XAG
                data = parts[1].split(',')
                if not data or len(data) < 13: continue
                
                price = float(data[0])
                date_str = data[12] # 日期通常在第13个位置
                metal = "gold" if "XAU" in symbol_part else "silver"
                
                records.append({
                    "date": date_str,
                    "market": "LBMA",
                    "metal": metal,
                    "price": price,
                    "currency": "USD",
                    "source": "sina_hq",
                })
            
            if records:
                logger.info("Sina 获取 %d 条 LBMA 现货记录", len(records))
                return records

        except Exception as e:
            logger.warning("Sina 直接获取 LBMA 失败: %s, 尝试回退...", e)

        # 如果新浪失败，尝试使用 Scrapling 直接抓取 LBMA 官网 (针对 Cloudflare)
        lbma_scrapling = self._fetch_lbma_spot_scrapling()
        if lbma_scrapling:
            return lbma_scrapling

        # 最后回退到 Yahoo CFFI (仅作为最后手段)
        return self._fetch_lbma_spot_yahoo_fallback(days)

    def _fetch_lbma_spot_scrapling(self) -> list[dict[str, Any]]:
        """使用 Scrapling 穿透 Cloudflare 抓取 LBMA 官网价格。"""
        records: list[dict[str, Any]] = []
        url = "https://www.lbma.org.uk/prices-and-data/precious-metal-prices"
        
        try:
            # 启用 solve_cloudflare，采用 headed 模式通过挑战，并移除不必要的 wait 参数 (Stealthy 会自动处理)
            resp = StealthyFetcher.fetch(url, headless=False, solve_cloudflare=True)
            if resp.status != 200:
                return records

            # LBMA 页面通常有 Price Table
            # 预期结构可能因页面变动，这里使用启发式提取包含 Gold/Silver 的行
            tables = resp.css('table')
            for table in tables:
                rows = table.css('tr')
                for row in rows:
                    text = row.get().upper()
                    if 'GOLD' in text or 'SILVER' in text:
                        cells = [c.strip() for c in row.css('td::text, th::text').getall() if c.strip()]
                        # 这是一个简化的提取逻辑，实际可能需要根据页面 HTML 进一步微调
                        if len(cells) >= 2:
                            metal = "gold" if "GOLD" in cells[0].upper() else "silver"
                            try:
                                # 寻找第一个看起来像数字的单元格作为价格
                                for cell in cells[1:]:
                                    price_match = re.search(r"[\d\.]+", cell.replace(",", ""))
                                    if price_match:
                                        price = float(price_match.group(0))
                                        records.append({
                                            "date": datetime.now().strftime("%Y-%m-%d"),
                                            "market": "LBMA",
                                            "metal": metal,
                                            "price": price,
                                            "currency": "USD",
                                            "source": "scrapling_lbma_web",
                                        })
                                        break
                            except Exception:
                                continue
            
            if records:
                logger.info("LBMA: 通过 Scrapling 穿透 Cloudflare 成功")
        except Exception as e:
            logger.debug("LBMA Scrapling fetch failed: %s", e)
            
        return records

    def _fetch_lbma_spot_yahoo_fallback(self, days: int = 5) -> list[dict[str, Any]]:
        """原有的 Yahoo CFFI 抓取逻辑作为回退方案。"""
        records: list[dict[str, Any]] = []
        if os.getenv("PM_SKIP_YFINANCE", "0") == "1":
            return records

        tickers = {"gold": "XAUUSD=X", "silver": "XAGUSD=X"}
        fetcher = Fetcher()
            
        for metal, symbol in tickers.items():
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range={days}d&interval=1d"
            headers = {"User-Agent": "Mozilla/5.0"}
            proxies = PROXIES if USE_PROXY else {"http": None, "https": None}
            
            try:
                resp = fetcher.get(url, timeout=15)
                if resp.status != 200: continue
                data = json.loads(resp.text)
                result = data.get("chart", {}).get("result", [])
                if not result: continue
                
                auth_data = result[0]
                timestamps = auth_data.get("timestamp", [])
                close_prices = auth_data.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                
                if timestamps and close_prices:
                    for t, p in zip(timestamps, close_prices):
                        if p is not None:
                            dt_str = datetime.fromtimestamp(t).strftime("%Y-%m-%d")
                            records.append({
                                "date": dt_str,
                                "market": "LBMA",
                                "metal": metal,
                                "price": float(p),
                                "currency": "USD",
                                "source": "yahoo_api_cffi_fallback",
                            })
            except Exception:
                pass
        return records

    def fetch_spot_prices(self, full_history: bool = False) -> list[dict[str, Any]]:
        """获取现货价格。"""
        records: list[dict[str, Any]] = []
        # SGE 现货
        for metal in ("gold", "silver"):
            result = self.fetch_sge_spot(metal, full_history=full_history)
            if result:
                records.extend(result)
        
        # LBMA 现货 (仅在不请求全量历史时简单追加，yfinance period 限制)
        if not full_history:
            records.extend(self.fetch_lbma_spot())
            
        return records

    # --------------------------------------------------------
    # 汇总入库
    # --------------------------------------------------------

    def update_daily(self) -> int:
        """执行日度价格数据抓取并写入数据库。"""
        total = 0

        # CME 期货
        futures_records: list[dict[str, Any]] = []
        for metal in ("gold", "silver"):
            futures_records.extend(self.fetch_cme_futures(metal))
            futures_records.extend(self.fetch_shfe_futures(metal))
        if futures_records:
            total += self.db.insert_batch("future_prices_daily", futures_records)

        # 现货
        spot_records = self.fetch_spot_prices()
        if spot_records:
            total += self.db.insert_batch("spot_prices_daily", spot_records)

        if total == 0:
            logger.warning("无价格数据可写入")
        return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    fetcher = PriceFetcher(db)
    count = fetcher.update_daily()
    print(f"✅ 价格数据抓取完成，共 {count} 条记录")
