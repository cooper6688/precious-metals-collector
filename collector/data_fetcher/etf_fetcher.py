"""
ETF 抓取器 - 获取 GLD / SLV ETF 持仓与价格变化数据。

数据源：yfinance（获取 ETF 日线数据）
"""
import logging
import os
from typing import Any

import pandas as pd

from collector.database import DatabaseManager

logger = logging.getLogger(__name__)


class ETFFetcher:
    """ETF 持仓数据抓取器。"""

    # ETF 与金属映射
    _ETF_MAP: dict[str, dict[str, str]] = {
        "GLD": {"metal": "gold", "description": "SPDR Gold Shares"},
        "SLV": {"metal": "silver", "description": "iShares Silver Trust"},
    }

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def fetch_etf(
        self, symbol: str, days: int = 5
    ) -> list[dict[str, Any]]:
        """
        通过 yfinance 获取 ETF 最近数据。

        由于 SPDR GLD 的真实吨位持仓数据需要官网档案（非免费 API），
        此处使用 yfinance 的成交量作为资金流近似指标，
        并用股价 * 成交量估算日度资金流。

        Args:
            symbol: ETF 代码，如 'GLD'、'SLV'。
            days: 回溯天数。

        Returns:
            ETF 持仓记录列表。
        """
        records: list[dict[str, Any]] = []
        meta = self._ETF_MAP.get(symbol)
        if meta is None:
            logger.warning("未知 ETF 代码: %s", symbol)
            return records

        if os.getenv("PM_SKIP_YFINANCE", "0") == "1":
            logger.info("%s ETF: PM_SKIP_YFINANCE=1, 跳过", symbol)
            return records

        try:
            import yfinance as yf
            import time

            # 🚨 增加冷启动检测：如果库中数据少于 2 天，强制拉取 30 天历史
            count_res = self.db.query(
                "SELECT COUNT(1) AS cnt FROM etf_holdings_daily WHERE symbol = ?",
                (symbol,)
            )
            if count_res and count_res[0]["cnt"] < 2:
                logger.info("%s ETF: 数据库数据不足，触发冷启动获取30天历史", symbol)
                fetch_days = 30
            else:
                fetch_days = days

            def _fetch():
                ticker = yf.Ticker(symbol)
                return ticker.history(period=f"{fetch_days}d")

            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
            
            df = None
            max_retries = 4
            backoff_factor = 2
            
            for attempt in range(max_retries):
                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(_fetch)
                        df = future.result(timeout=15)
                    break
                except FuturesTimeout:
                    logger.warning("%s ETF yfinance 超时，尝试重试...", symbol)
                except Exception as e:
                    logger.warning("%s ETF yfinance 异常: %s，尝试重试...", symbol, e)
                
                if attempt < max_retries - 1:
                    sleep_time = backoff_factor ** (attempt + 1)
                    logger.info("休眠 %d 秒后进行第 %d 次重试...", sleep_time, attempt + 2)
                    time.sleep(sleep_time)

            if df is None or df.empty:
                logger.warning("yfinance %s 数据为空或全部重试失败", symbol)
                return records

            prev_shares: float | None = None
            for idx, row in df.iterrows():
                date_str = pd.Timestamp(idx).strftime("%Y-%m-%d")
                shares_val = float(row["Volume"])
                close_price = float(row["Close"])
                oz_per_share = 0.1 if symbol == "GLD" else 1.0
                ounces_val = shares_val * oz_per_share

                change_val = (
                    (shares_val - prev_shares) if prev_shares is not None else 0.0
                )
                prev_shares = shares_val

                records.append({
                    "date": date_str,
                    "symbol": symbol,
                    "metal": meta["metal"],
                    "shares": shares_val,
                    "ounces": ounces_val,
                    "change": change_val,
                    "source": "yfinance",
                })

            logger.info("%s 获取 %d 条 ETF 记录", symbol, len(records))
        except FuturesTimeout:
            logger.warning("%s ETF yfinance 超时 (curl_cffi SSL 问题)", symbol)
        except Exception:
            logger.exception("%s ETF 抓取失败", symbol)
        return records

    def update_daily(self) -> int:
        """执行日度 ETF 数据抓取并写入数据库。"""
        all_records: list[dict[str, Any]] = []
        for symbol in self._ETF_MAP:
            all_records.extend(self.fetch_etf(symbol))

        if all_records:
            return self.db.insert_batch("etf_holdings_daily", all_records)
        logger.warning("无 ETF 数据可写入")
        return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    fetcher = ETFFetcher(db)
    count = fetcher.update_daily()
    print(f"✅ ETF 数据抓取完成，共 {count} 条记录")
