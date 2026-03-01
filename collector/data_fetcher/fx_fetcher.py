"""
汇率抓取器 - 获取离岸人民币汇率 (USDCNH)。

数据源优先级：
1. akshare fx_spot_quote（国内直连，速度快）
2. yfinance USDCNH=X（回退方案）
"""
import logging
import os
from datetime import datetime
from typing import Any

import pandas as pd
from collector.database import DatabaseManager

logger = logging.getLogger(__name__)


class FXFetcher:
    """汇率数据抓取器。"""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def fetch_usdcnh(self) -> list[dict[str, Any]]:
        """
        获取最新 USDCNH 汇率。

        优先使用 akshare（国内直连），失败时回退到 yfinance。

        Returns:
            汇率记录列表。
        """
        # 方法 1: akshare（推荐，国内无需代理）
        records = self._fetch_via_akshare()
        if records:
            return records

        # 方法 2: yfinance（回退，需要代理）
        if os.getenv("PM_SKIP_YFINANCE", "0") != "1":
            records = self._fetch_via_yfinance()
            if records:
                return records

        return []

    def _fetch_via_akshare(self) -> list[dict[str, Any]]:
        """通过 akshare 获取 USD/CNY 即时汇率。"""
        records: list[dict[str, Any]] = []
        try:
            import akshare as ak

            df = ak.fx_spot_quote()
            if df is None or df.empty:
                logger.warning("akshare fx_spot_quote 数据为空")
                return records

            # 找 USD/CNY 行
            usd_rows = df[df["货币对"] == "USD/CNY"]
            if usd_rows.empty:
                logger.warning("akshare fx_spot_quote 中无 USD/CNY 数据")
                return records

            # 取卖报价(ask)作为汇率
            ask_val = usd_rows.iloc[0]["卖报价"]
            if pd.isna(ask_val) or not ask_val:
                logger.warning("akshare USD/CNY 卖报价为空")
                return records
                
            rate = float(ask_val)
            today = datetime.now().strftime("%Y-%m-%d")

            records.append({
                "date": today,
                "pair": "USDCNH",
                "rate": round(rate, 4),
                "source": "akshare",
            })
            logger.info("akshare USDCNH 汇率: %.4f", rate)

        except Exception:
            logger.exception("akshare 汇率抓取失败")
        return records

    def _fetch_via_yfinance(self, period: str = "5d") -> list[dict[str, Any]]:
        """通过 yfinance 获取 USDCNH=X 汇率（回退方案）。"""
        records: list[dict[str, Any]] = []
        try:
            import yfinance as yf
            import pandas as pd
            import time

            def _fetch():
                ticker = yf.Ticker("USDCNH=X")
                return ticker.history(period=period)

            from concurrent.futures import (
                ThreadPoolExecutor,
                TimeoutError as FuturesTimeout,
            )
            
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
                    logger.warning("yfinance USDCNH 超时，尝试重试...")
                except Exception as e:
                    logger.warning("yfinance USDCNH 异常: %s，尝试重试...", e)
                
                if attempt < max_retries - 1:
                    sleep_time = backoff_factor ** (attempt + 1)
                    logger.info("休眠 %d 秒后进行第 %d 次重试...", sleep_time, attempt + 2)
                    time.sleep(sleep_time)

            if df is None or df.empty:
                logger.warning("yfinance USDCNH=X 数据为空或请求全部失败")
                return records

            for idx, row in df.iterrows():
                date_str = pd.Timestamp(idx).strftime("%Y-%m-%d")
                records.append({
                    "date": date_str,
                    "pair": "USDCNH",
                    "rate": round(float(row["Close"]), 4),
                    "source": "yfinance",
                })

            logger.info("yfinance USDCNH 获取 %d 条汇率记录", len(records))

        except Exception:
            logger.warning("yfinance USDCNH 汇率抓取失败")

        return records

    def fetch_usdcnh_history(self, period: str = "10y") -> list[dict[str, Any]]:
        """获取极长期的 USDCNH 历史供回填（仅 yfinance，不依赖 akshare 的即时汇率接口）。"""
        return self._fetch_via_yfinance(period=period)

    def get_latest_rate(self, date: str | None = None) -> float | None:
        """
        获取最近的 USDCNH 汇率值。

        Args:
            date: 截止日期，默认今天。

        Returns:
            汇率值，无数据时返回 None。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        rows = self.db.query(
            """
            SELECT rate FROM fx_rates_daily
            WHERE pair = 'USDCNH' AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            (date,),
        )
        if rows:
            return rows[0]["rate"]
        return None

    def update_daily(self) -> int:
        """执行日度汇率抓取并写入数据库。"""
        records = self.fetch_usdcnh()
        if records:
            return self.db.insert_batch("fx_rates_daily", records)
        logger.warning("无汇率数据可写入")
        return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    fetcher = FXFetcher(db)
    count = fetcher.update_daily()
    rate = fetcher.get_latest_rate()
    print(f"✅ 汇率数据抓取完成，共 {count} 条记录，最新汇率: {rate}")
