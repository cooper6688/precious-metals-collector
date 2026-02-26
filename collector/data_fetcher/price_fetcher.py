"""
价格抓取器 - 获取期货与现货价格数据。

数据源：
- CME 期货: yfinance（GC=F / SI=F 连续合约）
- SHFE 期货: akshare 上期所日行情
- 上海金交所现货: akshare spot_hist_sge（Au99.99 / Ag(T+D)）
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import pandas as pd

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
            return results
        except KeyError as e:
            logger.error("SGE %s 列名不匹配: %s", metal, e)
            return results
        except Exception:
            logger.exception("SGE 现货 %s 抓取失败", metal)
            return results

    # --------------------------------------------------------
    # 伦敦现货 (yfinance LBMA)
    # --------------------------------------------------------

    def fetch_lbma_spot(self, days: int = 5) -> list[dict[str, Any]]:
        """
        通过 yfinance 获取伦敦现货黄金/白银 (XAUUSD=X, XAGUSD=X)。
        """
        records: list[dict[str, Any]] = []
        if os.getenv("PM_SKIP_YFINANCE", "0") == "1":
            return records

        tickers = {"gold": "XAUUSD=X", "silver": "XAGUSD=X"}
        try:
            import yfinance as yf
            for metal, symbol in tickers.items():
                def _fetch():
                    ticker = yf.Ticker(symbol)
                    return ticker.history(period=f"{days}d")

                from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(_fetch)
                    df = future.result(timeout=15)

                if df is not None and not df.empty:
                    df = df.reset_index()
                    date_col = df.columns[0]
                    for _, row in df.iterrows():
                        records.append({
                            "date": pd.to_datetime(row[date_col]).strftime("%Y-%m-%d"),
                            "market": "LBMA",
                            "metal": metal,
                            "price": float(row["Close"]),
                            "currency": "USD",
                            "source": "yfinance_spot",
                        })
            logger.info("LBMA 获取 %d 条现货记录", len(records))
        except Exception:
            logger.exception("LBMA 现货抓取失败")
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
