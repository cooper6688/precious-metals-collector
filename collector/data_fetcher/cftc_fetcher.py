"""
CFTC 抓取器 - 获取 CFTC COT（Commitments of Traders）周度报告。

数据源优先级：
1. pycot-reports 库（如可用）
2. CFTC 官方 CSV zip 文件直接下载（回退方案）
"""
import io
import logging
import zipfile
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from collector.database import DatabaseManager
from collector.settings import PROXIES, USE_PROXY

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class CFTCFetcher:
    """CFTC COT 报告抓取器。"""

    # CFTC 合约名称关键词 → 金属映射
    _GOLD_KEYWORDS = ["GOLD", "088691"]
    _SILVER_KEYWORDS = ["SILVER", "084691"]

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _UA})
        if USE_PROXY:
            self.session.proxies.update(PROXIES)

    def fetch_cot_report(self, year: int | None = None) -> list[dict[str, Any]]:
        """
        获取 CFTC COT 报告数据。
        优先使用 pycot-reports；不可用时回退到直接下载 CFTC CSV。

        Args:
            year: 报告年份，默认当年。

        Returns:
            CFTC 持仓记录列表。
        """
        if year is None:
            year = datetime.now().year

        # 尝试方法 1: pycot-reports
        try:
            records = self._fetch_via_pycot(year)
            if records:
                return records
        except Exception:
            logger.info("pycot-reports 不可用，回退到 CFTC 官方 CSV")

        # 方法 2: 通过 CFTC Socrata Open Data API（当年无数据则回退去年）
        for try_year in (year, year - 1):
            try:
                records = self._fetch_via_api(try_year)
                if records:
                    return records
                logger.warning("CFTC API: %d 年无数据，尝试上一年", try_year)
            except Exception:
                logger.exception("CFTC API %d 年查询失败", try_year)
        return []

    def _fetch_via_pycot(self, year: int) -> list[dict[str, Any]]:
        """通过 pycot-reports 获取 COT 数据。"""
        import cot_reports as cot  # type: ignore

        records: list[dict[str, Any]] = []
        df = cot.cot_year(year=year, cot_report_type="legacy_fut")
        if df is None or df.empty:
            logger.warning("pycot-reports: %d 年数据为空", year)
            return records

        df.columns = [str(c).strip() for c in df.columns]

        # 找名称列
        name_col = self._find_name_column(df)

        for metal_val, keywords in [("gold", self._GOLD_KEYWORDS), ("silver", self._SILVER_KEYWORDS)]:
            mask = pd.Series(False, index=df.index)
            for kw in keywords:
                mask |= df[name_col].str.upper().str.contains(kw, na=False)
            sub = df[mask]
            if sub.empty:
                continue

            for _, row in sub.tail(4).iterrows():
                rec = self._extract_cot_row(row, metal_val, "pycot")
                if rec:
                    records.append(rec)

        logger.info("pycot: 获取 %d 条 COT 记录", len(records))
        return records

    def _fetch_via_api(self, year: int) -> list[dict[str, Any]]:
        """
        通过 CFTC Socrata Open Data API 获取 Legacy Futures COT 数据。

        API 文档: https://publicreporting.cftc.gov/resource/6dca-aqww.json
        - 该 API 支持 SoQL 查询
        - 黄金 cftc_contract_market_code = '088691'
        - 白银 cftc_contract_market_code = '084691'
        """
        records: list[dict[str, Any]] = []

        contracts = {
            "gold": "088691",
            "silver": "084691",
        }

        for metal, code in contracts.items():
            url = (
                "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
                f"?$where=cftc_contract_market_code='{code}'"
                f" AND report_date_as_yyyy_mm_dd >= '{year}-01-01'"
                "&$order=report_date_as_yyyy_mm_dd DESC"
                "&$limit=4"
            )
            logger.info("CFTC API: 获取 %s (%s) 数据...", metal, code)

            import time
            max_retries = 4
            backoff_factor = 2
            data = None

            for attempt in range(max_retries):
                try:
                    resp = self.session.get(url, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    logger.warning("CFTC API 请求异常 (%s): %s，尝试重试...", metal, e)
                    
                if attempt < max_retries - 1:
                    sleep_time = backoff_factor ** (attempt + 1)
                    logger.info("休眠 %d 秒后进行第 %d 次重试...", sleep_time, attempt + 2)
                    time.sleep(sleep_time)

            if not data:
                logger.warning("CFTC API: %s %d 年无数据或请求全部失败", metal, year)
                continue

            for item in data:
                report_date = str(item.get("report_date_as_yyyy_mm_dd", ""))[:10]
                if not report_date:
                    continue

                ncl = self._api_float(item, "noncomm_positions_long_all")
                ncs = self._api_float(item, "noncomm_positions_short_all")
                cl = self._api_float(item, "comm_positions_long_all")
                cs = self._api_float(item, "comm_positions_short_all")
                net_pos = (ncl - ncs) if ncl is not None and ncs is not None else None

                records.append({
                    "report_date": report_date,
                    "market": "COMEX",
                    "metal": metal,
                    "non_commercial_long": ncl,
                    "non_commercial_short": ncs,
                    "commercial_long": cl,
                    "commercial_short": cs,
                    "net_position": net_pos,
                    "source": "cftc_api",
                })

        logger.info("CFTC API: 获取 %d 条 COT 记录", len(records))
        return records

    @staticmethod
    def _api_float(item: dict, key: str) -> float | None:
        """从 API JSON 中安全提取浮点数。"""
        val = item.get(key)
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _find_name_column(self, df: pd.DataFrame) -> str:
        """找到包含合约名称的列。"""
        for c in df.columns:
            c_lower = c.lower()
            if "market" in c_lower or "name" in c_lower:
                return c
        return df.columns[0]

    def _extract_cot_row(self, row: pd.Series, metal: str, source: str) -> dict[str, Any] | None:
        """从 COT 行数据中提取持仓信息。"""
        report_date = self._extract_date(row)
        if report_date is None:
            return None

        ncl = self._safe_float(row, "NonComm_Positions_Long_All", "Noncommercial Positions-Long")
        ncs = self._safe_float(row, "NonComm_Positions_Short_All", "Noncommercial Positions-Short")
        cl = self._safe_float(row, "Comm_Positions_Long_All", "Commercial Positions-Long")
        cs = self._safe_float(row, "Comm_Positions_Short_All", "Commercial Positions-Short")

        net_pos = (ncl - ncs) if ncl is not None and ncs is not None else None

        return {
            "report_date": report_date,
            "market": "COMEX",
            "metal": metal,
            "non_commercial_long": ncl,
            "non_commercial_short": ncs,
            "commercial_long": cl,
            "commercial_short": cs,
            "net_position": net_pos,
            "source": source,
        }

    @staticmethod
    def _extract_date(row: pd.Series) -> str | None:
        """从 COT 行数据中提取日期。"""
        date_cols = [
            "As_of_Date_In_Form_YYMMDD",
            "Report_Date_as_YYYY-MM-DD",
            "As_of_Date_Form_MM/DD/YYYY",
            "Report_Date_as_MM_DD_YYYY",
            "As of Date in Form YYMMDD",
            "As of Date in Form YYYY-MM-DD",
            "Report Date as YYYY-MM-DD",
            "As of Date Form MM/DD/YYYY",
            "Report Date as MM DD YYYY",
        ]
        for col_name in date_cols:
            if col_name in row.index and pd.notna(row[col_name]):
                try:
                    dt = pd.to_datetime(str(row[col_name]))
                    return dt.strftime("%Y-%m-%d")
                except Exception:
                    continue
        return None

    @staticmethod
    def _safe_float(row: pd.Series, *col_names: str) -> float | None:
        """安全获取浮点数值（支持多个候选列名模糊匹配）。"""
        for col_name in col_names:
            # 精确匹配
            if col_name in row.index and pd.notna(row[col_name]):
                try:
                    return float(row[col_name])
                except (ValueError, TypeError):
                    continue
            # 模糊匹配
            for c in row.index:
                if col_name.lower() in str(c).lower() and pd.notna(row[c]):
                    try:
                        return float(row[c])
                    except (ValueError, TypeError):
                        continue
        return None

    # ============================================================
    # 更新入口
    # ============================================================

    def update_if_report_day(self) -> int:
        """
        仅在周五执行 CFTC 数据更新。

        Returns:
            插入记录数。
        """
        today = datetime.now()
        if today.weekday() != 4:
            logger.info("今天不是周五，跳过 CFTC 更新")
            return 0
        return self.update_weekly()

    def update_weekly(self) -> int:
        """强制执行 CFTC 周度更新。"""
        records = self.fetch_cot_report()
        if records:
            return self.db.insert_batch("cftc_positions_weekly", records)
        logger.warning("无 CFTC 数据可写入")
        return 0

    def backfill_history(self, start_year: int = 2010) -> int:
        """通过下载官方 ZIP 包全量回填历史 CFTC 数据（避免被 API 拒绝）。"""
        import io
        import zipfile
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        current_year = datetime.now().year
        total_inserted = 0
        
        contracts = {
            "088691": "gold",
            "084691": "silver"
        }

        for year in range(start_year, current_year + 1):
            url = f"https://cftc.gov/files/dea/history/deacot{year}.zip"
            logger.info("开始回填 %d 年 CFTC 数据 (下载 ZIP: %s)...", year, url)
            try:
                resp = self.session.get(url, verify=False, timeout=30)
                if resp.status_code != 200:
                    logger.warning("⚠️ %d 年 CFTC ZIP 下载失败 (状态码: %d)", year, resp.status_code)
                    continue

                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    txt_files = [f for f in z.namelist() if f.lower().endswith(".txt")]
                    if not txt_files:
                        logger.warning("⚠️ %d 年 CFTC ZIP 中无 txt 文件", year)
                        continue
                    
                    with z.open(txt_files[0]) as f:
                        df = pd.read_csv(f, low_memory=False)

                records = []
                df.columns = [str(c).strip() for c in df.columns]
                
                if "CFTC Contract Market Code" not in df.columns:
                    logger.warning("⚠️ %d 年 CFTC 格式不包含 CFTC Contract Market Code 列", year)
                    continue

                for code, metal in contracts.items():
                    sub = df[df["CFTC Contract Market Code"].astype(str).str.strip() == code]

                    if sub.empty:
                        logger.warning("%d 年 CFTC 数据中未找到 %s (%s)", year, metal, code)
                        continue

                    # 逐行解析
                    for _, row in sub.iterrows():
                        rec = self._extract_cot_row(row, metal, source="cftc_zip")
                        if rec:
                            records.append(rec)

                if records:
                    inserted = self.db.insert_batch("cftc_positions_weekly", records)
                    total_inserted += inserted
                    logger.info("✅ %d 年 CFTC 数据写入: %d 条", year, inserted)
                else:
                    logger.warning("⚠️ %d 年 CFTC 未解析出任何黄金白银数据", year)

            except zipfile.BadZipFile:
                 logger.warning("⚠️ %d 年 CFTC 无法解压 ZIP 文件", year)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                     logger.warning("⚠️ %d 年 CFTC ZIP 尚不存在", year)
                else:
                     logger.exception("⚠️ %d 年 CFTC 下载解析发生异常: %s", year, e)
            except Exception as e:
                logger.exception("⚠️ %d 年 CFTC 下载解析发生未知异常: %s", year, e)
                
        return total_inserted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    fetcher = CFTCFetcher(db)
    count = fetcher.update_weekly()
    print(f"✅ CFTC 数据抓取完成，共 {count} 条记录")
