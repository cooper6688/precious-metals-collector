"""
库存抓取器 - 从 COMEX / SHFE / LBMA 获取库存数据。

数据源：
- COMEX: CME 官方公开 XLS 文件（Gold_Stocks.xls / Silver_stocks.xls）
- SHFE:  上期所官方 JSON API (pm{date}.dat)
- LBMA:  伦敦金库月度 XLSX 文件
"""
import io
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from scrapling import Fetcher, StealthyFetcher

from collector.database import DatabaseManager
from collector.settings import PROXIES, USE_PROXY

logger = logging.getLogger(__name__)

# 盎司 → 吨 换算因子
OUNCE_TO_TON = 32150.7466

# CME 官方库存报告 URL
COMEX_URLS: dict[str, str] = {
    "gold": "https://www.cmegroup.com/delivery_reports/Gold_Stocks.xls",
    "silver": "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls",
}

# 常用 User-Agent
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class InventoryFetcher:
    """三大市场库存数据抓取器（COMEX / SHFE / LBMA）。"""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _UA})
        if USE_PROXY:
            self.session.proxies.update(PROXIES)

    # ============================================================
    # COMEX —— CME 官方 XLS 直接下载解析
    # ============================================================

    def fetch_comex(self, metal: str = "gold") -> list[dict[str, Any]]:
        """
        下载 CME 官方库存 XLS 文件，解析各仓库的 Registered/Eligible/Total。

        Args:
            metal: 'gold' 或 'silver'。

        Returns:
            库存记录列表。
        """
        url = COMEX_URLS[metal]
        records: list[dict[str, Any]] = []
        try:
            logger.info("COMEX %s: 下载 %s", metal, url)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            # 读取 XLS
            df = pd.read_excel(
                io.BytesIO(resp.content),
                header=None,
                engine="xlrd",  # .xls 格式需要 xlrd
            )

            # 1) 提取 Report Date
            report_date = self._extract_report_date(df, metal)

            # 2) 解析仓库数据
            records = self._parse_comex_xls(df, metal, report_date)

            logger.info(
                "COMEX %s: 获取 %d 条库存记录 (日期: %s)",
                metal, len(records), report_date,
            )
        except Exception:
            logger.exception("COMEX %s 库存抓取失败", metal)
        return records

    def _extract_report_date(self, df: pd.DataFrame, metal: str) -> str:
        """从 XLS 前几行提取 Report Date。"""
        for idx in range(min(10, len(df))):
            cell = str(df.iloc[idx, 0]) if pd.notna(df.iloc[idx, 0]) else ""
            # 匹配 "Report Date: 2/6/2026" 或类似格式
            m = re.search(r"Report\s*Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})", cell)
            if m:
                try:
                    return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                except ValueError:
                    pass
            # 也匹配 "Activity Date"
            m = re.search(r"Activity\s*Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})", cell)
            if m:
                try:
                    return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                except ValueError:
                    pass
        # 兜底用今天
        logger.warning("COMEX %s: 未找到 Report Date, 使用今天", metal)
        return datetime.now().strftime("%Y-%m-%d")

    def _parse_comex_xls(
        self, df: pd.DataFrame, metal: str, report_date: str
    ) -> list[dict[str, Any]]:
        """
        解析 CME XLS 仓库结构：
        - 仓库名行：第一列有值，其余列大多为 NaN
        - 然后是 Registered / Eligible / Total 等行
        """
        records: list[dict[str, Any]] = []
        current_warehouse = None

        # 找到表头行（包含 DEPOSITORY 或 PREV TOTAL 的行）
        header_idx = None
        for idx in range(len(df)):
            cell0 = str(df.iloc[idx, 0]).strip().upper() if pd.notna(df.iloc[idx, 0]) else ""
            if "DEPOSITORY" in cell0 or "PREV" in cell0:
                header_idx = idx
                break

        if header_idx is None:
            logger.warning("COMEX %s: 未找到表头行", metal)
            return records

        warehouse_data: dict[str, dict[str, float]] = {}
        total_data: dict[str, float] = {}

        for idx in range(header_idx + 1, len(df)):
            row = df.iloc[idx]
            cell0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

            if not cell0:
                continue

            # 判断是否是仓库名称行（第一列有值，后面列大部分 NaN）
            non_null_count = row.notna().sum()
            if non_null_count <= 2 and cell0 and not any(
                kw in cell0.upper() for kw in
                ["REGISTERED", "ELIGIBLE", "PLEDGED", "TOTAL", "GRAND", "---"]
            ):
                current_warehouse = cell0
                if current_warehouse not in warehouse_data:
                    warehouse_data[current_warehouse] = {}
                continue

            # 跳过分隔线
            if "---" in cell0 or cell0.startswith("="):
                continue

            # 提取 TOTAL TODAY 列值（通常是最后一个有数据的列）
            cell0_upper = cell0.upper()
            today_val = self._get_today_value(row)

            if current_warehouse and today_val is not None:
                if "REGISTERED" in cell0_upper:
                    warehouse_data[current_warehouse]["registered"] = today_val
                elif "ELIGIBLE" in cell0_upper:
                    warehouse_data[current_warehouse]["eligible"] = today_val

            # 检测 GRAND TOTAL 行
            if "GRAND" in cell0_upper and "TOTAL" in cell0_upper:
                # 全局汇总行
                break

            # 检测 TOTAL 行 (仓库级别汇总)
            if "TOTAL" in cell0_upper and current_warehouse:
                if today_val is not None:
                    warehouse_data[current_warehouse]["total"] = today_val

        # 生成记录
        for wh_name, data in warehouse_data.items():
            if not data:
                continue
            registered = data.get("registered", 0)
            eligible = data.get("eligible", 0)
            total = data.get("total", registered + eligible)

            if total > 0:
                # 注册库存
                records.append({
                    "date": report_date,
                    "exchange": "COMEX",
                    "metal": metal,
                    "category": "registered",
                    "warehouse": wh_name,
                    "inventory": registered,
                    "unit": "oz",
                    "source": "cme_xls",
                })
                # 合格库存
                records.append({
                    "date": report_date,
                    "exchange": "COMEX",
                    "metal": metal,
                    "category": "eligible",
                    "warehouse": wh_name,
                    "inventory": eligible,
                    "unit": "oz",
                    "source": "cme_xls",
                })
                # 总量（吨）
                records.append({
                    "date": report_date,
                    "exchange": "COMEX",
                    "metal": metal,
                    "category": "total",
                    "warehouse": wh_name,
                    "inventory": round(total / OUNCE_TO_TON, 4),
                    "unit": "ton",
                    "source": "cme_xls",
                })

        # 生成 COMEX 全局汇总行（warehouse=''）
        if warehouse_data:
            total_registered = sum(d.get("registered", 0) for d in warehouse_data.values() if d)
            total_eligible = sum(d.get("eligible", 0) for d in warehouse_data.values() if d)
            grand_total = total_registered + total_eligible
            for cat, val, unit in [
                ("registered", total_registered, "oz"),
                ("eligible", total_eligible, "oz"),
                ("total", round(grand_total / OUNCE_TO_TON, 4), "ton"),
            ]:
                records.append({
                    "date": report_date,
                    "exchange": "COMEX",
                    "metal": metal,
                    "category": cat,
                    "warehouse": "",
                    "inventory": val,
                    "unit": unit,
                    "source": "cme_xls",
                })

        return records

    @staticmethod
    def _get_today_value(row: pd.Series) -> float | None:
        """从行中提取 TOTAL TODAY 列值（通常倒数几列中最后一个有效数值）。"""
        # 使用 pd.to_numeric 统一处理：datetime/字符串/乱码 → NaN
        numeric_row = pd.to_numeric(row.iloc[1:], errors="coerce")
        valid = numeric_row.dropna()
        if valid.empty:
            return None
        # 返回最后一个有效数值（TOTAL TODAY 列）
        return float(valid.iloc[-1])

    # ============================================================
    # SHFE —— 上期所 JSON API
    # ============================================================

    def fetch_shfe(self, date: str | None = None) -> list[dict[str, Any]]:
        """
        从上期所 JSON API 获取仓单数据。
        增加回溯机制，如果当天未发布（如周末或节假日返回 404），则向前尝试最多 7 天。
        使用 curl_cffi 伪装浏览器 TLS 指纹以绕过反爬虫。
        """
        records: list[dict[str, Any]] = []
        fetcher = Fetcher()
            
        target_dt = datetime.strptime(date, "%Y%m%d") if date else datetime.now()
        
        # 尝试最多 7 天 (涵盖长假)
        for offset in range(7):
            curr_dt = target_dt - timedelta(days=offset)
            date_str = curr_dt.strftime("%Y%m%d")
            date_fmt = curr_dt.strftime("%Y-%m-%d")
            # 使用浏览器引擎处理潜伏的重定向和安全检查
            # 根据最新调研，SHFE 数据路径已变更为 /data/tradedata/future/dailydata/
            url = f"https://www.shfe.com.cn/data/tradedata/future/dailydata/pm{date_str}.dat"
            logger.info("尝试从 SHFE 抓取仓单数据 (Scrapling): %s", url)
            
            fetcher = StealthyFetcher(headless=True)
            resp = fetcher.fetch(url, timeout=15000)
            
            try:
                # Original headers are not needed if StealthyFetcher handles them
                # headers = {
                #     "User-Agent": _UA,
                #     "Accept": "application/json, text/javascript, */*; q=0.01",
                #     "Referer": "https://www.shfe.com.cn/",
                #     "Connection": "keep-alive"
                # }
                
                # 使用 scrapling StealthyFetcher 自动处理指纹和绕过
                resp = StealthyFetcher.fetch(url, timeout=15000, headless=True)
                
                if resp.status == 404:
                    logger.debug("SHFE pm%s.dat 报 404 (无数据/非交易日)，尝试回退...", date_str)
                    continue
                
                if resp.status != 200:
                    continue
                
                try:
                    data = json.loads(resp.text)
                except Exception:
                    logger.error("SHFE JSON 解析失败: %s", resp.text[:100])
                    continue

                for item in data.get("o_cursor", []):
                    var_name = str(item.get("VARNAME", "")).strip().upper()
                    if "AU" in var_name:
                        metal_val = "gold"
                    elif "AG" in var_name:
                        metal_val = "silver"
                    else:
                        continue

                    warehouse = str(item.get("REGNAME", "")).strip()
                    if not warehouse or warehouse == "nan":
                        warehouse = str(item.get("WHABBRNAME", "")).strip()

                    weight_str = str(item.get("WRTWGHTS", "0")).strip()
                    try:
                        weight = float(weight_str.replace(",", ""))
                    except (ValueError, TypeError):
                        weight = 0.0

                    unit_val = str(item.get("WGHTUNIT", "千克")).strip()

                    if weight > 0:
                        records.append({
                            "date": date_fmt,
                            "exchange": "SHFE",
                            "metal": metal_val,
                            "category": "warehouse",
                            "warehouse": warehouse,
                            "inventory": weight,
                            "unit": unit_val,
                            "source": "shfe_json_cffi",
                        })

                if records:
                    logger.info("SHFE 获取 %d 条仓单记录 (回溯 %d 天，日期: %s)", len(records), offset, date_fmt)
                    return records
                else:
                    logger.warning("SHFE %s 数据解析为空，继续尝试...", date_fmt)

            except Exception as e:
                logger.warning("SHFE %s 仓单抓取异常: %s", date_fmt, e)
                
            time.sleep(0.5)
            
        logger.warning("SHFE 回溯 7 天仍未获取到仓单数据")
        return records

    # ============================================================
    # LBMA —— 伦敦金库月度 XLSX
    # ============================================================

    def fetch_lbma(self, year: int | None = None, month: int | None = None) -> list[dict[str, Any]]:
        """
        下载 LBMA 月度伦敦金库数据。

        文件 URL 格式:
        https://cdn.lbma.org.uk/downloads/LBMA-London-Vault-Holdings-Data-{Month}-{Year}.xlsx

        Args:
            year: 年份，默认当前年。
            month: 月份，默认上个月（LBMA 数据有 1 个月延迟）。

        Returns:
            库存记录列表。
        """
        records: list[dict[str, Any]] = []
        # TODO: LBMA CDN has strengthened protection against bot downloads (even with StealthyFetcher/curl_cffi).
        # Temporarily disabling this to prevent pipeline failure until a new bypass or storage mirror is found.
        logger.warning("LBMA 金库 XLSX 下载暂时关闭（因 CDN 403 封锁）")
        return []

    # ============================================================
    # SGE —— 上海金交所 PDF
    # ============================================================

    def fetch_sge_pdf(self, date: str | None = None) -> list[dict[str, Any]]:
        """
        从上海金交所 JSON API 直接提取 PDF 链接，绕过 Vue 动态渲染问题。
        """
        records: list[dict[str, Any]] = []

        records: list[dict[str, Any]] = []
        fetcher = Fetcher()
        import os
        import tempfile
            
        # SGE 文章列表 API (menuId=1738 为每日行情)
        api_url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=15&menuId=1738"
        headers = {
            "User-Agent": _UA,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.sge.com.cn/sjzx/mrhq"
        }
        
        try:
            logger.debug("请求 SGE JSON API: %s", api_url)
            resp = StealthyFetcher.fetch(
                api_url, 
                timeout=15000,
                headless=True
            )
            if resp.status != 200:
                logger.warning("SGE JSON API 返回错误状态码: %s", resp.status)
                return records
                
            try:
                data = json.loads(resp.text)
            except Exception:
                logger.error("SGE JSON 解析失败: %s", resp.text[:100])
                return records
            
            articles = data.get("list", [])
            if not articles:
                logger.warning("SGE JSON API 未返回文章列表")
                return records

            # 寻找最新的“交割”或“交收”类文章
            target_article = None
            for item in articles:
                title = item.get("title", "")
                if "交割" in title or "交收" in title or "行情" in title:
                    target_article = item
                    break
            
            if not target_article:
                logger.warning("SGE JSON 列表内未找到交割相关报告")
                return records

            pdf_path_relative = target_article.get("fileUrl")
            if not pdf_path_relative:
                logger.warning("SGE 文章 '%s' 缺少 PDF 链接", target_article.get("title"))
                return records

            pdf_url = "https://www.sge.com.cn" + pdf_path_relative
            publish_date = target_article.get("publishDate", "").split(" ")[0]
            
            logger.info("准备下载 SGE PDF: %s (发布日期: %s)", pdf_url, publish_date)
            
            pdf_resp = StealthyFetcher.fetch(
                pdf_url, 
                timeout=30000,
                headless=True
            )
            if pdf_resp.status != 200:
                logger.error("SGE PDF 下载失败: %s", pdf_resp.status)
                return records
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(pdf_resp.body)
                tmp_path = tmp.name

            records = self._parse_sge_pdf(tmp_path, date_str=publish_date)
            os.remove(tmp_path)

        except Exception:
            logger.exception("SGE JSON 数据抓取或 PDF 解析失败")

        return records

    @staticmethod
    def _parse_sge_pdf(pdf_path: str, date_str: str) -> list[dict[str, Any]]:
        """
        解析 SGE PDF 文件，提取交割数据。
        """
        records: list[dict[str, Any]] = []
        gold_vol = 0.0
        silver_vol = 0.0

        try:
            import pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        # 动态表头寻找
                        delivery_col_idx = -1
                        for row_idx, row in enumerate(table):
                            if not row: continue
                            row_str_list = [str(x).replace('\n', '') for x in row if x]
                            row_text = "".join(row_str_list).upper()
                            
                            # 探测表头中的“交收”或“交割”字眼
                            if "交收" in row_text or "交割" in row_text:
                                for col_idx, cell_text in enumerate(row):
                                    if cell_text and re.search(r"(交收|交割)", str(cell_text)):
                                        delivery_col_idx = col_idx
                                        break
                                        
                            # 如果找到了表头，或者本身已经是数据行
                            if "AU" in row_text or "金" in row_text or "AG" in row_text or "银" in row_text:
                                # 如果没有找到明确的表头列，尝试用启发式的正则 (通常交割量在倒数第1/2列)
                                target_val = 0.0
                                
                                if delivery_col_idx != -1 and delivery_col_idx < len(row):
                                    # 按精确列索引取
                                    cell_str = str(row[delivery_col_idx]).replace(",", "").strip()
                                    num_match = re.search(r"[\d\.]+", cell_str)
                                    if num_match:
                                        target_val = float(num_match.group(0))
                                else:
                                    # 回退到提取所有数字
                                    nums = []
                                    for cell in row:
                                        if cell:
                                            num_match = re.search(r"[\d\.]+", str(cell).replace(",", ""))
                                            if num_match:
                                                try:
                                                    nums.append(float(num_match.group(0)))
                                                except ValueError:
                                                    pass
                                    if nums:
                                        # 保守提取：如果提取不到表头，暂估在最后一两个数字里
                                        # 优先取最后一个数字，因为交割量通常在表格的右侧
                                        target_val = nums[-1] if nums else 0.0

                                if target_val > 0:
                                    if "AU" in row_text or "金" in row_text:
                                        gold_vol += target_val
                                    elif "AG" in row_text or "银" in row_text:
                                        silver_vol += target_val

            if gold_vol > 0:
                records.append({
                    "date": date_str,
                    "exchange": "SGE",
                    "metal": "gold",
                    "category": "delivery_volume",
                    "warehouse": "SGE Main",
                    "inventory": round(gold_vol / 1000, 4),
                    "unit": "ton",
                    "source": "sge_pdf",
                })
            if silver_vol > 0:
                records.append({
                    "date": date_str,
                    "exchange": "SGE",
                    "metal": "silver",
                    "category": "delivery_volume",
                    "warehouse": "SGE Main",
                    "inventory": round(silver_vol / 1000, 4),
                    "unit": "ton",
                    "source": "sge_pdf",
                })

            if records:
                logger.info("SGE 获取 %d 条 PDF 解析记录 (日期: %s)", len(records), date_str)

        except Exception as e:
            logger.warning("SGE PDF 解析异常: %s", e)
        
        return records

    @staticmethod
    def _safe_float_val(val: Any) -> float | None:
        """安全转换浮点数。"""
        if pd.isna(val):
            return None
        try:
            return float(str(val).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    # ============================================================
    # 汇总入库
    # ============================================================

    def update_daily(self) -> int:
        """
        执行日度库存数据抓取并写入数据库。

        Returns:
            总插入记录数。
        """
        all_records: list[dict[str, Any]] = []

        # 1. COMEX 金/银
        for metal in ("gold", "silver"):
            comex = self.fetch_comex(metal)
            all_records.extend(comex)
            if not comex:
                logger.warning("COMEX %s 无数据（可能 CME 访问受限）", metal)
            time.sleep(1)  # 礼貌延迟

        # 2. SHFE 仓单
        shfe = self.fetch_shfe()
        all_records.extend(shfe)

        # 3. LBMA 月度（每天都尝试，靠数据库 INSERT OR REPLACE 去重）
        lbma = self.fetch_lbma()
        all_records.extend(lbma)

        # 4. SGE PDF 解析
        sge = self.fetch_sge_pdf()
        all_records.extend(sge)

        if all_records:
            return self.db.insert_batch("inventory_daily", all_records)
        logger.warning("无库存数据可写入")
        return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    fetcher = InventoryFetcher(db)
    count = fetcher.update_daily()
    print(f"✅ 库存数据抓取完成，共 {count} 条记录")
