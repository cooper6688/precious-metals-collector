"""
库存抓取器 - 从 COMEX / SHFE / LBMA 获取库存数据。

数据源：
- COMEX: CME 官方公开 XLS 文件（Gold_Stocks.xls / Silver_stocks.xls）
- SHFE:  上期所官方 JSON API (pm{date}.dat)
- LBMA:  伦敦金库月度 XLSX 文件
"""
import io
import logging
import re
import time
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests

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
        增加回溯机制，如果当天未发布（如周末或节假日返回 404），则向前尝试最多 5 天。

        Args:
            date: 日期 YYYYMMDD，默认今天。

        Returns:
            仓单记录列表。
        """
        records: list[dict[str, Any]] = []
        
        target_dt = datetime.strptime(date, "%Y%m%d") if date else datetime.now()
        
        # 尝试最多 5 天
        for offset in range(5):
            curr_dt = target_dt - timedelta(days=offset)
            date_str = curr_dt.strftime("%Y%m%d")
            date_fmt = curr_dt.strftime("%Y-%m-%d")
            url = f"https://www.shfe.com.cn/data/dailydata/kx/pm{date_str}.dat"

            try:
                # SHFE 在国内可直接访问，不需要代理
                resp = requests.get(url, headers={"User-Agent": _UA}, timeout=30,
                                proxies={"http": None, "https": None})
                resp.raise_for_status()
                data = resp.json()

                # 遍历 o_cursor
                for item in data.get("o_cursor", []):
                    var_name = str(item.get("VARNAME", "")).strip().upper()

                    # 只关心黄金(AU)和白银(AG)
                    metal_val = None
                    if "AU" in var_name:
                        metal_val = "gold"
                    elif "AG" in var_name:
                        metal_val = "silver"
                    else:
                        continue

                    # 仓库名
                    warehouse = str(item.get("REGNAME", "")).strip()
                    if not warehouse or warehouse == "nan":
                        warehouse = str(item.get("WHABBRNAME", "")).strip()

                    # 重量与单位
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
                            "source": "shfe_json",
                        })

                if records:
                    logger.info("SHFE 获取 %d 条仓单记录 (回溯 %d 天，日期: %s)", len(records), offset, date_fmt)
                    return records
                else:
                    logger.warning("SHFE %s 数据解析为空，可能非主力交割，继续尝试...", date_fmt)

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    logger.debug("SHFE 仓单数据不存在 (日期: %s, 尝试上一交易日)", date_fmt)
                else:
                    logger.exception("SHFE %s 仓单抓取失败", date_fmt)
            except Exception:
                logger.exception("SHFE %s 仓单抓取失败", date_fmt)
                
            time.sleep(1) # 避免请求过快
            
        logger.warning("SHFE 回溯 5 天仍未获取到仓单数据")
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
        now = datetime.now()

        if year is None or month is None:
            # LBMA 数据通常有 1 个月延迟，取上个月
            if now.month == 1:
                year = now.year - 1
                month = 12
            else:
                year = now.year
                month = now.month - 1

        month_names = [
            "", "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ]
        month_name = month_names[month]

        url = (
            f"https://cdn.lbma.org.uk/downloads/"
            f"LBMA-London-Vault-Holdings-Data-{month_name}-{year}.xlsx"
        )
        try:
            logger.info("LBMA: 下载 %s", url)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            df = pd.read_excel(io.BytesIO(resp.content), header=None)
            logger.info("LBMA 原始行数: %d, 列数: %d", len(df), len(df.columns))

            # 解析：从第 3 行（索引 2）开始，找 YYYY-MM 格式的日期列
            for idx in range(2, len(df)):
                month_end = str(df.iloc[idx, 0]).strip()
                if not re.match(r"\d{4}-\d{2}", month_end):
                    continue

                # Gold = 列 1, Silver = 列 2（千盎司）
                gold_koz = self._safe_float_val(df.iloc[idx, 1])
                silver_koz = self._safe_float_val(df.iloc[idx, 2])

                if gold_koz is not None and gold_koz > 0:
                    gold_ton = (gold_koz * 1000) / OUNCE_TO_TON
                    records.append({
                        "date": month_end,
                        "exchange": "LBMA",
                        "metal": "gold",
                        "category": "vault_total",
                        "warehouse": "London Vaults",
                        "inventory": round(gold_ton, 2),
                        "unit": "ton",
                        "source": "lbma_xlsx",
                    })
                if silver_koz is not None and silver_koz > 0:
                    silver_ton = (silver_koz * 1000) / OUNCE_TO_TON
                    records.append({
                        "date": month_end,
                        "exchange": "LBMA",
                        "metal": "silver",
                        "category": "vault_total",
                        "warehouse": "London Vaults",
                        "inventory": round(silver_ton, 2),
                        "unit": "ton",
                        "source": "lbma_xlsx",
                    })

            # 只取最新 2 条月份数据（避免写入过多历史）
            if len(records) > 4:
                records = records[:4]

            logger.info("LBMA 获取 %d 条金库记录", len(records))

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.warning("LBMA %d-%02d 数据不存在（可能尚未发布）", year, month)
            else:
                logger.exception("LBMA 金库数据抓取失败")
        except Exception:
            logger.exception("LBMA 金库数据抓取失败")
        return records

    # ============================================================
    # SGE —— 上海金交所 PDF
    # ============================================================

    def fetch_sge_pdf(self, date: str | None = None) -> list[dict[str, Any]]:
        """
        抓取上海黄金交易所官方每日/每周交割 PDF，提取库存。
        """
        records: list[dict[str, Any]] = []
        try:
            import pdfplumber
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("缺少 pdfplumber 或 bs4，无法解析 SGE PDF")
            return records
            
        target_dt = datetime.strptime(date, "%Y%m%d") if date else datetime.now()

        for offset in range(5):
            curr_dt = target_dt - timedelta(days=offset)
            date_str = curr_dt.strftime("%Y-%m-%d")
            
            try:
                url = "https://www.sge.com.cn/sjzx/mrhq"
                resp = self.session.get(url, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')

                pdf_url = None
                for a in soup.find_all('a'):
                    text = a.get_text(strip=True)
                    if "交割" in text and ("PDF" in text.upper() or str(a.get("href", "")).lower().endswith(".pdf")):
                        href = a.get("href", "")
                        if not href.startswith("http"):
                            href = "https://www.sge.com.cn" + href
                        pdf_url = href
                        break
                    elif "交割" in text and "article" in str(a.get("href", "")):
                        href = a.get("href", "")
                        if not href.startswith("http"):
                            href = "https://www.sge.com.cn" + href
                        
                        resp2 = self.session.get(href, timeout=15)
                        soup2 = BeautifulSoup(resp2.text, 'html.parser')
                        for a2 in soup2.find_all('a'):
                            h2 = a2.get("href", "")
                            if h2.lower().endswith(".pdf"):
                                if not h2.startswith("http"):
                                    h2 = "https://www.sge.com.cn" + h2
                                pdf_url = h2
                                break
                        if pdf_url:
                            break

                if not pdf_url:
                    logger.debug("SGE 官网未找到交割 PDF (尝试前一日: %s)", date_str)
                    continue

                logger.info("SGE 下载 PDF: %s", pdf_url)
                pdf_resp = self.session.get(pdf_url, timeout=20)
                pdf_resp.raise_for_status()

                gold_vol = 0.0
                silver_vol = 0.0

                with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
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
                                            target_val = nums[0] if len(nums) == 1 else nums[1] if len(nums) > 1 else nums[-1]

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
                    return records

            except Exception as e:
                logger.warning("SGE PDF 获取/解析异常: %s", e)
                
            time.sleep(2)
            
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
