"""
库存指标计算器 - 计算交割压力指数 (DPI)、reg/OI 比率、库存变动率。
"""
import logging
from datetime import datetime
from typing import Any

from collector.database import DatabaseManager
from collector.settings import CALC_CONFIG

logger = logging.getLogger(__name__)


class InventoryCalculator:
    """库存因子计算器。"""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def calculate_dpi(self, metal: str, date: str) -> float | None:
        """
        计算交割压力指数: DPI = 注册库存 / 主力合约 OI。

        Args:
            metal: 金属类型 ('gold' / 'silver')。
            date: 日期 YYYY-MM-DD。

        Returns:
            DPI 值，数据不足时返回 None。
        """
        # ℹ️ 只用 COMEX 注册库存总计（oz），避免混入单个仓库数据
        inv = self.db.query(
            """
            SELECT inventory FROM inventory_daily
            WHERE metal = ? AND category = 'registered'
              AND exchange = 'COMEX' AND unit = 'oz' AND warehouse = ''
              AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            (metal, date),
        )
        if not inv:
            logger.warning("%s DPI: 无 COMEX 注册库存数据", metal)
            return None
        reg_inventory = inv[0]["inventory"]

        # 获取主力合约 OI
        oi = self.db.query(
            """
            SELECT open_interest FROM future_prices_daily
            WHERE metal = ? AND open_interest IS NOT NULL AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            (metal, date),
        )
        if not oi or oi[0]["open_interest"] is None or oi[0]["open_interest"] == 0:
            logger.warning("%s DPI: 无 OI 数据", metal)
            return None

        if metal.lower() == "gold":
            contract_size = 100
        elif metal.lower() == "silver":
            contract_size = 5000
        else:
            contract_size = 1
            
        oi_oz = oi[0]["open_interest"] * contract_size
        
        # 交割压力指数 = OI(oz) / 注册库存(oz)
        dpi = oi_oz / reg_inventory
        logger.info("%s DPI = %.4f (OI_oz=%.0f, reg=%.0f)", metal, dpi, oi_oz, reg_inventory)
        return dpi

    def calculate_reg_oi_ratio(self, metal: str, date: str) -> float | None:
        """
        计算 reg/OI 比率（与 DPI 相同公式，但可用于不同上下文）。
        """
        return self.calculate_dpi(metal, date)

    def calculate_inventory_change(
        self, metal: str, date: str, days: int | None = None
    ) -> dict[str, Any]:
        """
        计算过去 N 天 COMEX 注册库存的变动率（统一使用盎司单位）。

        Args:
            metal: 金属类型。
            date: 截止日期。
            days: 回溯天数，默认从配置中读取。

        Returns:
            包含 change_abs (绝对变动) 和 change_pct (变动率) 的字典。
        """
        if days is None:
            days = CALC_CONFIG.get("inventory_change_days", 5)

        # ℹ️ 仅查询 COMEX 注册库存总计（oz），保证单位一致性和避免混入单仓数据
        #    否则全局总量(170M)比对单仓量(500k)会导致 31739% 变动率爆表
        rows = self.db.query(
            """
            SELECT date, inventory FROM inventory_daily
            WHERE metal = ? AND category = 'registered'
              AND exchange = 'COMEX' AND unit = 'oz' AND warehouse = ''
              AND date <= ?
            ORDER BY date DESC LIMIT ?
            """,
            (metal, date, days + 1),
        )

        result: dict[str, Any] = {
            "change_abs": None,
            "change_pct": None,
            "latest": None,
            "earliest": None,
        }

        if len(rows) < 2:
            return result

        latest_val = rows[0]["inventory"]
        earliest_val = rows[-1]["inventory"]
        result["latest"] = latest_val
        result["earliest"] = earliest_val
        result["change_abs"] = latest_val - earliest_val
        if earliest_val != 0:
            result["change_pct"] = (latest_val - earliest_val) / earliest_val * 100

        return result

    def compute_metrics(self, date: str | None = None) -> list[dict[str, Any]]:
        """
        计算所有库存指标并写入 computed_factors 表。

        Args:
            date: 目标日期，默认今天。

        Returns:
            写入的指标记录列表。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        records: list[dict[str, Any]] = []
        for metal in ("gold", "silver"):
            prefix = metal.upper()

            # DPI
            dpi = self.calculate_dpi(metal, date)
            if dpi is not None:
                threshold = CALC_CONFIG["dpi_threshold"]
                if dpi >= threshold["high"]:
                    desc = "高交割压力"
                elif dpi >= threshold["medium"]:
                    desc = "中等交割压力"
                else:
                    desc = "低交割压力"
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_dpi",
                    "value": round(dpi, 2),  # ← 2 位小数，避免 110.548371
                    "description": desc,
                })

            # reg/OI 比率
            ratio = self.calculate_reg_oi_ratio(metal, date)
            if ratio is not None:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_reg_oi_ratio",
                    "value": round(ratio, 6),
                    "description": "注册库存/OI 比率",
                })

            # 库存变动
            change = self.calculate_inventory_change(metal, date)
            if change["change_pct"] is not None:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_inv_change_pct",
                    "value": round(change["change_pct"], 4),
                    "description": f"{CALC_CONFIG.get('inventory_change_days', 5)}日库存变动率%",
                })
            if change["change_abs"] is not None:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_inv_change_abs",
                    "value": round(change["change_abs"], 2),
                    "description": "库存绝对变动",
                })

        if records:
            self.db.insert_batch("computed_factors", records)
            logger.info("库存指标计算完成，写入 %d 条记录", len(records))
        return records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    calc = InventoryCalculator(db)
    metrics = calc.compute_metrics()
    for m in metrics:
        print(f"  {m['metric_type']}: {m['value']} ({m['description']})")
