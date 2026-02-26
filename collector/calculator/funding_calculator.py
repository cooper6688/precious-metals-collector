"""
资金指标计算器 - 计算 ETF 日度净流入、投机拥挤度等。
"""
import logging
from datetime import datetime
from typing import Any

from collector.database import DatabaseManager

logger = logging.getLogger(__name__)


class FundingCalculator:
    """资金流因子计算器。"""

    # ETF 代码 → 金属映射
    _ETF_METAL: dict[str, str] = {"GLD": "gold", "SLV": "silver"}

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def calculate_etf_flow(
        self, metal: str, date: str
    ) -> dict[str, Any] | None:
        """
        计算 ETF 日度净流入（当日 shares - 前日 shares）。

        Args:
            metal: 金属类型。
            date: 日期。

        Returns:
            包含 symbol、flow、latest_shares、prev_shares 的字典。
        """
        symbol = "GLD" if metal == "gold" else "SLV"
        rows = self.db.query(
            """
            SELECT date, shares FROM etf_holdings_daily
            WHERE symbol = ? AND date <= ?
            ORDER BY date DESC LIMIT 2
            """,
            (symbol, date),
        )

        if len(rows) < 2:
            logger.warning("%s ETF 流入计算: 数据不足 (需要至少2天)", metal)
            return None

        latest = rows[0]["shares"]
        prev = rows[1]["shares"]
        flow = latest - prev

        result = {
            "symbol": symbol,
            "flow": flow,
            "latest_shares": latest,
            "prev_shares": prev,
        }
        logger.info(
            "%s ETF 流入: %.0f (最新: %.0f, 前日: %.0f)",
            symbol, flow, latest, prev,
        )
        return result

    def calculate_spec_crowding(
        self, metal: str, date: str
    ) -> float | None:
        """
        计算投机拥挤度: Managed Money 净头寸 / 注册库存。

        Args:
            metal: 金属类型。
            date: 日期。

        Returns:
            投机拥挤度值。
        """
        # CFTC 净头寸
        cftc = self.db.query(
            """
            SELECT net_position FROM cftc_positions_weekly
            WHERE metal = ? AND report_date <= ?
            ORDER BY report_date DESC LIMIT 1
            """,
            (metal, date),
        )
        if not cftc or cftc[0]["net_position"] is None:
            logger.warning("%s 投机拥挤度: 无 CFTC 数据", metal)
            return None

        # 注册库存
        inv = self.db.query(
            """
            SELECT inventory FROM inventory_daily
            WHERE metal = ? AND category = 'registered' AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            (metal, date),
        )
        if not inv or inv[0]["inventory"] == 0:
            logger.warning("%s 投机拥挤度: 无注册库存数据", metal)
            return None

        crowding = cftc[0]["net_position"] / inv[0]["inventory"]
        logger.info("%s 投机拥挤度: %.4f", metal, crowding)
        return crowding

    def compute_metrics(self, date: str | None = None) -> list[dict[str, Any]]:
        """
        计算所有资金指标并写入 computed_factors。

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

            # ETF 净流入
            etf_info = self.calculate_etf_flow(metal, date)
            if etf_info:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_etf_flow",
                    "value": round(etf_info["flow"], 2),
                    "description": f"{etf_info['symbol']} 日度净流入(份额变动)",
                })

            # 投机拥挤度
            crowding = self.calculate_spec_crowding(metal, date)
            if crowding is not None:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_spec_crowding",
                    "value": round(crowding, 6),
                    "description": "投机拥挤度(净头寸/注册库存)",
                })

        if records:
            self.db.insert_batch("computed_factors", records)
            logger.info("资金指标计算完成，写入 %d 条记录", len(records))
        return records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    calc = FundingCalculator(db)
    metrics = calc.compute_metrics()
    for m in metrics:
        print(f"  {m['metric_type']}: {m['value']} ({m['description']})")
