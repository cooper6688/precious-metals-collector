"""
价格指标计算器 - 计算基差、期限结构斜率、Contango/Backwardation 等。
"""
import logging
from datetime import datetime
from typing import Any

from collector.database import DatabaseManager

logger = logging.getLogger(__name__)


class PriceCalculator:
    """价格结构因子计算器。"""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def calculate_basis(self, metal: str, date: str) -> dict[str, Any] | None:
        """
        计算近月基差: basis = 期货收盘价 – 现货价。

        ⚠️ 强制同币种配对：
          - 国内基差: SHFE 期货 vs SGE 现货 (CNY)
          - 国际基差: CME 期货 vs LBMA 现货 (USD)
        优先返回国内基差；如果国内无数据则尝试国际基差。

        Args:
            metal: 金属类型 ('gold' / 'silver')。
            date: 日期 YYYY-MM-DD。

        Returns:
            包含 basis、basis_rel、futures_price、spot_price 的字典，或 None。
        """
        # 配对方案列表：(期货交易所, 现货市场, 币种标签)
        pairs = [
            ("SHFE", "SGE", "CNY"),
            ("CME", "LBMA", "USD"),
        ]

        for exchange, market, currency_tag in pairs:
            futures = self.db.query(
                """
                SELECT date, close_price FROM future_prices_daily
                WHERE metal = ? AND exchange = ? AND date <= ?
                ORDER BY date DESC LIMIT 1
                """,
                (metal, exchange, date),
            )
            if not futures:
                continue

            spot = self.db.query(
                """
                SELECT date, price FROM spot_prices_daily
                WHERE metal = ? AND market = ? AND date <= ?
                ORDER BY date DESC LIMIT 1
                """,
                (metal, market, date),
            )
            if not spot:
                continue

            futures_date_str = futures[0]["date"]
            spot_date_str = spot[0]["date"]

            # 强制日期对齐校验：跨度不得超过 3 天
            try:
                f_dt = datetime.strptime(futures_date_str[:10], "%Y-%m-%d")
                s_dt = datetime.strptime(spot_date_str[:10], "%Y-%m-%d")
                if abs((f_dt - s_dt).days) > 3:
                    logger.warning(
                        "%s 基差计算 (%s): 日期错配 (期货: %s, 现货: %s), 跳过",
                        metal, currency_tag, futures_date_str, spot_date_str
                    )
                    continue
            except Exception as e:
                logger.error("日期解析失败: %s", e)
                continue

            futures_price = futures[0]["close_price"]
            spot_price = spot[0]["price"]
            basis = futures_price - spot_price
            basis_rel = basis / spot_price if spot_price != 0 else 0

            result = {
                "basis": basis,
                "basis_rel": basis_rel,
                "futures_price": futures_price,
                "spot_price": spot_price,
                "currency": currency_tag,
            }
            logger.info(
                "%s 基差 (%s): %.2f (相对: %.4f%%), 期货: %.2f, 现货: %.2f",
                metal, currency_tag, basis, basis_rel * 100, futures_price, spot_price,
            )
            return result

        logger.warning("%s 基差计算: 无可配对的同币种期现数据", metal)
        return None


    def calculate_term_structure(
        self, metal: str, date: str
    ) -> dict[str, Any] | None:
        """
        从多个合约收盘价估计期限结构。

        - 如果只有一个合约，使用 basis 符号判断
        - 多合约时用线性回归斜率

        Args:
            metal: 金属类型。
            date: 日期。

        Returns:
            包含 slope、structure (Contango/Backwardation/Flat) 的字典。
        """
        contracts = self.db.query(
            """
            SELECT contract, close_price FROM future_prices_daily
            WHERE metal = ? AND date = ? AND close_price IS NOT NULL
            ORDER BY contract
            """,
            (metal, date),
        )

        if not contracts:
            # 尝试取最近的数据
            contracts = self.db.query(
                """
                SELECT contract, close_price FROM future_prices_daily
                WHERE metal = ? AND date <= ? AND close_price IS NOT NULL
                ORDER BY date DESC, contract
                LIMIT 5
                """,
                (metal, date),
            )

        if not contracts:
            logger.warning("%s 期限结构: 无合约数据", metal)
            return None

        prices = [c["close_price"] for c in contracts]

        # 简单判断：如果只有一个价格，用基差方向
        if len(prices) == 1:
            basis_info = self.calculate_basis(metal, date)
            if basis_info and basis_info["basis"] > 0:
                structure = "Contango"
                slope = basis_info["basis_rel"]
            elif basis_info and basis_info["basis"] < 0:
                structure = "Backwardation"
                slope = basis_info["basis_rel"]
            else:
                structure = "Flat"
                slope = 0.0
        else:
            # 多合约：用首尾价差除以间隔数
            slope = (prices[-1] - prices[0]) / max(len(prices) - 1, 1)
            if slope > 0:
                structure = "Contango"
            elif slope < 0:
                structure = "Backwardation"
            else:
                structure = "Flat"

        result = {
            "slope": slope,
            "structure": structure,
            "n_contracts": len(contracts),
        }
        logger.info(
            "%s 期限结构: %s (斜率: %.4f, %d 个合约)",
            metal, structure, slope, len(contracts),
        )
        return result

    def compute_metrics(self, date: str | None = None) -> list[dict[str, Any]]:
        """
        计算所有价格指标并写入 computed_factors。

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

            # 基差
            basis_info = self.calculate_basis(metal, date)
            if basis_info:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_basis",
                    "value": round(basis_info["basis"], 4),
                    "description": "近月基差",
                })
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_basis_rel",
                    "value": round(basis_info["basis_rel"] * 100, 4),
                    "description": "基差占现货比例%",
                })

            # 期限结构
            ts = self.calculate_term_structure(metal, date)
            if ts:
                records.append({
                    "date": date,
                    "metric_type": f"{prefix}_ts_slope",
                    "value": round(ts["slope"], 4),
                    "description": f"期限结构斜率 ({ts['structure']})",
                })

        if records:
            self.db.insert_batch("computed_factors", records)
            logger.info("价格指标计算完成，写入 %d 条记录", len(records))
        return records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    calc = PriceCalculator(db)
    metrics = calc.compute_metrics()
    for m in metrics:
        print(f"  {m['metric_type']}: {m['value']} ({m['description']})")
