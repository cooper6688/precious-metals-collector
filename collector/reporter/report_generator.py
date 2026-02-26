"""
报告生成器 - 从 computed_factors 和原始数据表生成 HTML 日报。
"""
import logging
import base64
from datetime import datetime
import pytz
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from collector.database import DatabaseManager
from collector.reporter.chart_generator import generate_price_chart, generate_inventory_chart

logger = logging.getLogger(__name__)

# HTML 模板所在目录
_TEMPLATE_DIR = Path(__file__).resolve().parent


class ReportGenerator:
    """HTML 日报生成器。"""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self.env = Environment(
            loader=FileSystemLoader(str(_TEMPLATE_DIR)),
            autoescape=True,
        )

    # --------------------------------------------------------
    # 数据获取
    # --------------------------------------------------------

    def get_daily_metrics(self, date: str) -> dict[str, dict[str, Any]]:
        """
        从 computed_factors 获取当日所有指标，组成以 metric_type 为 key 的字典。

        Args:
            date: 日期 YYYY-MM-DD。

        Returns:
            {metric_type: {value, description, date}} 字典。
        """
        rows = self.db.query(
            "SELECT * FROM computed_factors WHERE date = ?", (date,)
        )
        return {r["metric_type"]: r for r in rows}

    def get_fx_rate(self, date: str) -> float | None:
        """获取最近的 USDCNH 汇率。"""
        rows = self.db.query(
            """
            SELECT rate FROM fx_rates_daily
            WHERE pair = 'USDCNH' AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            (date,),
        )
        return rows[0]["rate"] if rows else None

    def get_inventory_snapshot(self, date: str) -> list[dict[str, Any]]:
        """
        获取当日库存快照。

        - COMEX: 只返回汇总行 (warehouse = '')
        - SHFE / LBMA: 返回全部
        """
        return self.db.query(
            """
            SELECT DISTINCT i.* FROM inventory_daily i
            INNER JOIN (
                SELECT exchange, metal, category, warehouse, MAX(date) AS max_date
                FROM inventory_daily WHERE date <= ?
                GROUP BY exchange, metal, category, warehouse
            ) latest ON i.exchange = latest.exchange
                AND i.metal = latest.metal
                AND i.category = latest.category
                AND i.warehouse = latest.warehouse
                AND i.date = latest.max_date
            WHERE (i.exchange != 'COMEX' OR i.warehouse = '')
            ORDER BY i.exchange, i.metal, i.category
            """,
            (date,),
        )

    def get_price_snapshot(self, date: str) -> list[dict[str, Any]]:
        """
        获取当日主要合约价格与现货价格，并附加 CNY 换算价格。
        仅返回 3 天内的新鲜数据，避免展示陈旧行情。
        """
        fx_rate = self.get_fx_rate(date)

        # ⚠️ 关键：加入 3 天新鲜度过滤，防止旧数据（如 yfinance 2024 缓存）
        futures = self.db.query(
            """
            SELECT DISTINCT f.* FROM future_prices_daily f
            INNER JOIN (
                SELECT exchange, metal, contract, MAX(date) AS max_date
                FROM future_prices_daily
                WHERE date <= ? AND date >= date(?, '-3 days')
                GROUP BY exchange, metal, contract
            ) latest ON f.exchange = latest.exchange
                AND f.metal = latest.metal
                AND f.contract = latest.contract
                AND f.date = latest.max_date
            ORDER BY f.exchange, f.metal
            """,
            (date, date),
        )
        spots = self.db.query(
            """
            SELECT DISTINCT s.* FROM spot_prices_daily s
            INNER JOIN (
                SELECT market, metal, MAX(date) AS max_date
                FROM spot_prices_daily
                WHERE date <= ?
                GROUP BY market, metal
            ) latest ON s.market = latest.market
                AND s.metal = latest.metal
                AND s.date = latest.max_date
            ORDER BY s.market, s.metal
            """,
            (date,),
        )

        # 统一转换为 CNY
        result: list[dict[str, Any]] = []
        for row in futures:
            r = dict(row)
            price = r.get("close_price", 0) or 0
            currency = r.get("currency", "USD")
            if currency == "USD" and fx_rate:
                r["price_cny"] = round(price * fx_rate, 2)
            elif currency == "CNY":
                r["price_cny"] = round(price, 2)
            else:
                r["price_cny"] = None
            result.append(r)

        for row in spots:
            r = dict(row)
            # ⚠️ spot 表用 'price' 字段，同时设置 'close_price' 别名以统一模板渲染
            price = r.get("price", 0) or 0
            r["close_price"] = price
            currency = r.get("currency", "USD")
            if currency == "USD" and fx_rate:
                r["price_cny"] = round(price * fx_rate, 2)
            elif currency == "CNY":
                r["price_cny"] = round(price, 2)
            else:
                r["price_cny"] = None
            result.append(r)

        return result

    # --------------------------------------------------------
    # 评分逻辑
    # --------------------------------------------------------

    def _calculate_overall_score(
        self, metrics: dict[str, dict[str, Any]]
    ) -> tuple[int, str]:
        """
        计算综合景气度评分 (0-100)。

        评分规则（初版，可后续优化）：
        - 基础分 50
        - DPI 低 → +15, 中 → +5, 高 → -10
        - 基差正 (Contango) → +5, 负 (Backwardation) → +10
        - ETF 净流入正 → +10, 负 → -5
        - 投机拥挤度极高 → -10

        Returns:
            (score, verdict) 元组。
        """
        score = 50

        # DPI 因子
        for metal in ("GOLD", "SILVER"):
            dpi = metrics.get(f"{metal}_dpi", {})
            desc = dpi.get("description", "")
            if "低" in desc:
                score += 8
            elif "中" in desc:
                score += 3
            elif "高" in desc:
                score -= 5

        # 基差因子
        for metal in ("GOLD", "SILVER"):
            ts = metrics.get(f"{metal}_ts_slope", {})
            desc = ts.get("description", "")
            if "Backwardation" in desc:
                score += 5  # 期现倒挂 → 供应紧张信号
            elif "Contango" in desc:
                score += 2

        # ETF 因子
        for metal in ("GOLD", "SILVER"):
            etf = metrics.get(f"{metal}_etf_flow", {})
            val = etf.get("value", 0)
            if val and val > 0:
                score += 5
            elif val and val < 0:
                score -= 3

        # 限制范围
        score = max(0, min(100, score))

        # 判定语
        if score >= 75:
            verdict = "强烈看多 🟢"
        elif score >= 60:
            verdict = "偏多 🔵"
        elif score >= 40:
            verdict = "中性 ⚪"
        elif score >= 25:
            verdict = "偏空 🟡"
        else:
            verdict = "强烈看空 🔴"

        return score, verdict

    # --------------------------------------------------------
    # 图表数据
    # --------------------------------------------------------

    def get_price_history(self, date: str, days: int = 30) -> list[dict[str, Any]]:
        """获取近期 SHFE 期货价格趋势（用于 ECharts 折线图）。"""
        return self.db.query(
            """
            SELECT date, metal, close_price
            FROM future_prices_daily
            WHERE exchange = 'SHFE' AND date <= ? AND date >= date(?, '-' || ? || ' days')
            ORDER BY metal, date
            """,
            (date, date, days),
        )

    def get_inventory_history(self, date: str, days: int = 30) -> list[dict[str, Any]]:
        """获取近期 COMEX 总库存趋势（用于 ECharts 柱状图）。"""
        return self.db.query(
            """
            SELECT date, metal, inventory
            FROM inventory_daily
            WHERE exchange = 'COMEX' AND category = 'total'
                  AND warehouse = '' AND date <= ?
                  AND date >= date(?, '-' || ? || ' days')
            ORDER BY metal, date
            """,
            (date, date, days),
        )

    # --------------------------------------------------------
    # HTML 生成
    # --------------------------------------------------------

    def generate_html(self, date: str | None = None) -> str:
        """
        生成完整 HTML 报告。

        Args:
            date: 报告日期，默认今天。

        Returns:
            渲染后的 HTML 字符串。
        """
        if date is None:
            tz_bj = pytz.timezone("Asia/Shanghai")
            date = datetime.now(tz_bj).strftime("%Y-%m-%d")

        metrics = self.get_daily_metrics(date)
        inventory = self.get_inventory_snapshot(date)
        prices = self.get_price_snapshot(date)
        overall_score, verdict = self._calculate_overall_score(metrics)

        # 图表（matplotlib → Base64 PNG）
        price_history = self.get_price_history(date)
        inventory_history = self.get_inventory_history(date)
        price_chart_b64 = generate_price_chart(price_history)
        inventory_chart_b64 = generate_inventory_chart(inventory_history)

        # 评分颜色
        if overall_score >= 60:
            score_color = "#27ae60"
        elif overall_score >= 40:
            score_color = "#f39c12"
        else:
            score_color = "#e74c3c"

        metal_dates = {"gold": date[:10], "silver": date[:10]}
        for item in inventory + prices:
            m = str(item.get("metal", "")).lower()
            item_date = str(item.get("date", ""))[:10]
            
            # 标记全局陈旧状态
            if item_date and item_date != date[:10]:
                has_stale_data = True
            
            # 统计各品种的“非今日”数据中最新的那一个作为后缀
            # 排除 LBMA 或 只有 7 位 (YYYY-MM) 的月份记录，避免其污染日度指标的时间戳
            if m in metal_dates and item_date and len(item_date) == 10:
                if item_date < date[:10]:
                    # 如果记录比当日旧，记录其中最接近今天的一个日期
                    if metal_dates[m] == date[:10] or item_date > metal_dates[m]:
                        metal_dates[m] = item_date
        
        # 为过时指标追加日期后缀 (仅在数据确实落后于当日报告日期时)
        for m_key, m_val in metrics.items():
            dt_str = ""
            m_type = "gold" if "GOLD" in m_key else "silver"
            if metal_dates[m_type] != date[:10]:
                # 取得月-日部分，例如 (02-25)
                dt_str = f" ({metal_dates[m_type][5:]})"
            m_val["date_suffix"] = dt_str

        template = self.env.get_template("html_template.html")
        fx_rate = self.get_fx_rate(date)
        html = template.render(
            target_date=date,
            overall_score=overall_score,
            score_verdict=verdict,
            score_color=score_color,
            metrics=metrics,
            inventory_snapshot=inventory,
            price_snapshot=prices,
            price_chart=price_chart_b64,
            inventory_chart=inventory_chart_b64,
            fx_rate=fx_rate,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            has_stale_data=has_stale_data
        )

        logger.info("HTML 报告生成完成 (日期: %s, 评分: %d)", date, overall_score)
        return html


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    gen = ReportGenerator(db)
    html = gen.generate_html()
    output_path = Path(__file__).resolve().parent.parent.parent / "data" / "report_test.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"✅ 测试报告已生成: {output_path}")
