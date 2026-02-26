"""
图表生成器 - 使用 matplotlib 生成 Base64 编码的 PNG 图片。

用于嵌入 HTML 邮件正文，确保在所有邮件客户端中正常显示。
"""
import base64
import io
import logging
from typing import Any

import matplotlib
matplotlib.use("Agg")  # 非交互式后端
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

logger = logging.getLogger(__name__)

# 统一中文字体配置
plt.rcParams.update({
    "font.sans-serif": ["Microsoft YaHei", "SimHei", "PingFang SC", "sans-serif"],
    "axes.unicode_minus": False,
    "figure.dpi": 300,
    "figure.facecolor": "white",
    "axes.facecolor": "#fafafa",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
})

# 配色方案
COLOR_GOLD = "#f39c12"
COLOR_SILVER = "#95a5a6"


def _fig_to_base64(fig: plt.Figure) -> str:
    """将 matplotlib Figure 转换为 Base64 编码的 PNG data URI。"""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def generate_price_chart(price_history: list[dict[str, Any]]) -> str | None:
    """
    生成近期期货价格走势折线图。

    Args:
        price_history: 包含 date, metal, close_price 的记录列表。

    Returns:
        Base64 data URI 字符串，或 None（数据不足时）。
    """
    if not price_history:
        return None

    gold_data: list[tuple[str, float]] = []
    silver_data: list[tuple[str, float]] = []

    for row in price_history:
        d = row["date"] if isinstance(row, dict) else row.get("date", "")
        metal = row["metal"] if isinstance(row, dict) else row.get("metal", "")
        price = float(row["close_price"] if isinstance(row, dict) else row.get("close_price", 0))
        if metal == "gold":
            gold_data.append((d, price))
        elif metal == "silver":
            silver_data.append((d, price))

    if not gold_data and not silver_data:
        return None

    fig, ax1 = plt.subplots(figsize=(7.5, 3))

    # 黄金（左轴）
    if gold_data:
        dates_g, prices_g = zip(*gold_data)
        ax1.plot(dates_g, prices_g, color=COLOR_GOLD, linewidth=2,
                 marker="o", markersize=4, label="黄金 (CNY/克)")
        ax1.set_ylabel("黄金 (CNY/克)", color=COLOR_GOLD, fontsize=10)
        ax1.tick_params(axis="y", labelcolor=COLOR_GOLD, labelsize=9)

    # 白银（右轴）
    if silver_data:
        ax2 = ax1.twinx()
        dates_s, prices_s = zip(*silver_data)
        ax2.plot(dates_s, prices_s, color=COLOR_SILVER, linewidth=2,
                 marker="s", markersize=4, label="白银 (CNY/千克)")
        ax2.set_ylabel("白银 (CNY/千克)", color=COLOR_SILVER, fontsize=10)
        ax2.tick_params(axis="y", labelcolor=COLOR_SILVER, labelsize=9)

    ax1.set_title("近期期货价格走势", fontsize=13, fontweight="bold", pad=10)
    ax1.tick_params(axis="x", rotation=30, labelsize=8)
    ax1.xaxis.set_major_locator(MaxNLocator(nbins=6))

    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    if silver_data:
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2,
                   loc="upper left", fontsize=9, framealpha=0.8)
    else:
        ax1.legend(loc="upper left", fontsize=9, framealpha=0.8)

    fig.tight_layout()
    return _fig_to_base64(fig)


def generate_inventory_chart(inventory_history: list[dict[str, Any]]) -> str | None:
    """
    生成 COMEX 库存趋势柱状图。

    Args:
        inventory_history: 包含 date, metal, inventory 的记录列表。

    Returns:
        Base64 data URI 字符串，或 None（数据不足时）。
    """
    if not inventory_history:
        return None

    gold_data: list[tuple[str, float]] = []
    silver_data: list[tuple[str, float]] = []

    for row in inventory_history:
        d = row["date"] if isinstance(row, dict) else row.get("date", "")
        metal = row["metal"] if isinstance(row, dict) else row.get("metal", "")
        inv = float(row["inventory"] if isinstance(row, dict) else row.get("inventory", 0))
        if metal == "gold":
            gold_data.append((d, inv))
        elif metal == "silver":
            silver_data.append((d, inv))

    if not gold_data and not silver_data:
        return None

    fig, ax1 = plt.subplots(figsize=(7.5, 3))
    bar_width = 0.35

    # 黄金（左轴）
    if gold_data:
        dates_g, inv_g = zip(*gold_data)
        x_pos = range(len(dates_g))
        bars1 = ax1.bar([x - bar_width / 2 for x in x_pos], inv_g,
                        bar_width, color=COLOR_GOLD, alpha=0.85, label="黄金 (吨)")
        ax1.set_ylabel("黄金 (吨)", color=COLOR_GOLD, fontsize=10)
        ax1.tick_params(axis="y", labelcolor=COLOR_GOLD, labelsize=9)
        ax1.set_xticks(list(x_pos))
        ax1.set_xticklabels(dates_g, rotation=30, fontsize=8)

    # 白银（右轴）
    if silver_data:
        ax2 = ax1.twinx()
        dates_s, inv_s = zip(*silver_data)
        x_pos_s = range(len(dates_s))
        bars2 = ax2.bar([x + bar_width / 2 for x in x_pos_s], inv_s,
                        bar_width, color=COLOR_SILVER, alpha=0.85, label="白银 (吨)")
        ax2.set_ylabel("白银 (吨)", color=COLOR_SILVER, fontsize=10)
        ax2.tick_params(axis="y", labelcolor=COLOR_SILVER, labelsize=9)

    ax1.set_title("COMEX 库存趋势 (吨)", fontsize=13, fontweight="bold", pad=10)

    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    if silver_data:
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2,
                   loc="upper left", fontsize=9, framealpha=0.8)
    else:
        ax1.legend(loc="upper left", fontsize=9, framealpha=0.8)

    fig.tight_layout()
    return _fig_to_base64(fig)
