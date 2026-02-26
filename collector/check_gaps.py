"""
数据空洞检测工具 - 检查 SQLite 数据库中自 2010 年以来的连续缺失工作日。
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# 确保项目根目录在 sys.path 中
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collector.database import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("check_gaps")


def check_gaps_for_series(db: DatabaseManager, table_name: str, filters: str, series_name: str, start_date: str = "2010-01-01"):
    """
    检查指定表和过滤条件下的数据空洞。
    """
    query = f"SELECT DISTINCT date FROM {table_name} WHERE date >= '{start_date}'"
    if filters:
        query += f" AND {filters}"
    
    rows = db.query(query)
    existing_dates = {row["date"] for row in rows}
    
    # 生成标准工作日（周一至周五）
    today = datetime.now().strftime("%Y-%m-%d")
    b_days = pd.bdate_range(start=start_date, end=today).strftime("%Y-%m-%d").tolist()
    
    missing_dates = [d for d in b_days if d not in existing_dates]
    
    if not missing_dates:
        logger.info("[✅ %s] 数据完整，无缺失工作日", series_name)
        return
        
    # 查找连续缺失的“空洞”
    gaps = []
    current_gap = []
    
    for d in missing_dates:
        if not current_gap:
            current_gap.append(d)
        else:
            # 检查是否是连续的工作日
            prev_d = pd.to_datetime(current_gap[-1])
            curr_d = pd.to_datetime(d)
            # 如果两个日期之间的工作日天数差为1，说明是连续的缺失
            if len(pd.bdate_range(prev_d, curr_d)) == 2:
                current_gap.append(d)
            else:
                gaps.append(current_gap)
                current_gap = [d]
                
    if current_gap:
        gaps.append(current_gap)
        
    # 过滤出连续缺失 >= 3 天的空洞
    major_gaps = [g for g in gaps if len(g) >= 3]
    
    if not major_gaps:
        logger.info("[✅ %s] 基本完整，只有零星 1-2 天的节假日停盘，无 >=3 天的连续空洞", series_name)
    else:
        logger.warning("[⚠️ %s] 发现 %d 处超过 3 天的连续空洞！", series_name, len(major_gaps))
        # 打印最大的 5 个空洞
        major_gaps.sort(key=len, reverse=True)
        for g in major_gaps[:5]:
            logger.warning("  - 空洞: %s 到 %s (共 %d 个工作日)", g[0], g[-1], len(g))
        if len(major_gaps) > 5:
            logger.warning("  ... （仅显示前 5 个最长空洞）")

def main():
    logger.info("=" * 60)
    logger.info("🔍 开始进行历史数据空洞探查 (Gap Analysis)...")
    logger.info("=" * 60)
    
    db = DatabaseManager()
    
    # 国内具有长假（春节、十一），可能会有 5-7 个工作日的空洞，属于正常现象。
    # 这里主要探查是否有非常长期的不明断层。
    
    check_gaps_for_series(db, "future_prices_daily", "exchange='SHFE' AND metal='gold'", "SHFE 黄金主力")
    check_gaps_for_series(db, "future_prices_daily", "exchange='SHFE' AND metal='silver'", "SHFE 白银主力")
    
    check_gaps_for_series(db, "future_prices_daily", "exchange='CME' AND metal='gold'", "CME 黄金连续")
    check_gaps_for_series(db, "future_prices_daily", "exchange='CME' AND metal='silver'", "CME 白银连续")
    
    check_gaps_for_series(db, "spot_prices_daily", "market='SGE' AND metal='gold'", "SGE 黄金现货")
    
    check_gaps_for_series(db, "etf_holdings_daily", "symbol='GLD'", "GLD ETF")
    check_gaps_for_series(db, "fx_rates_daily", "pair='USDCNH'", "USDCNH 汇率")
    
    logger.info("=" * 60)
    logger.info("🏁 空洞探查完成！注：中国市场的春节/国庆长假通常会导致 3-7 天的连续空洞，这是正常现象。")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
