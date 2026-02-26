"""
获取全面的历史数据 - 跑取历史全量数据
用法：
    python collector/run_backfill.py
"""
import logging
import sys
import time
from pathlib import Path

# 确保项目根目录在 sys.path 中
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 增大环境参数或临时开启代理（如有需要可自行取消注释）
# import os
# os.environ["PM_SKIP_YFINANCE"] = "0"
# os.environ["PM_SKIP_CFTC"] = "0"

from collector.database import DatabaseManager
from collector.data_fetcher.price_fetcher import PriceFetcher
from collector.data_fetcher.etf_fetcher import ETFFetcher
from collector.data_fetcher.cftc_fetcher import CFTCFetcher
from collector.data_fetcher.fx_fetcher import FXFetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backfill")

def main():
    logger.info("=" * 60)
    logger.info("🚀 开始历史数据全面回填...")
    logger.info("=" * 60)
    
    db = DatabaseManager()
    
    # 1. Price Fetcher
    logger.info("📥 1. 开始回填价格数据 (SGE / SHFE 历史，CME 10年)...")
    price_fetcher = PriceFetcher(db)
    
    all_prices = []
    for metal in ("gold", "silver"):
        logger.info("  抓取 SGE %s", metal)
        all_prices.extend(price_fetcher.fetch_sge_spot(metal, full_history=True))
        
        logger.info("  抓取 SHFE %s", metal)
        all_prices.extend(price_fetcher.fetch_shfe_futures(metal, full_history=True))
        
        logger.info("  抓取 CME %s", metal)
        all_prices.extend(price_fetcher.fetch_cme_futures(metal, days=3650))
        
    if all_prices:
        spot_records = [r for r in all_prices if r["source"] == "akshare_sge"]
        future_records = [r for r in all_prices if r["source"] != "akshare_sge"]
        if spot_records:
            cnt = db.insert_batch("spot_prices_daily", spot_records)
            logger.info("✅ 现货价格写入: %d 条 (SGE)", cnt)
        if future_records:
            cnt = db.insert_batch("future_prices_daily", future_records)
            logger.info("✅ 期货价格写入: %d 条 (SHFE & CME)", cnt)

    # 2. FX Fetcher
    logger.info("📥 2. 开始回填美元/离岸人民币 (USDCNH 10年)...")
    fx_fetcher = FXFetcher(db)
    fx_records = fx_fetcher.fetch_usdcnh_history(period="10y")
    if fx_records:
        cnt = db.insert_batch("fx_rates_daily", fx_records)
        logger.info("✅ 汇率数据写入: %d 条", cnt)
        
    # 3. ETF Fetcher
    # ETF 在 fetch_etf 里面也有个 timeout=15 的限制，一般来说 10 年数据够快
    logger.info("📥 3. 开始回填 ETF 历史记录 (GLD / SLV 10年)...")
    etf_fetcher = ETFFetcher(db)
    etf_records = []
    for symbol in ETFFetcher._ETF_MAP:
        logger.info("  抓取 %s", symbol)
        etf_records.extend(etf_fetcher.fetch_etf(symbol, days=3650))
    if etf_records:
        cnt = db.insert_batch("etf_holdings_daily", etf_records)
        logger.info("✅ ETF 数据写入: %d 条", cnt)
        
    # 4. CFTC Fetcher
    logger.info("📥 4. 开始回填 CFTC 历史 (持仓周报, 从 2010 年开始)...")
    cftc_fetcher = CFTCFetcher(db)
    cnt = cftc_fetcher.backfill_history(start_year=2010)
    logger.info("✅ CFTC 数据总计写入: %d 条", cnt)
    
    # 5. Inventory Limitations Logging
    logger.info("=" * 60)
    logger.info("⚠️ 注意: 库存历史数据 (Inventory) 未纳入全量回填")
    logger.info("   原因: CME 和 SHFE 官方免权 API 均不提供长周期每日库存历史全量包。")
    logger.info("   影响: 在早期历史回测中，涉及交割压力 (DPI) 等依赖库存的指标若缺乏数据将被安全静默跳过。")
    logger.info("=" * 60)

    logger.info("=" * 60)
    logger.info("🏁 历史数据回填全部完成！")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
