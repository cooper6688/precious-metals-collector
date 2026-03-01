
import logging
import sys
import os

# 确保可以导入项目模块
sys.path.append(os.getcwd())

from collector.database import DatabaseManager
from collector.data_fetcher.price_fetcher import PriceFetcher
from collector.data_fetcher.inventory_fetcher import InventoryFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_price_fetcher_scrapling():
    logger.info("开始测试 PriceFetcher Scrapling 集成...")
    db = DatabaseManager()
    fetcher = PriceFetcher(db)
    
    # 1. 测试 SGE Scrapling Fallback
    logger.info("测试 SGE Gold Scrapling Fallback...")
    sge_gold = fetcher._fetch_sge_spot_scrapling("gold")
    print(f"SGE Gold: {sge_gold}")
    
    # 2. 测试 LBMA Scrapling
    logger.info("测试 LBMA Scrapling (Cloudflare)...")
    lbma_data = fetcher._fetch_lbma_spot_scrapling()
    print(f"LBMA Data: {lbma_data}")

def test_inventory_fetcher_scrapling():
    logger.info("开始测试 InventoryFetcher Scrapling 集成...")
    db = DatabaseManager()
    fetcher = InventoryFetcher(db)
    
    # 1. 测试 SGE PDF (via StealthyFetcher)
    logger.info("测试 SGE PDF Fetching...")
    sge_pdf = fetcher.fetch_sge_pdf()
    print(f"SGE PDF Records: {len(sge_pdf)}")
    
    # 2. 测试 SHFE JSON (via StealthyFetcher)
    logger.info("测试 SHFE JSON Fetching...")
    shfe_data = fetcher.fetch_shfe()
    print(f"SHFE Data Records: {len(shfe_data)}")

if __name__ == "__main__":
    try:
        test_price_fetcher_scrapling()
        test_inventory_fetcher_scrapling()
        logger.info("✅ 集成测试脚本执行完毕")
    except Exception as e:
        logger.exception("❌ 集成测试过程中出现异常")
