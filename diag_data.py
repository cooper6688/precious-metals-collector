import logging
import json
from collector.database import DatabaseManager
from scrapling import Fetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_db():
    db = DatabaseManager()
    logger.info("检查 SHFE 数据...")
    res = db.query("SELECT date, exchange, metal, inventory FROM inventory_daily WHERE exchange='SHFE' ORDER BY date DESC LIMIT 5")
    logger.info(f"SHFE: {res}")
    
    logger.info("检查 SGE 数据...")
    res = db.query("SELECT date, exchange, metal, inventory FROM inventory_daily WHERE exchange='SGE' ORDER BY date DESC LIMIT 5")
    logger.info(f"SGE: {res}")

def check_network():
    logger.info("检查 Scrapling SSL 问题...")
    fetcher = Fetcher()
    # 尝试不带 verify=False
    try:
        resp = fetcher.get("https://www.shfe.com.cn/", timeout=5)
        logger.info(f"Direct connection: {resp.status}")
    except Exception as e:
        logger.error(f"Direct connection failed: {e}")
        
    # 尝试带 verify=False (如果支持)
    try:
        # FetcherSession 支持 verify，但 Fetcher.get 传给 curl_cffi.requests.Session().request()
        resp = fetcher.get("https://www.shfe.com.cn/", timeout=5, verify=False)
        logger.info(f"Insecure connection: {resp.status}")
    except Exception as e:
        logger.error(f"Insecure connection failed: {e}")

if __name__ == "__main__":
    check_db()
    check_network()
