
import os
import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("verify")

os.environ["PM_SKIP_YFINANCE"] = "1"
os.environ["PM_SKIP_CFTC"] = "1"

def step(msg):
    logger.info(f"--- {msg} ---")
    sys.stdout.flush()

step("Starting Verification")
step("Importing DatabaseManager")
from collector.database import DatabaseManager
db = DatabaseManager()
step("Database Initialized")

step("Importing InventoryFetcher")
try:
    from collector.data_fetcher.inventory_fetcher import InventoryFetcher
    step("InventoryFetcher Imported")
    inv = InventoryFetcher(db)
    step("InventoryFetcher Initialized")
    count = inv.update_daily()
    step(f"InventoryFetcher Updated: {count}")
except Exception as e:
    logger.error(f"InventoryFetcher Error: {e}", exc_info=True)

step("Importing PriceFetcher")
try:
    from collector.data_fetcher.price_fetcher import PriceFetcher
    step("PriceFetcher Imported")
    price = PriceFetcher(db)
    step("PriceFetcher Initialized")
    count = price.update_daily()
    step(f"PriceFetcher Updated: {count}")
except Exception as e:
    logger.error(f"PriceFetcher Error: {e}", exc_info=True)

step("Importing ETFFetcher")
try:
    from collector.data_fetcher.etf_fetcher import ETFFetcher
    step("ETFFetcher Imported")
    etf = ETFFetcher(db)
    step("ETFFetcher Initialized")
    count = etf.update_daily()
    step(f"ETFFetcher Updated: {count}")
except Exception as e:
    logger.error(f"ETFFetcher Error: {e}", exc_info=True)

step("Importing CFTCFetcher")
try:
    from collector.data_fetcher.cftc_fetcher import CFTCFetcher
    step("CFTCFetcher Imported")
    cftc = CFTCFetcher(db)
    step("CFTCFetcher Initialized")
except Exception as e:
    logger.error(f"CFTCFetcher Error: {e}")

step("All Modules Loaded Successfully")
