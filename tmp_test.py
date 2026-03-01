import logging
from datetime import datetime
from collector.database import DatabaseManager
from collector.data_fetcher.inventory_fetcher import InventoryFetcher
from collector.data_fetcher.price_fetcher import PriceFetcher
import sys

# Configure logging to see debug messages
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

db = DatabaseManager()
inv_fetcher = InventoryFetcher(db)
price_fetcher = PriceFetcher(db)

print("--- Testing SGE PDF ---")
sge = inv_fetcher.fetch_sge_pdf("20260301")
print(f"SGE result length: {len(sge)}")
for r in sge:
    print(r)

print("\n--- Testing SHFE Inventory ---")
shfe = inv_fetcher.fetch_shfe("20260301")
print(f"SHFE result length: {len(shfe)}")
for r in shfe:
    print(r)

print("\n--- Testing LBMA Spot ---")
lbma = price_fetcher.fetch_lbma_spot("gold")
print(f"LBMA result length: {len(lbma)}")
for r in lbma:
    print(r)
