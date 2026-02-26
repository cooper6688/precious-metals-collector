"""快速验证脚本 - 测试数据库建表和基本模块导入。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# 1. 测试数据库
print("=" * 40)
print("1. 测试数据库建表...")
from collector.database import DatabaseManager

db = DatabaseManager()
tables = db.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
print(f"  ✅ 数据库已创建: {db.db_path}")
print(f"  ✅ 共 {len(tables)} 张表:")
for t in tables:
    print(f"    📋 {t['name']}")

# 2. 测试配置模块
print("\n2. 测试配置模块...")
from collector.settings import DB_PATH, MAIL_CONFIG, DATA_SOURCES
print(f"  ✅ DB_PATH: {DB_PATH}")
print(f"  ✅ 数据源数量: {len(DATA_SOURCES)}")
print(f"  ✅ 邮件配置: {MAIL_CONFIG['smtp_server']}")

# 3. 测试模块导入
print("\n3. 测试模块导入...")
try:
    from collector.data_fetcher.inventory_fetcher import InventoryFetcher
    print("  ✅ InventoryFetcher")
except Exception as e:
    print(f"  ❌ InventoryFetcher: {e}")

try:
    from collector.data_fetcher.price_fetcher import PriceFetcher
    print("  ✅ PriceFetcher")
except Exception as e:
    print(f"  ❌ PriceFetcher: {e}")

try:
    from collector.data_fetcher.etf_fetcher import ETFFetcher
    print("  ✅ ETFFetcher")
except Exception as e:
    print(f"  ❌ ETFFetcher: {e}")

try:
    from collector.data_fetcher.cftc_fetcher import CFTCFetcher
    print("  ✅ CFTCFetcher")
except Exception as e:
    print(f"  ❌ CFTCFetcher: {e}")

try:
    from collector.calculator.inventory_calculator import InventoryCalculator
    print("  ✅ InventoryCalculator")
except Exception as e:
    print(f"  ❌ InventoryCalculator: {e}")

try:
    from collector.calculator.price_calculator import PriceCalculator
    print("  ✅ PriceCalculator")
except Exception as e:
    print(f"  ❌ PriceCalculator: {e}")

try:
    from collector.calculator.funding_calculator import FundingCalculator
    print("  ✅ FundingCalculator")
except Exception as e:
    print(f"  ❌ FundingCalculator: {e}")

try:
    from collector.reporter.report_generator import ReportGenerator
    print("  ✅ ReportGenerator")
except Exception as e:
    print(f"  ❌ ReportGenerator: {e}")

try:
    from collector.mailer import EmailSender
    print("  ✅ EmailSender")
except Exception as e:
    print(f"  ❌ EmailSender: {e}")

# 4. 测试报告生成（使用空数据）
print("\n4. 测试报告生成（空数据）...")
try:
    gen = ReportGenerator(db)
    html = gen.generate_html()
    report_path = Path(__file__).parent / "data" / "test_report.html"
    report_path.write_text(html, encoding="utf-8")
    print(f"  ✅ 报告生成成功: {report_path}")
    print(f"  📏 HTML 长度: {len(html)} 字符")
except Exception as e:
    print(f"  ❌ 报告生成失败: {e}")

print("\n" + "=" * 40)
print("✅ 验证完成!")
