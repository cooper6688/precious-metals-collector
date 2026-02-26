"""
贵金属日度自动化研究系统 - 每日调度主入口

完整日程：
1. 初始化日志 & 数据库
2. 抓取数据（库存、价格、ETF、CFTC）
3. 计算指标（库存、价格、资金）
4. 生成 HTML 报告
5. 发邮件

用法：
    python collector/run_daily.py              # 正常运行
    python collector/run_daily.py --dry-run    # 跳过邮件发送
    python collector/run_daily.py --date 2026-02-01  # 指定日期
"""
import argparse
import logging
import logging.config
import sys
import time
from datetime import datetime
from pathlib import Path

# 确保项目根目录在 sys.path 中
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collector.settings import LOGGING_CONFIG, ENVIRONMENT
from collector.database import DatabaseManager
from collector.data_fetcher.inventory_fetcher import InventoryFetcher
from collector.data_fetcher.price_fetcher import PriceFetcher
from collector.data_fetcher.etf_fetcher import ETFFetcher
from collector.data_fetcher.cftc_fetcher import CFTCFetcher
from collector.data_fetcher.fx_fetcher import FXFetcher
from collector.calculator.inventory_calculator import InventoryCalculator
from collector.calculator.price_calculator import PriceCalculator
from collector.calculator.funding_calculator import FundingCalculator
from collector.reporter.report_generator import ReportGenerator
from collector.mailer import EmailSender

logger = logging.getLogger("collector.daily")


def run_daily_pipeline(
    target_date: str | None = None,
    dry_run: bool = False,
) -> bool:
    """
    执行完整的每日数据采集→计算→报告→发邮件流程。

    Args:
        target_date: 目标日期 YYYY-MM-DD，默认今天。
        dry_run: 如果为 True，跳过邮件发送。

    Returns:
        流程是否成功完成。
    """
    start_time = time.time()
    today = target_date or datetime.now().strftime("%Y-%m-%d")
    logger.info("=" * 60)
    logger.info("🚀 贵金属日报流程启动 | 日期: %s | 环境: %s", today, ENVIRONMENT)
    logger.info("=" * 60)

    # --------------------------------------------------
    # 1. 初始化
    # --------------------------------------------------
    db = DatabaseManager()
    logger.info("✅ 数据库初始化完成")

    # --------------------------------------------------
    # 2. 数据抓取
    # --------------------------------------------------
    logger.info("📥 [阶段2] 数据抓取开始...")

    # --------------------------------------------------
    # 2. 数据抓取
    # --------------------------------------------------
    logger.info("📥 [阶段2] 数据抓取开始...")

    # 2.1 库存（基础链路）
    try:
        inv_fetcher = InventoryFetcher(db)
        inv_count = inv_fetcher.update_daily()
        logger.info("  ✅ 库存数据: %d 条记录", inv_count)
    except Exception:
        logger.exception("  ❌ 库存抓取失败")

    # 2.2 CFTC（周度，可跳过）
    import os
    if os.getenv("PM_SKIP_CFTC", "0") == "1":
        logger.info("  ⏭️ CFTC: 环境变量 PM_SKIP_CFTC=1，已跳过")
    else:
        try:
            cftc_fetcher = CFTCFetcher(db)
            cftc_count = cftc_fetcher.update_if_report_day()
            if cftc_count > 0:
                logger.info("  ✅ CFTC数据: %d 条记录", cftc_count)
            else:
                logger.info("  ⏭️ CFTC: 非报告日，已跳过")
        except Exception as e:
            # 🚨 软失败兜底：如果是 CFTC 官网挂死或 Timeout，仅记录错误，不阻断流程
            logger.warning("  ⚠️ CFTC 抓取失败，本期将使用上周缓存数据兜底: %s", e)

    # 2.3 汇率
    try:
        fx_fetcher = FXFetcher(db)
        fx_count = fx_fetcher.update_daily()
        logger.info("  ✅ 汇率数据: %d 条记录", fx_count)
    except Exception:
        logger.exception("  ❌ 汇率抓取失败")

    # 2.4 价格（含 CME 期货 + 现货）
    try:
        price_fetcher = PriceFetcher(db)
        price_count = price_fetcher.update_daily()
        logger.info("  ✅ 价格数据: %d 条记录", price_count)
    except Exception:
        logger.exception("  ❌ 价格抓取失败")

    # 2.5 ETF
    try:
        etf_fetcher = ETFFetcher(db)
        etf_count = etf_fetcher.update_daily()
        logger.info("  ✅ ETF数据: %d 条记录", etf_count)
    except Exception:
        logger.exception("  ❌ ETF 抓取失败")

    # --------------------------------------------------
    # 3. 指标计算
    # --------------------------------------------------
    logger.info("🧮 [阶段3] 指标计算开始...")

    calculators = [
        ("库存指标", InventoryCalculator(db)),
        ("价格指标", PriceCalculator(db)),
        ("资金指标", FundingCalculator(db)),
    ]

    for name, calc in calculators:
        try:
            metrics = calc.compute_metrics(today)
            logger.info("  ✅ %s: %d 个指标", name, len(metrics))
        except Exception:
            logger.exception("  ❌ %s 计算失败", name)

    # --------------------------------------------------
    # 4. 生成报告
    # --------------------------------------------------
    logger.info("📊 [阶段4] 报告生成...")
    try:
        generator = ReportGenerator(db)
        html_report = generator.generate_html(today)

        # 同时保存一份本地副本
        report_path = PROJECT_ROOT / "data" / f"report_{today}.html"
        report_path.write_text(html_report, encoding="utf-8")
        logger.info("  ✅ HTML 报告已保存: %s", report_path)
    except Exception:
        logger.exception("  ❌ 报告生成失败")
        html_report = None

    # --------------------------------------------------
    # 5. 轻量化数据断层检查 (Gap Check)
    # --------------------------------------------------
    logger.info("🔍 [阶段5] 数据新鲜度查验...")
    missing_items = []
    
    # 查验今日是否成功获取价格数据
    cme_gold = db.query("SELECT 1 FROM future_prices_daily WHERE exchange='CME' AND metal='gold' AND date=?", (today,))
    if not cme_gold: missing_items.append("CME 黄金期货")
        
    sge_gold = db.query("SELECT 1 FROM spot_prices_daily WHERE market='SGE' AND metal='gold' AND date=?", (today,))
    if not sge_gold: missing_items.append("SGE 黄金现货")
        
    if missing_items:
        warning_msg = f"今日 ({today}) 存在未获取到的关键数据: {', '.join(missing_items)}。可能是未收盘或接口延迟。"
        logger.warning("  ⚠️ %s", warning_msg)
        # 可以选择把这行警告直接附加到邮件标题或正文中
    else:
        logger.info("  ✅ 核心价格数据今日均已齐备")

    # --------------------------------------------------
    # 6. 发送邮件
    # --------------------------------------------------
    if dry_run:
        logger.info("📧 [阶段6] Dry-run 模式，跳过邮件发送")
    elif html_report:
        logger.info("📧 [阶段6] 发送邮件...")
        try:
            sender = EmailSender()
            success = sender.send_email(html_report, today)
            if success:
                logger.info("  ✅ 邮件发送成功")
            else:
                logger.warning("  ⚠️ 邮件发送失败（请检查 SMTP 配置）")
        except Exception:
            logger.exception("  ❌ 邮件发送异常")
    else:
        logger.warning("📧 [阶段6] 无报告内容，跳过邮件发送")

    # --------------------------------------------------
    # 完成
    # --------------------------------------------------
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("🏁 流程完成 | 耗时: %.1f 秒", elapsed)
    logger.info("=" * 60)
    return True


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="贵金属日度自动化研究系统 - 每日调度",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="跳过邮件发送（用于测试）",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="指定报告日期 (YYYY-MM-DD)，默认今天",
    )
    args = parser.parse_args()

    # 配置日志
    logging.config.dictConfig(LOGGING_CONFIG)

    try:
        run_daily_pipeline(
            target_date=args.date,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        logger.info("⛔ 用户中断")
        sys.exit(1)
    except Exception:
        logger.exception("💥 未捕获的异常")
        sys.exit(1)


if __name__ == "__main__":
    main()
