"""
贵金属日度自动化研究系统 - 全局配置

所有配置项集中管理，敏感信息优先从环境变量读取。
"""
import os
import shutil
import tempfile
from pathlib import Path


# ============================================================
# curl_cffi SSL 证书路径修复
# 当项目路径包含非 ASCII 字符（如中文）时，curl_cffi 无法加载
# certifi 的 cacert.pem，需要将证书文件复制到纯 ASCII 路径。
# 此修复必须在 import yfinance / akshare 之前生效。
# ============================================================
def _fix_curl_cffi_ssl() -> None:
    """将 cacert.pem 复制到纯 ASCII 临时路径，修复 curl_cffi SSL 错误。"""
    if os.environ.get("CURL_CA_BUNDLE"):
        return  # 用户已手动设置，不覆盖

    try:
        import certifi
        original = certifi.where()
        # 检查路径是否包含非 ASCII 字符
        try:
            original.encode("ascii")
            return  # 路径是纯 ASCII，无需修复
        except UnicodeEncodeError:
            pass

        dest = os.path.join(tempfile.gettempdir(), "cacert_pm.pem")
        # 仅在不存在或源文件更新时复制
        if not os.path.exists(dest) or os.path.getmtime(original) > os.path.getmtime(dest):
            shutil.copy2(original, dest)
        os.environ["CURL_CA_BUNDLE"] = dest
    except ImportError:
        pass  # certifi 未安装，跳过


_fix_curl_cffi_ssl()

# ============================================================
# 基础路径
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = str(PROJECT_ROOT / "data" / "precious_metals.db")
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = str(LOG_DIR / "collector.log")

# 确保目录存在
(PROJECT_ROOT / "data").mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 运行环境
# ============================================================
ENVIRONMENT = os.getenv("PM_ENV", "prod")  # dev / prod

# ============================================================
# 网络代理配置（用于访问 CME / LBMA 等境外数据源）
# ============================================================
PROXY_URL = os.getenv("PM_PROXY", "http://127.0.0.1:10808")
PROXIES: dict = {
    "http": PROXY_URL,
    "https": PROXY_URL,
}
# 是否启用代理（默认启用，因为 CME/LBMA 在国内受限）
USE_PROXY = os.getenv("PM_USE_PROXY", "1") == "1"

# 将代理配置同步到环境变量（curl_cffi / yfinance 通过环境变量读取代理）
if USE_PROXY:
    os.environ.setdefault("HTTP_PROXY", PROXY_URL)
    os.environ.setdefault("HTTPS_PROXY", PROXY_URL)


# ============================================================
# 邮件配置  (请在环境变量或此处填入真实值)
# ============================================================
MAIL_CONFIG: dict = {
    "smtp_server": os.getenv("PM_SMTP_SERVER", "smtp.163.com"),
    "smtp_port": int(os.getenv("PM_SMTP_PORT", "465")),
    "username": os.getenv("PM_SMTP_USER", "cooper666888"),
    "password": os.getenv("PM_SMTP_PASS", "WFpMJWy8MCMzaTXV"),  # QQ邮箱需APP密码
    "from_addr": os.getenv("PM_SMTP_FROM", "cooper666888@163.com"),
    "to_addr": os.getenv("PM_SMTP_TO", "491165233@qq.com").split(","),
    "subject": "贵金属市场每日报告 - {date}",
}

# ============================================================
# 数据源配置
# ============================================================
DATA_SOURCES: dict = {
    # --- 库存 ---
    "comex_inventory": {
        "description": "COMEX 注册/合格库存 (akshare)",
        "frequency": "daily",
        "enabled": True,
    },
    "shfe_warrant": {
        "url_pattern": (
            "https://www.shfe.com.cn/data/dailydata/"
            "kx/pm{date}.dat"
        ),
        "frequency": "daily",
        "enabled": True,
    },
    # --- 期货价格 ---
    "cme_futures": {
        "symbols": {"gold": "GC=F", "silver": "SI=F"},
        "source": "yfinance",
        "frequency": "daily",
        "enabled": True,
    },
    "shfe_futures": {
        "description": "上期所期货日行情 (akshare)",
        "frequency": "daily",
        "enabled": True,
    },
    # --- 现货价格 ---
    "spot_prices": {
        "description": "伦敦/上海现货 (akshare)",
        "frequency": "daily",
        "enabled": True,
    },
    # --- ETF ---
    "etf": {
        "symbols": ["GLD", "SLV"],
        "source": "yfinance",
        "frequency": "daily",
        "enabled": True,
    },
    # --- CFTC ---
    "cftc": {
        "description": "CFTC COT 周度报告 (pycot-reports)",
        "frequency": "weekly",
        "enabled": True,
    },
}

# ============================================================
# 计算参数
# ============================================================
CALC_CONFIG: dict = {
    "dpi_threshold": {"high": 1.2, "medium": 0.8, "low": 0.5},
    "inventory_change_days": 5,      # 库存变动计算天数
    "spec_crowding_lookback": 52,    # 投机拥挤度回溯周数
}

# ============================================================
# 日志配置
# ============================================================
LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "file": {
            "class": "logging.FileHandler",
            "filename": LOG_FILE,
            "formatter": "standard",
            "encoding": "utf-8",
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["file", "console"],
    },
}
