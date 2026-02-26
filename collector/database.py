"""
贵金属日度自动化研究系统 - 数据库管理模块

负责 SQLite 数据库的初始化（自动建表）、批量插入与查询。
所有表均设置 UNIQUE 约束，使用 INSERT OR REPLACE 避免重复。
"""
import logging
import sqlite3
from pathlib import Path
from typing import Any

from collector.settings import DB_PATH

logger = logging.getLogger(__name__)

# ============================================================
# 建表 SQL（7 张表）
# ============================================================

_CREATE_TABLES_SQL: list[str] = [
    # 1. 各交易所库存数据（日度）
    """
    CREATE TABLE IF NOT EXISTS inventory_daily (
        date        TEXT    NOT NULL,
        exchange    TEXT    NOT NULL,
        metal       TEXT    NOT NULL,
        category    TEXT    CHECK(category IN ('registered','eligible','pledged','total','warehouse','vault_total')),
        warehouse   TEXT    DEFAULT '',
        inventory   REAL    NOT NULL,
        unit        TEXT,
        source      TEXT,
        UNIQUE(date, exchange, metal, category, warehouse)
    )
    """,
    # 2. 期货价格数据
    """
    CREATE TABLE IF NOT EXISTS future_prices_daily (
        date            TEXT    NOT NULL,
        exchange        TEXT    NOT NULL,
        metal           TEXT    NOT NULL,
        contract        TEXT    NOT NULL,
        close_price     REAL,
        open_interest   REAL,
        volume          REAL,
        currency        TEXT,
        source          TEXT,
        UNIQUE(date, exchange, metal, contract)
    )
    """,
    # 3. 现货价格数据
    """
    CREATE TABLE IF NOT EXISTS spot_prices_daily (
        date        TEXT    NOT NULL,
        market      TEXT    NOT NULL,
        metal       TEXT    NOT NULL,
        price       REAL    NOT NULL,
        currency    TEXT,
        source      TEXT,
        UNIQUE(date, market, metal)
    )
    """,
    # 4. ETF 持仓数据
    """
    CREATE TABLE IF NOT EXISTS etf_holdings_daily (
        date    TEXT    NOT NULL,
        symbol  TEXT    NOT NULL,
        metal   TEXT    NOT NULL,
        shares  REAL,
        ounces  REAL,
        change  REAL,
        source  TEXT,
        UNIQUE(date, symbol, metal)
    )
    """,
    # 5. CFTC 持仓数据（周度）
    """
    CREATE TABLE IF NOT EXISTS cftc_positions_weekly (
        report_date             TEXT    NOT NULL,
        market                  TEXT    NOT NULL,
        metal                   TEXT    NOT NULL,
        non_commercial_long     REAL,
        non_commercial_short    REAL,
        commercial_long         REAL,
        commercial_short        REAL,
        net_position            REAL,
        source                  TEXT,
        UNIQUE(report_date, market, metal)
    )
    """,
    # 6. 供需数据（年度）
    """
    CREATE TABLE IF NOT EXISTS supply_demand_annual (
        year        INTEGER NOT NULL,
        metal       TEXT    NOT NULL,
        category    TEXT    NOT NULL,
        value       REAL    NOT NULL,
        unit        TEXT,
        source      TEXT,
        UNIQUE(year, metal, category)
    )
    """,
    # 7. 计算指标结果
    """
    CREATE TABLE IF NOT EXISTS computed_factors (
        date            TEXT    NOT NULL,
        metric_type     TEXT    NOT NULL,
        value           REAL    NOT NULL,
        description     TEXT,
        UNIQUE(date, metric_type)
    )
    """,
    # 8. 汇率数据（日度）
    """
    CREATE TABLE IF NOT EXISTS fx_rates_daily (
        date    TEXT    NOT NULL,
        pair    TEXT    NOT NULL,
        rate    REAL    NOT NULL,
        source  TEXT,
        UNIQUE(date, pair)
    )
    """,
]


class DatabaseManager:
    """SQLite 数据库管理器。"""

    def __init__(self, db_path: str | None = None) -> None:
        """
        初始化数据库连接并自动建表。

        Args:
            db_path: 数据库文件路径，默认使用 settings 中的配置。
        """
        self.db_path = db_path or DB_PATH
        # 确保目录存在
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()
        logger.info("数据库初始化完成: %s", self.db_path)

    # --------------------------------------------------------
    # 内部方法
    # --------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接。"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self) -> None:
        """执行建表 SQL。"""
        conn = self._get_conn()
        try:
            for sql in _CREATE_TABLES_SQL:
                conn.execute(sql)
            conn.commit()
        finally:
            conn.close()

    # --------------------------------------------------------
    # 公共接口
    # --------------------------------------------------------

    def insert_batch(self, table: str, records: list[dict[str, Any]]) -> int:
        """
        批量插入/更新记录（INSERT OR REPLACE）。

        Args:
            table: 目标表名。
            records: 字典列表，每个字典对应一条记录。

        Returns:
            成功插入的记录数。
        """
        if not records:
            return 0

        columns = list(records[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({placeholders})"

        conn = self._get_conn()
        count = 0
        try:
            for rec in records:
                values = tuple(rec[c] for c in columns)
                conn.execute(sql, values)
                count += 1
            conn.commit()
            logger.info("表 %s 插入/更新 %d 条记录", table, count)
        except Exception:
            conn.rollback()
            logger.exception("表 %s 批量插入失败", table)
            raise
        finally:
            conn.close()
        return count

    def query(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        """
        执行任意 SQL 查询并返回结果。

        Args:
            sql: SQL 语句。
            params: 参数元组。

        Returns:
            字典列表形式的查询结果。
        """
        conn = self._get_conn()
        try:
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_latest(
        self,
        table: str,
        metal: str,
        exchange: str | None = None,
    ) -> dict[str, Any] | None:
        """
        获取某张表中指定金属的最新一条记录。

        Args:
            table: 表名。
            metal: 金属类型 (gold / silver)。
            exchange: 交易所 (可选)。

        Returns:
            最新记录字典，无数据时返回 None。
        """
        conditions = ["metal = ?"]
        params: list[Any] = [metal]
        if exchange:
            conditions.append("exchange = ?")
            params.append(exchange)

        where = " AND ".join(conditions)
        sql = f"SELECT * FROM {table} WHERE {where} ORDER BY date DESC LIMIT 1"

        result = self.query(sql, tuple(params))
        return result[0] if result else None


# ============================================================
# 快速验证
# ============================================================
if __name__ == "__main__":
    db = DatabaseManager()
    print("✅ 数据库建表成功，路径:", db.db_path)
    # 列出所有表
    tables = db.query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    for t in tables:
        print(f"  📋 {t['name']}")
