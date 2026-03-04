"""
收益、现金流等模块共用的数据工具 - 统一使用数据库最新数据日
"""
import os
from datetime import date, datetime
from decimal import Decimal


def get_cache_dir():
    """
    返回缓存根目录。Vercel/AWS Lambda 等 serverless 仅 /tmp 可写，使用 /tmp/rt_risk_cache；
    本地使用 config/cache。
    """
    if os.getenv("VERCEL") or os.getenv("AWS_LAMBDA_FUNCTION_NAME") or os.getenv("LAMBDA_TASK_ROOT"):
        return os.path.join("/tmp", "rt_risk_cache")
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "config", "cache")

# 全量刷新时缓存最新数据日，避免 kn_revenue/kn_cashflow 等重复查询
_refresh_latest_date = None


def set_refresh_latest_date(dt):
    """全量刷新开始时调用，缓存最新数据日供后续复用"""
    global _refresh_latest_date
    _refresh_latest_date = dt


def clear_refresh_latest_date():
    """全量刷新结束时调用，清除缓存"""
    global _refresh_latest_date
    _refresh_latest_date = None


def serialize_for_json(val):
    """将 datetime/date/Decimal 转为 JSON 可序列化类型"""
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    return val


def get_calc_table(year_or_dt, month=None):
    """
    根据日期或年月返回 calc_overdue 表名。
    用法: get_calc_table(dt) 或 get_calc_table(year, month)
    """
    if month is not None:
        return f"calc_overdue_y{year_or_dt}m{month:02d}"
    dt = year_or_dt
    if isinstance(dt, str):
        dt = datetime.strptime(dt[:10], "%Y-%m-%d")
    return f"calc_overdue_y{dt.year}m{dt.month:02d}"


def get_latest_data_date():
    """
    从数据库 calc_overdue 表中获取最新的 stat_date（系统最新数据日）
    收益规模、现金流预测等计算均基于此日期，而非系统当前日期
    全量刷新时若已通过 set_refresh_latest_date 缓存，直接返回缓存值，避免重复查询
    返回: date 或 None（无数据时）
    """
    global _refresh_latest_date
    if _refresh_latest_date is not None:
        return _refresh_latest_date
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception:
        return None

    latest_dt = None
    for year in [2024, 2025, 2026, 2027]:
        for month in range(1, 13):
            tbl = get_calc_table(year, month)
            try:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                    (tbl,)
                )
                if not cur.fetchone():
                    continue
                cur.execute(f"SELECT MAX(stat_date)::date FROM {tbl}")
                row = cur.fetchone()
                if row and row[0]:
                    dt = row[0]
                    if hasattr(dt, 'date'):
                        dt = dt.date()
                    if latest_dt is None or dt > latest_dt:
                        latest_dt = dt
            except Exception:
                continue

    cur.close()
    conn.close()
    return latest_dt
