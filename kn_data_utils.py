"""
收益、现金流等模块共用的数据工具 - 统一使用数据库最新数据日
"""
from datetime import date, datetime
from decimal import Decimal


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
    返回: date 或 None（无数据时）
    """
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
