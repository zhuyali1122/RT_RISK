"""
收益、现金流等模块共用的数据工具 - 统一使用数据库最新数据日
"""
from datetime import date


def _get_calc_table(year: int, month: int) -> str:
    return f"calc_overdue_y{year}m{month:02d}"


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
            tbl = _get_calc_table(year, month)
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
