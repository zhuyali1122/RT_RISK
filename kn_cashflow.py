"""
现金流预测 - 基于在贷 Loan 的还款计划计算未来预期回收
用于资产商管理-现金流 Tab，支持 KN、Docking 等 spv_id
"""
from datetime import datetime, date


def _get_calc_table(year: int, month: int) -> str:
    return f"calc_overdue_y{year}m{month:02d}"


def compute_cashflow_forecast(spv_id: str = "kn", months_ahead: int = 12, collection_rate: float = 0.98):
    """
    从数据库计算指定 spv_id 的未来现金流预测
    基于 raw_loan.repayment_schedule 中未到期应还金额，按月份汇总
    仅考虑 calc_overdue 中 loan_status IN (1,2) 的活跃贷款

    返回: dict {
        "forecast": [ { month, expected_inflow, principal, interest, loan_count }, ... ],
        "total_expected": float,
        "as_of_date": str,
    }
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception:
        return {"forecast": [], "total_expected": 0, "as_of_date": datetime.now().strftime("%Y-%m-%d")}

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    # 1. 获取活跃 loan_id 列表（最新 calc_overdue 快照中 loan_status 1,2）
    active_loan_ids = set()
    latest_tbl = None
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
                cur.execute(
                    f"SELECT MAX(stat_date)::date FROM {tbl} WHERE spv_id = %s AND stat_date::date <= %s::date",
                    (spv_id, today_str)
                )
                row = cur.fetchone()
                if row and row[0] and (not latest_dt or row[0] > latest_dt):
                    latest_dt = row[0]
                    latest_tbl = tbl
            except Exception:
                continue

    if latest_tbl and latest_dt:
        try:
            cur.execute(
                f"SELECT loan_id FROM {latest_tbl} WHERE spv_id = %s AND loan_status IN (1, 2) AND stat_date::date = %s",
                (spv_id, latest_dt)
            )
            for row in cur.fetchall():
                if row[0]:
                    active_loan_ids.add(row[0])
        except Exception:
            pass

    if not active_loan_ids:
        cur.close()
        conn.close()
        return {"forecast": [], "total_expected": 0, "as_of_date": today_str}

    # 2. 从 raw_loan 获取还款计划，汇总未来各月应还金额（仅活跃贷款）
    loan_ids_list = list(active_loan_ids)
    forecast = []
    total_expected = 0.0
    try:
        cur.execute("""
            WITH future_due AS (
                SELECT
                    to_char((elem->>'due_date')::date, 'YYYY-MM') AS month,
                    SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric) AS principal,
                    SUM((COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS interest,
                    COUNT(DISTINCT rl.loan_id) AS loan_count
                FROM raw_loan rl
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
                WHERE rl.spv_id = %s
                AND rl.loan_id = ANY(%s)
                AND elem->>'due_date' IS NOT NULL
                AND (elem->>'due_date')::date > %s::date
                GROUP BY to_char((elem->>'due_date')::date, 'YYYY-MM')
            )
            SELECT month, principal, interest, loan_count
            FROM future_due
            ORDER BY month
            LIMIT %s
        """, (spv_id, loan_ids_list, today_str, months_ahead))
        for row in cur.fetchall():
            m, principal, interest, lc = row[0], float(row[1] or 0), float(row[2] or 0), int(row[3] or 0)
            expected = (principal + interest) * collection_rate
            total_expected += expected
            forecast.append({
                "month": m,
                "expected_inflow": int(round(expected)),
                "principal": int(round(principal)),
                "interest": int(round(interest)),
                "loan_count": lc,
            })
    except Exception:
        pass

    cur.close()
    conn.close()
    return {
        "forecast": forecast,
        "total_expected": int(round(total_expected)),
        "as_of_date": today_str,
    }
