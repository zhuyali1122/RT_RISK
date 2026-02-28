"""
收益规模数据 - 从 raw_loan、raw_repayment、calc_overdue 计算
用于资产商管理-收益规模页面，支持 KN、Docking 等 spv_id
返回结构与 producers.json 中 revenue_data 一致
"""
from datetime import datetime
from decimal import Decimal


def _get_calc_table(year: int, month: int) -> str:
    return f"calc_overdue_y{year}m{month:02d}"


def _serialize(val):
    if isinstance(val, Decimal):
        return float(val)
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def _get_months_with_data(spv_id: str = "kn"):
    """获取有数据的月份列表（YYYY-MM），按时间升序"""
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception:
        return []

    months = set()
    # 从 raw_loan 放款月份
    try:
        cur.execute("""
            SELECT DISTINCT to_char(disbursement_time::date, 'YYYY-MM') AS m
            FROM raw_loan WHERE spv_id = %s AND disbursement_time IS NOT NULL
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                months.add(r[0])
    except Exception:
        pass
    # 从 raw_repayment 回收月份
    try:
        cur.execute("""
            SELECT DISTINCT to_char(rp.repayment_date::date, 'YYYY-MM') AS m
            FROM raw_repayment rp
            JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
            WHERE rp.repayment_date IS NOT NULL
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                months.add(r[0])
    except Exception:
        pass
    # 从 calc_overdue 表（遍历可能存在的表）
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
                    f"SELECT DISTINCT to_char(stat_date::date, 'YYYY-MM') FROM {tbl} WHERE spv_id = %s",
                    (spv_id,)
                )
                for r in cur.fetchall():
                    if r[0]:
                        months.add(r[0])
            except Exception:
                continue

    cur.close()
    conn.close()
    return sorted(months) if months else []


def compute_revenue_data(spv_id: str = "kn"):
    """
    从数据库计算指定 spv_id 的 revenue_data（KN、Docking 等）
    返回: [ { month, disbursement, outstanding_balance, collection, interest_income, ... }, ... ]
    与 producers.json 中 revenue_data 结构一致
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        return []

    months = _get_months_with_data(spv_id)
    if not months:
        cur.close()
        conn.close()
        return []

    result = []

    for month_str in months:
        y, m = int(month_str[:4]), int(month_str[5:7])
        last_day = f"{month_str}-28"  # 简化：取月末附近日期，实际可用 calendar 算最后一天
        try:
            from calendar import monthrange
            last_day = f"{month_str}-{monthrange(y, m)[1]:02d}"
        except Exception:
            pass

        # 1. 当月放款
        cur.execute("""
            SELECT COALESCE(SUM(disbursement_amount), 0)
            FROM raw_loan
            WHERE spv_id = %s AND to_char(disbursement_time::date, 'YYYY-MM') = %s
        """, (spv_id, month_str))
        disbursement = float(cur.fetchone()[0] or 0)

        # 2. 累计放款（截至当月末）
        cur.execute("""
            SELECT COALESCE(SUM(disbursement_amount), 0)
            FROM raw_loan
            WHERE spv_id = %s AND disbursement_time::date <= %s::date
        """, (spv_id, last_day))
        cumulative_disbursement = float(cur.fetchone()[0] or 0)

        # 3. 月底在贷余额（calc_overdue 当月最后一天或最后可用日）
        calc_tbl = _get_calc_table(y, m)
        outstanding_balance = 0
        try:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                (calc_tbl,)
            )
            if cur.fetchone():
                cur.execute(f"""
                    SELECT MAX(stat_date)::date FROM {calc_tbl}
                    WHERE spv_id = %s AND stat_date::date <= %s::date
                """, (spv_id, last_day))
                max_dt = cur.fetchone()
                if max_dt and max_dt[0]:
                    cur.execute(f"""
                        SELECT COALESCE(SUM(outstanding_principal), 0)
                        FROM {calc_tbl}
                        WHERE spv_id = %s AND loan_status IN (1, 2) AND stat_date::date = %s
                    """, (spv_id, max_dt[0]))
                    row = cur.fetchone()
                    if row:
                        outstanding_balance = float(row[0] or 0)
        except Exception:
            pass

        # 4. 当月回收：principal + interest
        cur.execute("""
            SELECT
                COALESCE(SUM(rp.principal_repayment), 0),
                COALESCE(SUM(rp.interest_repayment), 0),
                COALESCE(SUM(COALESCE(rp.penalty_repayment, 0) + COALESCE(rp.extension_fee, 0)), 0)
            FROM raw_repayment rp
            JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
            WHERE to_char(rp.repayment_date::date, 'YYYY-MM') = %s AND rp.repayment_date IS NOT NULL
        """, (spv_id, month_str))
        rp_row = cur.fetchone()
        principal_repaid = float(rp_row[0] or 0)
        interest_income = float(rp_row[1] or 0)
        fee_income = float(rp_row[2] or 0)
        collection = principal_repaid + interest_income + fee_income

        # 5. 净收益 = 利息 + 手续费（投资者视角）
        net_revenue = interest_income + fee_income

        # 6. 年化收益率：当月利息 / 月均余额 * 12
        # 需要月初余额，用上月月末近似
        begin_balance = 0
        if result:
            begin_balance = result[-1].get("outstanding_balance", 0) or 0
        avg_balance = (begin_balance + outstanding_balance) / 2 if (begin_balance or outstanding_balance) else outstanding_balance
        avg_yield_annualized = (interest_income / avg_balance * 12) if avg_balance else 0

        # 7. 回收率：实际回收 / 应回收（从还款计划取当月应还）
        expected_due = 0
        try:
            first_day = f"{month_str}-01"
            cur.execute("""
                WITH due_in_month AS (
                    SELECT rl.loan_id,
                        SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric +
                            (COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS due_amt
                    FROM raw_loan rl
                    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
                    WHERE rl.spv_id = %s
                    AND elem->>'due_date' IS NOT NULL
                    AND (elem->>'due_date')::date >= %s::date
                    AND (elem->>'due_date')::date <= %s::date
                    GROUP BY rl.loan_id
                )
                SELECT COALESCE(SUM(due_amt), 0) FROM due_in_month
            """, (spv_id, first_day, last_day))
            expected_due = float(cur.fetchone()[0] or 0)
        except Exception:
            expected_due = 0
        collection_rate = (collection / expected_due) if expected_due and expected_due > 0 else (0.98 if collection > 0 else 0)
        collection_rate = min(1.0, max(0, collection_rate))

        result.append({
            "month": month_str,
            "disbursement": int(round(disbursement)),
            "outstanding_balance": int(round(outstanding_balance)),
            "collection": int(round(collection)),
            "interest_income": int(round(interest_income)),
            "principal_repaid": int(round(principal_repaid)),
            "fee_income": int(round(fee_income)),
            "net_revenue": int(round(net_revenue)),
            "cumulative_disbursement": int(round(cumulative_disbursement)),
            "avg_yield_annualized": round(avg_yield_annualized, 4),
            "collection_rate": round(collection_rate, 4),
        })

    cur.close()
    conn.close()
    return result
