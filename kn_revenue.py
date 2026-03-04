"""
收益规模数据 - 从 raw_loan、raw_repayment、calc_overdue 计算
用于资产商管理-收益规模页面，支持 KN、Docking 等 spv_id
计算基准：使用数据库最新数据日（get_latest_data_date），仅包含该日期及之前的月份
返回结构与 producers.json 中 revenue_data 一致
"""
import logging
from datetime import datetime
from decimal import Decimal

from kn_data_utils import get_calc_table

log = logging.getLogger("kn_revenue")


def _get_months_with_data(spv_id: str = "kn"):
    """获取有数据的月份列表（YYYY-MM），按时间升序。优化：批量查 information_schema"""
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
    # 从 calc_overdue 表：批量获取存在的表名，再只查存在的表
    tables_to_check = [get_calc_table(y, m) for y in [2024, 2025, 2026, 2027] for m in range(1, 13)]
    placeholders = ",".join(["%s"] * len(tables_to_check))
    try:
        cur.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ({placeholders})",
            tables_to_check
        )
        existing_tables = [r[0] for r in cur.fetchall() if r and r[0]]
        for tbl in existing_tables:
            try:
                cur.execute(
                    f"SELECT DISTINCT to_char(stat_date::date, 'YYYY-MM') FROM {tbl} WHERE spv_id = %s",
                    (spv_id,)
                )
                for r in cur.fetchall():
                    if r[0]:
                        months.add(r[0])
            except Exception:
                continue
    except Exception:
        pass

    cur.close()
    conn.close()
    return sorted(months) if months else []


def compute_revenue_data(spv_id: str = "kn"):
    """
    从数据库计算指定 spv_id 的 revenue_data（KN、Docking 等）
    返回: [ { month, disbursement, outstanding_balance, collection, interest_income, ... }, ... ]
    与 producers.json 中 revenue_data 结构一致
    优化：放款/回收/应回收按月份批量查询，减少循环内 SQL 次数
    """
    log.info("[收益] 开始计算 spv_id=%s", spv_id)
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        log.warning("[收益] 数据库连接失败: %s", e)
        return []

    log.info("[收益] 获取有数据月份列表...")
    months = _get_months_with_data(spv_id)
    if not months:
        cur.close()
        conn.close()
        log.info("[收益] 无可用月份")
        return []

    try:
        from kn_data_utils import get_latest_data_date
        latest_dt = get_latest_data_date()
        if latest_dt:
            latest_month = latest_dt.strftime("%Y-%m")
            months = [m for m in months if m <= latest_month]
            log.info("[收益] 最新数据日 %s，筛选后 %d 个月", latest_month, len(months))
    except Exception:
        pass

    if not months:
        cur.close()
        conn.close()
        return []

    # 批量预查：放款按月、回收按月、应回收按月（一次 jsonb 展开替代每月一次）
    log.info("[收益] 批量查询放款/回收/应回收...")
    disbursement_by_month = {}
    cumulative_by_month = {}
    try:
        cur.execute("""
            SELECT to_char(disbursement_time::date, 'YYYY-MM') AS m, COALESCE(SUM(disbursement_amount), 0)
            FROM raw_loan WHERE spv_id = %s AND disbursement_time IS NOT NULL
            GROUP BY 1
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                disbursement_by_month[r[0]] = float(r[1] or 0)
        cum = 0
        for m in months:
            cum += disbursement_by_month.get(m, 0)
            cumulative_by_month[m] = cum
    except Exception as e:
        log.warning("[收益] 放款汇总失败: %s", e)

    repayment_by_month = {}
    try:
        cur.execute("""
            SELECT to_char(rp.repayment_date::date, 'YYYY-MM') AS m,
                COALESCE(SUM(rp.principal_repayment), 0),
                COALESCE(SUM(rp.interest_repayment), 0),
                COALESCE(SUM(COALESCE(rp.penalty_repayment, 0) + COALESCE(rp.extension_fee, 0)), 0)
            FROM raw_repayment rp
            JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
            WHERE rp.repayment_date IS NOT NULL
            GROUP BY 1
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                repayment_by_month[r[0]] = (float(r[1] or 0), float(r[2] or 0), float(r[3] or 0))
    except Exception as e:
        log.warning("[收益] 回收汇总失败: %s", e)

    expected_due_by_month = {}
    try:
        cur.execute("""
            WITH due_by_month AS (
                SELECT to_char((elem->>'due_date')::date, 'YYYY-MM') AS m,
                    SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric +
                        (COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS due_amt
                FROM raw_loan rl
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
                WHERE rl.spv_id = %s AND elem->>'due_date' IS NOT NULL
                GROUP BY 1
            )
            SELECT m, COALESCE(SUM(due_amt), 0) FROM due_by_month GROUP BY m
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                expected_due_by_month[r[0]] = float(r[1] or 0)
    except Exception as e:
        log.warning("[收益] 应回收汇总失败: %s", e)

    log.info("[收益] 逐月计算在贷余额与指标...")
    result = []
    for i, month_str in enumerate(months):
        y, m = int(month_str[:4]), int(month_str[5:7])
        last_day = f"{month_str}-28"
        try:
            from calendar import monthrange
            last_day = f"{month_str}-{monthrange(y, m)[1]:02d}"
        except Exception:
            pass

        disbursement = disbursement_by_month.get(month_str, 0)
        cumulative_disbursement = cumulative_by_month.get(month_str, 0)

        rp = repayment_by_month.get(month_str, (0, 0, 0))
        principal_repaid, interest_income, fee_income = rp[0], rp[1], rp[2]
        collection = principal_repaid + interest_income + fee_income
        net_revenue = interest_income + fee_income
        expected_due = expected_due_by_month.get(month_str, 0)

        # 月底在贷余额（calc_overdue 当月最后一天或最后可用日）
        calc_tbl = get_calc_table(y, m)
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

        begin_balance = result[-1].get("outstanding_balance", 0) or 0 if result else 0
        avg_balance = (begin_balance + outstanding_balance) / 2 if (begin_balance or outstanding_balance) else outstanding_balance
        avg_yield_annualized = (interest_income / avg_balance * 12) if avg_balance else 0
        collection_rate = (collection / expected_due) if expected_due and expected_due > 0 else (0.98 if collection > 0 else 0)
        collection_rate = min(1.0, max(0, collection_rate))

        result.append({
            "month": month_str,
            "disbursement": int(round(disbursement)),
            "outstanding_balance": int(round(outstanding_balance)),
            "expected_due": int(round(expected_due)),
            "collection": int(round(collection)),
            "interest_income": int(round(interest_income)),
            "principal_repaid": int(round(principal_repaid)),
            "fee_income": int(round(fee_income)),
            "net_revenue": int(round(net_revenue)),
            "cumulative_disbursement": int(round(cumulative_disbursement)),
            "avg_yield_annualized": round(avg_yield_annualized, 4),
            "collection_rate": round(collection_rate, 4),
        })
        if (i + 1) % 6 == 0 or i == len(months) - 1:
            log.info("[收益] 已处理 %d/%d 月", i + 1, len(months))

    cur.close()
    conn.close()
    log.info("[收益] 完成，共 %d 条", len(result))
    return result
