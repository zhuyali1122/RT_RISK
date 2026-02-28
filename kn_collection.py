"""
KN 回收报表 - 从 raw_loan、raw_repayment、calc_overdue 计算
按到期月(maturity_month)汇总：到期金额、入催、回收
"""
from datetime import datetime
from decimal import Decimal


def _get_calc_table(dt):
    """根据日期返回 calc_overdue 表名"""
    return f"calc_overdue_y{dt.year}m{dt.month:02d}"


def _serialize(val):
    if isinstance(val, Decimal):
        return float(val)
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def _dpd_bucket_into_collection(dpd):
    """DPD 归入入催档位：d0,d1,d3,d7,d30,d60,d90"""
    if dpd is None or dpd < 0:
        return None
    if dpd == 0:
        return "d0"
    if dpd <= 2:
        return "d1"
    if dpd <= 6:
        return "d3"
    if dpd <= 29:
        return "d7"
    if dpd <= 59:
        return "d30"
    if dpd <= 89:
        return "d60"
    return "d90"


def _dpd_bucket_recovery(dpd):
    """DPD 归入回收档位：d1,d3,d7,d30,d60,d90"""
    if dpd is None or dpd < 1:
        return None
    if dpd <= 2:
        return "d1"
    if dpd <= 6:
        return "d3"
    if dpd <= 29:
        return "d7"
    if dpd <= 59:
        return "d30"
    if dpd <= 89:
        return "d60"
    return "d90"


def compute_collection_report(spv_id: str, stat_date: str):
    """
    计算回收报表
    返回: [ { maturity_month, due_amount, d0_into_collection, ..., d90_recovery }, ... ]
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception as e:
        return {"error": str(e)}

    stat_d = stat_date[:10] if stat_date else ""
    if not stat_d:
        return []

    try:
        dt = datetime.strptime(stat_d, "%Y-%m-%d")
    except ValueError:
        return []

    calc_table = _get_calc_table(dt)
    cur = conn.cursor()

    # 检查 calc_overdue 表存在
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
        (calc_table,)
    )
    if not cur.fetchone():
        cur.close()
        conn.close()
        return []

    # 1. 到期月与到期金额：从 raw_loan 取 loan_maturity_date，从 repayment_schedule 取应还总额
    # 若 raw_loan 无 loan_maturity_date，从 repayment_schedule 最后一期 due_date 取
    cur.execute("""
        WITH loan_maturity AS (
            SELECT
                rl.loan_id,
                rl.spv_id,
                COALESCE(
                    to_char(rl.loan_maturity_date::date, 'YYYY-MM'),
                    (SELECT to_char((elem->>'due_date')::date, 'YYYY-MM')
                     FROM jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
                     ORDER BY (elem->>'due_date')::date DESC
                     LIMIT 1)
                ) AS maturity_month
            FROM raw_loan rl
            WHERE rl.spv_id = %s
        ),
        loan_due AS (
            SELECT
                lm.loan_id,
                lm.maturity_month,
                COALESCE(SUM(
                    (COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric +
                    (COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric
                ), 0) AS total_due
            FROM loan_maturity lm
            JOIN raw_loan rl ON rl.loan_id = lm.loan_id AND rl.spv_id = lm.spv_id
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
            WHERE lm.maturity_month IS NOT NULL
            GROUP BY lm.loan_id, lm.maturity_month
        )
        SELECT maturity_month, SUM(total_due) AS due_amount
        FROM loan_due
        GROUP BY maturity_month
        ORDER BY maturity_month
    """, (spv_id,))
    due_rows = cur.fetchall()

    if not due_rows:
        cur.close()
        conn.close()
        return []

    # 2. 入催：按到期月 + 当前 DPD 档位汇总 outstanding_principal
    cur.execute(f"""
        SELECT
            COALESCE(to_char(r.loan_maturity_date::date, 'YYYY-MM'),
                (SELECT to_char((elem->>'due_date')::date, 'YYYY-MM')
                 FROM jsonb_array_elements(COALESCE(r.repayment_schedule->'schedule', '[]'::jsonb)) elem
                 ORDER BY (elem->>'due_date')::date DESC
                 LIMIT 1)) AS maturity_month,
            c.dpd,
            COALESCE(SUM(c.outstanding_principal), 0) AS bal
        FROM {calc_table} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        GROUP BY 1, 2
        ORDER BY 1, 2
    """, (stat_d, spv_id))
    into_rows = cur.fetchall()

    # 3. 回收：从 raw_repayment，按 repayment_date 当时的 DPD 归入档位
    # 需要 join calc_overdue 的 repayment_date 对应表
    cur.execute("""
        SELECT rp.loan_id, rp.repayment_date::date,
               COALESCE(rp.principal_repayment, 0) + COALESCE(rp.interest_repayment, 0) AS amt
        FROM raw_repayment rp
        JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
        WHERE rp.repayment_date IS NOT NULL
    """, (spv_id,))
    repay_rows = cur.fetchall()

    # loan_id -> maturity_month 映射
    cur.execute("""
        SELECT loan_id,
            COALESCE(to_char(loan_maturity_date::date, 'YYYY-MM'),
                (SELECT to_char((elem->>'due_date')::date, 'YYYY-MM')
                 FROM jsonb_array_elements(COALESCE(repayment_schedule->'schedule', '[]'::jsonb)) elem
                 ORDER BY (elem->>'due_date')::date DESC
                 LIMIT 1))
        FROM raw_loan WHERE spv_id = %s
    """, (spv_id,))
    loan_to_mm = {r[0]: r[1] for r in cur.fetchall() if r[0] and r[1]}

    # 按月份批量查 DPD：(loan_id, date) -> dpd
    dpd_map = {}
    months_needed = set()
    for loan_id, rep_date, _ in repay_rows:
        if rep_date:
            try:
                rd = rep_date if hasattr(rep_date, "year") else datetime.strptime(str(rep_date)[:10], "%Y-%m-%d")
                months_needed.add((rd.year, rd.month))
            except Exception:
                pass

    for (y, m) in months_needed:
        ct = f"calc_overdue_y{y}m{m:02d}"
        try:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                (ct,)
            )
            if not cur.fetchone():
                continue
            cur.execute(f"""
                SELECT loan_id, stat_date::date, dpd
                FROM {ct}
                WHERE spv_id = %s AND stat_date IS NOT NULL
            """, (spv_id,))
            for r in cur.fetchall():
                key = (r[0], str(r[1])[:10] if r[1] else None)
                if key[1]:
                    dpd_map[key] = int(r[2]) if r[2] is not None else None
        except Exception:
            continue

    cur.close()
    conn.close()

    # 构建入催按 maturity_month 的汇总
    into_by_month = {}
    for row in into_rows:
        mm, dpd, bal = row[0], int(row[1] or 0), float(row[2] or 0)
        if not mm:
            continue
        if mm not in into_by_month:
            into_by_month[mm] = {
                "d0_into_collection": 0, "d1_into_collection": 0, "d3_into_collection": 0,
                "d7_into_collection": 0, "d30_into_collection": 0, "d60_into_collection": 0, "d90_into_collection": 0,
            }
        bucket = _dpd_bucket_into_collection(dpd)
        if bucket:
            into_by_month[mm][f"{bucket}_into_collection"] += bal

    # 回收：需要按 repayment_date 查 DPD，可能跨多个月
    recovery_by_month = {}
    for mm, _ in due_rows:
        recovery_by_month[mm] = {
            "d1_recovery": 0, "d3_recovery": 0, "d7_recovery": 0,
            "d30_recovery": 0, "d60_recovery": 0, "d90_recovery": 0,
        }

    for loan_id, rep_date, amt in repay_rows:
        if not rep_date or amt <= 0:
            continue
        date_str = str(rep_date)[:10]
        dpd = dpd_map.get((loan_id, date_str))
        bucket = _dpd_bucket_recovery(dpd)
        if not bucket:
            continue
        mm = loan_to_mm.get(loan_id)
        if mm and mm in recovery_by_month:
            recovery_by_month[mm][f"{bucket}_recovery"] += float(amt)

    # 合并结果
    result = []
    for mm, due_amt in due_rows:
        into = into_by_month.get(mm, {})
        rec = recovery_by_month.get(mm, {})
        result.append({
            "maturity_month": mm,
            "due_amount": str(int(float(due_amt or 0))),
            "d0_into_collection": str(int(into.get("d0_into_collection", 0))),
            "d1_into_collection": str(int(into.get("d1_into_collection", 0))),
            "d3_into_collection": str(int(into.get("d3_into_collection", 0))),
            "d7_into_collection": str(int(into.get("d7_into_collection", 0))),
            "d30_into_collection": str(int(into.get("d30_into_collection", 0))),
            "d60_into_collection": str(int(into.get("d60_into_collection", 0))),
            "d90_into_collection": str(int(into.get("d90_into_collection", 0))),
            "d1_recovery": str(int(rec.get("d1_recovery", 0))),
            "d3_recovery": str(int(rec.get("d3_recovery", 0))),
            "d7_recovery": str(int(rec.get("d7_recovery", 0))),
            "d30_recovery": str(int(rec.get("d30_recovery", 0))),
            "d60_recovery": str(int(rec.get("d60_recovery", 0))),
            "d90_recovery": str(int(rec.get("d90_recovery", 0))),
        })
    return result
