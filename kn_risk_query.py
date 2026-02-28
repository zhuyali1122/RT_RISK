"""
KN 风控核心指标查询 - 从 calc_overdue 和 raw_loan 获取真实数据
spv_id=kn，stat_date 按日筛选
"""
from datetime import date
from decimal import Decimal


def _serialize(val):
    if isinstance(val, (date,)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    return val


def _get_calc_table(stat_date):
    """根据 stat_date 返回对应的 calc_overdue 表名"""
    y, m = stat_date.year, stat_date.month
    return f"calc_overdue_y{y}m{m:02d}"


def _compute_m0_accrued_interest(calc_table: str, stat_date: str, spv_id: str) -> float:
    """
    M0 应收利息 = 所有 M0 贷款的「应还未还利息」之和
    对每笔 M0 loan：还款计划中 due_date <= stat_date 的 interest_due - 对应期数已还的 interest_repayment
    数据来源：raw_loan.repayment_schedule (JSONB)、raw_repayment
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception:
        return 0

    try:
        # 1) 若 calc_overdue 有 accrued_interest 列且已填充，直接汇总
        cur.execute(f"""
            SELECT COALESCE(SUM(CASE WHEN dpd = 0 THEN COALESCE(accrued_interest, 0) ELSE 0 END), 0)
            FROM {calc_table}
            WHERE stat_date = %s AND spv_id = %s AND loan_status IN (1, 2)
        """, (stat_date, spv_id))
        val = float(cur.fetchone()[0] or 0)
        if val > 0:
            cur.close()
            conn.close()
            return val
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    # 2) 从还款计划 + 还款记录计算
    # raw_loan.repayment_schedule->'schedule' 为数组，元素含 term/period, due_date, interest/interest_due
    # raw_repayment 含 repayment_term, interest_repayment
    try:
        cur.execute("""
            WITH m0_loans AS (
                SELECT c.loan_id
                FROM """ + calc_table + """ c
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.dpd = 0 AND c.loan_status IN (1, 2)
            ),
            schedule_past AS (
                SELECT
                    rl.loan_id,
                    (COALESCE(elem->>'term', elem->>'period', '0')::int) AS period_no,
                    COALESCE(
                        (elem->>'interest')::numeric,
                        (elem->>'interest_due')::numeric,
                        0
                    ) AS interest_due
                FROM raw_loan rl
                CROSS JOIN LATERAL jsonb_array_elements(
                    COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)
                ) AS elem
                WHERE rl.spv_id = %s
                AND rl.loan_id IN (SELECT loan_id FROM m0_loans)
                AND elem->>'due_date' IS NOT NULL
                AND (elem->>'due_date')::date <= %s::date
            ),
            interest_due_total AS (
                SELECT COALESCE(SUM(interest_due), 0) AS total FROM schedule_past
            ),
            interest_paid_total AS (
                SELECT COALESCE(SUM(rp.interest_repayment), 0) AS total
                FROM raw_repayment rp
                WHERE rp.loan_id IN (SELECT loan_id FROM m0_loans)
                AND rp.repayment_term IN (SELECT period_no FROM schedule_past)
            )
            SELECT (SELECT total FROM interest_due_total) - (SELECT total FROM interest_paid_total) AS m0_accrued
        """, (stat_date, spv_id, spv_id, stat_date))
        row = cur.fetchone()
        if row and row[0] is not None:
            val = max(0, float(row[0]))
            cur.close()
            conn.close()
            return val
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
    return 0


def get_available_stat_dates(spv_id: str = "kn", limit: int = 30):
    """
    获取可用的 stat_date 列表（用于日期选择器）
    返回: [ "2026-02-25", "2026-02-24", ... ] 或 []
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return []
    cur = conn.cursor()
    tables = ["calc_overdue_y2026m02", "calc_overdue_y2026m03", "calc_overdue_y2026m04", "calc_overdue_y2026m05"]
    dates = []
    for table in tables:
        try:
            cur.execute(
                f"SELECT DISTINCT stat_date::text FROM {table} WHERE spv_id = %s ORDER BY stat_date DESC LIMIT %s",
                (spv_id, limit)
            )
            for r in cur.fetchall():
                dates.append(r[0][:10] if r[0] else "")
        except Exception:
            continue
    cur.close()
    conn.close()
    # 去重并排序
    seen = set()
    out = []
    for d in dates:
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    out.sort(reverse=True)
    return out[:limit]


def query_kn_core_metrics(stat_date: str, spv_id: str = "kn"):
    """
    查询 KN 核心指标，用于风控面板
    stat_date: 如 '2026-02-25'
    返回: { stat_date, cumulative_disbursement, current_balance, ... } 或 { error: str }
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception as e:
        return {"error": f"数据库连接失败: {e}"}

    stat_d = stat_date.strip() if isinstance(stat_date, str) else str(stat_date)
    if not stat_d:
        return {"error": "请指定 stat_date"}

    try:
        from datetime import datetime
        dt = datetime.strptime(stat_d, "%Y-%m-%d").date()
    except ValueError:
        return {"error": f"stat_date 格式错误，应为 YYYY-MM-DD: {stat_d}"}

    table = _get_calc_table(dt)
    cur = conn.cursor()

    # 检查表是否存在
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
        (table,)
    )
    if not cur.fetchone():
        cur.close()
        conn.close()
        return {"error": f"表 {table} 不存在"}

    # loan_status: 1=正常, 2=逾期, 3=结清。统计口径：1+2（未结清），排除3
    # 1. 从 calc_overdue 聚合
    cur.execute(f"""
        SELECT
            COUNT(*) AS active_loans,
            COALESCE(SUM(outstanding_principal), 0) AS current_balance,
            COALESCE(SUM(CASE WHEN dpd = 0 THEN outstanding_principal ELSE 0 END), 0) AS m0_balance,
            SUM(CASE WHEN dpd >= 1 THEN 1 ELSE 0 END) AS overdue_1_count,
            SUM(CASE WHEN dpd >= 3 THEN 1 ELSE 0 END) AS overdue_3_count,
            SUM(CASE WHEN dpd >= 7 THEN 1 ELSE 0 END) AS overdue_7_count,
            SUM(CASE WHEN dpd >= 15 THEN 1 ELSE 0 END) AS overdue_15_count,
            SUM(CASE WHEN dpd >= 30 THEN 1 ELSE 0 END) AS overdue_30_count,
            COALESCE(SUM(CASE WHEN dpd = 0 THEN outstanding_principal ELSE 0 END), 0) AS bal_m0,
            COALESCE(SUM(CASE WHEN dpd BETWEEN 1 AND 30 THEN outstanding_principal ELSE 0 END), 0) AS bal_m1,
            COALESCE(SUM(CASE WHEN dpd BETWEEN 31 AND 60 THEN outstanding_principal ELSE 0 END), 0) AS bal_m2,
            COALESCE(SUM(CASE WHEN dpd BETWEEN 61 AND 90 THEN outstanding_principal ELSE 0 END), 0) AS bal_m3,
            COALESCE(SUM(CASE WHEN dpd BETWEEN 91 AND 120 THEN outstanding_principal ELSE 0 END), 0) AS bal_m4,
            COALESCE(SUM(CASE WHEN dpd BETWEEN 121 AND 150 THEN outstanding_principal ELSE 0 END), 0) AS bal_m5,
            COALESCE(SUM(CASE WHEN dpd >= 151 THEN outstanding_principal ELSE 0 END), 0) AS bal_m6
        FROM {table}
        WHERE stat_date = %s AND spv_id = %s AND loan_status IN (1, 2)
    """, (stat_d, spv_id))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return {"error": f"无数据: {table} stat_date={stat_d} spv_id={spv_id}"}

    (active_loans, current_balance, m0_balance,
     o1, o3, o7, o15, o30,
     bal_m0, bal_m1, bal_m2, bal_m3, bal_m4, bal_m5, bal_m6) = row

    total_bal = float(current_balance or 0)
    m0_ratio = float(m0_balance or 0) / total_bal if total_bal else 0

    # M0 应收利息 = 所有 M0 贷款的「应还未还利息」之和
    # 对每笔 M0 loan：还款计划中 due_date <= stat_date 的 interest_due 之和 - raw_repayment 中对应期数已还的 interest_repayment 之和
    m0_accrued_interest = _compute_m0_accrued_interest(table, stat_d, spv_id)
    n = int(active_loans or 0)
    overdue_1_plus_ratio = (int(o1 or 0) / n) if n else 0
    overdue_3_plus_ratio = (int(o3 or 0) / n) if n else 0
    overdue_7_plus_ratio = (int(o7 or 0) / n) if n else 0
    overdue_15_plus_ratio = (int(o15 or 0) / n) if n else 0
    overdue_30_plus_ratio = (int(o30 or 0) / n) if n else 0

    # 2. 从 raw_loan 获取 cumulative_disbursement, active_borrowers, avg_duration, rates
    cur.execute("""
        SELECT
            COALESCE(SUM(r.disbursement_amount), 0) AS cumulative_disbursement,
            COUNT(DISTINCT r.customer_id) AS active_borrowers,
            AVG(r.term_months) AS avg_duration,
            AVG(r.customer_rate) AS avg_daily_rate,
            SUM(r.customer_rate * r.disbursement_amount) / NULLIF(SUM(r.disbursement_amount), 0) AS disbursement_weighted_rate
        FROM """ + table + """ c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
    """, (stat_d, spv_id))
    r2 = cur.fetchone()
    cum_disb, active_borrowers, avg_duration, avg_rate, weighted_rate = r2 or (0, 0, 0, 0, 0)

    # cumulative_disbursement: 截至 stat_date 的所有放款
    cur.execute("""
        SELECT COALESCE(SUM(disbursement_amount), 0)
        FROM raw_loan
        WHERE spv_id = %s AND disbursement_time::date <= %s
    """, (spv_id, stat_d))
    cum_disb = cur.fetchone()[0] or 0

    # DPD 分布：按 bucket 聚合 loan_count, balance
    cur.execute(f"""
        SELECT
            CASE
                WHEN dpd = 0 THEN 'M0'
                WHEN dpd BETWEEN 1 AND 30 THEN 'M1'
                WHEN dpd BETWEEN 31 AND 60 THEN 'M2'
                WHEN dpd BETWEEN 61 AND 90 THEN 'M3'
                WHEN dpd BETWEEN 91 AND 120 THEN 'M4'
                WHEN dpd BETWEEN 121 AND 150 THEN 'M5'
                ELSE 'M6+'
            END AS bucket,
            COUNT(*) AS loan_count,
            COALESCE(SUM(outstanding_principal), 0) AS balance
        FROM {table}
        WHERE stat_date = %s AND spv_id = %s AND loan_status IN (1, 2)
        GROUP BY 1
        ORDER BY 1
    """, (stat_d, spv_id))
    bucket_rows = cur.fetchall()

    # 客户信用评级分布：从 raw_customer.rating_a 按余额占比
    credit_rating_distribution = []
    try:
        cur.execute(f"""
            SELECT
                COALESCE(TRIM(cu.rating_a::text), '-') AS rating,
                COUNT(*) AS loan_count,
                COALESCE(SUM(c.outstanding_principal), 0) AS balance
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            LEFT JOIN raw_customer cu ON cu.customer_id = r.customer_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            GROUP BY COALESCE(TRIM(cu.rating_a::text), '-')
            ORDER BY rating
        """, (stat_d, spv_id))
        for rating, lc, bal in cur.fetchall():
            b = float(bal or 0)
            ratio = b / total_bal if total_bal else 0
            credit_rating_distribution.append({
                "rating": str(rating or "-"),
                "loan_count": int(lc or 0),
                "ratio": f"{ratio:.4f}",
            })
    except Exception:
        pass

    cur.close()
    conn.close()

    dpd_distribution = []
    for bucket, loan_count, balance in bucket_rows:
        b = float(balance or 0)
        ratio = b / total_bal if total_bal else 0
        lc = int(loan_count or 0)
        dpd_distribution.append({
            "bucket": bucket,
            "balance": str(int(b)),
            "loan_count": lc,
            "balance_ratio": f"{ratio:.4f}",
            "borrower_count": lc,  # 简化：与 loan_count 相同，后续可 join raw_loan 精确计算
        })

    return {
        "stat_date": stat_d,
        "cumulative_disbursement": str(int(float(cum_disb))),
        "current_balance": str(int(total_bal)),
        "m0_balance": str(int(float(m0_balance or 0))),
        "m0_accrued_interest": str(int(round(float(m0_accrued_interest)))),
        "cash": "0",  # 现金，暂无确定来源，暂写 0
        "avg_duration": round(float(avg_duration or 0), 1),
        "m0_ratio": f"{m0_ratio:.4f}",
        "avg_daily_rate": f"{float(avg_rate or 0):.6f}",
        "disbursement_weighted_rate": f"{float(weighted_rate or 0):.6f}",
        "active_loans": str(int(active_loans or 0)),
        "active_borrowers": str(int(active_borrowers or 0)),
        "overdue_1_plus_ratio": f"{overdue_1_plus_ratio:.4f}",
        "overdue_3_plus_ratio": f"{overdue_3_plus_ratio:.4f}",
        "overdue_7_plus_ratio": f"{overdue_7_plus_ratio:.4f}",
        "overdue_15_plus_ratio": f"{overdue_15_plus_ratio:.4f}",
        "overdue_30_plus_ratio": f"{overdue_30_plus_ratio:.4f}",
        "dpd_distribution": dpd_distribution,
        "credit_rating_distribution": credit_rating_distribution,
        "vintage_data": _load_vintage_for_row(stat_d, spv_id),
        "collection_report": _load_collection_report(stat_d, spv_id),
    }


def query_loans_by_dpd_bucket(spv_id: str, stat_date: str, bucket: str, page: int = 1, per_page: int = 200):
    """
    按 DPD 账龄档位查询底层资产 Loan 列表（支持分页）
    bucket: M0, M1, M2, M3, M4, M5, M6+
    返回: (loans, total_count)
    """
    bucket_upper = (bucket or "").strip().upper()
    dpd_conditions = {
        "M0": "c.dpd = 0",
        "M1": "c.dpd BETWEEN 1 AND 30",
        "M2": "c.dpd BETWEEN 31 AND 60",
        "M3": "c.dpd BETWEEN 61 AND 90",
        "M4": "c.dpd BETWEEN 91 AND 120",
        "M5": "c.dpd BETWEEN 121 AND 150",
        "M6+": "c.dpd >= 151",
    }
    if bucket_upper not in dpd_conditions:
        return [], 0

    try:
        from datetime import datetime
        dt = datetime.strptime(stat_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return [], 0

    table = _get_calc_table(dt)
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return [], 0

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
            (table,)
        )
        if not cur.fetchone():
            return [], 0
    except Exception:
        cur.close()
        conn.close()
        return [], 0

    stat_d = stat_date[:10]
    cond = dpd_conditions[bucket_upper]
    offset = max(0, (page - 1) * per_page)
    limit = max(1, min(per_page, 500))

    # 先查总数
    try:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            AND {cond}
        """, (stat_d, spv_id))
        total_count = cur.fetchone()[0]
    except Exception:
        total_count = 0

    # 产品类型 = (Repayment_type, Terms) 组合；信用评级 = raw_customer.rating_a
    try:
        cur.execute(f"""
            SELECT
                c.loan_id,
                r.disbursement_amount,
                r.disbursement_time,
                r.term_months,
                r.customer_rate,
                COALESCE(r.repayment_method::text, '') AS repayment_type,
                cu.rating_a,
                c.dpd,
                c.outstanding_principal,
                c.loan_status
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            LEFT JOIN raw_customer cu ON cu.customer_id = r.customer_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            AND {cond}
            ORDER BY c.loan_id
            LIMIT %s OFFSET %s
        """, (stat_d, spv_id, limit, offset))
    except Exception:
        try:
            cur.execute(f"""
                SELECT
                    c.loan_id,
                    r.disbursement_amount,
                    r.disbursement_time,
                    r.term_months,
                    r.customer_rate,
                    '' AS repayment_type,
                    NULL AS rating_a,
                    c.dpd,
                    c.outstanding_principal,
                    c.loan_status
                FROM {table} c
                JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
                AND {cond}
                ORDER BY c.loan_id
                LIMIT %s OFFSET %s
            """, (stat_d, spv_id, limit, offset))
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.execute(f"""
                SELECT
                    c.loan_id,
                    r.disbursement_amount,
                    r.disbursement_time,
                    r.term_months,
                    r.customer_rate,
                    c.dpd,
                    c.outstanding_principal,
                    c.loan_status
                FROM {table} c
                JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
                AND {cond}
                ORDER BY c.loan_id
                LIMIT %s OFFSET %s
            """, (stat_d, spv_id, limit, offset))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return _build_loans_from_rows(rows), total_count


def query_loans_by_vintage_month(spv_id: str, stat_date: str, disbursement_month: str, page: int = 1, per_page: int = 200):
    """
    按放款月(vintage)查询底层资产 Loan 列表（支持分页）
    disbursement_month: YYYY-MM
    返回: (loans, total_count)
    """
    if not disbursement_month or len(disbursement_month) < 7:
        return [], 0
    try:
        from datetime import datetime
        dt = datetime.strptime(stat_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return [], 0

    table = _get_calc_table(dt)
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return [], 0

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
            (table,)
        )
        if not cur.fetchone():
            return [], 0
    except Exception:
        cur.close()
        conn.close()
        return [], 0

    stat_d = stat_date[:10]
    offset = max(0, (page - 1) * per_page)
    limit = max(1, min(per_page, 500))

    # 先查总数：to_char(r.disbursement_time::date, 'YYYY-MM') = disbursement_month
    try:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            AND to_char(r.disbursement_time::date, 'YYYY-MM') = %s
        """, (stat_d, spv_id, disbursement_month))
        total_count = cur.fetchone()[0]
    except Exception:
        total_count = 0

    base_sql = f"""
        SELECT c.loan_id, r.disbursement_amount, r.disbursement_time, r.term_months, r.customer_rate,
               COALESCE(r.repayment_method::text, '') AS repayment_type, cu.rating_a,
               c.dpd, c.outstanding_principal, c.loan_status
        FROM {table} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        LEFT JOIN raw_customer cu ON cu.customer_id = r.customer_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        AND to_char(r.disbursement_time::date, 'YYYY-MM') = %s
        ORDER BY c.loan_id
        LIMIT %s OFFSET %s
    """
    try:
        cur.execute(base_sql, (stat_d, spv_id, disbursement_month, limit, offset))
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        base_sql_fallback = f"""
            SELECT c.loan_id, r.disbursement_amount, r.disbursement_time, r.term_months, r.customer_rate,
                   '' AS repayment_type, NULL AS rating_a, c.dpd, c.outstanding_principal, c.loan_status
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            AND to_char(r.disbursement_time::date, 'YYYY-MM') = %s
            ORDER BY c.loan_id
            LIMIT %s OFFSET %s
        """
        try:
            cur.execute(base_sql_fallback, (stat_d, spv_id, disbursement_month, limit, offset))
        except Exception:
            cur.execute(f"""
                SELECT c.loan_id, r.disbursement_amount, r.disbursement_time, r.term_months, r.customer_rate,
                       c.dpd, c.outstanding_principal, c.loan_status
                FROM {table} c
                JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
                AND to_char(r.disbursement_time::date, 'YYYY-MM') = %s
                ORDER BY c.loan_id
                LIMIT %s OFFSET %s
            """, (stat_d, spv_id, disbursement_month, limit, offset))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return _build_loans_from_rows(rows), total_count


def query_loans_by_maturity_month(spv_id: str, stat_date: str, maturity_month: str, page: int = 1, per_page: int = 200):
    """
    按到期月(maturity_month)查询底层资产 Loan 列表（支持分页）
    maturity_month: YYYY-MM
    返回: (loans, total_count)
    """
    if not maturity_month or len(maturity_month) < 7:
        return [], 0
    try:
        from datetime import datetime
        dt = datetime.strptime(stat_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return [], 0

    table = _get_calc_table(dt)
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return [], 0

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
            (table,)
        )
        if not cur.fetchone():
            return [], 0
    except Exception:
        cur.close()
        conn.close()
        return [], 0

    stat_d = stat_date[:10]
    offset = max(0, (page - 1) * per_page)
    limit = max(1, min(per_page, 500))

    # maturity_month 来自 loan_maturity_date 或 repayment_schedule 最后一期 due_date
    mm_cond = """
        COALESCE(to_char(r.loan_maturity_date::date, 'YYYY-MM'),
            (SELECT to_char((elem->>'due_date')::date, 'YYYY-MM')
             FROM jsonb_array_elements(COALESCE(r.repayment_schedule->'schedule', '[]'::jsonb)) elem
             ORDER BY (elem->>'due_date')::date DESC
             LIMIT 1)
        ) = %s
    """

    try:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            AND {mm_cond}
        """, (stat_d, spv_id, maturity_month))
        total_count = cur.fetchone()[0]
    except Exception:
        total_count = 0

    base_sql = f"""
        SELECT c.loan_id, r.disbursement_amount, r.disbursement_time, r.term_months, r.customer_rate,
               COALESCE(r.repayment_method::text, '') AS repayment_type, cu.rating_a,
               c.dpd, c.outstanding_principal, c.loan_status
        FROM {table} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        LEFT JOIN raw_customer cu ON cu.customer_id = r.customer_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        AND {mm_cond}
        ORDER BY c.loan_id
        LIMIT %s OFFSET %s
    """
    try:
        cur.execute(base_sql, (stat_d, spv_id, maturity_month, limit, offset))
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        base_sql_fallback = f"""
            SELECT c.loan_id, r.disbursement_amount, r.disbursement_time, r.term_months, r.customer_rate,
                   '' AS repayment_type, NULL AS rating_a, c.dpd, c.outstanding_principal, c.loan_status
            FROM {table} c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            AND {mm_cond}
            ORDER BY c.loan_id
            LIMIT %s OFFSET %s
        """
        try:
            cur.execute(base_sql_fallback, (stat_d, spv_id, maturity_month, limit, offset))
        except Exception:
            cur.execute(f"""
                SELECT c.loan_id, r.disbursement_amount, r.disbursement_time, r.term_months, r.customer_rate,
                       c.dpd, c.outstanding_principal, c.loan_status
                FROM {table} c
                JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
                AND to_char(r.loan_maturity_date::date, 'YYYY-MM') = %s
                ORDER BY c.loan_id
                LIMIT %s OFFSET %s
            """, (stat_d, spv_id, maturity_month, limit, offset))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return _build_loans_from_rows(rows), total_count


# repayment_method 定义：1=等额本息，2=等本等息
_REPAYMENT_METHOD_LABELS = {"1": "等额本息", "2": "等本等息"}


def _repayment_type_label(val):
    """将 repayment_method 数值转为文字"""
    s = str(val or "").strip()
    return _REPAYMENT_METHOD_LABELS.get(s, s) if s else "-"


def _build_loans_from_rows(rows):
    """从查询结果构建 loan 列表，含 product_type(Repayment_type, Terms) 和 credit_rating(rating_a)"""
    loans = []
    for r in rows:
        if len(r) >= 10:
            loan_id, disb_amt, disb_time, term_months, customer_rate, repayment_type, rating_a, dpd, out_principal, loan_status = r[:10]
        else:
            loan_id, disb_amt, disb_time, term_months, customer_rate, dpd, out_principal, loan_status = r[:8]
            repayment_type = ""
            rating_a = None

        status_str = "active" if loan_status == 1 else "overdue" if loan_status == 2 else "closed"
        term = int(term_months or 0)
        rate = float(customer_rate or 0) if customer_rate is not None else 0
        rep_label = _repayment_type_label(repayment_type)
        # 产品类型 = (Repayment_type, Terms)，repayment_method 1=等额本息 2=等本等息
        product_type = f"{rep_label}_{term}月" if rep_label != "-" or term else "-"
        credit_rating = str(rating_a).strip() if rating_a is not None and str(rating_a).strip() else "-"

        loans.append({
            "loan_id": loan_id,
            "disbursement_amount": float(disb_amt or 0),
            "disbursement_time": str(disb_time)[:19] if disb_time else "-",
            "term_month": term,
            "custom_rate": rate if customer_rate is not None else None,
            "penalty_rate": None,
            "loan_status": status_str,
            "dpd": int(dpd or 0),
            "outstanding_principal": float(out_principal or 0),
            "overdue_principal": 0,
            "overdue_interest": 0,
            "overdue_penalty": 0,
            "customer_type": "returning",
            "product_type": product_type,
            "credit_rating": credit_rating,
        })
    return loans


def _load_collection_report(stat_date: str, spv_id: str):
    """计算回收报表"""
    try:
        from kn_collection import compute_collection_report
        result = compute_collection_report(spv_id, stat_date)
        if isinstance(result, dict) and "error" in result:
            return []
        return result if isinstance(result, list) else []
    except Exception:
        return []


def _load_vintage_for_row(stat_date: str, spv_id: str):
    """加载 vintage_data：优先缓存（需 stat_date 匹配），无则返回空"""
    try:
        from kn_vintage import load_vintage_cache
        cached = load_vintage_cache(spv_id, stat_date)
        if cached:
            return cached
    except Exception:
        pass
    return []


def get_loan_overdue_info(loan_id: str, spv_id: str, stat_date: str = None):
    """从 calc_overdue 获取 loan 的 dpd、loan_status、outstanding_principal"""
    try:
        from datetime import datetime
        from db_connect import get_connection
        if not stat_date:
            try:
                from kn_producer_cache import get_risk_data_from_full_cache
                risk_data, cache_exists = get_risk_data_from_full_cache(spv_id)
                if not cache_exists:
                    from kn_risk_cache import load_risk_cache
                    risk_data, _ = load_risk_cache(spv_id)
                if risk_data:
                    latest = sorted(risk_data, key=lambda r: r.get("stat_date", ""), reverse=True)[0]
                    stat_date = latest.get("stat_date", "")
            except Exception:
                pass
        if not stat_date:
            return {}
        dt = datetime.strptime(stat_date[:10], "%Y-%m-%d")
        table = _get_calc_table(dt)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
            (table,)
        )
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {}
        cur.execute(
            f"SELECT dpd, loan_status, outstanding_principal FROM {table} WHERE loan_id = %s AND spv_id = %s AND stat_date = %s",
            (loan_id, spv_id, stat_date[:10])
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return {"dpd": int(row[0] or 0), "loan_status": int(row[1] or 0), "outstanding_principal": float(row[2] or 0)}
    except Exception:
        pass
    return {}


def query_portfolio_cumulative_stats(spv_ids: list):
    """
    从 raw_loan 按 spv_id 汇总：累计放款总额、累计借款笔数、累计借款人数
    spv_ids: 已投资平台列表（来自 spv_internal_params 的 spv_id）
    金额为 raw_loan 本币，需按 spv_config.exchange_rate 转为 USD 后汇总
    返回: { cumulative_disbursement_usd, cumulative_loan_count, cumulative_borrower_count }
    """
    if not spv_ids:
        return {"cumulative_disbursement": 0, "cumulative_loan_count": 0, "cumulative_borrower_count": 0}
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception:
        return {"cumulative_disbursement": 0, "cumulative_loan_count": 0, "cumulative_borrower_count": 0}

    cum_disb_usd = 0
    cum_loans = 0
    cum_borrowers = 0
    try:
        for spv_id in spv_ids:
            sid = str(spv_id).strip().lower()
            cur.execute("""
                SELECT
                    COALESCE(SUM(disbursement_amount), 0),
                    COUNT(*),
                    COUNT(DISTINCT customer_id)
                FROM raw_loan
                WHERE spv_id = %s AND disbursement_amount IS NOT NULL
            """, (sid,))
            row = cur.fetchone()
            if not row:
                continue
            local_disb, loan_cnt, borrower_cnt = float(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
            cum_loans += loan_cnt
            cum_borrowers += borrower_cnt
            rate = 1
            try:
                from app import _get_producer_config
                cfg = _get_producer_config(sid)
                if cfg and cfg.get("exchange_rate"):
                    rate = float(cfg["exchange_rate"])
            except Exception:
                pass
            if rate and rate > 0:
                cum_disb_usd += local_disb / rate
            else:
                cum_disb_usd += local_disb
        cur.close()
        conn.close()
    except Exception:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"cumulative_disbursement": 0, "cumulative_loan_count": 0, "cumulative_borrower_count": 0}
    return {
        "cumulative_disbursement": int(round(cum_disb_usd)),
        "cumulative_loan_count": cum_loans,
        "cumulative_borrower_count": cum_borrowers,
    }


def get_customer_info(customer_id: str):
    """从 raw_customer 获取客户基础信息"""
    if not customer_id:
        return {}
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM raw_customer WHERE customer_id = %s", (customer_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return {}
        cols = [d[0] for d in cur.description]
        raw = {cols[i]: row[i] for i in range(len(cols))}
        cur.close()
        conn.close()
        # 映射到展示字段：rating_a->credit_rating，其余按列名取（industry/region/education 等）
        result = {}
        if "rating_a" in raw and raw["rating_a"] is not None:
            result["credit_rating"] = str(raw["rating_a"]).strip() or "-"
        else:
            result["credit_rating"] = "-"
        for k in ("industry", "region", "education", "age", "gender"):
            if k in raw and raw[k] is not None:
                result[k] = str(raw[k]).strip() or "-"
            else:
                result[k] = "-"
        return result
    except Exception:
        pass
    return {}
