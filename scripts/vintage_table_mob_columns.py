#!/usr/bin/env python3
"""
Vintage 表 - 按新格式
列：放款月、放款额、当前余额、MOB1、MOB2、MOB3

MOB1/MOB2/MOB3 = 该 cohort 在对应 MOB 时的 dpd30_rate（需回看历史 stat_date）

运行：cd RT_RISK && python3 scripts/vintage_table_mob_columns.py
"""
import os
import sys
from datetime import datetime


def _add_months(y, m, delta):
    m += delta
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return y, m

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def _get_calc_table(dt):
    return f"calc_overdue_y{dt.year}m{dt.month:02d}"


def _query_dpd30_for_cohort(cur, table, stat_str, spv_id, disbursement_month):
    """查询某 cohort 在某 stat_date 的 dpd30_rate"""
    cur.execute(f"""
        SELECT
            COALESCE(SUM(c.outstanding_principal), 0) AS current_balance,
            COALESCE(SUM(CASE WHEN c.dpd >= 30 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_30_bal
        FROM {table} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
          AND to_char(r.disbursement_time::date, 'YYYY-MM') = %s
    """, (stat_str, spv_id, disbursement_month))
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    cb = float(row[0] or 0)
    o30 = float(row[1] or 0)
    return o30 / cb if cb else 0


def _get_stat_date_in_month(cur, year, month, spv_id):
    """获取该月表中该 spv 可用的 stat_date（取最大）"""
    tbl = _get_calc_table(datetime(year, month, 1))
    try:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
            (tbl,)
        )
        if not cur.fetchone():
            return None
        cur.execute(
            f"SELECT MAX(stat_date)::text FROM {tbl} WHERE spv_id = %s",
            (spv_id,)
        )
        r = cur.fetchone()
        return r[0][:10] if r and r[0] else None
    except Exception:
        return None


def main():
    spv_id = "docking"
    print("=" * 95)
    print("Vintage 表（列：放款月、放款额、当前余额、MOB1、MOB2、MOB3）")
    print("=" * 95)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    from kn_data_utils import get_latest_data_date
    latest_dt = get_latest_data_date() or datetime.now().date()
    stat_str = latest_dt.strftime("%Y-%m-%d")

    # 1. 放款月列表 + 放款额
    cur.execute("""
        SELECT
            to_char(disbursement_time::date, 'YYYY-MM') AS disbursement_month,
            SUM(disbursement_amount) AS disbursement_amount
        FROM raw_loan
        WHERE spv_id = %s
        GROUP BY 1
        ORDER BY 1
    """, (spv_id,))
    disb_rows = {r[0]: float(r[1] or 0) for r in cur.fetchall()}

    # 2. 当前余额（最新 stat_date）
    tbl = _get_calc_table(latest_dt)
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
        (tbl,)
    )
    if not cur.fetchone():
        for y, m in [(2024, 12), (2025, 1), (2025, 2), (2025, 3), (2025, 4), (2025, 5), (2025, 6),
                     (2025, 7), (2025, 8), (2025, 9), (2025, 10), (2025, 11), (2025, 12),
                     (2026, 1), (2026, 2)]:
            cur.execute(f"SELECT MAX(stat_date)::text FROM calc_overdue_y{y}m{m:02d} WHERE spv_id = %s", (spv_id,))
            r = cur.fetchone()
            if r and r[0]:
                stat_str = r[0][:10]
                latest_dt = datetime.strptime(stat_str, "%Y-%m-%d").date()
                tbl = f"calc_overdue_y{y}m{m:02d}"
                break

    cur.execute(f"""
        SELECT
            to_char(r.disbursement_time::date, 'YYYY-MM') AS disbursement_month,
            COALESCE(SUM(c.outstanding_principal), 0) AS current_balance
        FROM {tbl} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        GROUP BY 1
        ORDER BY 1
    """, (stat_str, spv_id))
    balance_rows = {r[0]: float(r[1] or 0) for r in cur.fetchall()}

    # 3. 对每个放款月，查 MOB1/MOB2/MOB3 的 dpd30_rate
    all_months = sorted(set(disb_rows.keys()) | set(balance_rows.keys()))
    results = []

    for dm in all_months:
        try:
            y, m = int(dm[:4]), int(dm[5:7])
        except (ValueError, TypeError):
            continue

        disb_amt = disb_rows.get(dm, 0)
        curr_bal = balance_rows.get(dm, 0)

        # MOB1: stat_date 在 disbursement_month + 1 月
        y1, m1 = _add_months(y, m, 1)
        stat1 = _get_stat_date_in_month(cur, y1, m1, spv_id)
        mob1 = _query_dpd30_for_cohort(cur, f"calc_overdue_y{y1}m{m1:02d}", stat1, spv_id, dm) if stat1 else None

        # MOB2: disbursement_month + 2 月
        y2, m2 = _add_months(y, m, 2)
        stat2 = _get_stat_date_in_month(cur, y2, m2, spv_id)
        mob2 = _query_dpd30_for_cohort(cur, f"calc_overdue_y{y2}m{m2:02d}", stat2, spv_id, dm) if stat2 else None

        # MOB3: disbursement_month + 3 月
        y3, m3 = _add_months(y, m, 3)
        stat3 = _get_stat_date_in_month(cur, y3, m3, spv_id)
        mob3 = _query_dpd30_for_cohort(cur, f"calc_overdue_y{y3}m{m3:02d}", stat3, spv_id, dm) if stat3 else None

        results.append({
            "dm": dm,
            "disb": disb_amt,
            "balance": curr_bal,
            "mob1": mob1,
            "mob2": mob2,
            "mob3": mob3,
        })

    print(f"\n  stat_date (当前余额基准): {stat_str}")
    print("-" * 95)
    print(f"  {'放款月':^10} | {'放款额':>18} | {'当前余额':>18} | {'MOB1':>10} | {'MOB2':>10} | {'MOB3':>10}")
    print("-" * 95)

    for r in results:
        mob1_s = f"{r['mob1']:.2%}" if r['mob1'] is not None else "-"
        mob2_s = f"{r['mob2']:.2%}" if r['mob2'] is not None else "-"
        mob3_s = f"{r['mob3']:.2%}" if r['mob3'] is not None else "-"
        print(f"  {r['dm']:^10} | {r['disb']:>18,.0f} | {r['balance']:>18,.0f} | {mob1_s:>10} | {mob2_s:>10} | {mob3_s:>10}")

    print("-" * 95)
    print("\n  说明: MOB1/MOB2/MOB3 = 该 cohort 在对应账龄月时的 dpd30_rate (overdue_30_bal/current_balance)")
    print("        MOB1 取 stat_date 在放款月+1月, MOB2 取放款月+2月, MOB3 取放款月+3月")

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
