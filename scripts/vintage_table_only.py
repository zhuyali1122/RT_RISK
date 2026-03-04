#!/usr/bin/env python3
"""
单纯计算 Vintage 表 - 便于排查数据问题

直接执行与 kn_vintage 相同的 SQL，输出原始数值和计算过程。

运行：cd RT_RISK && python3 scripts/vintage_table_only.py
"""
import os
import sys
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def main():
    spv_id = "docking"
    print("=" * 95)
    print("Vintage 表（单纯计算）")
    print("=" * 95)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    from kn_data_utils import get_latest_data_date
    stat_date = get_latest_data_date() or datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d")
    stat_dt = datetime.strptime(stat_str, "%Y-%m-%d")

    table = f"calc_overdue_y{stat_dt.year}m{stat_dt.month:02d}"
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (table,))
    if not cur.fetchone():
        for y in [2024, 2025, 2026, 2027]:
            for m in range(1, 13):
                t = f"calc_overdue_y{y}m{m:02d}"
                cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (t,))
                if cur.fetchone():
                    cur.execute(f"SELECT MAX(stat_date) FROM {t} WHERE spv_id = %s AND stat_date::date <= %s", (spv_id, stat_str))
                    if cur.fetchone()[0]:
                        table = t
                        break
            else:
                continue
            break

    print(f"\n  spv_id: {spv_id}")
    print(f"  stat_date: {stat_str}")
    print(f"  表: {table}")
    print(f"  口径: loan_status IN (1, 2) 的活跃贷款")

    # 与 kn_vintage 完全相同的 SQL
    cur.execute(f"""
        SELECT
            to_char(r.disbursement_time::date, 'YYYY-MM') AS disbursement_month,
            COALESCE(SUM(c.outstanding_principal), 0) AS current_balance,
            COALESCE(SUM(CASE WHEN c.dpd >= 1 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_1_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 3 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_3_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 7 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_7_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 15 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_15_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 30 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_30_bal,
            COUNT(*) AS loan_count,
            SUM(CASE WHEN c.dpd >= 30 THEN 1 ELSE 0 END) AS dpd30_count
        FROM {table} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        GROUP BY 1
        ORDER BY 1
    """, (stat_str, spv_id))
    rows = cur.fetchall()

    print("\n" + "-" * 95)
    print("【Vintage 表】")
    print("-" * 95)
    print(f"  {'disbursement':^12} | {'MOB':>4} | {'current_bal':>18} | {'overdue_30_bal':>18} | {'dpd30_rate':>10} | {'loan_cnt':>8} | {'dpd30_cnt':>8}")
    print("-" * 95)

    for row in rows:
        dm = row[0]
        current_balance = float(row[1] or 0)
        o1 = float(row[2] or 0)
        o3 = float(row[3] or 0)
        o7 = float(row[4] or 0)
        o15 = float(row[5] or 0)
        overdue_30_bal = float(row[6] or 0)
        loan_count = int(row[7] or 0)
        dpd30_count = int(row[8] or 0)

        try:
            dm_year, dm_month = int(dm[:4]), int(dm[5:7])
            mob = (stat_dt.year - dm_year) * 12 + (stat_dt.month - dm_month)
            mob = max(0, mob)
        except (ValueError, TypeError):
            mob = 0

        dpd30_rate = overdue_30_bal / current_balance if current_balance else 0
        dpd1_rate = o1 / current_balance if current_balance else 0

        print(f"  {dm:^12} | {mob:>4} | {current_balance:>18,.0f} | {overdue_30_bal:>18,.0f} | {dpd30_rate:>9.2%} | {loan_count:>8} | {dpd30_count:>8}")

    print("-" * 95)
    print("\n  计算说明:")
    print("    dpd30_rate = overdue_30_bal / current_balance")
    print("    overdue_30_bal = SUM(outstanding_principal) WHERE dpd >= 30")
    print("    current_balance = SUM(outstanding_principal) 该 cohort 总未偿本金")
    print("    MOB = (stat_date 年-月) - (disbursement_month 年-月)")

    # 额外：检查 dpd 分布
    print("\n" + "-" * 95)
    print("【DPD 分布明细（按 cohort）】")
    print("-" * 95)
    print(f"  {'disbursement':^12} | {'o1_bal':>14} | {'o3_bal':>14} | {'o7_bal':>14} | {'o15_bal':>14} | {'o30_bal':>14} | dpd1_rate | dpd30_rate")
    print("-" * 95)
    for row in rows:
        dm = row[0]
        current_balance = float(row[1] or 0)
        o1, o3, o7, o15, o30 = float(row[2] or 0), float(row[3] or 0), float(row[4] or 0), float(row[5] or 0), float(row[6] or 0)
        dpd1 = o1 / current_balance if current_balance else 0
        dpd30 = o30 / current_balance if current_balance else 0
        print(f"  {dm:^12} | {o1:>14,.0f} | {o3:>14,.0f} | {o7:>14,.0f} | {o15:>14,.0f} | {o30:>14,.0f} | {dpd1:>8.2%} | {dpd30:>8.2%}")
    print("-" * 95)

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
