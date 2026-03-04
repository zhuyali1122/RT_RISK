#!/usr/bin/env python3
"""
计算 Docking 和 KN 的合同平均久期（按 disbursement_time 与 loan_maturity_date）
运行：cd RT_RISK && python scripts/avg_duration_docking_kn.py
"""
import os
import sys
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

def main():
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    from kn_data_utils import get_calc_table, get_latest_data_date
    stat_date = get_latest_data_date()
    if not stat_date:
        stat_date = datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d") if hasattr(stat_date, 'strftime') else str(stat_date)[:10]
    tbl = get_calc_table(stat_str)

    # 检查表是否存在
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (tbl,))
    if not cur.fetchone():
        for y in [2026, 2025, 2024]:
            for m in range(1, 13):
                t = f"calc_overdue_y{y}m{m:02d}"
                cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (t,))
                if cur.fetchone():
                    cur.execute(f"SELECT MAX(stat_date)::date FROM {t}")
                    r = cur.fetchone()
                    if r and r[0]:
                        tbl = t
                        stat_str = str(r[0])[:10]
                        break
            else:
                continue
            break

    print("=" * 60)
    print("合同平均久期（disbursement_time 与 loan_maturity_date）")
    print("=" * 60)
    print(f"stat_date: {stat_str}")
    print(f"calc_table: {tbl}\n")

    for spv_id in ["docking", "kn"]:
        sql = """
            WITH dur AS (
                SELECT
                    (COALESCE(r.loan_maturity_date::date,
                        (SELECT MAX((elem->>'due_date')::date)
                         FROM jsonb_array_elements(COALESCE(r.repayment_schedule->'schedule', '[]'::jsonb)) elem
                         WHERE elem->>'due_date' IS NOT NULL)
                    ) - r.disbursement_time::date) * 1.0 / 30.44 AS dm
                FROM """ + tbl + """ c
                JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
                  AND r.disbursement_time IS NOT NULL
                  AND (r.loan_maturity_date IS NOT NULL
                       OR (r.repayment_schedule->'schedule' IS NOT NULL
                           AND jsonb_array_length(COALESCE(r.repayment_schedule->'schedule', '[]'::jsonb)) > 0))
            )
            SELECT AVG(dm) AS avg_duration, COUNT(*) AS loan_count
            FROM dur
            WHERE dm > 0
        """
        cur.execute(sql, (stat_str, spv_id))
        row = cur.fetchone()
        avg_duration = round(float(row[0] or 0), 1) if row and row[0] else 0
        loan_count = int(row[1] or 0) if row else 0
        print(f"【{spv_id.upper()}】")
        print(f"  合同平均久期: {avg_duration} 月")
        print(f"  参与计算贷款数: {loan_count}")
        print()

    cur.close()
    conn.close()
    print("完成。")

if __name__ == "__main__":
    main()
