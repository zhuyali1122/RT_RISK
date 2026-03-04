#!/usr/bin/env python3
"""
Docking M0 应收利息计算 - 输出各分项数字
运行：cd RT_RISK && python scripts/m0_accrued_docking.py
"""
import os
import sys
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

def main():
    spv_id = "docking"
    print("=" * 60)
    print("Docking M0 应收利息计算")
    print("=" * 60)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    # 获取最新 stat_date
    from kn_data_utils import get_calc_table, get_latest_data_date
    stat_date = get_latest_data_date()
    if not stat_date:
        stat_date = datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d") if hasattr(stat_date, 'strftime') else str(stat_date)[:10]
    tbl = get_calc_table(stat_str)

    # 检查表是否存在
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (tbl,))
    if not cur.fetchone():
        # 尝试其他表
        for y in [2026, 2025, 2024]:
            for m in range(1, 13):
                t = f"calc_overdue_y{y}m{m:02d}"
                cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (t,))
                if cur.fetchone():
                    cur.execute(f"SELECT MAX(stat_date)::date FROM {t} WHERE spv_id = %s", (spv_id,))
                    r = cur.fetchone()
                    if r and r[0]:
                        tbl = t
                        stat_str = str(r[0])[:10]
                        break
            else:
                continue
            break

    print(f"\nspv_id: {spv_id}")
    print(f"stat_date: {stat_str}")
    print(f"calc_table: {tbl}")

    # 1) M0 贷款数
    cur.execute("""
        SELECT COUNT(*) FROM """ + tbl + """ c
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.dpd = 0 AND c.loan_status IN (1, 2)
    """, (stat_str, spv_id))
    m0_count = cur.fetchone()[0] or 0
    print(f"\nM0 贷款笔数: {m0_count}")

    if m0_count == 0:
        print("无 M0 贷款，无法计算")
        cur.close()
        conn.close()
        return

    # 2) 已到期应还利息总额 (due_date <= stat_date)
    cur.execute("""
        WITH m0_loans AS (
            SELECT c.loan_id FROM """ + tbl + """ c
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.dpd = 0 AND c.loan_status IN (1, 2)
        ),
        schedule_past AS (
            SELECT rl.loan_id,
                (COALESCE(elem->>'term', elem->>'period', '0')::int) AS period_no,
                COALESCE((elem->>'interest')::numeric, (elem->>'interest_due')::numeric, 0) AS interest_due
            FROM raw_loan rl
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) AS elem
            WHERE rl.spv_id = %s AND rl.loan_id IN (SELECT loan_id FROM m0_loans)
            AND elem->>'due_date' IS NOT NULL AND (elem->>'due_date')::date <= %s::date
        )
        SELECT COALESCE(SUM(interest_due), 0) AS total FROM schedule_past
    """, (stat_str, spv_id, spv_id, stat_str))
    interest_due_total = float(cur.fetchone()[0] or 0)
    print(f"\n【已到期应还利息】due_date <= {stat_str}")
    print(f"  应还利息总额 (interest_due_total): {interest_due_total:,.2f} 本币")

    # 3) 已到期已还利息
    cur.execute("""
        WITH m0_loans AS (
            SELECT c.loan_id FROM """ + tbl + """ c
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.dpd = 0 AND c.loan_status IN (1, 2)
        ),
        schedule_past AS (
            SELECT rl.loan_id, (COALESCE(elem->>'term', elem->>'period', '0')::int) AS period_no
            FROM raw_loan rl
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) AS elem
            WHERE rl.spv_id = %s AND rl.loan_id IN (SELECT loan_id FROM m0_loans)
            AND elem->>'due_date' IS NOT NULL AND (elem->>'due_date')::date <= %s::date
        )
        SELECT COALESCE(SUM(rp.interest_repayment), 0) AS total
        FROM raw_repayment rp
        INNER JOIN schedule_past sp ON rp.loan_id = sp.loan_id AND rp.repayment_term = sp.period_no
        WHERE rp.spv_id = %s
    """, (stat_str, spv_id, spv_id, stat_str, spv_id))
    interest_paid_total = float(cur.fetchone()[0] or 0)
    print(f"  已还利息总额 (interest_paid_total): {interest_paid_total:,.2f} 本币")

    accrued_past = max(0, interest_due_total - interest_paid_total)
    print(f"  已到期未还 (accrued_past) = 应还 - 已还 = {accrued_past:,.2f} 本币")

    # 4) 未来应还利息 (due_date > stat_date)
    cur.execute("""
        WITH m0_loans AS (
            SELECT c.loan_id FROM """ + tbl + """ c
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.dpd = 0 AND c.loan_status IN (1, 2)
        ),
        future_interest AS (
            SELECT COALESCE(SUM(
                (COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric
            ), 0) AS total
            FROM raw_loan rl
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
            WHERE rl.spv_id = %s AND rl.loan_id IN (SELECT loan_id FROM m0_loans)
            AND elem->>'due_date' IS NOT NULL AND (elem->>'due_date')::date > %s::date
        )
        SELECT total FROM future_interest
    """, (stat_str, spv_id, spv_id, stat_str))
    future_interest = float(cur.fetchone()[0] or 0)
    print(f"\n【未来应还利息】due_date > {stat_str}")
    print(f"  未来应还利息 (future_interest): {future_interest:,.2f} 本币")

    # 5) M0 应收利息总额
    m0_accrued = accrued_past + future_interest
    print(f"\n【M0 应收利息】")
    print(f"  M0 应收利息 = 已到期未还 + 未来应还")
    print(f"             = {accrued_past:,.2f} + {future_interest:,.2f}")
    print(f"             = {m0_accrued:,.2f} 本币")

    # 校验：与 kn_risk_query 结果一致
    from kn_risk_query import _compute_m0_accrued_interest
    computed = _compute_m0_accrued_interest(tbl, stat_str, spv_id)
    print(f"\n【校验】kn_risk_query._compute_m0_accrued_interest = {computed:,.2f} 本币")
    print(f"  与上述计算 {'一致' if abs(m0_accrued - computed) < 0.01 else '不一致'}")

    cur.close()
    conn.close()
    print("\n完成。")

if __name__ == "__main__":
    main()
