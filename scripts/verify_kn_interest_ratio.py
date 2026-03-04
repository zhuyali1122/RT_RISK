#!/usr/bin/env python3
"""
验证 KN 应收利息/本金比例
用户预期：日利率 0.7%，剩余加权久期 8.57 月 → 应收利息/本金 ≈ 0.7% × 8.57 × 30 = 179%

对比：
- all_accrued_interest（当前）：due_date <= stat_date 的应还未还
- 剩余全量应收利息（应有）：due_date > stat_date 的未来应还 + 已到期未还
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

    spv_id = "kn"
    from kn_data_utils import get_latest_data_date
    stat_date = get_latest_data_date() or datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d")
    tbl = f"calc_overdue_y{stat_date.year}m{stat_date.month:02d}"

    print("=" * 70)
    print("KN 应收利息/本金比例验证")
    print("=" * 70)
    print(f"  stat_date: {stat_str}")
    print(f"  预期：日利率 0.7%，剩余加权久期 8.57 月 → 应收利息/本金 ≈ 179%")
    print()

    # 1. 在贷余额 current_balance
    cur.execute(f"""
        SELECT COALESCE(SUM(outstanding_principal), 0)
        FROM {tbl}
        WHERE stat_date = %s AND spv_id = %s AND loan_status IN (1, 2)
    """, (stat_str, spv_id))
    current_balance = float(cur.fetchone()[0] or 0)
    print(f"【在贷余额 current_balance】= {current_balance:,.2f} MXN")

    # 2. all_accrued_interest（当前逻辑：due_date <= stat_date 的应还未还）
    from kn_risk_query import _compute_all_accrued_interest
    all_accrued = _compute_all_accrued_interest(tbl, stat_str, spv_id)
    print(f"【all_accrued_interest（已到期未还）】= {all_accrued:,.2f} MXN")
    ratio_accrued = (all_accrued / current_balance * 100) if current_balance else 0
    print(f"  比例 all_accrued/本金 = {ratio_accrued:.1f}%")
    print()

    # 3. 未来应还利息（due_date > stat_date）
    cur.execute("""
        WITH active_loans AS (
            SELECT c.loan_id FROM """ + tbl + """ c
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        ),
        future_interest AS (
            SELECT SUM((COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS total
            FROM raw_loan rl
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
            WHERE rl.spv_id = %s AND rl.loan_id IN (SELECT loan_id FROM active_loans)
            AND elem->>'due_date' IS NOT NULL AND (elem->>'due_date')::date > %s::date
        )
        SELECT COALESCE(total, 0) FROM future_interest
    """, (stat_str, spv_id, spv_id, stat_str))
    future_interest = float(cur.fetchone()[0] or 0)
    print(f"【未来应还利息（due_date > stat_date）】= {future_interest:,.2f} MXN")
    ratio_future = (future_interest / current_balance * 100) if current_balance else 0
    print(f"  比例 未来应还利息/本金 = {ratio_future:.1f}%")
    print()

    # 4. 剩余全量应收利息 = 已到期未还 + 未来应还
    total_future_accrued = all_accrued + future_interest
    print(f"【剩余全量应收利息】= 已到期未还 + 未来应还 = {all_accrued:,.2f} + {future_interest:,.2f} = {total_future_accrued:,.2f} MXN")
    ratio_total = (total_future_accrued / current_balance * 100) if current_balance else 0
    print(f"  比例 剩余全量应收利息/本金 = {ratio_total:.1f}%")
    print()

    # 5. 平均日利率
    cur.execute("""
        SELECT AVG(r.customer_rate) AS avg_rate,
               SUM(r.customer_rate * c.outstanding_principal) / NULLIF(SUM(c.outstanding_principal), 0) AS weighted_rate
        FROM """ + tbl + """ c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
    """, (stat_str, spv_id))
    row = cur.fetchone()
    avg_rate = float(row[0] or 0) * 100 if row else 0  # 转为百分比
    weighted_rate = float(row[1] or 0) * 100 if row else 0
    print(f"【利率】平均日利率 = {avg_rate:.4f}%, 余额加权利率 = {weighted_rate:.4f}%")
    print()

    print("=" * 70)
    print("结论：")
    if ratio_total < 100:
        print(f"  当前 all_accrued_interest 仅包含「已到期未还」，比例 {ratio_accrued:.1f}%")
        print(f"  方案二（ABS）应使用「剩余全量应收利息」= {total_future_accrued:,.2f}，比例 {ratio_total:.1f}%")
        print(f"  与预期 179% 的差异可能来自：实际利率、剩余期限分布、还款计划结构")
    else:
        print(f"  剩余全量应收利息/本金 = {ratio_total:.1f}%，接近预期 179%")
    print("=" * 70)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
