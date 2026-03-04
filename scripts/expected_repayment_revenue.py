#!/usr/bin/env python3
"""
基于 Vintage 表 + 平均期限，预估当前所有贷款的未来总回款和总收益

算法：
  预期总回款 = Σ [ (未来本金+利息) × survival_rate × (survival×正常回收率 + (1-survival)×违约回收率) ]
  预期总收益 = Σ [ 未来利息 × survival_rate × (survival×正常回收率 + (1-survival)×违约回收率) ]
  survival_rate = ∏(1 - default_mob_i), i = 1..min(残余月数, max_mob)

运行：cd RT_RISK && python3 scripts/expected_repayment_revenue.py
"""
import math
import os
import sys
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

# 回收率假设（可配置）
NORMAL_RECOVERY_RATE = 0.98   # 正常还款回收率
DEFAULT_RECOVERY_RATE = 0.30  # 违约后催收回收率


def main():
    spv_id = "docking"
    print("=" * 75)
    print("预期未来总回款 & 总收益（基于 Vintage + 平均期限）")
    print("=" * 75)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    cur.execute("""
        SELECT principal_amount, product_term, vtg_30_plus_predicted
        FROM spv_internal_params WHERE spv_id = %s ORDER BY effective_date DESC NULLS LAST LIMIT 1
    """, (spv_id,))
    row = cur.fetchone()
    if not row:
        print("无 spv_internal_params 数据")
        cur.close()
        conn.close()
        return

    vtg30_default = float(row[2] or 0)
    vtg30_default = vtg30_default / 100 if vtg30_default > 1 else vtg30_default

    try:
        from spv_config import load_producers_from_spv_config
        cfg = (load_producers_from_spv_config(json_only=True) or {}).get(spv_id) or {}
        exchange_rate = float(cfg.get("exchange_rate", 1) or 1)
    except Exception:
        exchange_rate = float(os.getenv("DOCKING_EXCHANGE_RATE", 15800))

    from kn_data_utils import get_latest_data_date
    stat_date = get_latest_data_date() or datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d")
    stat_dt = datetime.strptime(stat_str, "%Y-%m-%d")

    tbl = f"calc_overdue_y{stat_dt.year}m{stat_dt.month:02d}"
    for y in [2024, 2025, 2026, 2027]:
        for m in range(1, 13):
            t = f"calc_overdue_y{y}m{m:02d}"
            try:
                cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s", (t,))
                if not cur.fetchone():
                    continue
                cur.execute(f"SELECT MAX(stat_date) FROM {t} WHERE spv_id = %s AND stat_date::date <= %s", (spv_id, stat_str))
                r = cur.fetchone()
                if r and r[0]:
                    tbl = t
                    break
            except Exception:
                continue
        else:
            continue
        break

    # 平均期限
    cur.execute("""
        SELECT AVG(r.term_months) AS avg_term, COUNT(*) AS cnt
        FROM """ + tbl + """ c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
    """, (stat_str, spv_id))
    row_avg = cur.fetchone()
    avg_product_term = float(row_avg[0] or 0) if row_avg else 0
    loan_count = int(row_avg[1] or 0) if row_avg else 0
    max_mob = min(4, max(1, math.ceil(avg_product_term))) if avg_product_term else 4

    # Vintage default by MOB
    from kn_vintage import compute_vintage_data
    vtg = compute_vintage_data(spv_id, stat_str)
    default_by_mob = {}
    if not (isinstance(vtg, dict) and "error" in vtg):
        default_by_mob_raw = {}
        for v in (vtg or []):
            mob = v.get("mob", 0)
            if mob < 1 or mob > max_mob:
                continue
            dpd30 = float(v.get("dpd30_rate", 0) or 0)
            if mob not in default_by_mob_raw:
                default_by_mob_raw[mob] = []
            default_by_mob_raw[mob].append(dpd30)
        default_by_mob = {k: sum(v) / len(v) for k, v in default_by_mob_raw.items() if v}

    # 批量获取 loan 数据
    cur.execute("""
        WITH active_loans AS (
            SELECT c.loan_id, r.disbursement_time, r.term_months
            FROM """ + tbl + """ c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        ),
        future_due AS (
            SELECT rl.loan_id, rl.disbursement_time, rl.term_months,
                SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric) AS principal,
                SUM((COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS interest
            FROM raw_loan rl
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
            WHERE rl.spv_id = %s AND rl.loan_id IN (SELECT loan_id FROM active_loans)
              AND elem->>'due_date' IS NOT NULL AND (elem->>'due_date')::date > %s::date
            GROUP BY rl.loan_id, rl.disbursement_time, rl.term_months
        )
        SELECT loan_id, disbursement_time, term_months, principal, interest FROM future_due
    """, (stat_str, spv_id, spv_id, stat_str))
    loans = cur.fetchall()

    total_contract_principal = 0.0
    total_contract_interest = 0.0
    total_contract_value = 0.0
    sum_survival_weighted = 0.0  # Σ(合同金额 × survival)
    expected_repayment = 0.0
    expected_revenue = 0.0

    for r in (loans or []):
        disb_time, term_months, principal, interest = r[1], r[2], r[3], r[4]
        if not disb_time or not term_months:
            continue
        dm = disb_time.strftime("%Y-%m") if hasattr(disb_time, "strftime") else str(disb_time)[:7]
        try:
            dm_year, dm_month = int(dm[:4]), int(dm[5:7])
        except (ValueError, TypeError):
            continue
        mob = (stat_dt.year - dm_year) * 12 + (stat_dt.month - dm_month)
        mob = max(0, mob)
        term = int(term_months or 12)
        remaining = max(0, term - mob)
        remaining_cap = min(remaining, max_mob)

        loan_principal = float(principal or 0)
        loan_interest = float(interest or 0)
        loan_contract = loan_principal + loan_interest

        total_contract_principal += loan_principal
        total_contract_interest += loan_interest
        total_contract_value += loan_contract

        # survival_rate = ∏(1 - default_mob_i)
        survival = 1.0
        for i in range(remaining_cap):
            mob_i = mob + i + 1
            d = default_by_mob.get(mob_i, vtg30_default)
            survival *= (1 - d)

        # 预期回收率 = P(不违约)×正常回收 + P(违约)×违约回收
        effective_recovery = survival * NORMAL_RECOVERY_RATE + (1 - survival) * DEFAULT_RECOVERY_RATE

        sum_survival_weighted += loan_contract * survival
        expected_repayment += loan_contract * effective_recovery
        expected_revenue += loan_interest * effective_recovery

    expected_repayment_usd = expected_repayment / exchange_rate if exchange_rate else expected_repayment
    expected_revenue_usd = expected_revenue / exchange_rate if exchange_rate else expected_revenue

    print(f"\n  数据日期: {stat_str}")
    print(f"  平均产品期限: {avg_product_term:.2f} 月 ({avg_product_term*30:.0f} 天)")
    print(f"  活跃 loan 数: {loan_count}")
    print(f"  max_mob: {max_mob}")
    print(f"  正常回收率: {NORMAL_RECOVERY_RATE:.0%}, 违约回收率: {DEFAULT_RECOVERY_RATE:.0%}")

    print("\n" + "=" * 75)
    print("【合同价值（未折损）】")
    print("-" * 75)
    print(f"  未来应还本金 (本币):     {total_contract_principal:>20,.2f}")
    print(f"  未来应还利息 (本币):     {total_contract_interest:>20,.2f}")
    print(f"  合同总价值 (本币):       {total_contract_value:>20,.2f}")

    weighted_avg_survival = sum_survival_weighted / total_contract_value if total_contract_value else 0

    print("\n【Survival Rate】")
    print("-" * 75)
    print(f"  加权平均 Survival Rate:   {weighted_avg_survival:>20.4%}")
    print("  (按合同金额加权: Σ(合同×survival) / Σ合同)")
    print("-" * 75)

    print("\n【预期未来总回款 & 总收益】")
    print("-" * 75)
    print("  公式: 预期值 = Σ [ 合同金额 × (survival×正常回收 + (1-survival)×违约回收) ]")
    print("        survival = ∏(1 - default_mob_i), i = 1..min(残余月数, max_mob)")
    print("-" * 75)
    print(f"  预期总回款 (本币):       {expected_repayment:>20,.2f}")
    print(f"  预期总收益 (本币):       {expected_revenue:>20,.2f}")
    print("-" * 75)
    print(f"  汇率:                    {exchange_rate:>20,.2f}")
    print(f"  预期总回款 (USD):        {expected_repayment_usd:>20,.2f}")
    print(f"  预期总收益 (USD):        {expected_revenue_usd:>20,.2f}")
    print("=" * 75)

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
