#!/usr/bin/env python3
"""
仅计算 Method 2: Vintage 覆盖率
输出：1. 平均期限  2. Vintage 表  3. 计算公式数值

运行：cd RT_RISK && python3 scripts/coverage_method2_only.py
"""
import math
import os
import sys
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def main():
    spv_id = "docking"
    print("=" * 70)
    print("Method 2: Vintage 覆盖率（仅此方式）")
    print("=" * 70)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    # spv_internal_params
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

    principal_amount = float(row[0] or 0)
    product_term = float(row[1] or 0)
    vtg30_default = float(row[2] or 0)
    vtg30_default = vtg30_default / 100 if vtg30_default > 1 else vtg30_default
    unallocated = principal_amount * product_term / 12 if product_term > 0 else 0
    loan_usd = principal_amount + unallocated

    # 汇率
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

    # 确定 calc 表
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

    # 1. 平均期限
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

    print(f"\n  数据日期 stat_date: {stat_str}")
    print("\n【1. 平均期限】")
    print(f"  平均产品期限: {avg_product_term:.2f} 月 ({avg_product_term*30:.0f} 天)")
    print(f"  活跃 loan 数: {loan_count}")
    print(f"  max_mob (只看 MOB 1~{max_mob}): {max_mob}")

    # 2. Vintage 表
    from kn_vintage import compute_vintage_data
    vtg = compute_vintage_data(spv_id, stat_str)
    if isinstance(vtg, dict) and "error" in vtg:
        print(f"\n【2. Vintage 表】计算失败: {vtg['error']}")
        default_by_mob = {}
    else:
        # 原始 Vintage 表（按 cohort）
        print("\n【2. Vintage 表】")
        print("-" * 85)
        print(f"  {'disbursement_month':^18} | {'MOB':>4} | {'current_balance':>16} | {'dpd30_rate':>12} | 纳入计算")
        print("-" * 85)
        for v in sorted(vtg or [], key=lambda x: (x.get("mob", 0), x.get("disbursement_month", ""))):
            dm = v.get("disbursement_month", "")
            mob = v.get("mob", 0)
            cb = v.get("current_balance", 0)
            try:
                cb = float(cb) if cb else 0
            except (ValueError, TypeError):
                cb = 0
            dpd30 = float(v.get("dpd30_rate", 0) or 0)
            in_scope = "是" if 1 <= mob <= max_mob else "否"
            print(f"  {dm:^18} | {mob:>4} | {cb:>16,.0f} | {dpd30:>11.4%} | {in_scope}")
        print("-" * 85)

        # 按 MOB 聚合 default（仅 MOB 1~max_mob）
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

        print(f"\n  按 MOB 聚合 (MOB 1~{max_mob})，用于 survival 计算:")
        for mob in sorted(default_by_mob.keys()):
            print(f"    MOB{mob} default_rate = {default_by_mob[mob]:.4%}")
        for mob in range(1, max_mob + 1):
            if mob not in default_by_mob:
                print(f"    MOB{mob} default_rate = {vtg30_default:.4%} (无数据，用 vtg30)")

    # 3. 计算公式数值
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

    value2_weighted = 0.0
    total_contract_value = 0.0
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

        loan_contract = float(principal or 0) + float(interest or 0)
        total_contract_value += loan_contract

        survival = 1.0
        for i in range(remaining_cap):
            mob_i = mob + i + 1
            d = default_by_mob.get(mob_i, vtg30_default)
            survival *= (1 - d)
        value2_weighted += loan_contract * survival

    value2_usd = value2_weighted / exchange_rate if exchange_rate else value2_weighted
    ratio2 = value2_usd / loan_usd if loan_usd else 0

    print("\n【3. 计算公式数值】")
    print("-" * 70)
    print("  公式: Value2 = Σ (每笔 loan 的合同剩余价值 × survival_rate)")
    print("        survival_rate = ∏(1 - default_mob_i), i = 1..min(残余月数, max_mob)")
    print("-" * 70)
    print(f"  总合同剩余价值 (本币):     {total_contract_value:>18,.2f}")
    print(f"  按 MOB 折损后 Value (本币): {value2_weighted:>18,.2f}")
    print(f"  汇率:                      {exchange_rate:>18,.2f}")
    print(f"  Value2 (USD):              {value2_usd:>18,.2f}")
    print("-" * 70)
    print(f"  分母 Loan (USD):           {loan_usd:>18,.2f}")
    print(f"  Vintage覆盖率 = Value2 / Loan = {value2_usd:,.2f} / {loan_usd:,.2f} = {ratio2:.4f}x")
    print("-" * 70)

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
