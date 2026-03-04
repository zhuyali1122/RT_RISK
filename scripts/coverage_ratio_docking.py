#!/usr/bin/env python3
"""
覆盖倍数三种计算方式 - Docking 本地测试
1. M0覆盖率（现有）：(M0本金 + M0应收利息*折损) * (1 - Vtg30) + 现金
2. 合同覆盖率：按合同尚未偿还的本金+利息 * (1 - Vtg30)
3. Vintage覆盖率：按产品久期回看Vintage，按残余周期取MOB对应default，计算综合Value

运行：cd RT_RISK && python scripts/coverage_ratio_docking.py
"""
import math
import os
import sys
from datetime import datetime

# 确保项目根目录在 path 中
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def _num(rec, k, *alts, default=0):
    for key in [k] + list(alts):
        v = rec.get(key)
        if v is not None and v != "":
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return default


def main():
    spv_id = "docking"
    print("=" * 60)
    print("Docking 覆盖倍数计算（三种方式）")
    print("=" * 60)

    # 1. 获取 spv_internal_params（Loan 分母）
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    # spv_internal_params
    cur.execute("""
        SELECT * FROM spv_internal_params
        WHERE spv_id = %s
        ORDER BY effective_date DESC NULLS LAST
        LIMIT 1
    """, (spv_id,))
    row = cur.fetchone()
    if not row:
        print("无 spv_internal_params 数据")
        cur.close()
        conn.close()
        return

    cols = [d[0].lower() for d in cur.description]
    rec = dict(zip(cols, row))
    principal_amount = _num(rec, "principal_amount")
    product_term = _num(rec, "product_term")
    vtg30_default = _num(rec, "vtg_30_plus_predicted", "vtg30_predicted_default_rate", "vtg30_plus_predicted")
    vtg30_default = vtg30_default / 100 if vtg30_default > 1 else vtg30_default
    unallocated = principal_amount * product_term / 12 if product_term > 0 else 0
    loan_usd = principal_amount + unallocated

    print("\n【分母 Loan】")
    print(f"  合作本金 principal_amount: {principal_amount:,.2f} USD")
    print(f"  产品期限 product_term: {product_term} 月")
    print(f"  未分配收益 unallocated: {unallocated:,.2f} USD")
    print(f"  Loan (分母) = {loan_usd:,.2f} USD")
    print(f"  Vtg30 预估 default rate: {vtg30_default*100:.2f}%")

    # 汇率
    try:
        from spv_config import load_producers_from_spv_config
        producers = load_producers_from_spv_config(json_only=True)
        if not producers:
            producers = load_producers_from_spv_config()
        cfg = producers.get(spv_id) or {}
        if cfg:
            exchange_rate = float(cfg.get("exchange_rate", 1) or 1)
        else:
            exchange_rate = float(os.getenv("DOCKING_EXCHANGE_RATE", 15800))
    except Exception:
        exchange_rate = float(os.getenv("DOCKING_EXCHANGE_RATE", 15800))
    print(f"  汇率 exchange_rate: {exchange_rate}")

    # 获取最新 stat_date
    from kn_data_utils import get_latest_data_date
    stat_date = get_latest_data_date()
    if not stat_date:
        stat_date = datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d")
    stat_dt = datetime.strptime(stat_str, "%Y-%m-%d")
    print(f"  数据日期 stat_date: {stat_str}")

    # 2. 合同尚未偿还本金+利息（从 repayment_schedule，未来应还）
    contract_principal = 0.0
    contract_interest = 0.0
    tbl = None
    for year in [2024, 2025, 2026, 2027]:
        for month in range(1, 13):
            tbl = f"calc_overdue_y{year}m{month:02d}"
            try:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                    (tbl,)
                )
                if not cur.fetchone():
                    continue
                cur.execute(
                    f"SELECT MAX(stat_date)::date FROM {tbl} WHERE spv_id = %s AND stat_date::date <= %s::date",
                    (spv_id, stat_str)
                )
                r = cur.fetchone()
                if not r or not r[0]:
                    continue
                latest_dt = r[0]
                cur.execute("""
                    WITH active_loans AS (
                        SELECT c.loan_id
                        FROM """ + tbl + """ c
                        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
                    ),
                    future_due AS (
                        SELECT
                            rl.loan_id,
                            SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric) AS principal,
                            SUM((COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS interest
                        FROM raw_loan rl
                        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
                        WHERE rl.spv_id = %s
                        AND rl.loan_id IN (SELECT loan_id FROM active_loans)
                        AND elem->>'due_date' IS NOT NULL
                        AND (elem->>'due_date')::date > %s::date
                        GROUP BY rl.loan_id
                    )
                    SELECT COALESCE(SUM(principal), 0), COALESCE(SUM(interest), 0)
                    FROM future_due
                """, (stat_str, spv_id, spv_id, stat_str))
                row = cur.fetchone()
                if row:
                    contract_principal = float(row[0] or 0)
                    contract_interest = float(row[1] or 0)
                break
            except Exception as e:
                continue
        else:
            continue
        break

    if not tbl:
        tbl = f"calc_overdue_y{stat_dt.year}m{stat_dt.month:02d}"

    # 合同价值 = 未来应还本金 + 未来应还利息
    contract_value_local = contract_principal + contract_interest
    print("\n【Method 1: 合同覆盖率】")
    print(f"  按合同尚未偿还本金 (未来应还): {contract_principal:,.2f} 本币")
    print(f"  按合同尚未偿还利息 (未来应还): {contract_interest:,.2f} 本币")
    print(f"  合同价值 (分子原始) = 本金 + 利息 = {contract_value_local:,.2f} 本币")
    value1 = contract_value_local * (1 - vtg30_default)
    value1_usd = value1 / exchange_rate if exchange_rate else value1
    ratio1 = value1_usd / loan_usd if loan_usd else 0
    print(f"  分子 (1 - Vtg30) = {contract_value_local:,.2f} * (1 - {vtg30_default:.4f}) = {value1:,.2f} 本币")
    print(f"  分子 (USD) = {value1_usd:,.2f} USD")
    print(f"  【Value1】= {value1_usd:,.2f} USD")
    print(f"  【合同覆盖率】= Value1 / Loan = {ratio1:.4f}x")

    # 3. 获取 risk_data（M0 覆盖率）
    try:
        from kn_producer_cache import load_producer_full_cache
        data, _ = load_producer_full_cache()
        producers = (data or {}).get("producers", {})
        pc = producers.get(spv_id) or producers.get("docking")
        risk_data = (pc or {}).get("risk_data", [])
    except Exception:
        risk_data = []
    m0_bal = 0
    m0_interest = 0
    cash = 0
    if risk_data:
        latest = sorted(risk_data, key=lambda r: r.get("stat_date", ""), reverse=True)[0]
        m0_bal = float(latest.get("m0_balance") or 0)
        if m0_bal <= 0:
            cb = float(latest.get("current_balance") or 0)
            m0r = float(latest.get("m0_ratio") or 0)
            m0_bal = cb * m0r
        m0_interest = float(latest.get("m0_accrued_interest") or 0)
        cash = float(latest.get("cash") or 0)

    early_discount = _num(rec, "early_repayment_loss_rate", "early_repayment_overdue_discount") or 1.0
    core_value_m0 = m0_bal + m0_interest * early_discount
    after_default_m0 = core_value_m0 * (1 - vtg30_default)
    value_m0 = after_default_m0 + cash
    value_m0_usd = value_m0 / exchange_rate if exchange_rate else value_m0
    ratio_m0 = value_m0_usd / loan_usd if loan_usd else 0
    print("\n【M0覆盖率（现有）】")
    print(f"  M0本金: {m0_bal:,.2f} 本币")
    print(f"  M0应收利息: {m0_interest:,.2f} 本币")
    print(f"  早偿逾期折损: {early_discount:.4f}")
    print(f"  现金: {cash:,.2f} 本币")
    print(f"  核心价值 (M0本金 + M0利息×折损) = {core_value_m0:,.2f} 本币")
    print(f"  分子 (1 - Vtg30) + 现金 = {value_m0:,.2f} 本币")
    print(f"  【Value_M0】= {value_m0_usd:,.2f} USD")
    print(f"  【M0覆盖率】= {ratio_m0:.4f}x")

    # 4. Method 2: Vintage default by MOB（简化版：按产品久期只看未来 N 个月，仅用 MOB 1~max_mob）
    value2_usd = None
    ratio2 = 0
    from kn_vintage import compute_vintage_data
    vtg = compute_vintage_data(spv_id, stat_str)
    if isinstance(vtg, dict) and "error" in vtg:
        print(f"\n【Method 2: Vintage覆盖率】Vintage 计算失败: {vtg['error']}")
        default_by_mob = {}
    else:
        # 先获取平均产品期限，确定只看未来 max_mob 个月（如 90 天产品≈3月，看 4 个月）
        cur.execute("""
            SELECT AVG(r.term_months) AS avg_term, COUNT(*) AS cnt
            FROM """ + tbl + """ c
            JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
            WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        """, (stat_str, spv_id))
        row_avg = cur.fetchone()
        avg_product_term = float(row_avg[0] or 0) if row_avg else 0
        loan_count = int(row_avg[1] or 0) if row_avg else 0
        # 产品久期 90 天 ≈ 3 月，只看未来 4 个月；max_mob = min(4, ceil(avg_term))
        max_mob = min(4, max(1, math.ceil(avg_product_term))) if avg_product_term else 4

        # 按 MOB 聚合 dpd30_rate，仅保留 MOB 1~max_mob
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

        # 批量获取所有 loan 的 (loan_id, disbursement_time, term_months, contract_value)
        cur.execute("""
            WITH active_loans AS (
                SELECT c.loan_id, r.disbursement_time, r.term_months
                FROM """ + tbl + """ c
                JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
                WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
            ),
            future_due AS (
                SELECT
                    rl.loan_id,
                    rl.disbursement_time,
                    rl.term_months,
                    SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric) AS principal,
                    SUM((COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS interest
                FROM raw_loan rl
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
                WHERE rl.spv_id = %s AND rl.loan_id IN (SELECT loan_id FROM active_loans)
                  AND elem->>'due_date' IS NOT NULL AND (elem->>'due_date')::date > %s::date
                GROUP BY rl.loan_id, rl.disbursement_time, rl.term_months
            )
            SELECT loan_id, disbursement_time, term_months, principal, interest
            FROM future_due
        """, (stat_str, spv_id, spv_id, stat_str))
        loans_with_contract = cur.fetchall()

        value2_weighted = 0.0
        total_contract_value = 0.0
        for r in (loans_with_contract or []):
            loan_id, disb_time, term_months, principal, interest = r[0], r[1], r[2], r[3], r[4]
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
            # 只看未来 max_mob 个月，残余期限取 min(remaining, max_mob)
            remaining_cap = min(remaining, max_mob)

            loan_contract = float(principal or 0) + float(interest or 0)
            total_contract_value += loan_contract

            # 按残余期限取对应 MOB 的 default，计算 survival
            survival = 1.0
            for i in range(remaining_cap):
                mob_i = mob + i + 1
                d = default_by_mob.get(mob_i, vtg30_default)
                survival *= (1 - d)
            value2_weighted += loan_contract * survival

        value2_usd = value2_weighted / exchange_rate if exchange_rate else value2_weighted
        ratio2 = value2_usd / loan_usd if loan_usd else 0
        print("\n【Method 2: Vintage覆盖率（简化版）】")
        print(f"  平均产品期限 avg_product_term: {avg_product_term:.2f} 月 ({avg_product_term*30:.0f} 天)，活跃 loan 数: {loan_count}")
        print(f"  只看未来 max_mob 个月: {max_mob}（MOB 1~{max_mob}）")
        print(f"  Default by MOB (dpd30_rate 代理，仅 MOB 1~{max_mob}): {default_by_mob}")
        print(f"  总合同剩余价值: {total_contract_value:,.2f} 本币")
        print(f"  按 MOB 折损后 Value: {value2_weighted:,.2f} 本币")
        print(f"  【Value2】= {value2_usd:,.2f} USD")
        print(f"  【Vintage覆盖率】= {ratio2:.4f}x")

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("汇总")
    print("=" * 60)
    print(f"  Loan (分母): {loan_usd:,.2f} USD")
    print(f"  M0覆盖率 Value: {value_m0_usd:,.2f} USD -> {ratio_m0:.4f}x")
    print(f"  合同覆盖率 Value: {value1_usd:,.2f} USD -> {ratio1:.4f}x")
    if value2_usd is not None:
        print(f"  Vintage覆盖率 Value: {value2_usd:,.2f} USD -> {ratio2:.4f}x")

    print("\n完成。")


if __name__ == "__main__":
    main()
