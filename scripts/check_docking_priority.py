#!/usr/bin/env python3
"""查询 Docking 风控页面的优先级本金、覆盖倍数"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    spv_id = "docking"
    print(f"查询 {spv_id} 的优先级指标...\n")

    # 数据库表：spv_internal_params
    from db_connect import get_connection
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='spv_internal_params')")
    if not cur.fetchone()[0]:
        print("spv_internal_params 表不存在")
        cur.close()
        conn.close()
        return

    table = "spv_internal_params"
    print(f"数据来源表: {table}\n")

    # 优先级本金
    cur.execute("""
        SELECT principal_amount, effective_date FROM spv_internal_params WHERE spv_id = %s ORDER BY effective_date DESC LIMIT 1
    """, (spv_id,))
    row = cur.fetchone()
    if not row:
        print(f"{spv_id} 在 {table} 中无记录")
        cur.close()
        conn.close()
        return

    principal_amount, effective_date = row
    principal_amount = float(principal_amount or 0)
    print("--- 优先级本金 (priority_principal) ---")
    print(f"  值: {principal_amount:,.0f} USD ({principal_amount/1e6:.2f}M)")
    print(f"  来源: {table}.principal_amount")
    print(f"  生效日期: {effective_date}")

    # 覆盖倍数：需要 risk_data 中的 M0、现金等，用 spv_internal_params 模块计算
    print("\n--- 覆盖倍数 (coverage_ratio) ---")
    risk_data = []
    try:
        from kn_risk_cache import load_risk_cache
        risk_data, _ = load_risk_cache(spv_id)
        risk_data = risk_data or []
    except Exception:
        pass

    if risk_data:
        try:
            from spv_internal_params import load_priority_indicators_for_spv
            from spv_config import load_producers_from_spv_config
            cfg = load_producers_from_spv_config(json_only=True).get(spv_id, {})
            exchange_rate = float(cfg.get("exchange_rate") or 1)
            pi = load_priority_indicators_for_spv(spv_id, risk_data=risk_data, exchange_rate=exchange_rate)
            if pi and pi.get("coverage_ratio"):
                cov = pi["coverage_ratio"]
                c = cov.get("current") if isinstance(cov, dict) else cov
                print(f"  当前值: {c}x")
                if isinstance(cov, dict):
                    print(f"  斩仓线: {cov.get('liquidation')}, 平仓线: {cov.get('margin_call')}, 基准: {cov.get('baseline')}")
                    b = cov.get("breakdown")
                    if b:
                        print(f"  Value(USD): {b.get('value_usd')}, Loan(USD): {b.get('loan_usd')}")
        except Exception as e:
            print(f"  计算异常: {e}")
    else:
        print("  无 risk_data 缓存，无法计算覆盖倍数（需先刷新风控数据）")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
