#!/usr/bin/env python3
"""直接 SQL 计算投资组合累计统计（不依赖 app）"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    from db_connect import get_connection
    conn = get_connection()
    cur = conn.cursor()

    # 从 spv_internal_params 取 spv_id
    cur.execute("""
        SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='spv_internal_params')
    """)
    if cur.fetchone()[0]:
        cur.execute("SELECT DISTINCT spv_id FROM spv_internal_params WHERE spv_id IS NOT NULL AND spv_id != ''")
        spv_ids = [str(r[0]).strip().lower() for r in cur.fetchall() if r[0]]
    else:
        spv_ids = []
    print(f"已投资平台: {spv_ids}")

    if not spv_ids:
        print("无数据")
        cur.close()
        conn.close()
        return

    # 从 spv_config 取汇率（或 producers.json），json_only 避免 DB 慢
    exchange_rates = {}
    try:
        from spv_config import load_producers_from_spv_config
        prods = load_producers_from_spv_config(json_only=True)
        for sid in spv_ids:
            p = prods.get(sid) or prods.get(sid.upper()) or {}
            rate = p.get("exchange_rate") or 1
            exchange_rates[sid] = float(rate) if rate else 1
    except Exception:
        for sid in spv_ids:
            exchange_rates[sid] = 1

    cum_usd = 0
    cum_loans = 0
    cum_borrowers = 0
    for spv_id in spv_ids:
        cur.execute("""
            SELECT COALESCE(SUM(disbursement_amount), 0), COUNT(*), COUNT(DISTINCT customer_id)
            FROM raw_loan WHERE spv_id = %s AND disbursement_amount IS NOT NULL
        """, (spv_id,))
        row = cur.fetchone()
        local_disb, loan_cnt, borrower_cnt = float(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
        rate = exchange_rates.get(spv_id, 1) or 1
        cum_usd += local_disb / rate if rate > 0 else local_disb
        cum_loans += loan_cnt
        cum_borrowers += borrower_cnt

    cur.close()
    conn.close()

    print("\n--- 累计统计结果 ---")
    print(f"累计放款总额 (USD): {int(round(cum_usd)):,}")
    print(f"累计借款总量 (笔):  {cum_loans:,}")
    print(f"累计借款人数:       {cum_borrowers:,}")

if __name__ == "__main__":
    main()
