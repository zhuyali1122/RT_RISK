#!/usr/bin/env python3
"""
查询全量应收利息（raw_loan + raw_repayment 计算）
运行：cd RT_RISK && python3 scripts/query_all_accrued_interest.py
"""
import os
import sys
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def main():
    spv_id = "kn"
    print("=" * 60)
    print(f"全量应收利息（{spv_id}）")
    print("=" * 60)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    from kn_risk_query import get_available_stat_dates, _get_calc_table, _compute_all_accrued_interest

    dates = get_available_stat_dates(spv_id=spv_id, limit=1)
    stat_str = dates[0] if dates else "2026-02-25"
    dt = datetime.strptime(stat_str[:10], "%Y-%m-%d")
    table = _get_calc_table(dt)

    all_accrued = _compute_all_accrued_interest(table, stat_str, spv_id)

    print(f"\n  spv_id: {spv_id}")
    print(f"  stat_date: {stat_str}")
    print(f"  表: {table}")
    print(f"  计算方式: raw_loan.repayment_schedule 应还利息 - raw_repayment 已还利息")
    print(f"\n  【全量应收利息】= {all_accrued:,.2f} 本币")

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
