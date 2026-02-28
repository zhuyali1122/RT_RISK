#!/usr/bin/env python3
"""本地计算投资组合累计统计：累计放款总额、累计借款总量、累计借款人数"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    from spv_initial_params import load_invested_spv_ids_for_portfolio
    from kn_risk_query import query_portfolio_cumulative_stats

    spv_ids = load_invested_spv_ids_for_portfolio()
    print(f"已投资平台 (spv_initial_params): {spv_ids}")

    if not spv_ids:
        print("无已投资平台，累计统计为 0")
        return

    result = query_portfolio_cumulative_stats(spv_ids)
    print("\n--- 累计统计结果 ---")
    print(f"累计放款总额 (USD): {result.get('cumulative_disbursement', 0):,}")
    print(f"累计借款总量 (笔):  {result.get('cumulative_loan_count', 0):,}")
    print(f"累计借款人数:       {result.get('cumulative_borrower_count', 0):,}")

if __name__ == "__main__":
    main()
