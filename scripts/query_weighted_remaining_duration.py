#!/usr/bin/env python3
"""
计算全量数据的剩余加权久期

公式：剩余加权久期 = Σ(单笔剩余期限 × 未偿本金) / Σ(未偿本金)
剩余期限 = term_months - mob（月），mob = 放款月到 stat_date 的月数

运行：cd RT_RISK && python scripts/query_weighted_remaining_duration.py
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

    from kn_data_utils import get_latest_data_date
    stat_date = get_latest_data_date()
    if not stat_date:
        stat_date = datetime.now().date()
    stat_str = stat_date.strftime("%Y-%m-%d")
    stat_year, stat_month = stat_date.year, stat_date.month

    tbl = f"calc_overdue_y{stat_year}m{stat_month:02d}"
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
        (tbl,)
    )
    if not cur.fetchone():
        # 尝试查找有数据的表
        for y in [2024, 2025, 2026, 2027]:
            for m in range(1, 13):
                t = f"calc_overdue_y{y}m{m:02d}"
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                    (t,)
                )
                if cur.fetchone():
                    cur.execute(f"SELECT MAX(stat_date)::date FROM {t}")
                    r = cur.fetchone()
                    if r and r[0]:
                        stat_date = r[0]
                        if hasattr(stat_date, 'date'):
                            stat_date = stat_date.date()
                        stat_str = stat_date.strftime("%Y-%m-%d")
                        stat_year, stat_month = stat_date.year, stat_date.month
                        tbl = t
                        break
            else:
                continue
            break

    print("=" * 60)
    print("全量数据 - 剩余加权久期")
    print("=" * 60)
    print(f"  数据日期 stat_date: {stat_str}")
    print(f"  表: {tbl}")
    print()

    # 全量：所有 spv_id 的活跃贷款
    # 剩余期限 = term_months - mob，mob = (stat_year - disb_year)*12 + (stat_month - disb_month)
    # 加权久期 = Σ(remaining_months × outstanding_principal) / Σ(outstanding_principal)
    cur.execute(f"""
        SELECT
            c.spv_id,
            c.loan_id,
            c.outstanding_principal,
            r.term_months,
            r.disbursement_time
        FROM {tbl} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.loan_status IN (1, 2)
    """, (stat_str,))

    rows = cur.fetchall()
    cols = [d[0].lower() for d in cur.description]
    cur.close()
    conn.close()

    if not rows:
        print("  无活跃贷款数据")
        return

    # 按 spv 和全量分别计算
    by_spv = {}
    total_weighted_sum = 0.0
    total_balance = 0.0
    valid_loan_count = 0

    for row in rows:
        rec = dict(zip(cols, row))
        spv_id = rec.get("spv_id", "")
        out_principal = float(rec.get("outstanding_principal") or 0)
        term_months = rec.get("term_months")
        disb_time = rec.get("disbursement_time")

        if not term_months or not disb_time or out_principal <= 0:
            continue

        dm = disb_time.strftime("%Y-%m") if hasattr(disb_time, "strftime") else str(disb_time)[:7]
        try:
            dm_year, dm_month = int(dm[:4]), int(dm[5:7])
        except (ValueError, TypeError):
            continue

        mob = (stat_year - dm_year) * 12 + (stat_month - dm_month)
        mob = max(0, mob)
        term = float(term_months) if isinstance(term_months, (int, float)) else int(term_months or 0)
        remaining_months = max(0.0, term - mob)

        weighted = remaining_months * out_principal
        total_weighted_sum += weighted
        total_balance += out_principal
        valid_loan_count += 1

        if spv_id not in by_spv:
            by_spv[spv_id] = {"weighted_sum": 0.0, "balance": 0.0}
        by_spv[spv_id]["weighted_sum"] += weighted
        by_spv[spv_id]["balance"] += out_principal

    # 输出
    print("【按 SPV 分拆】")
    for spv_id in sorted(by_spv.keys()):
        d = by_spv[spv_id]
        bal = d["balance"]
        ws = d["weighted_sum"]
        if bal > 0:
            wrd_months = ws / bal
            wrd_days = wrd_months * 30  # 近似
            print(f"  {spv_id}: 剩余加权久期 = {wrd_months:.2f} 月 ({wrd_days:.0f} 天), 在贷余额 = {bal:,.2f}")

    print()
    print("【全量汇总】")
    if total_balance > 0:
        wrd_months = total_weighted_sum / total_balance
        wrd_days = wrd_months * 30
        print(f"  剩余加权久期 = {wrd_months:.2f} 月 ({wrd_days:.0f} 天)")
        print(f"  在贷余额 = {total_balance:,.2f} 本币")
        print(f"  活跃贷款数 = {valid_loan_count}")
    else:
        print("  无有效数据")


if __name__ == "__main__":
    main()
