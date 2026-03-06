#!/usr/bin/env python3
"""
月度周转率计算

公式：V_m = 当月实际回收本金 / 月初在贷余额 (AUM)

- 当月实际回收本金：raw_repayment 中 repayment_date 落在当月的 principal_repayment 之和
- 月初在贷余额：calc_overdue 中上月最后一天的 outstanding_principal 之和（loan_status IN (1,2)）

运行：cd RT_RISK && python3 scripts/monthly_turnover_rate.py
"""
import os
import sys
from datetime import datetime
from calendar import monthrange

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def main():
    print("=" * 70)
    print("月度周转率 V_m = 当月实际回收本金 / 月初在贷余额 (AUM)")
    print("=" * 70)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    from kn_data_utils import get_calc_table, get_latest_data_date

    latest_dt = get_latest_data_date()
    if latest_dt:
        latest_month = latest_dt.strftime("%Y-%m") if hasattr(latest_dt, "strftime") else str(latest_dt)[:7]
        print(f"\n最新数据日对应月份: {latest_month}\n")

    for spv_id in ["docking", "kn"]:
        print(f"\n【{spv_id.upper()}】")
        print("-" * 60)

        # 1. 获取有数据的月份
        months = set()
        cur.execute("""
            SELECT DISTINCT to_char(rp.repayment_date::date, 'YYYY-MM')
            FROM raw_repayment rp
            JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
            WHERE rp.repayment_date IS NOT NULL
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                months.add(r[0])
        for year in [2024, 2025, 2026, 2027]:
            for m in range(1, 13):
                tbl = get_calc_table(year, m)
                try:
                    cur.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                        (tbl,),
                    )
                    if cur.fetchone():
                        cur.execute(
                            f"SELECT DISTINCT to_char(stat_date::date, 'YYYY-MM') FROM {tbl} WHERE spv_id = %s",
                            (spv_id,),
                        )
                        for r in cur.fetchall():
                            if r[0]:
                                months.add(r[0])
                except Exception:
                    continue

        months = sorted(months)
        if latest_dt:
            months = [m for m in months if m <= latest_month]
        if not months:
            print("  无可用月份数据")
            continue

        # 2. 当月实际回收本金（按月）
        principal_by_month = {}
        cur.execute("""
            SELECT to_char(rp.repayment_date::date, 'YYYY-MM'),
                   COALESCE(SUM(rp.principal_repayment), 0)
            FROM raw_repayment rp
            JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
            WHERE rp.repayment_date IS NOT NULL
            GROUP BY 1
        """, (spv_id,))
        for r in cur.fetchall():
            if r[0]:
                principal_by_month[r[0]] = float(r[1] or 0)

        # 3. 每月底在贷余额（用于下月月初 AUM）
        balance_by_month = {}
        prev_balance = 0
        for month_str in months:
            y, m = int(month_str[:4]), int(month_str[5:7])
            last_day = f"{month_str}-{monthrange(y, m)[1]:02d}"
            calc_tbl = get_calc_table(y, m)
            ob = 0
            try:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                    (calc_tbl,),
                )
                if cur.fetchone():
                    cur.execute(
                        f"""
                        SELECT MAX(stat_date)::date FROM {calc_tbl}
                        WHERE spv_id = %s AND stat_date::date <= %s::date
                        """,
                        (spv_id, last_day),
                    )
                    max_dt = cur.fetchone()
                    if max_dt and max_dt[0]:
                        cur.execute(
                            f"""
                            SELECT COALESCE(SUM(outstanding_principal), 0)
                            FROM {calc_tbl}
                            WHERE spv_id = %s AND loan_status IN (1, 2) AND stat_date::date = %s
                            """,
                            (spv_id, max_dt[0]),
                        )
                        row = cur.fetchone()
                        if row:
                            ob = float(row[0] or 0)
            except Exception:
                pass
            balance_by_month[month_str] = ob
            prev_balance = ob

        # 4. 计算月度周转率：V_m = 当月回收本金 / 月初AUM（=上月月底余额）
        rows = []
        prev_month_balance = 0
        for month_str in months:
            principal_repaid = principal_by_month.get(month_str, 0)
            begin_aum = prev_month_balance
            prev_month_balance = balance_by_month.get(month_str, 0)

            if begin_aum > 0:
                v_m = principal_repaid / begin_aum
            else:
                v_m = None  # 月初无余额，无法计算

            rows.append({
                "month": month_str,
                "principal_repaid": principal_repaid,
                "begin_aum": begin_aum,
                "v_m": v_m,
            })

        # 5. 输出（最近 24 个月或全部）
        display_rows = rows[-24:] if len(rows) > 24 else rows
        print(f"  {'月份':<8} {'月初AUM':>16} {'当月回收本金':>16} {'周转率 V_m':>12}")
        print("  " + "-" * 56)
        for r in display_rows:
            v_str = f"{r['v_m']:.2%}" if r["v_m"] is not None else "-"
            print(f"  {r['month']:<8} {r['begin_aum']:>16,.0f} {r['principal_repaid']:>16,.0f} {v_str:>12}")

        # 6. 汇总：有有效周转率的月份平均
        valid = [r["v_m"] for r in rows if r["v_m"] is not None]
        if valid:
            avg_v = sum(valid) / len(valid)
            print(f"\n  有效月份数: {len(valid)}, 平均周转率: {avg_v:.2%}")
            recent_valid = [r["v_m"] for r in rows[-12:] if r["v_m"] is not None]
            if recent_valid:
                print(f"  近12月平均周转率: {sum(recent_valid)/len(recent_valid):.2%}")

        # 7. 年化周转倍数 = Σ(近12月 V_m) × (1 - 年化核销率)，核销率=0 时 = Σ V_m
        writeoff_rate = 0
        last_12 = [r["v_m"] for r in rows[-12:] if r["v_m"] is not None]
        if last_12:
            sum_vm = sum(last_12)
            annual_turnover = sum_vm * (1 - writeoff_rate)
            print(f"\n  ★ 年化周转倍数 = Σ(近12月V_m) × (1-核销率) = {sum_vm:.4f} × (1-{writeoff_rate}) = {annual_turnover:.4f}")

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
