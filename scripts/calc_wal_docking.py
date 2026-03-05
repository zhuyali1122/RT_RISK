#!/usr/bin/env python3
"""
计算单笔贷款的实际 WAL（Weighted Average Life）

公式：WAL = Σ(t × P_t) / P_total
- t：本金回收的时间点（以月为单位，从放款日算起）
- P_t：在 t 时刻回收的本金金额（计划还款 + 提前还款）
- P_total：该笔贷款发放的总本金

已结清贷款识别：
- 方式1：总回收本金 >= 放款本金（raw_repayment）
- 方式2：calc_overdue 中 loan_status=3（当方式1无结果时使用）

运行：cd RT_RISK && python scripts/calc_wal_docking.py [spv_id]
示例：python scripts/calc_wal_docking.py kn
      python scripts/calc_wal_docking.py docking
"""
import os
import sys
from datetime import datetime
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

DAYS_PER_MONTH = 30.44  # 365.25 / 12


def _get_closed_loan_ids_from_calc(cur, spv_id):
    """从 calc_overdue 获取 loan_status=3（结清）的 loan_id 集合"""
    from kn_data_utils import get_calc_table
    closed = set()
    for year in [2024, 2025, 2026, 2027]:
        for month in range(1, 13):
            tbl = get_calc_table(year, month)
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
                (tbl,)
            )
            if not cur.fetchone():
                continue
            try:
                cur.execute(
                    f"SELECT loan_id FROM {tbl} WHERE spv_id = %s AND loan_status = 3",
                    (spv_id,)
                )
                for r in cur.fetchall():
                    if r and r[0]:
                        closed.add(r[0])
            except Exception:
                continue
    return closed


def main():
    spv_id = (sys.argv[1] if len(sys.argv) > 1 else "docking").lower()
    print("=" * 70)
    print(f"{spv_id.upper()} 实际 WAL 计算（已结清贷款）")
    print("公式: WAL = Σ(t × P_t) / P_total")
    print("=" * 70)

    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    # 1. 获取所有 Docking 贷款的放款信息
    cur.execute("""
        SELECT loan_id, disbursement_time, disbursement_amount
        FROM raw_loan
        WHERE spv_id = %s AND disbursement_time IS NOT NULL AND disbursement_amount IS NOT NULL
    """, (spv_id,))
    loans = {}
    for row in cur.fetchall():
        loan_id, disb_time, disb_amt = row[0], row[1], float(row[2] or 0)
        if disb_amt <= 0:
            continue
        loans[loan_id] = {
            "disbursement_time": disb_time,
            "disbursement_amount": disb_amt,
            "repayments": [],
        }

    if not loans:
        print(f"  无 {spv_id} 贷款数据")
        cur.close()
        conn.close()
        return

    # 2. 获取所有本金回收记录
    cur.execute("""
        SELECT rp.loan_id, rp.repayment_date, rp.principal_repayment
        FROM raw_repayment rp
        JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
        WHERE rp.repayment_date IS NOT NULL
    """, (spv_id,))

    for row in cur.fetchall():
        loan_id, rep_date, principal = row[0], row[1], float(row[2] or 0)
        if loan_id not in loans or principal <= 0:
            continue
        loans[loan_id]["repayments"].append((rep_date, principal))

    # 3. 确定已结清贷款集合
    closed_ids = set()
    use_95_threshold = False
    for loan_id, data in loans.items():
        repayments = data["repayments"]
        if not repayments:
            continue
        p_total = data["disbursement_amount"]
        total_principal_repaid = sum(p for _, p in repayments)
        if total_principal_repaid >= p_total - 0.01:
            closed_ids.add(loan_id)

    # 若 raw_repayment 方式无已结清，尝试 calc_overdue loan_status=3
    if not closed_ids:
        closed_ids = _get_closed_loan_ids_from_calc(cur, spv_id)
        if closed_ids:
            print(f"  （通过 calc_overdue loan_status=3 识别已结清，共 {len(closed_ids)} 笔）")

    # 若仍无，放宽为回收本金>=95%放款（近似已结清，如 KN 等）
    if not closed_ids:
        for loan_id, data in loans.items():
            repayments = data["repayments"]
            if not repayments:
                continue
            p_total = data["disbursement_amount"]
            total_principal_repaid = sum(p for _, p in repayments)
            if total_principal_repaid >= p_total * 0.95:
                closed_ids.add(loan_id)
        if closed_ids:
            use_95_threshold = True
            print(f"  （无完全结清贷款，使用 回收本金>=95%放款 作为近似已结清，共 {len(closed_ids)} 笔）")

    # 4. 计算每笔已结清贷款的 WAL
    wal_list = []
    total_principal = 0.0
    total_weighted = 0.0

    for loan_id in closed_ids:
        if loan_id not in loans:
            continue
        data = loans[loan_id]
        disb_time = data["disbursement_time"]
        p_total = data["disbursement_amount"]
        repayments = data["repayments"]

        if not repayments:
            continue
        total_principal_repaid = sum(p for _, p in repayments)
        # calc_overdue 识别的已结清：要求回收本金 >= 95% 放款本金（避免数据不全）
        if total_principal_repaid < p_total * 0.95:
            continue

        # WAL = Σ(t × P_t) / P_total，t 以月为单位
        weighted_sum = 0.0
        disb_date = disb_time.date() if hasattr(disb_time, "date") else disb_time

        for rep_date, p_t in repayments:
            rep_d = rep_date.date() if hasattr(rep_date, "date") else rep_date
            t_days = (rep_d - disb_date).days
            t_months = t_days / DAYS_PER_MONTH
            weighted_sum += t_months * p_t

        wal = weighted_sum / p_total if p_total > 0 else 0
        wal_list.append((loan_id, wal, p_total, len(repayments)))
        total_principal += p_total
        total_weighted += wal * p_total

    # 5. 输出
    print(f"\n  spv_id: {spv_id}")
    print(f"  已结清贷款数: {len(wal_list)}")
    print(f"  总放款本金: {total_principal:,.2f}")
    if wal_list:
        wal_avg_simple = sum(w for _, w, _, _ in wal_list) / len(wal_list)
        wal_weighted = total_weighted / total_principal if total_principal > 0 else 0
        if use_95_threshold:
            print(f"  （注：基于回收本金>=95%放款的近似已结清贷款）")
        print(f"  简单平均 WAL: {wal_avg_simple:.2f} 月")
        print(f"  按本金加权平均 WAL: {wal_weighted:.2f} 月")
        print("\n  【前 20 笔明细】")
        for i, (lid, w, p, nrep) in enumerate(sorted(wal_list, key=lambda x: -x[2])[:20]):
            print(f"    {lid}: WAL={w:.2f} 月, 本金={p:,.2f}, 还款笔数={nrep}")
    else:
        print("  无已结清贷款（回收本金 >= 放款本金，且无 95% 近似已结清）")
        loans_with_rep = sum(1 for d in loans.values() if d["repayments"])
        print(f"\n  [诊断] raw_loan 贷款数: {len(loans)}, 有还款记录的: {loans_with_rep}")
        print(f"  [诊断] calc_overdue loan_status=3 已结清数: {len(_get_closed_loan_ids_from_calc(cur, spv_id))}")
        if closed_ids and loans_with_rep:
            overlap = [lid for lid in closed_ids if lid in loans and loans[lid]["repayments"]]
            print(f"  [诊断] 已结清且有还款记录的重叠数: {len(overlap)}")
            if overlap:
                sample = overlap[0]
                d = loans[sample]
                rep_sum = sum(p for _, p in d["repayments"])
                print(f"  [诊断] 示例 loan_id={sample}: 放款={d['disbursement_amount']:,.0f}, 回收本金={rep_sum:,.0f}, 比例={rep_sum/d['disbursement_amount']*100:.1f}%")

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
