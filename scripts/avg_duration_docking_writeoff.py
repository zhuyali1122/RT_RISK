#!/usr/bin/env python3
"""
Docking 平均久期计算（按天，考虑核销观察期）

仅考虑已走完整个周期的 Loan：
1. 已结清：按原有方式，分子 = Σ(回收天数 × 回收本金)，分母 = 放款本金
2. 超过到期日未结清：设定核销观察期（Docking=30天）
   - 若超过观察期仍未还清，视同在观察期后彻底终止
   - 分子：已回收部分 = 每笔实际回收本金 × 对应天数；未回收部分 = 0
   - 分母：仅使用最终实际收回的本金总额（非原始发放额）
   - 公式：WAL_active = Σ(实收本金 × t) / Σ(实收本金)

已结清：分母 = 放款本金
逾期过观察期：分母 = 实收本金（观察期内回收之和）

运行：cd RT_RISK && python scripts/avg_duration_docking_writeoff.py
"""
import os
import sys
import json
from datetime import datetime, date, timedelta

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

SPV_ID = "docking"
WRITEOFF_OBSERVATION_DAYS = 30  # Docking 核销观察期 30 天


def get_maturity_date(loan_maturity_date, repayment_schedule):
    """从 loan_maturity_date 或 repayment_schedule 最后一期 due_date 获取到期日"""
    if loan_maturity_date:
        d = loan_maturity_date
        return d.date() if hasattr(d, "date") else d
    if not repayment_schedule:
        return None
    if isinstance(repayment_schedule, str):
        try:
            repayment_schedule = json.loads(repayment_schedule)
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(repayment_schedule, dict):
        return None
    schedule = repayment_schedule.get("schedule") or []
    if not schedule:
        return None
    last_elem = schedule[-1] if isinstance(schedule, list) else None
    if not last_elem or not isinstance(last_elem, dict):
        return None
    due_str = last_elem.get("due_date")
    if not due_str:
        return None
    try:
        return datetime.strptime(str(due_str)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def main():
    print("=" * 70)
    print(f"Docking 平均久期（按天，核销观察期={WRITEOFF_OBSERVATION_DAYS}天）")
    print("仅考虑已走完整个周期的 Loan：已结清 + 超过到期日且过观察期未结清")
    print("=" * 70)

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
        stat_date = date.today()
    if hasattr(stat_date, "date"):
        stat_date = stat_date.date()
    stat_str = stat_date.strftime("%Y-%m-%d") if hasattr(stat_date, "strftime") else str(stat_date)[:10]
    print(f"\nstat_date（参考日）: {stat_str}\n")

    # 1. 获取 Docking 贷款：loan_id, disbursement_time, disbursement_amount, loan_maturity_date, repayment_schedule
    cur.execute("""
        SELECT loan_id, disbursement_time, disbursement_amount,
               loan_maturity_date, repayment_schedule
        FROM raw_loan
        WHERE spv_id = %s AND disbursement_time IS NOT NULL AND disbursement_amount IS NOT NULL
    """, (SPV_ID,))
    loans = {}
    for row in cur.fetchall():
        loan_id = row[0]
        disb_time = row[1]
        disb_amt = float(row[2] or 0)
        loan_maturity = row[3]
        rep_schedule = row[4]
        if disb_amt <= 0:
            continue
        maturity_date = get_maturity_date(loan_maturity, rep_schedule)
        loans[loan_id] = {
            "disbursement_time": disb_time,
            "disbursement_amount": disb_amt,
            "maturity_date": maturity_date,
        }

    if not loans:
        print(f"  无 {SPV_ID} 贷款数据")
        cur.close()
        conn.close()
        return

    # 2. 获取所有本金回收记录
    cur.execute("""
        SELECT rp.loan_id, rp.repayment_date, rp.principal_repayment
        FROM raw_repayment rp
        JOIN raw_loan rl ON rl.loan_id = rp.loan_id AND rl.spv_id = %s
        WHERE rp.repayment_date IS NOT NULL
    """, (SPV_ID,))

    for loan_id, rep_date, principal in cur.fetchall():
        if loan_id not in loans or not principal:
            continue
        p = float(principal or 0)
        if p <= 0:
            continue
        if "repayments" not in loans[loan_id]:
            loans[loan_id]["repayments"] = []
        loans[loan_id]["repayments"].append((rep_date, p))

    # 3. 筛选已走完整个周期的 Loan
    completed_loans = []
    for loan_id, data in loans.items():
        disb_time = data["disbursement_time"]
        disb_amt = data["disbursement_amount"]
        maturity_date = data["maturity_date"]
        repayments = data.get("repayments") or []

        disb_date = disb_time.date() if hasattr(disb_time, "date") else disb_time
        if isinstance(disb_date, str):
            disb_date = datetime.strptime(disb_date[:10], "%Y-%m-%d").date()

        total_repaid = sum(p for _, p in repayments)

        # 已结清：回收本金 >= 放款本金（允许 0.01 误差）
        is_settled = total_repaid >= disb_amt - 0.01

        # 超过到期日未结清：需满足 maturity + 观察期 <= stat_date 才视为"走完周期"
        if maturity_date is None:
            if not is_settled:
                continue  # 无到期日且未结清，无法判断
            # 已结清但无到期日，仍纳入
        else:
            cutoff_date = maturity_date + timedelta(days=WRITEOFF_OBSERVATION_DAYS)
            if not is_settled and cutoff_date > stat_date:
                continue  # 未过观察期，尚未"走完周期"

        completed_loans.append({
            "loan_id": loan_id,
            "disb_date": disb_date,
            "disb_amt": disb_amt,
            "maturity_date": maturity_date,
            "repayments": repayments,
            "is_settled": is_settled,
        })

    # 4. 计算每笔贷款的分子、分母（按天）
    total_numerator = 0.0
    total_denominator = 0.0
    settled_count = 0
    overdue_count = 0
    settled_numerator = 0.0
    settled_denominator = 0.0
    overdue_numerator = 0.0
    overdue_denominator = 0.0

    for loan in completed_loans:
        disb_date = loan["disb_date"]
        disb_amt = loan["disb_amt"]
        repayments = loan["repayments"]
        is_settled = loan["is_settled"]
        maturity_date = loan["maturity_date"]

        if is_settled:
            # 已结清：所有回收均计入
            cutoff_date = None
        else:
            # 超过到期日未结清：仅计入观察期内的回收（rep_date <= maturity + 30）
            cutoff_date = maturity_date + timedelta(days=WRITEOFF_OBSERVATION_DAYS) if maturity_date else None

        numerator = 0.0
        recovered_in_period = 0.0  # 观察期内实收本金（逾期单用）
        for rep_date, principal in repayments:
            rep_d = rep_date.date() if hasattr(rep_date, "date") else rep_date
            if isinstance(rep_d, str):
                rep_d = datetime.strptime(rep_d[:10], "%Y-%m-%d").date()
            if cutoff_date is not None and rep_d > cutoff_date:
                continue  # 超过观察期后的回收不计入
            days = (rep_d - disb_date).days
            if days < 0:
                days = 0
            numerator += days * principal
            recovered_in_period += principal

        # 已结清：分母=放款本金；逾期过观察期：分母=实收本金（观察期内回收）
        if is_settled:
            denominator = disb_amt
        else:
            denominator = recovered_in_period  # WAL_active = Σ(实收×t)/Σ(实收)

        # 逾期且实收为0：不参与计算（0/0 无意义）
        if not is_settled and denominator <= 0:
            continue

        total_numerator += numerator
        total_denominator += denominator

        if is_settled:
            settled_count += 1
            settled_numerator += numerator
            settled_denominator += denominator
        else:
            overdue_count += 1
            overdue_numerator += numerator
            overdue_denominator += denominator

    # 5. 输出
    print(f"【{SPV_ID.upper()}】")
    print(f"  参与计算贷款数: {len(completed_loans)}（已结清 {settled_count} 笔，逾期过观察期 {overdue_count} 笔）")
    print(f"  分母合计（已结清用放款本金，逾期用实收本金）: {total_denominator:,.2f}")
    print(f"  分子合计 Σ(天数×回收本金): {total_numerator:,.0f}")

    if total_denominator > 0:
        avg_duration_days = total_numerator / total_denominator
        avg_duration_months = avg_duration_days / 30.44
        print(f"\n  ★ 平均久期（按天）: {avg_duration_days:.1f} 天")
        print(f"  ★ 平均久期（按月，30.44天/月）: {avg_duration_months:.2f} 月")

        if settled_count > 0 and settled_denominator > 0:
            s_days = settled_numerator / settled_denominator
            print(f"\n  [已结清子集] 贷款数={settled_count}, 平均久期={s_days:.1f} 天")
        if overdue_count > 0 and overdue_denominator > 0:
            o_days = overdue_numerator / overdue_denominator
            print(f"  [逾期过观察期子集] 贷款数={overdue_count}, 实收本金={overdue_denominator:,.2f}, WAL_active={o_days:.1f} 天")
    else:
        print("  无有效数据可计算")

    cur.close()
    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
