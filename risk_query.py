"""
风控数据查询模块 - 连接 PostgreSQL 执行 Loan 与放款查询
"""
import json
import os
from datetime import datetime, date
from decimal import Decimal

BASE_DIR = os.path.dirname(__file__)


def _serialize(obj):
    """将 datetime/Decimal 转为 JSON 可序列化类型"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return obj
SCHEMA_PATH = os.path.join(BASE_DIR, "config", "query_schema.json")


def load_schema():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("comment") and isinstance(v, dict)}


def get_db():
    try:
        from db_connect import get_connection
        return get_connection()
    except Exception as e:
        return None


def _get_schedule_for_loan(cur, loan_id, schema):
    """获取单个 loan 的还款计划"""
    sched_cfg = schema.get("repayment_schedule", {})
    sched_cols = sched_cfg.get("columns", ["period_no", "due_date", "principal_due", "interest_due", "total_due"])
    schedule = []
    if sched_cfg.get("source") == "jsonb":
        sched_table = sched_cfg.get("table", "raw_loan")
        jsonb_col = sched_cfg.get("jsonb_column", "repayment_schedule")
        jsonb_path = sched_cfg.get("jsonb_path", "schedule")
        keys = sched_cfg.get("jsonb_keys", {"period_no": "term", "due_date": "due_date", "principal_due": "principal", "interest_due": "interest", "total_due": "total"})
        id_col = schema.get("loan", {}).get("id_column", "loan_id")
        cur.execute(
            f"SELECT term_months, {jsonb_col} FROM {sched_table} WHERE {id_col} = %s",
            (loan_id,)
        )
        row = cur.fetchone()
        if row:
            term_months, rs_json = row[0], row[1]
            schedule_arr = (rs_json or {}).get(jsonb_path, []) if isinstance(rs_json, dict) else []
            sched_by_term = {}
            for elem in schedule_arr:
                if not isinstance(elem, dict):
                    continue
                t = elem.get(keys["period_no"])
                if t is not None:
                    try:
                        term_no = int(t)
                    except (TypeError, ValueError):
                        continue
                    sched_by_term[term_no] = {
                        "period_no": term_no,
                        "due_date": elem.get(keys["due_date"]),
                        "principal_due": elem.get(keys["principal_due"]),
                        "interest_due": elem.get(keys["interest_due"]),
                        "total_due": elem.get(keys["total_due"]),
                    }
            total_periods = None
            if term_months is not None:
                try:
                    total_periods = int(term_months)
                except (TypeError, ValueError):
                    pass
            if total_periods is None and sched_by_term:
                total_periods = max(sched_by_term.keys())
            if total_periods is None:
                total_periods = 1
            for i in range(1, total_periods + 1):
                rec = sched_by_term.get(i) or {"period_no": i, "due_date": None, "principal_due": None, "interest_due": None, "total_due": None}
                schedule.append({k: _serialize(rec.get(k)) for k in sched_cols})
    else:
        sched_table = sched_cfg.get("table", "loan_repayment_schedule")
        sched_loan_col = sched_cfg.get("loan_id_column", "loan_id")
        sched_cols = sched_cfg.get("columns", ["period_no", "due_date", "principal_due", "interest_due", "total_due", "status"])
        sched_col_list = ", ".join(sched_cols)
        cur.execute(
            f'SELECT {sched_col_list} FROM {sched_table} WHERE {sched_loan_col} = %s ORDER BY period_no',
            (loan_id,)
        )
        for r in cur.fetchall():
            schedule.append({k: _serialize(v) for k, v in zip(sched_cols, r)})
    return schedule


def _get_records_for_loan(cur, loan_id, schema):
    """获取单个 loan 的还款记录"""
    rec_cfg = schema.get("repayment_records", {})
    rec_table = rec_cfg.get("table", "raw_repayment")
    rec_loan_col = rec_cfg.get("loan_id_column", "loan_id")
    rec_cols = rec_cfg.get("columns", ["repayment_type", "repayment_term", "repayment_date", "total_repayment", "principal_repayment", "interest_repayment", "penalty_repayment", "extension_fee", "waiver_amount", "repayment_txn_id", "is_settled"])
    rec_col_list = ", ".join(rec_cols)
    order_col = rec_cfg.get("order_column", "repayment_date")
    cur.execute(
        f'SELECT {rec_col_list} FROM {rec_table} WHERE {rec_loan_col} = %s ORDER BY {order_col}, repayment_term',
        (loan_id,)
    )
    return [{k: _serialize(v) for k, v in zip(rec_cols, r)} for r in cur.fetchall()]


def query_loan_detail(loan_id: str):
    """
    根据 Loan ID 查 Contract_No，再找同合同下所有 Loan，按放款时间展示每个 Loan 的还款计划和还款信息
    返回: { contract_no, loans: [{ loan_id, status, schedule, records }] } 或 { error: str }
    """
    schema = load_schema()
    conn = get_db()
    if not conn:
        return {"error": "数据库未配置或连接失败，请检查 .env 配置"}

    loan_id = (loan_id or "").strip()
    if not loan_id:
        return {"error": "请输入 Loan ID"}

    try:
        cur = conn.cursor()
        loan_cfg = schema.get("loan", {})
        loan_table = loan_cfg.get("table", "raw_loan")
        id_col = loan_cfg.get("id_column", "loan_id")
        cols = loan_cfg.get("columns", ["loan_id", "disbursement_time", "disbursement_amount", "term_months", "loan_maturity_date", "customer_id", "contract_no", "spv_id"])
        col_list = ", ".join(cols) if cols else "*"

        # 1. 查输入 loan_id 的 contract_no
        cur.execute(
            f'SELECT {col_list} FROM {loan_table} WHERE {id_col} = %s',
            (loan_id,)
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return {"error": f"未找到 Loan ID: {loan_id}"}

        first_status = {k: _serialize(v) for k, v in zip(cols, row)}
        contract_no = first_status.get("contract_no")
        if contract_no is None or contract_no == "":
            contract_no = "-"

        # 2. 找同 contract_no 的所有 loan_id，按 disbursement_time 排序（若 contract_no 为空则仅当前 loan）
        if contract_no == "-":
            loan_ids = [loan_id]
        else:
            cur.execute(
                f'SELECT {id_col} FROM {loan_table} WHERE contract_no = %s ORDER BY disbursement_time NULLS LAST, {id_col}',
                (contract_no,)
            )
            loan_ids = [r[0] for r in cur.fetchall()]

        # 3. 对每个 loan 获取 status、schedule、records
        loans = []
        for lid in loan_ids:
            cur.execute(f'SELECT {col_list} FROM {loan_table} WHERE {id_col} = %s', (lid,))
            r = cur.fetchone()
            status = {k: _serialize(v) for k, v in zip(cols, r)} if r else {}
            schedule = _get_schedule_for_loan(cur, lid, schema)
            records = _get_records_for_loan(cur, lid, schema)
            loans.append({"loan_id": lid, "status": status, "schedule": schedule, "records": records})

        cur.close()
        conn.close()
        return {"contract_no": contract_no, "loans": loans}
    except Exception as e:
        return {"error": str(e)}


def query_daily_disbursements(query_date: str):
    """
    查询某一天的所有新增放款
    返回: { disbursements: [], total_count: int, total_amount: float } 或 { error: str }
    """
    schema = load_schema()
    conn = get_db()
    if not conn:
        return {"error": "数据库未配置或连接失败，请检查 .env 配置"}

    result = {"disbursements": [], "total_count": 0, "total_amount": 0}
    query_date = (query_date or "").strip()
    if not query_date:
        return {"error": "请选择查询日期"}

    try:
        cur = conn.cursor()
        disb_cfg = schema.get("disbursements", {})
        table = disb_cfg.get("table", "loans")
        date_col = disb_cfg.get("date_column", "disbursement_date")
        cols = disb_cfg.get("columns", ["loan_id", "disbursement_date", "principal", "currency", "partner_id"])
        col_list = ", ".join(cols)

        order_col = disb_cfg.get("id_column", "loan_id")
        cur.execute(
            f'SELECT {col_list} FROM {table} WHERE ({date_col})::date = %s::date ORDER BY {order_col}',
            (query_date,)
        )
        rows = cur.fetchall()
        try:
            amount_col_idx = cols.index("principal") if "principal" in cols else -1
        except ValueError:
            amount_col_idx = -1

        for r in rows:
            row_dict = {k: _serialize(v) for k, v in zip(cols, r)}
            result["disbursements"].append(row_dict)
            if amount_col_idx >= 0:
                try:
                    result["total_amount"] += float(r[amount_col_idx] or 0)
                except (TypeError, ValueError):
                    pass

        result["total_count"] = len(rows)
        cur.close()
        conn.close()
    except Exception as e:
        result["error"] = str(e)
        try:
            conn.close()
        except Exception:
            pass

    return result
