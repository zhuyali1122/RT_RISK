"""
KN Vintage 账龄分析 - 从 calc_overdue、raw_loan 计算，结果缓存到本地文件
"""
import json
import os
from datetime import datetime
from decimal import Decimal

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "config", "cache")
CACHE_FILE_PREFIX = "vintage_cache_"


def _get_calc_table(stat_date):
    """根据 stat_date 返回 calc_overdue 表名"""
    try:
        dt = datetime.strptime(stat_date[:10], "%Y-%m-%d")
        return f"calc_overdue_y{dt.year}m{dt.month:02d}"
    except (ValueError, TypeError):
        return None


def _serialize(val):
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime,)):
        return val.isoformat()
    return val


def compute_vintage_data(spv_id: str, stat_date: str):
    """
    从 calc_overdue + raw_loan 计算 vintage_data
    返回: [ { disbursement_month, disbursement_amount, current_balance, dpd1_rate, ... }, ... ]
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception as e:
        return {"error": str(e)}

    table = _get_calc_table(stat_date)
    if not table:
        return {"error": f"无效 stat_date: {stat_date}"}

    cur = conn.cursor()
    # 检查表存在
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = %s",
        (table,)
    )
    if not cur.fetchone():
        cur.close()
        conn.close()
        return {"error": f"表 {table} 不存在"}

    # 1. 各 cohort 的 disbursement 汇总（raw_loan）
    cur.execute("""
        SELECT
            to_char(disbursement_time::date, 'YYYY-MM') AS disbursement_month,
            SUM(disbursement_amount) AS disbursement_amount,
            COUNT(*) AS disbursement_count,
            COUNT(DISTINCT customer_id) AS borrower_count
        FROM raw_loan
        WHERE spv_id = %s
        GROUP BY 1
        ORDER BY 1
    """, (spv_id,))
    disb_rows = {r[0]: {"disbursement_amount": r[1], "disbursement_count": r[2], "borrower_count": r[3]} for r in cur.fetchall()}

    # 2. 各 cohort 的余额与 DPD 分布（calc_overdue + raw_loan）
    cur.execute(f"""
        SELECT
            to_char(r.disbursement_time::date, 'YYYY-MM') AS disbursement_month,
            COALESCE(SUM(c.outstanding_principal), 0) AS current_balance,
            COALESCE(SUM(CASE WHEN c.dpd >= 1 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_1_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 3 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_3_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 7 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_7_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 15 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_15_bal,
            COALESCE(SUM(CASE WHEN c.dpd >= 30 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_30_bal
        FROM {table} c
        JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
        WHERE c.stat_date = %s AND c.spv_id = %s AND c.loan_status IN (1, 2)
        GROUP BY 1
        ORDER BY 1
    """, (stat_date[:10], spv_id))
    balance_rows = cur.fetchall()

    cur.close()
    conn.close()

    # 3. 合并并计算 DPD 率、MOB 率
    stat_dt = datetime.strptime(stat_date[:10], "%Y-%m-%d")
    vintage_data = []
    for row in balance_rows:
        dm = row[0]
        current_balance = float(row[1] or 0)
        o1, o3, o7, o15, o30 = float(row[2] or 0), float(row[3] or 0), float(row[4] or 0), float(row[5] or 0), float(row[6] or 0)

        dpd1_rate = o1 / current_balance if current_balance else 0
        dpd3_rate = o3 / current_balance if current_balance else 0
        dpd7_rate = o7 / current_balance if current_balance else 0
        dpd15_rate = o15 / current_balance if current_balance else 0
        dpd30_rate = o30 / current_balance if current_balance else 0

        disb_info = disb_rows.get(dm, {})
        disbursement_amount = float(disb_info.get("disbursement_amount") or 0)
        disbursement_count = int(disb_info.get("disbursement_count") or 0)
        borrower_count = int(disb_info.get("borrower_count") or 0)

        # MOB = 完整月数。(stat_date 所在月 - disbursement_month)
        try:
            dm_year, dm_month = int(dm[:4]), int(dm[5:7])
            mob = (stat_dt.year - dm_year) * 12 + (stat_dt.month - dm_month)
            mob = max(0, mob)
        except (ValueError, TypeError):
            mob = 0

        mob_rates = {}
        for i in range(1, 13):
            if i == mob:
                mob_rates[f"mob{i}_rate"] = dpd1_rate  # 当前 MOB 的逾期率用 DPD1+ 近似
            else:
                mob_rates[f"mob{i}_rate"] = None

        vintage_data.append({
            "disbursement_month": dm,
            "mob": mob,
            "disbursement_amount": str(int(disbursement_amount)),
            "current_balance": str(int(current_balance)),
            "borrower_count": borrower_count,
            "disbursement_count": disbursement_count,
            "dpd1_rate": f"{dpd1_rate:.4f}",
            "dpd3_rate": f"{dpd3_rate:.4f}",
            "dpd7_rate": f"{dpd7_rate:.4f}",
            "dpd15_rate": f"{dpd15_rate:.4f}",
            "dpd30_rate": f"{dpd30_rate:.4f}",
            **mob_rates,
        })

    return vintage_data


def _cache_path(spv_id: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{CACHE_FILE_PREFIX}{spv_id}.json")


def load_vintage_cache(spv_id: str, stat_date: str = None):
    """
    从缓存文件加载 vintage_data
    stat_date: 若指定，仅当缓存 stat_date 匹配时返回；否则返回缓存内容
    返回: vintage_data 或 None
    """
    path = _cache_path(spv_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cached_stat = data.get("stat_date", "")[:10] if data.get("stat_date") else ""
        if stat_date and cached_stat != stat_date[:10]:
            return None
        return data.get("vintage_data")
    except Exception:
        return None


def save_vintage_cache(spv_id: str, stat_date: str, vintage_data: list):
    """将 vintage_data 写入缓存"""
    path = _cache_path(spv_id)
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "spv_id": spv_id,
            "stat_date": stat_date[:10],
            "last_updated": datetime.now().isoformat(),
            "vintage_data": vintage_data,
        }, f, ensure_ascii=False, indent=2)


def refresh_vintage_cache(spv_id: str, stat_date: str):
    """
    重新计算并保存 vintage 缓存
    返回: { "ok": True, "vintage_data": [...] } 或 { "error": "..." }
    """
    result = compute_vintage_data(spv_id, stat_date)
    if isinstance(result, dict) and "error" in result:
        return result
    save_vintage_cache(spv_id, stat_date, result)
    return {"ok": True, "vintage_data": result}
