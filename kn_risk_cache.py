"""
生产商风控数据统一缓存 - 核心指标、DPD、Vintage 等一次性缓存
打开页面时直接读缓存，无需访问数据库；用户点击刷新时从 DB 拉取并更新缓存
"""
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "config", "cache")
CACHE_FILE_PREFIX = "risk_cache_"


def _cache_path(spv_id: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{CACHE_FILE_PREFIX}{spv_id}.json")


def _to_usd(val, rate):
    if val is None or val == "" or not rate or rate <= 0:
        return val
    try:
        return str(int(float(val) / rate))
    except (ValueError, TypeError):
        return val


def load_risk_cache(spv_id: str):
    """
    从缓存加载 risk_data（含 local currency 如 MXN 和 USD 两部分）
    返回: (risk_data_merged, last_updated)，每行含 _usd 子对象
    """
    path = _cache_path(spv_id)
    if not os.path.exists(path):
        return None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        local_data = data.get("risk_data", [])
        usd_data = data.get("risk_data_usd", [])
        if not local_data:
            return None, data.get("last_updated")
        if usd_data and len(usd_data) == len(local_data):
            for i, row in enumerate(local_data):
                row["_usd"] = usd_data[i] if i < len(usd_data) else {}
        else:
            for row in local_data:
                row["_usd"] = {}
        return local_data, data.get("last_updated")
    except Exception:
        return None, None


def save_risk_cache(spv_id: str, risk_data_local: list, risk_data_usd: list, currency: str = "USD", exchange_rate: float = 1):
    """保存 local currency（如 MXN）和 USD 两部分到缓存"""
    path = _cache_path(spv_id)
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "spv_id": spv_id,
            "currency": currency,
            "exchange_rate": exchange_rate,
            "last_updated": datetime.now().isoformat(),
            "risk_data": risk_data_local,
            "risk_data_usd": risk_data_usd,
        }, f, ensure_ascii=False, indent=2)


def refresh_risk_cache(spv_id: str, exchange_rate: float = 1, currency: str = "USD"):
    """
    从数据库计算 risk_data，同时保存 local currency（如 MXN）和 USD 两部分
    返回: { "ok": True, "risk_data": [...], "last_updated": "..." } 或 { "error": "..." }
    """
    try:
        from kn_risk_query import query_kn_core_metrics, get_available_stat_dates
        from kn_vintage import compute_vintage_data
        from copy import deepcopy
    except ImportError as e:
        return {"error": str(e)}

    dates = get_available_stat_dates(spv_id=spv_id, limit=14)
    if not dates:
        dates = ["2026-02-25"]

    risk_data_local = []
    for d in dates:
        row = query_kn_core_metrics(stat_date=d, spv_id=spv_id)
        if "error" in row:
            continue
        vintage = compute_vintage_data(spv_id, d)
        if isinstance(vintage, list):
            row["vintage_data"] = vintage
        else:
            row["vintage_data"] = []
        risk_data_local.append(row)

    if not risk_data_local:
        row = query_kn_core_metrics(stat_date=dates[0], spv_id=spv_id)
        if "error" not in row:
            vintage = compute_vintage_data(spv_id, dates[0])
            row["vintage_data"] = vintage if isinstance(vintage, list) else []
            risk_data_local = [row]

    if not risk_data_local:
        return {"error": "无可用数据"}

    rate = exchange_rate or 1
    risk_data_usd = []
    for row in risk_data_local:
        r = deepcopy(row)
        r["cumulative_disbursement"] = _to_usd(r.get("cumulative_disbursement"), rate)
        r["current_balance"] = _to_usd(r.get("current_balance"), rate)
        r["cash"] = _to_usd(r.get("cash"), rate)
        r["m0_balance"] = _to_usd(r.get("m0_balance"), rate)
        r["m0_accrued_interest"] = _to_usd(r.get("m0_accrued_interest"), rate)
        for d in r.get("dpd_distribution", []):
            d["balance"] = _to_usd(d.get("balance"), rate)
        for v in r.get("vintage_data", []):
            for k in ("disbursement_amount", "current_balance"):
                if k in v:
                    v[k] = _to_usd(v[k], rate)
        for c in r.get("collection_report", []):
            for k in ("due_amount", "d0_into_collection", "d1_into_collection", "d3_into_collection",
                      "d7_into_collection", "d30_into_collection", "d60_into_collection", "d90_into_collection",
                      "d1_recovery", "d3_recovery", "d7_recovery", "d30_recovery", "d60_recovery", "d90_recovery"):
                if k in c:
                    c[k] = _to_usd(c[k], rate)
        risk_data_usd.append(r)

    save_risk_cache(spv_id, risk_data_local, risk_data_usd, currency, rate)
    return {
        "ok": True,
        "risk_data": risk_data_local,
        "last_updated": datetime.now().isoformat(),
    }
