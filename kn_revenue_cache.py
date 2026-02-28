"""
生产商收益数据统一缓存 - 收益规模页面数据一次性缓存
打开页面时直接读缓存，无需访问数据库；用户点击刷新时从 DB 拉取并更新缓存
"""
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "config", "cache")
CACHE_FILE_PREFIX = "revenue_cache_"


def _cache_path(spv_id: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{CACHE_FILE_PREFIX}{spv_id}.json")


def load_revenue_cache(spv_id: str):
    """
    从缓存加载 revenue_data
    返回: (revenue_data, last_updated)，无缓存时返回 (None, None)
    """
    path = _cache_path(spv_id)
    if not os.path.exists(path):
        return None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        revenue_data = data.get("revenue_data", [])
        if not revenue_data:
            return None, data.get("last_updated")
        return revenue_data, data.get("last_updated")
    except Exception:
        return None, None


def save_revenue_cache(spv_id: str, revenue_data: list, currency: str = "USD", exchange_rate: float = 1):
    """保存 revenue_data 到缓存"""
    path = _cache_path(spv_id)
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "spv_id": spv_id,
            "currency": currency,
            "exchange_rate": exchange_rate,
            "last_updated": datetime.now().isoformat(),
            "revenue_data": revenue_data,
        }, f, ensure_ascii=False, indent=2)


def refresh_revenue_cache(spv_id: str, exchange_rate: float = 1, currency: str = "USD"):
    """
    从数据库计算 revenue_data 并保存到缓存
    返回: { "ok": True, "revenue_data": [...], "last_updated": "..." } 或 { "error": "..." }
    """
    try:
        from kn_revenue import compute_revenue_data
    except ImportError as e:
        return {"error": str(e)}

    revenue_data = compute_revenue_data(spv_id=spv_id)
    if not revenue_data:
        return {"error": "无可用数据"}

    save_revenue_cache(spv_id, revenue_data, currency, exchange_rate or 1)
    return {
        "ok": True,
        "revenue_data": revenue_data,
        "last_updated": datetime.now().isoformat(),
    }
