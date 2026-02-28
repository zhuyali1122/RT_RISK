"""
生产商现金流预测统一缓存 - 现金流页面数据一次性缓存
打开页面时直接读缓存，无需访问数据库；用户点击刷新时从 DB 拉取并更新缓存
"""
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "config", "cache")
CACHE_FILE_PREFIX = "cashflow_cache_"


def _cache_path(spv_id: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{CACHE_FILE_PREFIX}{spv_id}.json")


def load_cashflow_cache(spv_id: str):
    """
    从缓存加载 cashflow forecast 数据
    返回: (forecast_list, last_updated)，无缓存时返回 (None, None)
    """
    path = _cache_path(spv_id)
    if not os.path.exists(path):
        return None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        forecast = data.get("forecast", [])
        return forecast, data.get("last_updated")
    except Exception:
        return None, None


def save_cashflow_cache(spv_id: str, forecast: list, total_expected: float = 0,
                       currency: str = "USD", exchange_rate: float = 1):
    """保存现金流预测到缓存"""
    path = _cache_path(spv_id)
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "spv_id": spv_id,
            "currency": currency,
            "exchange_rate": exchange_rate,
            "last_updated": datetime.now().isoformat(),
            "forecast": forecast,
            "total_expected": total_expected,
        }, f, ensure_ascii=False, indent=2)


def refresh_cashflow_cache(spv_id: str, exchange_rate: float = 1, currency: str = "USD",
                          collection_rate: float = 0.98):
    """
    从数据库计算现金流预测并保存到缓存
    返回: { "ok": True, "forecast": [...], "last_updated": "..." } 或 { "error": "..." }
    """
    try:
        from kn_cashflow import compute_cashflow_forecast
    except ImportError as e:
        return {"error": str(e)}

    cf = compute_cashflow_forecast(spv_id=spv_id, months_ahead=12, collection_rate=collection_rate)
    forecast = cf.get("forecast", [])
    total_expected = cf.get("total_expected", 0)

    save_cashflow_cache(spv_id, forecast, total_expected, currency, exchange_rate or 1)
    return {
        "ok": True,
        "forecast": forecast,
        "total_expected": total_expected,
        "last_updated": datetime.now().isoformat(),
    }
