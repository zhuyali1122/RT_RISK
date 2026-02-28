"""
生产商全量数据统一缓存 - 风控、收益、现金流一次性加载
- 登录时自动刷新（后台线程）
- PM 点击「刷新全部」时强制从 DB 重新加载
- 其他情况一律从缓存文件读取，提升访问速度
"""
import json
import os
import threading
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "config", "cache")
CACHE_FILE = os.path.join(CACHE_DIR, "producer_full_cache.json")


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def get_risk_data_from_full_cache(spv_id: str):
    """
    从统一缓存获取单个生产商 risk_data，供其他模块调用（避免循环导入 app）
    返回: (risk_data, cache_exists)
    - cache_exists=True: 缓存文件存在，仅用此数据，不再访问单独缓存
    - cache_exists=False: 无统一缓存，可回退到 load_risk_cache
    """
    producers, _ = load_producer_full_cache()
    if not producers:
        return None, False
    sid = str(spv_id or "").strip().lower()
    pc = producers.get(spv_id) or producers.get(sid)
    risk_data = (pc or {}).get("risk_data", []) if pc else []
    return risk_data, True


def load_producer_full_cache():
    """
    从缓存文件加载所有生产商数据
    返回: (data, last_updated) 或 (None, None)
    data = { spv_id: { "risk_data": [...], "revenue_data": [...], "cashflow_data": [...] }, ... }
    """
    if not os.path.isfile(CACHE_FILE):
        return None, None
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        producers = d.get("producers", {})
        last_updated = d.get("last_updated")
        if not producers:
            return None, last_updated
        return producers, last_updated
    except Exception:
        return None, None


def get_cache_debug_info():
    """调试用：返回缓存文件路径、是否存在、加载结果"""
    abs_path = os.path.abspath(CACHE_FILE)
    exists = os.path.isfile(CACHE_FILE)
    info = {
        "cache_file": abs_path,
        "file_exists": exists,
        "producer_count": 0,
        "last_updated": None,
        "load_ok": False,
        "error": None,
    }
    if not exists:
        info["error"] = "缓存文件不存在"
        return info
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        producers = d.get("producers", {})
        info["producer_count"] = len(producers)
        info["last_updated"] = d.get("last_updated")
        info["load_ok"] = bool(producers)
        if not producers:
            info["error"] = "producers 为空"
    except Exception as e:
        info["error"] = str(e)
    return info


def save_producer_full_cache(producers: dict):
    """保存生产商全量数据到缓存文件"""
    _ensure_cache_dir()
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "last_updated": datetime.now().isoformat(),
            "producers": producers,
        }, f, ensure_ascii=False, indent=2)


def refresh_producer_full_cache():
    """
    从数据库重新加载所有生产商的风控、收益、现金流数据并写入缓存
    返回: { "ok": True, "last_updated": "...", "producer_count": N } 或 { "error": "..." }
    """
    try:
        from spv_config import load_producers_from_spv_config
        producers_raw = load_producers_from_spv_config()
        if not producers_raw:
            from app import load_producers
            producers_raw = load_producers()
        if not producers_raw:
            return {"error": "无生产商数据"}

        producers_cache = {}
        for spv_id, prod in producers_raw.items():
            sid = str(spv_id).strip().lower()
            cfg = _get_producer_config(sid)
            rate = float((cfg.get("exchange_rate") if cfg else 1) or 1)
            currency = (cfg.get("currency") if cfg else "USD") or "USD"

            risk_data = []
            try:
                from kn_risk_cache import refresh_risk_cache, load_risk_cache
                refresh_risk_cache(sid, rate, currency)
                merged, _ = load_risk_cache(sid)
                if merged:
                    risk_data = merged
            except Exception:
                pass

            revenue_data = []
            try:
                from kn_revenue_cache import refresh_revenue_cache
                r = refresh_revenue_cache(sid, rate, currency)
                if "revenue_data" in r:
                    revenue_data = r["revenue_data"]
            except Exception:
                pass

            coll_rate = 0.98
            if revenue_data:
                coll_rate = revenue_data[-1].get("collection_rate", 0.98) or 0.98
            cashflow_data = []
            try:
                from kn_cashflow_cache import refresh_cashflow_cache
                r = refresh_cashflow_cache(sid, rate, currency, coll_rate)
                if "forecast" in r:
                    cashflow_data = r["forecast"]
            except Exception:
                pass

            producers_cache[sid] = {
                "risk_data": risk_data,
                "revenue_data": revenue_data,
                "cashflow_data": cashflow_data,
                "exchange_rate": rate,
                "currency": currency,
            }

        save_producer_full_cache(producers_cache)
        return {
            "ok": True,
            "last_updated": datetime.now().isoformat(),
            "producer_count": len(producers_cache),
        }
    except Exception as e:
        return {"error": str(e)}


def _get_producer_config(spv_id):
    """获取生产商配置（避免循环导入，从 app 延迟导入）"""
    try:
        from app import _get_producer_config
        return _get_producer_config(spv_id)
    except Exception:
        return None


def refresh_producer_full_cache_async():
    """后台线程刷新缓存，不阻塞主流程"""
    def _run():
        try:
            refresh_producer_full_cache()
        except Exception:
            pass
    t = threading.Thread(target=_run, daemon=True)
    t.start()
