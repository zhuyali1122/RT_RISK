"""
生产商全量数据统一缓存 - 风控、收益、现金流一次性加载
- PM/Investor 仅从缓存读取，不访问数据库
- Admin 通过 Dashboard「管理」按钮每日刷新全量缓存
- 缓存保留最多 30 天历史
"""
import json
import os
import threading
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Vercel 等 serverless 仅 /tmp 可写，部署环境 config/cache 在 .gitignore 中不存在
_CACHE_BASE = os.path.join("/tmp", "rt_risk_cache") if os.getenv("VERCEL") else os.path.join(BASE_DIR, "config", "cache")
CACHE_DIR = _CACHE_BASE
DAILY_CACHE_DIR = os.path.join(CACHE_DIR, "daily")
CACHE_FILE = os.path.join(CACHE_DIR, "producer_full_cache.json")
CACHE_META_FILE = os.path.join(CACHE_DIR, "cache_meta.json")
REFRESH_LOG_FILE = os.path.join(CACHE_DIR, "refresh_log.txt")
CACHE_RETENTION_DAYS = 30


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(DAILY_CACHE_DIR, exist_ok=True)


def _purge_old_daily_cache():
    """删除超过 30 天的每日缓存文件"""
    if not os.path.isdir(DAILY_CACHE_DIR):
        return
    cutoff = datetime.now() - timedelta(days=CACHE_RETENTION_DAYS)
    for f in os.listdir(DAILY_CACHE_DIR):
        if not f.endswith(".json"):
            continue
        path = os.path.join(DAILY_CACHE_DIR, f)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
        except Exception:
            pass


def get_risk_data_from_full_cache(spv_id: str):
    """
    从统一缓存获取单个生产商 risk_data，供其他模块调用（避免循环导入 app）
    返回: (risk_data, cache_exists)
    - cache_exists=True: 缓存文件存在，仅用此数据，不再访问单独缓存
    - cache_exists=False: 无统一缓存，可回退到 load_risk_cache
    """
    data, _ = load_producer_full_cache()
    if not data:
        return None, False
    producers = data.get("producers", {}) if isinstance(data, dict) else data
    if not producers:
        return None, False
    sid = str(spv_id or "").strip().lower()
    pc = producers.get(spv_id) or producers.get(sid)
    risk_data = (pc or {}).get("risk_data", []) if pc else []
    return risk_data, True


# 进程级缓存：文件未变更时复用，避免重复解析大 JSON
_producer_cache_memory = None
_producer_cache_mtime = 0
_cache_meta_memory = None
_cache_meta_mtime = 0


def load_producer_full_cache():
    """
    从缓存文件加载所有生产商数据及投资组合数据
    1. 请求内复用（Flask g）
    2. 进程内复用（文件未变更时）
    返回: (data, last_updated) 或 (None, None)
    """
    global _producer_cache_memory, _producer_cache_mtime
    try:
        from flask import g
        if hasattr(g, "_rt_producer_full_cache"):
            return g._rt_producer_full_cache
    except RuntimeError:
        pass  # 非请求上下文（如后台刷新线程）
    except Exception:
        pass

    if not os.path.isfile(CACHE_FILE):
        return None, None
    try:
        mtime = os.path.getmtime(CACHE_FILE)
        if _producer_cache_memory is not None and mtime == _producer_cache_mtime:
            result = _producer_cache_memory
        else:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            producers = d.get("producers", {})
            last_updated = d.get("last_updated")
            result = (
                {
                    "producers": producers,
                    "portfolio_cumulative_stats": d.get("portfolio_cumulative_stats"),
                    "allocation_by_platform": d.get("allocation_by_platform"),
                    "system_cutover_date": d.get("system_cutover_date"),
                },
                last_updated,
            )
            _producer_cache_memory = result
            _producer_cache_mtime = mtime
        try:
            from flask import g
            g._rt_producer_full_cache = result
        except (RuntimeError, Exception):
            pass
        return result
    except Exception:
        return None, None


_cache_meta_memory = None
_cache_meta_mtime = 0


def load_cache_meta():
    """
    加载缓存元数据（轻量，供全局展示）
    进程内复用，保存时失效
    """
    global _cache_meta_memory, _cache_meta_mtime
    if not os.path.isfile(CACHE_META_FILE):
        return None
    try:
        mtime = os.path.getmtime(CACHE_META_FILE)
        if _cache_meta_memory is not None and mtime == _cache_meta_mtime:
            return _cache_meta_memory
        with open(CACHE_META_FILE, "r", encoding="utf-8") as f:
            out = json.load(f)
        _cache_meta_memory = out
        _cache_meta_mtime = mtime
        return out
    except Exception:
        return None


def save_producer_full_cache(payload: dict):
    """
    保存全量缓存到主文件，并归档到每日目录（保留 30 天）
    同时写入 cache_meta.json 供全局展示
    payload: { producers, portfolio_cumulative_stats?, allocation_by_platform?, system_cutover_date? }
    """
    global _producer_cache_memory, _producer_cache_mtime, _cache_meta_memory, _cache_meta_mtime
    _producer_cache_memory = None
    _producer_cache_mtime = 0
    _cache_meta_memory = None
    _cache_meta_mtime = 0
    _ensure_cache_dir()
    now = datetime.now()
    last_updated = now.isoformat()
    system_cutover_date = payload.get("system_cutover_date")
    data = {
        "last_updated": last_updated,
        "system_cutover_date": system_cutover_date,
        "producers": payload.get("producers", {}),
        "portfolio_cumulative_stats": payload.get("portfolio_cumulative_stats"),
        "allocation_by_platform": payload.get("allocation_by_platform"),
    }
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # 轻量元数据，供 top_bar 等全局展示
    try:
        with open(CACHE_META_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "last_updated": last_updated,
                "system_cutover_date": system_cutover_date or "",
            }, f, ensure_ascii=False)
    except Exception:
        pass
    # 每日归档
    daily_path = os.path.join(DAILY_CACHE_DIR, f"producer_full_cache_{now.strftime('%Y-%m-%d')}.json")
    try:
        with open(daily_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _purge_old_daily_cache()
    except Exception:
        pass


def _append_log(logs: list, msg: str, truncate_first: bool = False):
    """追加日志并写入文件。truncate_first=True 时先清空文件（每次刷新开始时调用）"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    logs.append(line)
    try:
        _ensure_cache_dir()
        mode = "w" if truncate_first else "a"
        with open(REFRESH_LOG_FILE, mode, encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def refresh_producer_full_cache():
    """
    从数据库重新加载所有生产商的风控、收益、现金流数据并写入缓存
    返回: { "ok": True, "last_updated": "...", "system_cutover_date": "...", "producer_count": N, "logs": [...] } 或 { "error": "..." }
    """
    logs = []
    try:
        _append_log(logs, "开始刷新全量缓存...", truncate_first=True)
        from spv_config import load_producers_from_spv_config
        producers_raw = load_producers_from_spv_config()
        if not producers_raw:
            from app import load_producers
            producers_raw = load_producers()
        if not producers_raw:
            _append_log(logs, "错误: 无生产商数据")
            return {"error": "无生产商数据", "logs": logs}

        _append_log(logs, f"加载到 {len(producers_raw)} 个生产商")
        producers_cache = {}
        all_stat_dates = []
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
                    for r in merged:
                        sd = r.get("stat_date")
                        if sd:
                            all_stat_dates.append(sd)
                _append_log(logs, f"  {sid}: 风控 {len(risk_data)} 条")
            except Exception as e:
                _append_log(logs, f"  {sid}: 风控失败 - {e}")

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
                cr = revenue_data[-1].get("collection_rate", 0.98) or 0.98
                coll_rate = cr if cr >= 0.5 else (revenue_data[-2].get("collection_rate", 0.98) or 0.98 if len(revenue_data) >= 2 else 0.98)
            cashflow_data = []
            try:
                from kn_cashflow_cache import refresh_cashflow_cache
                r = refresh_cashflow_cache(sid, rate, currency, coll_rate)
                if "forecast" in r:
                    cashflow_data = r["forecast"]
            except Exception:
                pass

            priority_indicators = None
            try:
                from spv_internal_params import load_priority_indicators_for_spv, compute_priority_from_risk_data
                pi = load_priority_indicators_for_spv(sid, risk_data=risk_data, exchange_rate=rate)
                if pi:
                    priority_indicators = pi
                elif risk_data:
                    pi = compute_priority_from_risk_data(sid, risk_data, rate)
                    if pi:
                        priority_indicators = pi
                if not priority_indicators or (not priority_indicators.get("priority_principal") and not priority_indicators.get("priority_yield")):
                    _append_log(logs, f"  {sid}: 优先级指标缺失（spv_internal_params 无数据或 principal_amount=0）")
            except Exception as e:
                _append_log(logs, f"  {sid}: 优先级指标加载失败 - {e}")

            producers_cache[sid] = {
                "risk_data": risk_data,
                "revenue_data": revenue_data,
                "cashflow_data": cashflow_data,
                "exchange_rate": rate,
                "currency": currency,
                "priority_indicators": priority_indicators,
                "id": sid,
                "name": prod.get("name", sid),
                "region": prod.get("region", prod.get("country", "-")),
                "product_type": prod.get("product_type", "-"),
                "status": prod.get("status", "active"),
                "onboard_date": prod.get("onboard_date", "-"),
                "contact": prod.get("contact", "-"),
            }

        # 投资组合累计统计与平台持仓
        portfolio_cumulative_stats = None
        allocation_by_platform = None
        try:
            from spv_internal_params import load_invested_spv_ids_for_portfolio, load_all_spv_internal_params_for_portfolio
            from kn_risk_query import query_portfolio_cumulative_stats
            spv_ids = load_invested_spv_ids_for_portfolio()
            portfolio_cumulative_stats = query_portfolio_cumulative_stats(spv_ids)
            trades = load_all_spv_internal_params_for_portfolio()
            if trades:
                total_principal = sum(t.get("principal_amount") or 0 for t in trades)
                allocation_by_platform = []
                for t in trades:
                    pct = (t.get("principal_amount") or 0) / total_principal if total_principal > 0 else 0
                    agreed = t.get("agreed_rate") or 0
                    agreed_pct = agreed * 100 if agreed <= 1 else agreed
                    allocation_by_platform.append({
                        "name": t.get("name") or t.get("spv_id") or "-",
                        "value": t.get("principal_amount") or 0,
                        "pct": pct,
                        "type": t.get("product_type") or "-",
                        "region": t.get("region") or "-",
                        "principal_amount": t.get("principal_amount"),
                        "agreed_rate": agreed_pct,
                        "effective_date": t.get("effective_date") or "-",
                    })
        except Exception:
            pass

        system_cutover_date = max(all_stat_dates) if all_stat_dates else ""
        _append_log(logs, f"系统切日: {system_cutover_date or '(无)'}")

        save_producer_full_cache({
            "producers": producers_cache,
            "portfolio_cumulative_stats": portfolio_cumulative_stats,
            "allocation_by_platform": allocation_by_platform,
            "system_cutover_date": system_cutover_date,
        })
        last_updated = datetime.now().isoformat()
        _append_log(logs, f"刷新完成，共 {len(producers_cache)} 个生产商")
        return {
            "ok": True,
            "last_updated": last_updated,
            "system_cutover_date": system_cutover_date,
            "producer_count": len(producers_cache),
            "logs": logs,
        }
    except Exception as e:
        _append_log(logs, f"错误: {e}")
        return {"error": str(e), "logs": logs}


def _get_producer_config(spv_id):
    """获取生产商配置（避免循环导入，从 app 延迟导入）"""
    try:
        from app import _get_producer_config
        return _get_producer_config(spv_id)
    except Exception:
        return None


def update_producer_risk_in_full_cache(spv_id: str, exchange_rate: float = 1, currency: str = "USD"):
    """
    刷新单个生产商的风控数据后，同步更新 producer_full_cache 中该生产商的 risk_data 和 priority_indicators。
    供 api_refresh_risk 调用，确保页面刷新后显示最新数据。
    """
    data, _ = load_producer_full_cache()
    if not data:
        return
    producers = data.get("producers", {})
    sid = str(spv_id or "").strip().lower()
    pc = producers.get(spv_id) or producers.get(sid)
    if not pc:
        return
    try:
        from kn_risk_cache import load_risk_cache
        merged, _ = load_risk_cache(sid)
        if merged:
            pc["risk_data"] = merged
        priority_indicators = None
        try:
            from spv_internal_params import load_priority_indicators_for_spv, compute_priority_from_risk_data
            pi = load_priority_indicators_for_spv(sid, risk_data=pc.get("risk_data", []), exchange_rate=exchange_rate)
            if pi:
                priority_indicators = pi
            elif pc.get("risk_data"):
                pi = compute_priority_from_risk_data(sid, pc["risk_data"], exchange_rate)
                if pi:
                    priority_indicators = pi
        except Exception:
            pass
        pc["priority_indicators"] = priority_indicators
        producers[sid] = pc
        save_producer_full_cache({
            "producers": producers,
            "portfolio_cumulative_stats": data.get("portfolio_cumulative_stats"),
            "allocation_by_platform": data.get("allocation_by_platform"),
            "system_cutover_date": data.get("system_cutover_date"),
        })
    except Exception:
        pass


def update_producer_revenue_in_full_cache(spv_id: str):
    """
    刷新单个生产商的收益数据后，同步更新 producer_full_cache 中该生产商的 revenue_data。
    供 api_refresh_revenue 调用，确保页面刷新后显示最新数据。
    """
    data, _ = load_producer_full_cache()
    if not data:
        return
    producers = data.get("producers", {})
    sid = str(spv_id or "").strip().lower()
    pc = producers.get(spv_id) or producers.get(sid)
    if not pc:
        return
    try:
        from kn_revenue_cache import load_revenue_cache
        cached_rev, _ = load_revenue_cache(sid)
        if cached_rev is not None:
            pc["revenue_data"] = cached_rev
            producers[sid] = pc
            save_producer_full_cache({
                "producers": producers,
                "portfolio_cumulative_stats": data.get("portfolio_cumulative_stats"),
                "allocation_by_platform": data.get("allocation_by_platform"),
                "system_cutover_date": data.get("system_cutover_date"),
            })
    except Exception:
        pass


def update_producer_cashflow_in_full_cache(spv_id: str):
    """
    刷新单个生产商的现金流数据后，同步更新 producer_full_cache 中该生产商的 cashflow_data。
    供 api_refresh_cashflow 调用，确保页面刷新后显示最新数据。
    """
    data, _ = load_producer_full_cache()
    if not data:
        return
    producers = data.get("producers", {})
    sid = str(spv_id or "").strip().lower()
    pc = producers.get(spv_id) or producers.get(sid)
    if not pc:
        return
    try:
        from kn_cashflow_cache import load_cashflow_cache
        cached_cf, _, _ = load_cashflow_cache(sid)
        if cached_cf is not None:
            pc["cashflow_data"] = cached_cf
            producers[sid] = pc
            save_producer_full_cache({
                "producers": producers,
                "portfolio_cumulative_stats": data.get("portfolio_cumulative_stats"),
                "allocation_by_platform": data.get("allocation_by_platform"),
                "system_cutover_date": data.get("system_cutover_date"),
            })
    except Exception:
        pass


def load_refresh_log():
    """读取最近一次刷新的日志（最后 500 行）"""
    if not os.path.isfile(REFRESH_LOG_FILE):
        return []
    try:
        with open(REFRESH_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return lines[-500:] if len(lines) > 500 else lines
    except Exception:
        return []


# 后台刷新状态：供 GET /api/partner/refresh-status 轮询
_refresh_status = {"running": False, "result": None}


def get_refresh_status():
    """返回当前刷新状态，供 API 轮询"""
    return dict(_refresh_status)


def refresh_producer_full_cache_async():
    """后台线程刷新缓存，不阻塞主流程；状态可通过 get_refresh_status() 轮询"""
    global _refresh_status
    if _refresh_status.get("running"):
        return
    _refresh_status["running"] = True
    _refresh_status["result"] = None

    def _run():
        global _refresh_status
        try:
            # 后台线程需推入 Flask app context，否则 DB/扩展可能异常
            from app import app
            with app.app_context():
                result = refresh_producer_full_cache()
            _refresh_status["running"] = False
            _refresh_status["result"] = result
        except Exception as e:
            _refresh_status["running"] = False
            _refresh_status["result"] = {"error": str(e), "logs": []}

    t = threading.Thread(target=_run, daemon=True)
    t.start()
