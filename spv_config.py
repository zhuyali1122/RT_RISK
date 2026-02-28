"""
从数据库 spv_config 表读取生产商配置，替代 config/producers.json
表结构需包含：spv_id, name, region, currency, exchange_rate, status 等
"""
import os
import json
from decimal import Decimal


def _serialize(val):
    """将 Decimal/date 等转为 JSON 可序列化类型"""
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    return val


def load_spv_config():
    """
    从数据库 spv_config 表加载生产商配置
    返回: { spv_id: { id, name, region, currency, exchange_rate, status, ... }, ... }
    若表不存在或查询失败，返回 {}（可配合 producers.json 作为 fallback）
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return {}

    cur = conn.cursor()
    try:
        # 检查表是否存在
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'spv_config'
            )
        """)
        if not cur.fetchone()[0]:
            return {}

        # 查询 spv_config 表（SELECT * 兼容不同列结构）
        cur.execute("SELECT * FROM spv_config")
        cols = [d[0].lower() for d in cur.description]
        out = {}
        for row in cur.fetchall():
            rec = dict(zip(cols, row))
            spv_id = rec.get("spv_id") or rec.get("id")
            if not spv_id:
                continue
            spv_id = str(spv_id).strip().lower()
            status = str(_serialize(rec.get("status") or "active")).strip().lower()
            if status and status not in ("active", ""):
                continue  # 只取 status=active 或空
            # 转为与 producers.json 兼容的格式
            def _f(k, *alts, default="-"):
                for key in [k] + list(alts):
                    v = rec.get(key)
                    if v is not None and v != "":
                        return _serialize(v)
                return default

            def _num(k, default=0):
                v = rec.get(k)
                if v is None or v == "":
                    return default
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return default

            p = {
                "id": spv_id,
                "name": _f("name", "spv_id", default=spv_id),
                "region": _f("region", "country"),
                "contact": _f("contact"),
                "product_type": _f("product_type"),
                "onboard_date": _f("onboard_date"),
                "currency": _f("currency", default="USD"),
                "exchange_rate": _num("exchange_rate", 1),
                "status": status or "active",
                "leverage_ratio": _f("leverage_ratio", default=""),
                "priority_yield_pct": _num("priority_yield_pct") or None,
                "liquidation_line": _num("liquidation_line") or None,
                "margin_call_line": _num("margin_call_line") or None,
                "baseline": _num("baseline") or None,
                "margin_deposit": rec.get("margin_deposit"),
                "guarantee_deposit": rec.get("guarantee_deposit"),
            }
            out[spv_id] = p
        return out
    except Exception:
        # 表不存在或列不匹配时返回空
        return {}
    finally:
        cur.close()
        conn.close()


def _load_revenue_data_from_json():
    """从 producers.json 加载 revenue_data，用于补充 DB 中缺失的收益数据"""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    revenue_by_id = {}
    producers_path = os.path.join(BASE_DIR, "config", "producers.json")
    if os.path.exists(producers_path):
        try:
            with open(producers_path, "r", encoding="utf-8") as f:
                for pid, p in (json.load(f).get("producers") or {}).items():
                    if p.get("revenue_data"):
                        revenue_by_id[str(pid).strip().lower()] = p["revenue_data"]
        except Exception:
            pass
    return revenue_by_id


def load_producers_from_spv_config(skip_revenue_compute=False, json_only=False):
    """
    优先从 spv_config 读取，若为空则 fallback 到 config/producers.json
    保持与 load_producers() 相同的返回格式
    当从 DB 读取时，会合并 producers.json 中的 revenue_data（DB 表无此列）
    skip_revenue_compute=True: 跳过 compute_revenue_data（DB 查询），仅用 JSON 或空，用于已有全量缓存时加速
    json_only=True: 仅从 producers.json 读取，不访问 DB，用于已有全量缓存时完全避免 DB
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PRODUCERS_PATH = os.path.join(BASE_DIR, "config", "producers.json")

    if json_only:
        if os.path.exists(PRODUCERS_PATH):
            try:
                with open(PRODUCERS_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("producers", {})
            except Exception:
                pass
        return {}

    db_producers = load_spv_config()
    if db_producers:
        revenue_by_id = _load_revenue_data_from_json()
        for spv_id, p in db_producers.items():
            sid = str(spv_id).strip().lower()
            rev = []
            if not skip_revenue_compute:
                try:
                    from kn_revenue import compute_revenue_data
                    rev = compute_revenue_data(spv_id=sid)
                except Exception:
                    pass
            if not rev:
                rev = revenue_by_id.get(sid)
            if rev:
                p["revenue_data"] = rev
            else:
                p["revenue_data"] = []
        return db_producers

    # Fallback: 从 producers.json 读取
    if os.path.exists(PRODUCERS_PATH):
        try:
            with open(PRODUCERS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("producers", {})
        except Exception:
            pass
    return {}
