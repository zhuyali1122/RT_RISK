"""
项目列表：从 spv_config 获取，与 spv_initial_params 交叉校验得到完整配置
每个项目的 default currency 来自 spv_config.currency
"""
from spv_config import load_producers_from_spv_config
from spv_initial_params import load_priority_indicators_for_spv


def load_projects_with_internal_params(skip_revenue_compute=False, skip_priority_indicators=False, json_only=False):
    """
    加载项目列表：spv_config 为主，与 spv_initial_params 交叉校验
    返回: { spv_id: { id, name, region, currency, exchange_rate, ..., priority_indicators?, ... }, ... }
    skip_revenue_compute=True: 跳过 compute_revenue_data（DB），用于已有全量缓存时加速
    skip_priority_indicators=True: 跳过 load_priority_indicators_for_spv（DB），用于列表页加速
    json_only=True: 仅从 producers.json 读取，完全避免 DB
    """
    producers = load_producers_from_spv_config(skip_revenue_compute=skip_revenue_compute, json_only=json_only)
    if not producers:
        return {}

    out = {}
    for spv_id, p in producers.items():
        if p.get("status") and str(p.get("status")).lower() not in ("active", ""):
            continue
        proj = dict(p)
        if not skip_priority_indicators:
            pi = load_priority_indicators_for_spv(spv_id)
            if pi:
                proj["priority_indicators"] = pi
            else:
                proj["priority_indicators"] = None
        else:
            proj["priority_indicators"] = None
        out[spv_id] = proj
    return out


def get_partner_spv_map(json_only=False):
    """从 spv_config 派生 partner_id -> spv_id 映射（生产商 id 即 spv_id）"""
    producers = load_producers_from_spv_config(json_only=json_only)
    m = {
        pid: pid
        for pid, p in producers.items()
        if str(p.get("status", "active")).lower() in ("active", "")
    }
    if "kn" in m:
        m["partner_beta"] = "kn"  # 兼容旧别名
    return m
