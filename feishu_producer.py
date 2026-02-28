"""
飞书多维表格 - 生产商拓展列表数据获取
需配置 FEISHU_APP_ID、FEISHU_APP_SECRET
Wiki 表格：wiki_node=Py0TwI8uRiyW9Qkj8W3cVIAJnzp, table_id=tbl7nNDG29dEYFva
"""
import os
import requests
from typing import Optional

# 飞书字段 -> 展示用统一字段映射（便于聚合与表格展示）
FEISHU_FIELD_MAP = {
    "项目": "name",
    "资产地域": "region",
    "类型": "type",
    "场景": "scenario",
    "负责人": "owner",
    "编号": "number",
}


def _get_access_token():
    """获取飞书 app_access_token"""
    app_id = os.getenv("FEISHU_APP_ID", "").strip()
    app_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        return None
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    try:
        r = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=10)
        data = r.json()
        if data.get("code") == 0:
            return data.get("app_access_token")
    except Exception:
        pass
    return None


def _get_wiki_node_obj_token(node_token: str, access_token: str) -> Optional[str]:
    """获取 wiki 节点对应的 obj_token（多维表格 app_token）"""
    url = "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node"
    try:
        r = requests.get(
            url,
            params={"token": node_token},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        data = r.json()
        if data.get("code") == 0:
            node = data.get("data", {}).get("node", {})
            return node.get("obj_token")
    except Exception:
        pass
    return None


def _normalize_field(f):
    """将飞书字段转为标量"""
    if f is None:
        return ""
    if isinstance(f, dict):
        if "text" in f:
            return f.get("text", "")
        if "name" in f:
            return f.get("name", "")
        return str(f)
    return str(f) if f else ""


def fetch_from_feishu() -> Optional[dict]:
    """
    从飞书 Wiki 多维表格读取数据
    node_token: Py0TwI8uRiyW9Qkj8W3cVIAJnzp
    table_id: tbl7nNDG29dEYFva
    """
    token = _get_access_token()
    if not token:
        return None

    node_token = os.getenv("FEISHU_WIKI_NODE", "Py0TwI8uRiyW9Qkj8W3cVIAJnzp")
    table_id = os.getenv("FEISHU_TABLE_ID", "tbl7nNDG29dEYFva")

    app_token = os.getenv("FEISHU_APP_TOKEN", "").strip()
    if not app_token:
        app_token = _get_wiki_node_obj_token(node_token, token)
    if not app_token:
        return None

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    all_records = []
    page_token = None

    try:
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            r = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=15,
            )
            data = r.json()
            if data.get("code") != 0:
                return None
            items = data.get("data", {}).get("items", [])
            for it in items:
                fields = it.get("fields", {})
                rec = {}
                for k, v in fields.items():
                    if isinstance(v, list) and len(v) > 0:
                        rec[k] = _normalize_field(v[0])
                    else:
                        rec[k] = _normalize_field(v)
                # 添加统一字段用于聚合与展示（兼容前端）
                rec.setdefault("name", rec.get("项目", "-"))
                rec.setdefault("region", rec.get("资产地域", "未分类"))
                rec.setdefault("region_detail", rec.get("资产地域", "未分类"))
                rec.setdefault("type", rec.get("类型", "未分类"))
                rec.setdefault("scenario", rec.get("场景", "未分类"))
                rec.setdefault("industry", "未分类")
                rec.setdefault("maturity", rec.get("优先级（1-5）", "") or "未分类")
                rec.setdefault("owner", rec.get("负责人", ""))
                rec.setdefault("number", rec.get("编号", ""))
                all_records.append(rec)
            page_token = data.get("data", {}).get("page_token")
            if not page_token or not items:
                break
    except Exception:
        return None

    return {
        "records": all_records,
        "source": "feishu",
        "updated_at": "",
    }


def load_producer_data() -> dict:
    """加载生产商拓展数据：仅从飞书多维表格读取，无 mock"""
    feishu_data = fetch_from_feishu()
    if feishu_data and feishu_data.get("records"):
        return feishu_data
    return {"records": [], "source": "empty", "updated_at": ""}
