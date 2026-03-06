"""
缓存存储后端 - 支持文件系统与 Vercel Blob（跨实例共享）
- Vercel + Blob：使用 Vercel Blob 存储，Admin 刷新后所有实例可访问
- 本地：使用 config/cache 或 /tmp
"""
import json
import logging
import os
from typing import Optional, Union

log = logging.getLogger("kn_cache_storage")

BLOB_PREFIX = "rt_risk/"
BLOB_PATH_CACHE = BLOB_PREFIX + "producer_full_cache.json"
BLOB_PATH_META = BLOB_PREFIX + "cache_meta.json"
BLOB_PATH_LOG = BLOB_PREFIX + "refresh_log.txt"


def _use_blob():
    """是否使用 Vercel Blob（Vercel 上需配置以实现跨实例共享）"""
    if not os.getenv("VERCEL"):
        return False
    return bool(os.getenv("BLOB_READ_WRITE_TOKEN"))


def _blob_put(path: str, content: Union[str, bytes]) -> bool:
    """上传内容到 Blob（allowOverwrite 确保每次刷新可覆盖）"""
    token = os.getenv("BLOB_READ_WRITE_TOKEN")
    if not token:
        return False
    try:
        import vercel_blob
        data = content.encode("utf-8") if isinstance(content, str) else content
        vercel_blob.put(path, data, {"allowOverwrite": "true"})
        return True
    except Exception as e:
        log.error("[blob_put] 失败 path=%s: %s", path, e)
        return False


def _blob_get(path: str) -> Optional[str]:
    """从 Blob 读取内容（用文件夹 prefix 列出后按 pathname 精确匹配）"""
    token = os.getenv("BLOB_READ_WRITE_TOKEN")
    if not token:
        return None
    try:
        import vercel_blob
        # 用 rt_risk/ 前缀列出，避免精确 path 作为 prefix 时漏匹配
        blobs = vercel_blob.list({"prefix": BLOB_PREFIX, "limit": "20"})
        blobs_list = blobs.get("blobs", []) if isinstance(blobs, dict) else []
        b = None
        for x in blobs_list:
            if x.get("pathname") == path:
                b = x
                break
        if not b:
            return None
        url = b.get("url") or b.get("downloadUrl")
        if not url:
            return None
        import requests
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        r.raise_for_status()
        return r.content.decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("[blob_get] 失败 path=%s: %s", path, e)
        return None


def _blob_append(path: str, content: str) -> bool:
    """追加内容到 Blob（读-追加-写）"""
    existing = _blob_get(path) or ""
    return _blob_put(path, existing + content)


def cache_get_json(path: str) -> Optional[dict]:
    """从 Blob 读取 JSON"""
    raw = _blob_get(path)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def cache_set_json(path: str, data: dict) -> bool:
    """写入 JSON 到 Blob"""
    try:
        return _blob_put(path, json.dumps(data, ensure_ascii=False))
    except Exception as e:
        log.error("[cache_set_json] 失败 path=%s: %s", path, e)
        return False


def cache_append(path: str, value: str, truncate_first: bool = False) -> bool:
    """追加到 Blob 字符串"""
    if truncate_first:
        return _blob_put(path, value)
    return _blob_append(path, value)


def cache_get(path: str) -> Optional[str]:
    """从 Blob 读取"""
    return _blob_get(path)


def cache_set(path: str, value: str) -> bool:
    """写入 Blob"""
    return _blob_put(path, value)
