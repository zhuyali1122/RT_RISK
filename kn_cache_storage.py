"""
缓存存储后端 - 支持文件系统与 Redis（Vercel 共享缓存）
- 本地 / 非 Vercel：使用 config/cache 或 /tmp
- Vercel + KV：使用 Vercel KV (Upstash Redis)，所有实例共享，Admin 刷新后其他用户可直接访问
"""
import json
import os

# Redis 键前缀
REDIS_KEY_CACHE = "rt_risk:producer_full_cache"
REDIS_KEY_META = "rt_risk:cache_meta"
REDIS_KEY_LOG = "rt_risk:refresh_log"

_redis_client = None


def _use_redis():
    """是否使用 Redis 作为缓存后端（Vercel 上需配置 KV）"""
    if not os.getenv("VERCEL"):
        return False
    url = os.getenv("KV_REST_API_URL") or os.getenv("UPSTASH_REDIS_REST_URL")
    token = os.getenv("KV_REST_API_TOKEN") or os.getenv("UPSTASH_REDIS_REST_TOKEN")
    return bool(url and token)


def _get_redis():
    """懒加载 Redis 客户端，兼容 Vercel KV 与 Upstash 环境变量"""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if not _use_redis():
        return None
    try:
        from upstash_redis import Redis
        url = os.getenv("KV_REST_API_URL") or os.getenv("UPSTASH_REDIS_REST_URL")
        token = os.getenv("KV_REST_API_TOKEN") or os.getenv("UPSTASH_REDIS_REST_TOKEN")
        _redis_client = Redis(url=url, token=token)
        return _redis_client
    except Exception:
        return None


def cache_get(key: str) -> str | None:
    """从 Redis 读取，key 为 REDIS_KEY_* 常量"""
    r = _get_redis()
    if not r:
        return None
    try:
        return r.get(key)
    except Exception:
        return None


def cache_set(key: str, value: str) -> bool:
    """写入 Redis"""
    r = _get_redis()
    if not r:
        return False
    try:
        r.set(key, value)
        return True
    except Exception:
        return False


def cache_append(key: str, value: str, truncate_first: bool = False) -> bool:
    """追加到 Redis 字符串；truncate_first=True 时先覆盖"""
    r = _get_redis()
    if not r:
        return False
    try:
        if truncate_first:
            r.set(key, value)
        else:
            r.append(key, value)
        return True
    except Exception:
        return False


def cache_get_json(key: str) -> dict | None:
    """从 Redis 读取 JSON"""
    raw = cache_get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def cache_set_json(key: str, data: dict) -> bool:
    """写入 JSON 到 Redis"""
    try:
        return cache_set(key, json.dumps(data, ensure_ascii=False))
    except Exception:
        return False
