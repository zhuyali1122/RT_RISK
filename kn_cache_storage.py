"""
缓存存储后端 - 支持文件系统与 Redis（Vercel 共享缓存）
- 本地 / 非 Vercel：使用 config/cache 或 /tmp
- Vercel + KV：使用 Vercel KV (Upstash Redis)，所有实例共享，Admin 刷新后其他用户可直接访问
"""
import json
import logging
import os

log = logging.getLogger("kn_cache_storage")

# Redis 键前缀
REDIS_KEY_CACHE = "rt_risk:producer_full_cache"
REDIS_KEY_META = "rt_risk:cache_meta"
REDIS_KEY_LOG = "rt_risk:refresh_log"

_redis_client = None


def _get_redis_url_and_token():
    """
    获取 Redis REST API 的 url 和 token。
    支持多种环境变量组合：
    - KV_REST_API_URL + KV_REST_API_TOKEN
    - UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN
    - REDIS_URL + REDIS_TOKEN（或 REDIS_PASSWORD）
    - REDIS_URL 为 rediss:// 或 redis:// 时，从 URL 解析 host 和 token，转为 REST
    """
    url = os.getenv("KV_REST_API_URL") or os.getenv("UPSTASH_REDIS_REST_URL")
    token = os.getenv("KV_REST_API_TOKEN") or os.getenv("UPSTASH_REDIS_REST_TOKEN")
    if url and token:
        return url, token
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        return None, None
    redis_token = os.getenv("REDIS_TOKEN") or os.getenv("REDIS_PASSWORD")
    if redis_url.startswith("https://"):
        if redis_token:
            return redis_url, redis_token
        return None, None
    if redis_url.startswith("rediss://") or redis_url.startswith("redis://"):
        from urllib.parse import urlparse
        parsed = urlparse(redis_url)
        if parsed.hostname and parsed.password:
            return f"https://{parsed.hostname}", parsed.password
        if parsed.hostname and redis_token:
            return f"https://{parsed.hostname}", redis_token
    return None, None


def _use_redis():
    """是否使用 Redis 作为缓存后端（Vercel 上需配置 Redis，且必须用可写 token）"""
    if not os.getenv("VERCEL"):
        return False
    url, token = _get_redis_url_and_token()
    return bool(url and token)


def _get_redis():
    """懒加载 Redis 客户端，兼容多种环境变量"""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    url, token = _get_redis_url_and_token()
    if not url or not token:
        return None
    try:
        from upstash_redis import Redis
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
    """写入 Redis，失败时记录日志"""
    r = _get_redis()
    if not r:
        return False
    try:
        r.set(key, value)
        return True
    except Exception as e:
        log.error("[cache_set] Redis 写入失败 key=%s: %s", key, e)
        raise


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
    except Exception as e:
        log.error("[cache_set_json] Redis 写入失败 key=%s: %s", key, e)
        return False
