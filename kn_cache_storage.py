"""
缓存存储后端 - 支持文件系统与 Redis（Vercel 跨实例共享）
- Vercel + KV：使用 Redis，Admin 刷新后所有用户实例可访问
- 本地：使用 config/cache 或 /tmp
"""
import json
import logging
import os

log = logging.getLogger("kn_cache_storage")

REDIS_KEY_CACHE = "rt_risk:producer_full_cache"
REDIS_KEY_META = "rt_risk:cache_meta"
REDIS_KEY_LOG = "rt_risk:refresh_log"

_redis_client = None


def _get_redis_url_and_token():
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
    """是否使用 Redis（Vercel 上需配置以实现跨实例共享）"""
    if not os.getenv("VERCEL"):
        return False
    url, token = _get_redis_url_and_token()
    return bool(url and token)


def _get_redis():
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
    r = _get_redis()
    if not r:
        return None
    try:
        return r.get(key)
    except Exception:
        return None


def cache_set(key: str, value: str) -> bool:
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
    raw = cache_get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def cache_set_json(key: str, data: dict) -> bool:
    try:
        return cache_set(key, json.dumps(data, ensure_ascii=False))
    except Exception as e:
        log.error("[cache_set_json] Redis 写入失败 key=%s: %s", key, e)
        return False
