"""
PostgreSQL 数据库连接与连接池
"""
import os
import time
import psycopg2
from psycopg2 import pool
from db_config import get_db_config, get_connection_string, get_pool_config

_pool = None
_CONNECT_RETRIES = 3
_CONNECT_RETRY_DELAY = 2


class _PooledConnWrapper:
    """包装连接池中的连接，使 close() 归还到池而非真正关闭"""

    def __init__(self, conn, pool_obj):
        self._conn = conn
        self._pool = pool_obj

    def close(self):
        if self._pool and self._conn:
            try:
                self._pool.putconn(self._conn)
            except Exception:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = None

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _get_connect_kwargs():
    """获取 psycopg2.connect 的通用参数"""
    cfg = get_db_config()
    kwargs = {
        "host": cfg["host"],
        "port": cfg["port"],
        "dbname": cfg["database"],
        "user": cfg["user"],
        "password": cfg["password"],
        "connect_timeout": 15,
    }
    if cfg.get("sslmode"):
        kwargs["sslmode"] = cfg["sslmode"]
    elif cfg["host"] and ("rds.aliyuncs.com" in cfg["host"] or os.getenv("DB_HOST_IP")):
        kwargs["sslmode"] = "require"
    return kwargs


def _connect_with_string():
    """使用连接字符串连接（与 DATABASE_URL 一致）"""
    return psycopg2.connect(get_connection_string(), connect_timeout=15)


def _connect_with_retry(connect_fn):
    """带重试的连接，应对 DNS 解析不稳定"""
    last_err = None
    for attempt in range(1, _CONNECT_RETRIES + 1):
        try:
            return connect_fn()
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if "translate host name" in err_str or "nodename" in err_str or "timeout" in err_str:
                if attempt < _CONNECT_RETRIES:
                    time.sleep(_CONNECT_RETRY_DELAY)
                    continue
            raise
    raise last_err


def _get_pool():
    """获取或创建连接池"""
    global _pool
    if _pool is None:
        kwargs = _get_connect_kwargs()
        pool_cfg = get_pool_config()
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=pool_cfg["pool_size"],
            **kwargs,
        )
    return _pool


def get_connection():
    """创建并返回数据库连接（含 DNS 不稳定时的重试）"""
    def _do_connect():
        if (os.getenv("DATABASE_URL") or "").strip():
            return _connect_with_string()
        pool_cfg = get_pool_config()
        if pool_cfg["pool_size"] > 1:
            p = _get_pool()
            conn = p.getconn()
            return _PooledConnWrapper(conn, p)
        return psycopg2.connect(**_get_connect_kwargs())

    return _connect_with_retry(_do_connect)


def test_connection():
    """测试数据库连接"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        cur.close()
        conn.close()
        print("连接成功:", version[0])
        return True
    except Exception as e:
        print("连接失败:", e)
        return False


if __name__ == "__main__":
    test_connection()
