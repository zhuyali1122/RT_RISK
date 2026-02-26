"""
PostgreSQL 数据库连接配置
使用环境变量存储敏感信息，请勿将 .env 提交到版本控制
支持 DATABASE_URL 或 DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD
"""
import os
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv

# 从 db_config.py 所在目录加载 .env，避免因 cwd 不同而找不到
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_env_path)


def get_db_config():
    """获取数据库连接配置（优先从 DATABASE_URL 解析）"""
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        # 支持 postgresql:// 或 postgresql+asyncpg://
        clean_url = url.replace("postgresql+asyncpg://", "postgresql://")
        parsed = urlparse(clean_url)
        port_override = os.getenv("DB_PORT")
        host = parsed.hostname or "localhost"
        if os.getenv("DB_HOST_IP"):
            host = os.getenv("DB_HOST_IP").strip()
        cfg = {
            "host": host,
            "port": int(port_override) if port_override else (parsed.port or 5432),
            "database": (parsed.path or "").lstrip("/").split("?")[0].strip(),
            "user": parsed.username or "",
            "password": parsed.password or "",
        }
        # 解析 query 中的 sslmode 等参数
        if parsed.query:
            from urllib.parse import parse_qs
            qs = parse_qs(parsed.query)
            if "sslmode" in qs:
                cfg["sslmode"] = qs["sslmode"][0]
        return cfg
    host = os.getenv("DB_HOST", "localhost")
    if os.getenv("DB_HOST_IP"):
        host = os.getenv("DB_HOST_IP").strip()
    return {
        "host": host,
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def get_connection_string():
    """获取 psycopg2 连接字符串（postgresql://）"""
    import re
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        s = url.replace("postgresql+asyncpg://", "postgresql://")
        port_override = os.getenv("DB_PORT")
        host_ip = os.getenv("DB_HOST_IP", "").strip()
        if host_ip:
            s = re.sub(r"@([^:/]+):", f"@{host_ip}:", s)
        if port_override:
            s = re.sub(r"@([^:/]+):\d+(/|$)", rf"@\1:{port_override}\2", s)
        # 阿里云 RDS 需 SSL，若 URL 未指定则追加 sslmode=require
        ssl_override = os.getenv("DB_SSLMODE")
        if ssl_override:
            sep = "&" if "?" in s else "?"
            s = f"{s}{sep}sslmode={ssl_override}" if "sslmode=" not in s else s
        elif "sslmode=" not in s and ("rds.aliyuncs.com" in s or host_ip):
            sep = "&" if "?" in s else "?"
            s = f"{s}{sep}sslmode=require"
        return s
    cfg = get_db_config()
    return (
        f"postgresql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def get_pool_config():
    """获取连接池配置"""
    return {
        "pool_size": int(os.getenv("DATABASE_POOL_SIZE", "5")),
        "max_overflow": int(os.getenv("DATABASE_MAX_OVERFLOW", "10")),
    }
