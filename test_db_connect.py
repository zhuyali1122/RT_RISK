#!/usr/bin/env python3
"""
多方式数据库连接测试：依次尝试 psycopg2、不同 sslmode，帮助定位问题
"""
import os
import sys
from pathlib import Path

# 确保加载 .env
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

def try_psycopg2(conn_str, label):
    """尝试 psycopg2 连接"""
    try:
        import psycopg2
        conn = psycopg2.connect(conn_str, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        print(f"  ✓ {label}")
        return True
    except Exception as e:
        print(f"  ✗ {label}: {e}")
        return False

def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("未找到 DATABASE_URL，请检查 .env")
        return 1

    base = url.replace("postgresql+asyncpg://", "postgresql://")
    print("=" * 55)
    print("数据库连接测试")
    print("=" * 55)
    print(f"\n主机: {base.split('@')[1].split('/')[0] if '@' in base else 'N/A'}\n")

    # 1. 原样（含 URL 中的 sslmode）
    print("[1] psycopg2 使用 DATABASE_URL 转换后的连接串:")
    try_psycopg2(base, "当前配置")

    # 2. 强制 sslmode=require
    if "sslmode=" not in base:
        s2 = base + ("&" if "?" in base else "?") + "sslmode=require"
        print("\n[2] 追加 sslmode=require:")
        try_psycopg2(s2, "sslmode=require")

    # 3. 尝试 sslmode=prefer
    s3 = base.split("?")[0] + "?sslmode=prefer"
    if "sslmode=" in base:
        s3 = base.replace("sslmode=require", "sslmode=prefer").replace("sslmode=verify-full", "sslmode=prefer")
    print("\n[3] sslmode=prefer:")
    try_psycopg2(s3, "sslmode=prefer")

    # 4. 尝试 sslmode=disable（仅作诊断）
    s4 = base.split("?")[0]
    print("\n[4] 无 sslmode:")
    try_psycopg2(s4, "无 SSL 参数")

    # 5. 尝试 asyncpg（若已安装）
    print("\n[5] asyncpg（若已安装）:")
    try:
        import asyncio
        import asyncpg
        async def _try():
            conn_str = url.replace("postgresql+asyncpg://", "postgresql://")
            conn = await asyncpg.connect(conn_str, timeout=10)
            await conn.fetchval("SELECT 1")
            await conn.close()
        asyncio.run(_try())
        print("  ✓ asyncpg 连接成功")
    except ImportError:
        print("  (跳过: pip install asyncpg)")
    except Exception as e:
        print(f"  ✗ asyncpg: {e}")

    print("\n" + "=" * 55)
    return 0

if __name__ == "__main__":
    sys.exit(main())
