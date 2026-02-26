#!/usr/bin/env python3
"""数据库连接网络诊断：检查端口是否可达、当前公网 IP 等"""
import os
import socket
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

_url = os.getenv("DATABASE_URL", "")
if _url:
    _p = urlparse(_url.replace("postgresql+asyncpg://", "postgresql://"))
    HOST = _p.hostname or "localhost"
    PORT = _p.port or 5432
else:
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = int(os.getenv("DB_PORT", "5432"))
TIMEOUT = 8


def get_public_ip():
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=5) as r:
            return r.read().decode().strip()
    except Exception:
        return None


def main():
    if "--ip-only" in sys.argv:
        try:
            ip = socket.gethostbyname(HOST)
            print(ip)
            return 0
        except Exception:
            return 1

    print("=" * 50)
    print("RT_RISK 数据库连接诊断")
    print("=" * 50)
    print(f"\n目标: {HOST}:{PORT}")
    print(f"超时: {TIMEOUT} 秒")

    # 0. 当前公网 IP
    pub_ip = get_public_ip()
    if pub_ip:
        print(f"\n[0] 当前公网 IP: {pub_ip}")
        print(f"    请确认此 IP 已加入 RDS 白名单")

    # 1. 解析域名
    try:
        ip = socket.gethostbyname(HOST)
        print(f"[1] 域名解析: {HOST} -> {ip}")
    except Exception as e:
        print(f"[1] 域名解析失败: {e}")
        return 1

    # 2. TCP 端口连通性
    print(f"\n[2] 测试 TCP 端口 {PORT} 连通性...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    try:
        sock.connect((ip, PORT))
        print(f"    ✓ 端口可达，可尝试连接数据库")
        sock.close()
    except socket.timeout:
        print(f"    ✗ 连接超时")
        print(f"\n可能原因:")
        print(f"  - 当前公网 IP 未加入 RDS 白名单")
        print(f"  - RDS 未开启公网访问")
        print(f"  - 需通过 VPN/内网访问")
        print(f"\n建议: 将本机公网 IP 加入 RDS 白名单后重试")
        return 1
    except PermissionError:
        print(f"    ✗ 权限不足")
        return 1
    except Exception as e:
        print(f"    ✗ 连接失败: {e}")
        return 1

    # 3. 尝试 psycopg2 连接
    print(f"\n[3] 尝试 PostgreSQL 连接...")
    try:
        import psycopg2
        from db_config import get_connection_string
        conn_str = get_connection_string()
        conn = psycopg2.connect(conn_str, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        print(f"    ✓ 数据库连接成功")
        print(f"\n若应用内仍报「域名解析失败」，可在 .env 中添加（绕过 DNS）：")
        print(f"  DB_HOST_IP={ip}")
        return 0
    except ImportError:
        print(f"    (跳过: 未安装 psycopg2)")
        return 0
    except Exception as e:
        print(f"    ✗ 连接失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
