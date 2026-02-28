#!/usr/bin/env python3
"""
测试飞书 API 连接 - 验证 app_id/app_secret 并尝试读取多维表格
用法: cd RT_RISK && python scripts/test_feishu.py
"""
import os
import sys

# 加载 .env
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

def main():
    app_id = os.getenv("FEISHU_APP_ID", "").strip()
    app_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
    print("=== 飞书连接测试 ===\n")
    print(f"FEISHU_APP_ID: {app_id[:12]}... (已配置)" if app_id else "FEISHU_APP_ID: 未配置")
    print(f"FEISHU_APP_SECRET: {'*' * 8} (已配置)" if app_secret else "FEISHU_APP_SECRET: 未配置")

    if not app_id or not app_secret:
        print("\n请先在 .env 中配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        return 1

    import requests

    # 1. 获取 access token
    print("\n1. 获取 app_access_token...")
    r = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=10,
    )
    data = r.json()
    if data.get("code") != 0:
        print(f"   失败: {data}")
        print("   请检查: 1) app_id/app_secret 是否正确 2) 应用是否已发布/启用")
        return 1
    token = data.get("app_access_token")
    print(f"   成功, token 前缀: {token[:20]}...")

    # 2. 尝试获取多维表格数据
    app_token = os.getenv("FEISHU_APP_TOKEN", "").strip()
    table_id = os.getenv("FEISHU_TABLE_ID", "tbl7nNDG29dEYFva").strip()
    wiki_node = os.getenv("FEISHU_WIKI_NODE", "Py0TwI8uRiyW9Qkj8W3cVIAJnzp").strip()

    if not app_token and wiki_node:
        print("\n2. 从 Wiki 节点获取 app_token...")
        r2 = requests.get(
            "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
            params={"token": wiki_node},
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        d2 = r2.json()
        if d2.get("code") == 0:
            app_token = d2.get("data", {}).get("node", {}).get("obj_token", "")
            print(f"   成功, app_token: {app_token[:20] if app_token else '(空)'}...")
        else:
            print(f"   失败: {d2}")
            print("   若表格不在 Wiki 中，请直接配置 FEISHU_APP_TOKEN（多维表格 URL 中 base/ 后面的部分）")

    if not app_token:
        print("\n未配置 FEISHU_APP_TOKEN 且无法从 Wiki 获取。")
        print("请提供多维表格的 app_token：打开多维表格，URL 形如")
        print("  https://xxx.feishu.cn/base/【这里就是 app_token】?table=xxx")
        print("将 app_token 填入 .env 的 FEISHU_APP_TOKEN")
        return 1

    print(f"\n3. 读取多维表格 records (app={app_token[:16]}..., table={table_id})...")
    r3 = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        params={"page_size": 10},
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    d3 = r3.json()
    if d3.get("code") != 0:
        print(f"   失败: {d3}")
        print("   可能原因: 1) app_token/table_id 错误 2) 应用未开通「多维表格」读取权限")
        return 1

    items = d3.get("data", {}).get("items", [])
    total = d3.get("data", {}).get("total", 0)
    print(f"   成功! 共 {total} 条记录，前 {len(items)} 条:")
    for i, it in enumerate(items[:3]):
        fields = it.get("fields", {})
        print(f"   - 记录 {i+1}: {list(fields.keys())[:5]}...")

    print("\n=== 测试通过，飞书数据可正常读取 ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
