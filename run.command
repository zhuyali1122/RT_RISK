#!/bin/bash
# 双击此文件，在系统终端中启动 RT_RISK（确保数据库可连接）
cd "$(dirname "$0")"

echo "正在启动 RT_RISK..."
echo ""

# 预检查数据库连接
if python3 -c "
from db_connect import get_connection
conn = get_connection()
conn.close()
" 2>/dev/null; then
    echo "✓ 数据库连接正常"
else
    echo "✗ 数据库连接失败，请检查："
    echo "  1) 网络是否正常、是否需连接 VPN"
    echo "  2) .env 中的 DATABASE_URL 或 DB_HOST_IP 配置"
    echo "  3) 运行 python3 check_db_network.py 诊断"
    echo ""
fi

PORT=${PORT:-5002}
echo "启动服务: http://localhost:$PORT"
echo "按 Ctrl+C 停止"
echo ""

python3 app.py
