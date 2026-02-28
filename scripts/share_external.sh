#!/bin/bash
# 外部分享部署脚本
# 从 GitHub 克隆一份代码，在独立端口运行，通过 Cloudflare Tunnel 生成外网链接
# 你的主项目可继续在 localhost:5001 调试，互不影响

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SHARE_DIR="$PROJECT_ROOT/RT_RISK_share"
SHARE_PORT=5002
REPO="https://github.com/zhuyali1122/RT_RISK.git"

echo "=========================================="
echo "  RT_RISK 外部分享部署"
echo "=========================================="

# 1. 克隆或更新代码
if [ -d "$SHARE_DIR" ]; then
    echo "[1/4] 更新已有克隆..."
    (cd "$SHARE_DIR" && git pull --quiet 2>/dev/null || true)
else
    echo "[1/4] 从 GitHub 克隆..."
    git clone "$REPO" "$SHARE_DIR"
fi

# 2. 复制 .env（数据库等配置）
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "[2/4] 复制 .env 配置..."
    cp "$PROJECT_ROOT/.env" "$SHARE_DIR/.env"
else
    echo "[2/4] 未找到 .env，使用默认配置（可能无法连数据库）"
fi

# 3. 安装依赖
echo "[3/4] 安装依赖..."
(cd "$SHARE_DIR" && (pip3 install -q -r requirements.txt 2>/dev/null || pip install -q -r requirements.txt))

# 4. 启动 Flask（后台）
echo "[4/4] 启动服务 (端口 $SHARE_PORT)..."
cd "$SHARE_DIR"
PORT=$SHARE_PORT python3 app.py &
FLASK_PID=$!

# 退出时清理
cleanup() {
    echo ""
    echo "正在停止分享实例..."
    kill $FLASK_PID 2>/dev/null || true
    exit 0
}
trap cleanup SIGINT SIGTERM

sleep 3

# 5. 启动 Cloudflare Tunnel
echo "启动 Cloudflare Tunnel，获取外网链接..."
LOG_FILE="/tmp/rtrisk_tunnel_$$.log"
cloudflared tunnel --url "http://localhost:$SHARE_PORT" 2>&1 | tee "$LOG_FILE" &
TUNNEL_PID=$!
sleep 6

URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$LOG_FILE" 2>/dev/null | tail -1)

echo ""
echo "=========================================="
echo "  部署完成"
echo "=========================================="
echo ""
echo "  外网链接（可分享给他人）: $URL"
echo ""
echo "  本地调试: http://localhost:5001  （在主项目运行 python app.py）"
echo "  分享实例: http://localhost:$SHARE_PORT"
echo ""
echo "  按 Ctrl+C 停止分享"
echo "=========================================="

wait $TUNNEL_PID 2>/dev/null || wait
