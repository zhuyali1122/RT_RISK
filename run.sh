#!/bin/bash
# 在系统终端运行: ./run.sh  或  bash run.sh
cd "$(dirname "$0")"
PORT=${PORT:-5002}
echo "启动 RT_RISK: http://localhost:$PORT"
python3 app.py
