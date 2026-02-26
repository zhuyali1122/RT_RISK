"""
Vercel Serverless 入口 - 将请求转发到 Flask 应用
"""
import sys
import os

# 确保项目根目录在 Python 路径中
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)

from app import app
