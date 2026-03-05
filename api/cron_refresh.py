"""
Vercel Cron 定时刷新全量缓存 - 独立函数，maxDuration 300 秒
每天 UTC 00:00（北京时间 08:00）执行，加载全量数据并写入 Redis
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        cron_secret = os.getenv("CRON_SECRET", "")
        if not cron_secret or auth != f"Bearer {cron_secret}":
            self._send_json(401, {"error": "Unauthorized"})
            return

        try:
            from kn_producer_cache import refresh_producer_full_cache
            result = refresh_producer_full_cache()
            if "error" in result:
                self._send_json(500, result)
                return
            self._send_json(200, {
                "ok": True,
                "last_updated": result.get("last_updated"),
                "producer_count": result.get("producer_count"),
            })
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _send_json(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
