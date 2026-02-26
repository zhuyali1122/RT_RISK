# RT_RISK

PostgreSQL 数据库连接与 Web 查询项目。

## 环境配置

1. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

2. 配置连接信息：
   ```bash
   cp .env.example .env
   # 编辑 .env，填入实际的数据库连接信息（参考 数据库配置清单.md）
   ```

3. 测试连接：
   ```bash
   python db_connect.py
   ```

## 启动 Web 查询页面

**重要**：必须在**系统终端**（Terminal.app / iTerm）中运行，不要从 Cursor/IDE 内运行，否则数据库可能无法连接。

```bash
python3 app.py
# 或使用启动脚本（会预检查数据库连接）
./run.sh
```

macOS 用户也可**双击** `run.command` 在终端中启动。

浏览器访问 **http://localhost:5001**（或 `PORT=5002 python3 app.py` 使用 5002），可：
- 查看所有表列表
- 选择表并查询数据（支持分页）

## 数据库连接失败排查

若页面显示「数据库未连接」或「could not translate host name」：

1. **从系统终端运行**：关闭 Cursor，打开 Terminal.app，执行 `cd RT_RISK && python3 app.py`
2. **使用 IP 直连**：在终端运行 `python3 check_db_network.py --ip-only`，将输出的 IP 添加到 `.env`：
   ```env
   DB_HOST_IP=输出的IP
   ```
3. **网络诊断**：运行 `python3 check_db_network.py` 查看详细诊断

## 文件说明

- `app.py` - Web 应用（Flask），提供表列表与数据查询 API
- `db_config.py` - 数据库配置（从环境变量读取）
- `db_connect.py` - 连接与测试
- `数据库配置清单.md` - 从阿里云 DMS 获取配置的详细清单
- `.env.example` - 配置模板（复制为 .env 使用）
