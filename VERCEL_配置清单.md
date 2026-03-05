# Vercel 配置清单

RT_RISK 部署到 Vercel 的完整配置说明。

---

## 一、项目导入

1. 登录 [Vercel](https://vercel.com) → **Add New** → **Project**
2. 选择 **Import Git Repository** → 连接 GitHub
3. 选择仓库 `zhuyali1122/RT_RISK`
4. **Root Directory**：留空或 `.`
5. **Framework Preset**：Other

---

## 二、环境变量（Settings → Environment Variables）

### 必填

| 变量 | 值 | 环境 |
|------|-----|------|
| `DATABASE_URL` | `postgresql://user:password@pgm-xxx.pg.rds.aliyuncs.com:5432/dbname?sslmode=require` | Production, Preview |
| `SECRET_KEY` | 随机字符串（如 `openssl rand -hex 32` 生成） | Production, Preview |
| `CRON_SECRET` | 至少 16 字符随机字符串（用于每日定时刷新鉴权） | Production |

### 数据库优化（建议）

| 变量 | 值 | 说明 |
|------|-----|------|
| `DATABASE_POOL_SIZE` | `1` | Serverless 单实例，不宜大连接池 |
| `DATABASE_CONNECT_TIMEOUT` | `8` | 缩短连接超时 |
| `DB_SSLMODE` | `require` | 阿里云 RDS 公网需 SSL |

### 共享缓存（Admin 刷新后其他用户可访问）

| 变量 | 来源 | 说明 |
|------|------|------|
| `KV_REST_API_URL` | 添加 Vercel KV 后**自动注入** | Redis REST API 地址 |
| `KV_REST_API_TOKEN` | 添加 Vercel KV 后**自动注入** | 认证 Token |

### 可选

| 变量 | 值 | 说明 |
|------|-----|------|
| `APP_ROOT` | `/rtrisk` | 若部署在子路径 |
| `FEISHU_APP_ID` | 飞书应用 ID | 生产商拓展列表从飞书读取 |
| `FEISHU_APP_SECRET` | 飞书应用 Secret | 同上 |
| `FEISHU_WIKI_NODE` | Wiki 节点 token | 表格在 Wiki 时 |
| `FEISHU_TABLE_ID` | 多维表格 ID | 同上 |

### 不要设置

| 变量 | 原因 |
|------|------|
| `DB_HOST_IP` | 本地 VPN 解析的 IP 在 Vercel 不可达，会导致连接失败 |

---

## 三、Vercel KV 配置（共享缓存）

### 为什么需要

Vercel Serverless 的 `/tmp` 是**实例级**存储，不同请求可能落在不同实例。Admin 刷新写入的缓存在其他用户请求时可能读不到。使用 **Vercel KV** 后，所有实例共享同一份 Redis 缓存。

### 配置步骤

1. 进入 RT_RISK 项目 → **Storage** 标签
2. 点击 **Create Database**
3. 选择 **KV**（或从 Marketplace 添加 Upstash Redis）
4. 创建后，Vercel 自动将 `KV_REST_API_URL`、`KV_REST_API_TOKEN` 注入到项目环境变量
5. 在 **Settings → Environment Variables** 中确认已存在上述两个变量

### 工作流程

- **Admin** 登录 → 进入「缓存管理」→ 点击「刷新全量缓存」
- 缓存写入 Redis，所有实例共享
- **PM/Investor** 登录后可直接读取最新缓存

### 每日自动刷新（Cron）

- 每天 **UTC 00:00**（北京时间 08:00）自动执行全量缓存刷新
- 需配置 `CRON_SECRET` 环境变量（至少 16 字符）
- Admin 仍可随时在「缓存管理」手动刷新

### 重要：Vercel 上为同步刷新

Vercel Serverless 在响应返回后会立即终止函数，后台线程会被杀死，导致缓存无法写入 Redis。因此 Vercel 上刷新为**同步执行**，需等待完成（约 60 秒内）。若生产商较多超时，Pro 计划可在 Settings → Functions 将 `maxDuration` 调至 300 秒。

---

## 四、阿里云 RDS 配置

### 1. 外网地址

- 阿里云 RDS 控制台 → 实例 → **数据库连接**
- 申请并保留**外网地址**
- `DATABASE_URL` 中的 host 使用该外网地址

### 2. 白名单

- RDS 控制台 → **数据安全性** → **白名单设置**
- 添加 `0.0.0.0/0`（测试阶段）或 Vercel 出站 IP 段

### 3. SSL

- 在 `DATABASE_URL` 末尾加 `?sslmode=require`

---

## 五、项目设置（Settings → General）

| 项 | 建议值 |
|----|--------|
| Root Directory | 空 或 `.` |
| Framework Preset | Other |
| Build Command | 留空 |
| Output Directory | 留空 |
| Install Command | 留空（Vercel 自动识别 requirements.txt） |

### Functions 设置

- **Settings → Functions**
- 若有 Function Pattern 等自定义配置，建议**删除**，使用项目内 `vercel.json` 的配置

---

## 六、vercel.json（项目内已配置）

```json
{
  "rewrites": [
    { "source": "/(.*)", "destination": "/api/index" }
  ]
}
```

所有请求转发到 `api/index.py`（Flask 入口）。

---

## 七、子路径部署（如 chuanx.xyz/rtrisk）

### 1. RT_RISK 项目

- 环境变量添加：`APP_ROOT` = `/rtrisk`

### 2. 主站项目（chuanx.xyz）

在 `vercel.json` 中添加：

```json
{
  "rewrites": [
    { "source": "/rtrisk", "destination": "https://rt-risk-xxx.vercel.app" },
    { "source": "/rtrisk/:path*", "destination": "https://rt-risk-xxx.vercel.app/:path*" }
  ]
}
```

将 `rt-risk-xxx.vercel.app` 替换为 RT_RISK 的实际 Vercel 部署域名。

---

## 八、部署后检查

1. **首次部署**：Admin 登录后进入「缓存管理」，点击「刷新全量缓存」
2. **数据库连接失败**：检查 `DATABASE_URL`、白名单、删除 `DB_HOST_IP`
3. **api/index.py 错误**：检查 Root Directory、移除 Functions 自定义配置
4. **PM/Investor 无数据**：确认已添加 Vercel KV 并完成首次缓存刷新
