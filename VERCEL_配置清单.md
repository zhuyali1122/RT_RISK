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

## 三、缓存（纯文件，无 Redis）

- **Admin/Cron** 刷新时写入 `/tmp/rt_risk_cache/` 下的 `producer_full_cache.json`、`cache_meta.json`
- **其他页面** 只读，不修改缓存文件
- 每日 **UTC 00:00** Cron 自动刷新，需配置 `CRON_SECRET`

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
4. **PM/Investor 无数据**：Admin 完成首次缓存刷新后即可
