# Vercel 部署说明

---

## 数据库连接超时排查（Vercel → 阿里云 RDS）

部署到 Vercel 后若出现 `connection timeout` 或 `connection to server at "xxx" failed: timeout expired`，请逐项检查：

### 1. 不要设置 DB_HOST_IP（最常见原因）

`DB_HOST_IP` 用于本地「域名解析失败时用 IP 直连」。若你在本地通过 VPN 或内网获取了 IP（如 198.18.x.x、10.x.x.x、172.x.x.x），该 IP 在 Vercel 上**不可达**。

- **操作**：在 Vercel 环境变量中**删除** `DB_HOST_IP`，仅保留 `DATABASE_URL`
- `DATABASE_URL` 中的 host 必须为**外网地址**（如 `pgm-xxx.pg.rds.aliyuncs.com`），不要用内网 IP

### 2. 使用 RDS 外网地址

- 阿里云 RDS 控制台 → 实例 → **数据库连接**
- 确认已申请**外网地址**（未释放）
- `DATABASE_URL` 中的 host 使用该外网地址，格式如：`postgresql://user:pass@pgm-xxx.pg.rds.aliyuncs.com:5432/dbname?sslmode=require`

### 3. 白名单

- RDS 控制台 → **数据安全性** → **白名单设置**
- 添加 `0.0.0.0/0` 允许所有 IP（或添加 Vercel 出站 IP 段，但 Vercel IP 动态变化，建议测试阶段用 0.0.0.0/0）

### 4. SSL

- 阿里云 RDS 公网通常需 SSL，在 `DATABASE_URL` 末尾加 `?sslmode=require`
- 或设置环境变量 `DB_SSLMODE=require`

### 5. 连接超时与连接池（Vercel 限制）

- Vercel Serverless 单次执行有超时（Hobby 10s，Pro 60s）
- 建议在 Vercel 环境变量中设置：
  - `DATABASE_POOL_SIZE=1`（Serverless 不宜用大连接池）
  - `DATABASE_CONNECT_TIMEOUT=8`（缩短连接超时，避免长时间等待）

### 6. 检查 Vercel 环境变量

确认以下变量正确且**仅包含必要项**：

| 变量 | 说明 | 注意 |
|------|------|------|
| `DATABASE_URL` | 必填，含外网 host | host 不要用内网 IP |
| `DB_HOST_IP` | **建议删除** | 本地 VPN 解析的 IP 在 Vercel 不可达 |
| `DB_SSLMODE` | 可选，`require` 或 `disable` | 阿里云公网通常需 `require` |
| `DATABASE_POOL_SIZE` | 可选，建议 `1` | Serverless 场景 |
| `APP_ROOT` | 若子路径部署则设置 | 如 `/rtrisk` |

---

## 共享缓存（Admin 刷新后其他用户可访问）

Vercel Serverless 的 `/tmp` 是**实例级** ephemeral 存储，不同请求可能落在不同实例，Admin 刷新的缓存在其他用户请求时可能读不到。需使用 **Vercel KV (Upstash Redis)** 作为共享缓存。

### 1. 添加 Vercel KV

1. 进入项目 → **Storage** → **Create Database**
2. 选择 **KV**（或从 Marketplace 添加 Upstash Redis）
3. 创建后，Vercel 会自动注入 `KV_REST_API_URL` 和 `KV_REST_API_TOKEN`

### 2. 环境变量（自动注入）

添加 KV 后，以下变量会自动注入，**无需手动配置**：

| 变量 | 说明 |
|------|------|
| `KV_REST_API_URL` | Redis REST API 地址 |
| `KV_REST_API_TOKEN` | 认证 Token |

若使用 Upstash 直接创建，可手动配置 `UPSTASH_REDIS_REST_URL` 和 `UPSTASH_REDIS_REST_TOKEN`。

### 3. 工作流程

- **Admin** 登录 → 进入「缓存管理」→ 点击「刷新全量缓存」
- 缓存写入 **Redis**，所有 Vercel 实例共享
- **PM/Investor** 登录后，任意实例均可读取最新缓存，无需再次刷新

---

## 设置入口为 chuanx.xyz/rtrisk

### 1. 在 RT_RISK 项目中设置环境变量
- 进入 RT_RISK 项目 → **Settings** → **Environment Variables**
- 添加：`APP_ROOT` = `/rtrisk`

### 2. 在 chuanx.xyz 主站项目中添加 Rewrite
若 chuanx.xyz 是另一个 Vercel 项目（主站），在其 `vercel.json` 中添加：

```json
{
  "rewrites": [
    { "source": "/rtrisk", "destination": "https://rt-risk-xxx.vercel.app" },
    { "source": "/rtrisk/:path*", "destination": "https://rt-risk-xxx.vercel.app/:path*" }
  ]
}
```

将 `rt-risk-xxx.vercel.app` 替换为 RT_RISK 的实际 Vercel 部署域名。

### 3. 若 chuanx.xyz 直接指向 RT_RISK
若 chuanx.xyz 是 RT_RISK 的域名，则访问根路径即为应用。要使用 /rtrisk 作为入口，需在主站配置 rewrite 将 /rtrisk 转发到 RT_RISK。

---

## 若出现 "api/index.py defined in functions doesn't match" 错误

该错误通常来自 **Vercel 项目设置** 中的 Functions 配置。请按以下步骤检查：

### 1. 检查 Root Directory
- 进入项目 → **Settings** → **General**
- **Root Directory** 必须为 **空** 或 **`.`**（项目根目录）
- 若设置为其他路径，`api/` 文件夹将无法被正确识别

### 2. 移除 Functions 配置
- 进入项目 → **Settings** → **Functions**
- 若存在 **Function Pattern** 或类似配置（如 `api/index.py`），请**删除**
- 保留为空，让项目使用 `vercel.json` 的配置

### 3. 检查 Framework Preset
- 进入项目 → **Settings** → **General**
- **Framework Preset** 建议设为 **Other**，避免自动添加不兼容的配置

### 4. 重新部署
- 在 **Deployments** 中点击 **Redeploy** 重新部署
