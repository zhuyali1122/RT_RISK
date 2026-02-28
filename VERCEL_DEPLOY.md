# Vercel 部署说明

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
