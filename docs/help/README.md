# Help 文档目录

本目录存放网站 Help 页面的 Markdown 文档。

## 文档列表

- **指标计算说明.md**：风控、收益规模、现金流各页面指标的计算公式与数据来源

## 维护规则

**当以下模块中的指标算法发生变化时，请同步更新 `指标计算说明.md`：**

- `kn_risk_query.py` - 风控核心指标
- `kn_revenue.py` - 收益规模指标
- `kn_cashflow.py` - 现金流预测
- `kn_vintage.py` - Vintage 账龄分析
- `kn_collection.py` - 回收报表
- `spv_internal_params.py` - 优先级指标

## 添加新文档

1. 在本目录创建 `.md` 文件
2. 在 `templates/help.html` 的侧边栏添加链接
3. 访问 `/help/文档名`（不含 .md）即可查看
