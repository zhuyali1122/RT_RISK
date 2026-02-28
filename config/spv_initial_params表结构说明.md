# spv_initial_params 表结构说明

优先级指标数据表，用于 KN 等生产商风控主页的「优先级指标」模块展示，以及投资组合的已投资平台列表。

**读取规则**：每个 spv_id 按 **最新 effective_date** 取一条记录。

## 建表 SQL 示例

```sql
CREATE TABLE IF NOT EXISTS spv_initial_params (
    spv_id                    VARCHAR(32) NOT NULL,
    effective_date            DATE NOT NULL,
    agreed_rate               NUMERIC(10,4),   -- 优先级收益率（target），如 15 或 0.15
    coverage_current          NUMERIC(10,4),
    leverage_current          NUMERIC(10,4),
    margin_deposit_current    NUMERIC(18,2),
    margin_deposit_required   NUMERIC(18,2),
    guarantee_deposit_current NUMERIC(18,2),
    guarantee_deposit_required NUMERIC(18,2),
    early_repayment_overdue_discount NUMERIC(10,4),  -- 早偿逾期折损
    vtg30_predicted_default_rate     NUMERIC(10,4),  -- Vtg30 预估 default rate
    principal_amount                NUMERIC(18,2),  -- 合作本金（USD），KN 页面优先本金=此值
    product_term                    NUMERIC(10,2),  -- 产品期限（月），未分配收益=principal_amount*product_term/12
    PRIMARY KEY (spv_id, effective_date)
);

-- 按 effective_date 取最新
-- SELECT * FROM spv_initial_params WHERE spv_id = 'kn' ORDER BY effective_date DESC LIMIT 1
```

## 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| spv_id | VARCHAR | 生产商 ID |
| effective_date | DATE | 生效日期，**按最新取一条** |
| agreed_rate | NUMERIC | 优先级收益率（target），即优先级的 rate |
| coverage_current | NUMERIC | 当前覆盖倍数 |
| leverage_current | NUMERIC | 当前杠杆倍数 |
| margin_deposit_current | NUMERIC | 当前保证金（USD） |
| margin_deposit_required | NUMERIC | 最低保证金要求 |
| guarantee_deposit_current | NUMERIC | 当前担保金 |
| guarantee_deposit_required | NUMERIC | 最低担保金要求 |
| early_repayment_overdue_discount | NUMERIC | 早偿逾期折损 |
| vtg30_predicted_default_rate | NUMERIC | Vtg30 预估 default rate |
| principal_amount | NUMERIC | 合作本金（USD），KN 页面优先本金=此值 |
| product_term | NUMERIC | 产品期限（月），未分配收益=principal_amount×product_term/12 |

## 覆盖倍数 V/L 计算公式

- **Value** = (M0本金 + M0应收利息 × 早偿逾期折损) × (1 - Vtg30预估default rate) + 现金余额（本币，需 / 汇率 转 USD）
- **Loan** = 合作本金 + 未分配收益（spv_initial_params 中已是 USD）
- 合作本金 = principal_amount；未分配收益 = principal_amount × product_term / 12
- M0本金、M0应收利息、现金来自 risk_data；早偿逾期折损、vtg30_predicted_default_rate、principal_amount、product_term 来自本表

## 与 spv_config.config 的配合

- **Senior:Junior 比例**：从 `spv_config.config`（JSONB）的 `senior_junior_ratio` 或 `leverage_ratio` 读取
- **斩仓线、平仓线、基准线**：从 `spv_config.config` 的 `liquidation_line`、`margin_call_line`、`baseline` 读取
- 若 `spv_config` 无 `config` 列，则从顶层字段 fallback
