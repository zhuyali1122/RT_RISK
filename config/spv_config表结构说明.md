# spv_config 表结构说明

生产商配置已从 `config/producers.json` 迁移至数据库表 `spv_config`。应用优先从该表读取，若表不存在或为空则 fallback 到 JSON 文件。

## 建表 SQL 示例

```sql
CREATE TABLE IF NOT EXISTS spv_config (
    spv_id       VARCHAR(32) PRIMARY KEY,
    name         VARCHAR(128),
    region       VARCHAR(64),
    contact      VARCHAR(128),
    product_type VARCHAR(32),
    onboard_date VARCHAR(32),
    currency     VARCHAR(8) DEFAULT 'USD',
    exchange_rate NUMERIC(18,6) DEFAULT 1,
    status       VARCHAR(16) DEFAULT 'active',
    leverage_ratio VARCHAR(32),
    priority_yield_pct NUMERIC(10,4),
    liquidation_line NUMERIC(10,4),
    margin_call_line NUMERIC(10,4),
    baseline     NUMERIC(10,4),
    margin_deposit NUMERIC(18,2),
    guarantee_deposit NUMERIC(18,2),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 示例数据（与 producers.json 对应）
INSERT INTO spv_config (spv_id, name, region, contact, product_type, onboard_date, currency, exchange_rate, status, leverage_ratio, priority_yield_pct, liquidation_line, margin_call_line, baseline)
VALUES
  ('kn', 'KN', '墨西哥', 'Kevin Yung', 'MCA', '-', 'MXN', 17.2, 'active', '7:3', 15, 1.02, 1.15, 1.43),
  ('docking', 'Docking', '印尼', 'Andrew Yin', 'PL', '-', 'IDR', 15800, 'active', '4:1', 14, 1.05, 1.1, 1.25)
ON CONFLICT (spv_id) DO UPDATE SET
  name = EXCLUDED.name,
  region = EXCLUDED.region,
  currency = EXCLUDED.currency,
  exchange_rate = EXCLUDED.exchange_rate,
  status = EXCLUDED.status,
  updated_at = CURRENT_TIMESTAMP;
```

## 最小必需字段

| 字段 | 类型 | 说明 |
|------|------|------|
| spv_id | VARCHAR | 生产商 ID，与 raw_loan.spv_id 对应 |
| name | VARCHAR | 显示名称 |
| currency | VARCHAR | 本币，如 MXN、IDR |
| exchange_rate | NUMERIC | 本币兑 USD 汇率 |
| status | VARCHAR | active / inactive |

## 可选字段

| 字段 | 说明 |
|------|------|
| region | 地区 |
| contact | 联系人 |
| product_type | 产品类型 |
| onboard_date | 上线日期 |
| leverage_ratio | 杠杆比例（Senior:Junior，如 7:3） |
| priority_yield_pct | 优先收益 |
| liquidation_line | 斩仓线 |
| margin_call_line | 平仓线 |
| baseline | 基准线 |
| margin_deposit | 保证金 |
| guarantee_deposit | 担保金 |
| **config** | **JSONB**，可包含：senior_junior_ratio, liquidation_line, margin_call_line, baseline（优先于顶层字段） |

## 数据币种

**数据库中的金额始终以 spv_config.currency 存储**（如 KN 为 MXN，Docking 为 IDR）。calc_overdue、raw_loan、raw_repayment 表内均为本币，统计时直接按本币汇总，缓存亦存本币。USD 由前端按 exchange_rate 换算展示，通过右上角货币按钮切换，页面内不显示 MXN 等字眼。

## 汇率覆盖

环境变量 `{SPV_ID}_EXCHANGE_RATE` 可覆盖数据库中的汇率，例如：

- `KN_EXCHANGE_RATE=17.5`
- `DOCKING_EXCHANGE_RATE=15850`
