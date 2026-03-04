# Method 2: Vintage 覆盖率 - 计算逻辑与 SQL 整理

## 一、整体思路（简化版）

按产品**平均久期**确定只看未来 N 个月（如 90 天产品≈3 月，看 4 个月），仅用 MOB 1~max_mob 的 Vintage 数据。根据当前 loan 的**按月残余期限**，取过去最新的 Vintage 对应 MOB 的 default 来计算，不做全量计算。

**公式**：`Value2 = Σ (每笔 loan 的合同剩余价值 × survival_rate)`  
其中 `survival_rate = ∏(1 - default_mob_i)`，i 为 `min(残余月数, max_mob)`，仅考虑 MOB 1~max_mob。

---

## 二、简化规则

| 项目 | 说明 |
|-----|------|
| 平均产品期限 | 从活跃 loan 的 `AVG(term_months)` 计算，输出供参考 |
| max_mob | `min(4, ceil(avg_product_term))`，如 90 天产品≈3 月，只看 MOB 1~4 |
| default_by_mob | 仅保留 MOB 1~max_mob，其他 MOB 忽略 |
| 残余期限 | 每笔 loan 的 `remaining = min(term - mob, max_mob)`，只算这几个月 |

## 三、计算步骤

### Step 1：获取 Vintage 数据，构建 default_by_mob（仅 MOB 1~max_mob）

**来源**：`kn_vintage.compute_vintage_data(spv_id, stat_date)`

**Vintage 内部 SQL（kn_vintage.py）**：

```sql
-- 1. 各 cohort 的 disbursement 汇总
SELECT
    to_char(disbursement_time::date, 'YYYY-MM') AS disbursement_month,
    SUM(disbursement_amount) AS disbursement_amount,
    COUNT(*) AS disbursement_count,
    COUNT(DISTINCT customer_id) AS borrower_count
FROM raw_loan
WHERE spv_id = :spv_id
GROUP BY 1
ORDER BY 1;

-- 2. 各 cohort 的余额与 DPD 分布（按 disbursement_month 聚合）
SELECT
    to_char(r.disbursement_time::date, 'YYYY-MM') AS disbursement_month,
    COALESCE(SUM(c.outstanding_principal), 0) AS current_balance,
    COALESCE(SUM(CASE WHEN c.dpd >= 1 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_1_bal,
    COALESCE(SUM(CASE WHEN c.dpd >= 3 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_3_bal,
    COALESCE(SUM(CASE WHEN c.dpd >= 7 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_7_bal,
    COALESCE(SUM(CASE WHEN c.dpd >= 15 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_15_bal,
    COALESCE(SUM(CASE WHEN c.dpd >= 30 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_30_bal
FROM calc_overdue_y{year}m{month} c
JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
WHERE c.stat_date = :stat_date AND c.spv_id = :spv_id AND c.loan_status IN (1, 2)
GROUP BY 1
ORDER BY 1;
```

**Vintage 输出**（每行一个 cohort）：
- `disbursement_month`：放款月
- `mob`：当前 stat_date 下的账龄月数 = (stat_date 年-月) - (disbursement 年-月)
- `dpd30_rate`：该 cohort 的 DPD30+ 余额占比 = overdue_30_bal / current_balance（作为 default 代理）

**构建 default_by_mob（仅 MOB 1~max_mob）**：
```
default_by_mob[mob] = AVG(dpd30_rate) 对同一 mob 下的所有 cohort 取平均，且 mob 仅在 [1, max_mob]
```
- 若某 MOB 无数据，则用 `vtg30_predicted_default_rate` 作为 fallback

---

### Step 2：获取平均产品期限与 max_mob

```sql
SELECT AVG(r.term_months) AS avg_term, COUNT(*) AS cnt
FROM calc_overdue c
JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
WHERE c.stat_date = :stat_date AND c.spv_id = :spv_id AND c.loan_status IN (1, 2);
```
`max_mob = min(4, ceil(avg_term))`，输出 avg_product_term 供参考。

### Step 3：批量获取活跃 loan 及合同剩余价值

一次性查出 `(loan_id, disbursement_time, term_months, principal, interest)`，见下方 SQL 汇总 3.1。

### Step 4：对每笔 loan 计算（Python 循环）

#### 4.1 计算当前 MOB 与残余期数（cap 到 max_mob）

```
mob = (stat_date 年 - disbursement 年) * 12 + (stat_date 月 - disbursement 月)
remaining = term_months - mob
remaining_cap = min(remaining, max_mob)   -- 只看未来 max_mob 个月
```

#### 4.2 合同剩余价值

`loan_contract = principal + interest`（来自 Step 3 批量结果）

#### 4.3 计算 survival_rate

```
survival = 1.0
for i in 0..(remaining_cap-1):
    mob_i = mob + i + 1   -- 对应第 i+1 个剩余月的 MOB
    d = default_by_mob.get(mob_i, vtg30_default)   -- 仅 MOB 1~max_mob 有值
    survival *= (1 - d)
```

#### 4.4 累计

```
value2_weighted += loan_contract * survival
```

---

### Step 5：汇总

```
Value2_usd = value2_weighted / exchange_rate
Vintage覆盖率 = Value2_usd / Loan
```

---

## 四、SQL 汇总（便于直接执行）

### 4.1 一次性获取所有 loan 的合同剩余价值（批量）

```sql
-- 活跃 loan 及其合同剩余价值（批量）
WITH active_loans AS (
    SELECT c.loan_id, r.disbursement_time, r.term_months
    FROM calc_overdue_y2026m02 c
    JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
    WHERE c.stat_date = '2026-02-25' AND c.spv_id = 'docking' AND c.loan_status IN (1, 2)
),
future_due AS (
    SELECT
        rl.loan_id,
        rl.disbursement_time,
        rl.term_months,
        SUM((COALESCE(elem->>'principal', elem->>'principal_due', '0'))::numeric) AS principal,
        SUM((COALESCE(elem->>'interest', elem->>'interest_due', '0'))::numeric) AS interest
    FROM raw_loan rl
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rl.repayment_schedule->'schedule', '[]'::jsonb)) elem
    WHERE rl.spv_id = 'docking'
      AND rl.loan_id IN (SELECT loan_id FROM active_loans)
      AND elem->>'due_date' IS NOT NULL
      AND (elem->>'due_date')::date > '2026-02-25'
    GROUP BY rl.loan_id, rl.disbursement_time, rl.term_months
)
SELECT loan_id, disbursement_time, term_months, principal, interest,
       (principal + interest) AS contract_value
FROM future_due;
```

### 4.2 Vintage 的 default_by_mob 来源（kn_vintage 内部）

```sql
-- 按 cohort 的 dpd30_rate，需在 Python 中按 mob 聚合
SELECT
    to_char(r.disbursement_time::date, 'YYYY-MM') AS disbursement_month,
    COALESCE(SUM(c.outstanding_principal), 0) AS current_balance,
    COALESCE(SUM(CASE WHEN c.dpd >= 30 THEN c.outstanding_principal ELSE 0 END), 0) AS overdue_30_bal
FROM calc_overdue_y2026m02 c
JOIN raw_loan r ON r.loan_id = c.loan_id AND r.spv_id = c.spv_id
WHERE c.stat_date = '2026-02-25' AND c.spv_id = 'docking' AND c.loan_status IN (1, 2)
GROUP BY 1;
-- dpd30_rate = overdue_30_bal / current_balance
-- mob = (stat_date 年-月) - (disbursement_month 年-月)
```

---

## 五、性能说明

已采用批量 SQL 一次性查出所有 loan 的 `(loan_id, disbursement_time, term_months, principal, interest)`，在 Python 中循环计算 survival 和累加，避免 N 次数据库往返。
