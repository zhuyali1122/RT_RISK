"""
从数据库 spv_initial_params 表读取：
- 已投资平台列表，供投资组合累计统计使用
- 优先级指标，用于 KN 等生产商风控主页的优先级模块展示
- 按 spv_id + 最新 effective_date 取一条
- agreed_rate = 优先级收益率（target）
- Senior/Junior 比例、斩仓线、平仓线等从 spv_config.config (JSONB) 读取
"""
from decimal import Decimal


def _serialize(val):
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    return val


def _num(rec, k, *alts, default=0):
    for key in [k] + list(alts):
        v = rec.get(key)
        if v is not None and v != "":
            try:
                return float(_serialize(v))
            except (ValueError, TypeError):
                pass
    return default


def _compute_coverage_ratio(rec, spv_cfg, risk_data, exchange_rate, base_default=1.43):
    """
    覆盖倍数 V/L
    Value = (M0本金 + M0应收利息 * 早偿逾期折损) * (1 - Vtg30预估default rate) + 现金余额
    Loan = 合作本金 + 未分配收益（spv_initial_params 中已是 USD）
    早偿逾期折损、vtg30_predicted_default_rate、合作本金、未分配收益 从 spv_initial_params
    返回: (ratio, breakdown_dict)
    """
    early_discount = _num(rec, "early_repayment_loss_rate", "early_repayment_overdue_discount")
    if early_discount <= 0:
        early_discount = 1.0  # 默认不折损
    vtg30_default = _num(rec, "vtg_30_plus_predicted", "vtg30_predicted_default_rate", "vtg30_plus_predicted")
    vtg30_default = vtg30_default / 100 if vtg30_default > 1 else vtg30_default
    # 合作本金：principal_amount；未分配收益：principal_amount * product_term / 12
    principal_amount = _num(rec, "principal_amount")
    product_term = _num(rec, "product_term")
    coop_principal = principal_amount
    unallocated = principal_amount * product_term / 12 if product_term > 0 else 0
    rate = exchange_rate or 1
    if rate <= 0:
        rate = 1

    # 从 risk_data 取最新一行的 M0本金、M0应收利息、现金（本币）
    m0_bal = 0
    m0_interest = 0
    cash = 0
    stat_date = ""
    if risk_data:
        latest = sorted(risk_data, key=lambda r: r.get("stat_date", ""), reverse=True)[0]
        stat_date = latest.get("stat_date", "")
        m0_bal = float(latest.get("m0_balance") or 0)
        if m0_bal <= 0:
            cb = float(latest.get("current_balance") or 0)
            m0r = float(latest.get("m0_ratio") or 0)
            m0_bal = cb * m0r
        m0_interest = float(latest.get("m0_accrued_interest") or 0)
        cash = float(latest.get("cash") or 0)

    # Value = (M0本金 + M0应收利息 * 早偿逾期折损) * (1 - Vtg30) + 现金（本币）
    m0_interest_discounted = m0_interest * early_discount
    core_value_local = m0_bal + m0_interest_discounted
    after_default_local = core_value_local * (1 - vtg30_default)
    value_part = after_default_local + cash

    # Loan = 合作本金 + 未分配收益（spv_initial_params 中已是 USD，无需换算）
    loan_usd = coop_principal + unallocated
    if loan_usd <= 0:
        return base_default, _coverage_breakdown(
            m0_bal, m0_interest, early_discount, vtg30_default, coop_principal, unallocated,
            cash, value_part, 0, loan_usd, base_default, rate, stat_date
        )
    # Value 为本币，需与 Loan 同单位：value_usd = value_part / rate
    value_usd = value_part / rate if rate > 0 else value_part
    ratio = value_usd / loan_usd
    return ratio, _coverage_breakdown(
        m0_bal, m0_interest, early_discount, vtg30_default, coop_principal, unallocated,
        cash, value_part, value_usd, loan_usd, ratio, rate, stat_date
    )


def _coverage_breakdown(m0_bal, m0_interest, early_discount, vtg30_default, coop_principal, unallocated,
                       cash, value_part, value_usd, loan_usd, ratio, rate, stat_date):
    """构建覆盖倍数拆解数据，供前端展示"""
    m0_interest_discounted = m0_interest * early_discount
    core_value_local = m0_bal + m0_interest_discounted
    after_default_local = core_value_local * (1 - vtg30_default)
    return {
        "stat_date": stat_date,
        "m0_balance": round(m0_bal, 2),
        "m0_accrued_interest": round(m0_interest, 2),
        "early_repayment_overdue_discount": round(early_discount, 4),
        "m0_interest_discounted": round(m0_interest_discounted, 2),
        "vtg30_predicted_default_rate": round(vtg30_default * 100, 2) if vtg30_default <= 1 else round(vtg30_default, 2),
        "core_value_local": round(core_value_local, 2),
        "after_default_local": round(after_default_local, 2),
        "cash": round(cash, 2),
        "value_local": round(value_part, 2),
        "exchange_rate": round(rate, 4),
        "value_usd": round(value_usd, 2),
        "coop_principal": round(coop_principal, 2),
        "unallocated": round(unallocated, 2),
        "loan_usd": round(loan_usd, 2),
        "coverage_ratio": round(ratio, 2),
    }


def load_invested_spv_ids_for_portfolio():
    """
    从 spv_initial_params 加载已投资平台的 spv_id 列表
    返回: [ spv_id, ... ]
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return []

    cur = conn.cursor()
    out = []
    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'spv_initial_params'
            )
        """)
        if not cur.fetchone()[0]:
            return []

        cur.execute("""
            SELECT DISTINCT spv_id FROM spv_initial_params WHERE spv_id IS NOT NULL AND spv_id != ''
        """)
        for row in cur.fetchall():
            spv_id = str(row[0]).strip().lower() if row[0] else ""
            if spv_id:
                out.append(spv_id)
    except Exception:
        pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
    return out


def load_all_spv_initial_params_for_portfolio():
    """
    加载所有 SPV 的 spv_initial_params（按最新 effective_date 各取一条），供投资组合「平台持仓明细」使用
    返回: [ { spv_id, name, region, product_type, principal_amount, agreed_rate, effective_date }, ... ]
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return []

    producers = {}
    try:
        from spv_config import load_producers_from_spv_config
        producers = load_producers_from_spv_config()
    except Exception:
        pass

    cur = conn.cursor()
    out = []
    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'spv_initial_params'
            )
        """)
        if not cur.fetchone()[0]:
            return []

        cur.execute("""
            SELECT DISTINCT ON (spv_id) spv_id, effective_date, principal_amount, agreed_rate
            FROM spv_initial_params
            ORDER BY spv_id, effective_date DESC NULLS LAST
        """)
        for row in cur.fetchall():
            spv_id = str(row[0]).strip().lower() if row[0] else ""
            if not spv_id:
                continue
            eff = row[1]
            rec = {"principal_amount": row[2], "agreed_rate": row[3]}
            principal = _num(rec, "principal_amount")
            agreed = _num(rec, "agreed_rate")
            prod = producers.get(spv_id) or producers.get(spv_id.upper()) or {}
            name = prod.get("name") or prod.get("id") or spv_id
            region = prod.get("region") or prod.get("country") or ""
            product_type = prod.get("product_type") or ""
            effective_date = eff.isoformat()[:10] if hasattr(eff, "isoformat") else str(eff)[:10] if eff else ""
            out.append({
                "spv_id": spv_id,
                "name": name,
                "region": region,
                "product_type": product_type,
                "principal_amount": principal,
                "agreed_rate": agreed,
                "effective_date": effective_date,
            })
    except Exception:
        pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
    return out


def load_priority_indicators_for_spv(spv_id, risk_data=None, exchange_rate=1):
    """
    从 spv_initial_params 表加载优先级指标（按最新 effective_date）
    risk_data、exchange_rate 用于计算覆盖倍数 V/L
    返回与 partner_risk 模板兼容的 priority_indicators 结构，若表不存在或无数据返回 None
    """
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception:
        return None

    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'spv_initial_params'
            )
        """)
        if not cur.fetchone()[0]:
            return None

        # 按 spv_id + 最新 effective_date 取一条（若无 effective_date 列则取任意一条）
        try:
            cur.execute("""
                SELECT * FROM spv_initial_params
                WHERE spv_id = %s
                ORDER BY effective_date DESC NULLS LAST
                LIMIT 1
            """, (spv_id,))
        except Exception:
            cur.execute("SELECT * FROM spv_initial_params WHERE spv_id = %s LIMIT 1", (spv_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0].lower() for d in cur.description] if cur.description else []
        rec = dict(zip(cols, row)) if cols else {}

        # 从 spv_config.config 读取：Senior/Junior 比例、斩仓线、平仓线、基准线
        spv_cfg = _load_spv_config_config(spv_id)

        # 杠杆比例：Senior:Junior 从 spv_config.config 解析，limit = Senior/Junior
        lev_str = spv_cfg.get("senior_junior_ratio") or spv_cfg.get("leverage_ratio") or "7:3"
        lev_limit = _parse_ratio_to_limit(lev_str)
        lev_current = _num(rec, "leverage_current", "leverage_ratio_current", "coverage_current")
        if lev_current <= 0:
            lev_current = lev_limit * 0.6  # 占位
        leverage_ratio = {"current": round(lev_current, 1), "limit": lev_limit, "unit": "x"}

        # 优先收益率：目标固定 15%，当前值从 spv_initial_params 获取，找不到则缺失
        py_target = 0.15  # 目标固定写死 15%
        py_current = _num(rec, "priority_yield_current", "priority_yield_pct_current", "agreed_rate")
        py_current = py_current / 100 if py_current > 1 else py_current
        if py_current <= 0:
            priority_yield = None  # 缺失
        else:
            priority_yield = {"current": py_current, "target": py_target, "unit": "%"}

        # 覆盖倍数：V/L，斩仓线、平仓线、基准线从 spv_config.config
        liq = spv_cfg.get("liquidation_line") or 1.02
        mc = spv_cfg.get("margin_call_line") or 1.15
        base = spv_cfg.get("baseline") or 1.43
        cov_current, cov_breakdown = _compute_coverage_ratio(
            rec, spv_cfg, risk_data, exchange_rate, base
        )
        coverage_ratio = {
            "current": round(cov_current, 2),
            "liquidation": liq,
            "margin_call": mc,
            "baseline": base,
            "unit": "x",
            "breakdown": cov_breakdown,
        }

        # 优先本金：KN Risk 页面中 优先本金 = 合作本金 = principal_amount
        priority_principal = _num(rec, "principal_amount")

        # 保证金、担保金
        margin_deposit = None
        mg_cur = _num(rec, "margin_deposit_current", "margin_deposit")
        mg_req = _num(rec, "margin_deposit_required", "margin_deposit_required")
        if mg_cur > 0 or mg_req > 0:
            margin_deposit = {"current": mg_cur, "required": mg_req or mg_cur, "currency": "USD"}

        guarantee_deposit = None
        gt_cur = _num(rec, "guarantee_deposit_current", "guarantee_deposit")
        gt_req = _num(rec, "guarantee_deposit_required", "guarantee_deposit_required")
        if gt_cur > 0 or gt_req > 0:
            guarantee_deposit = {"current": gt_cur, "required": gt_req or gt_cur, "currency": "USD"}

        return {
            "priority_principal": priority_principal if priority_principal > 0 else None,
            "leverage_ratio": leverage_ratio,
            "priority_yield": priority_yield,
            "coverage_ratio": coverage_ratio,
            "margin_deposit": margin_deposit,
            "guarantee_deposit": guarantee_deposit,
        }
    except Exception:
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _parse_ratio_to_limit(ratio_str):
    """解析 Senior:Junior 如 7:3 -> limit = 7/3"""
    try:
        s = str(ratio_str or "5:1").replace("：", ":")
        parts = s.split(":")
        if len(parts) >= 2:
            a, b = float(parts[0]), float(parts[1])
            if b > 0:
                return round(a / b, 1)
    except (ValueError, TypeError):
        pass
    return 5.0


def _load_spv_config_config(spv_id):
    """
    从 spv_config 的 config 列（JSONB）读取：senior_junior_ratio, liquidation_line, margin_call_line, baseline
    若无 config 列则从顶层字段 fallback
    """
    try:
        from db_connect import get_connection
        import json
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM spv_config WHERE spv_id = %s", (spv_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return _load_spv_config_fallback(spv_id)
        cols = [d[0].lower() for d in cur.description]
        rec = dict(zip(cols, row))
        cur.close()
        conn.close()

        out = {}
        config_json = rec.get("config")
        if config_json:
            if isinstance(config_json, dict):
                out = {k: v for k, v in config_json.items() if v is not None}
            elif isinstance(config_json, str):
                try:
                    out = json.loads(config_json)
                except Exception:
                    pass
        def _f(key, *alts, default=None):
            for k in [key] + list(alts):
                v = out.get(k) or rec.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        pass
            return default
        return {
            "senior_junior_ratio": out.get("senior_junior_ratio") or rec.get("leverage_ratio"),
            "leverage_ratio": out.get("leverage_ratio") or rec.get("leverage_ratio"),
            "liquidation_line": _f("liquidation_line", default=1.02),
            "margin_call_line": _f("margin_call_line", default=1.15),
            "baseline": _f("baseline", default=1.43),
            "priority_yield_pct": _f("priority_yield_pct", default=15),
        }
    except Exception:
        return _load_spv_config_fallback(spv_id)


def _load_spv_config_fallback(spv_id):
    """从 load_producers 获取（无 config 列时）"""
    try:
        from spv_config import load_producers_from_spv_config
        p = load_producers_from_spv_config().get(spv_id, {})
        return {
            "senior_junior_ratio": p.get("leverage_ratio"),
            "leverage_ratio": p.get("leverage_ratio"),
            "liquidation_line": p.get("liquidation_line") or 1.02,
            "margin_call_line": p.get("margin_call_line") or 1.15,
            "baseline": p.get("baseline") or 1.43,
            "priority_yield_pct": p.get("priority_yield_pct") or 15,
        }
    except Exception:
        return {"liquidation_line": 1.02, "margin_call_line": 1.15, "baseline": 1.43, "priority_yield_pct": 15}


def _load_spv_config_thresholds(spv_id):
    """兼容旧接口：返回与 _load_spv_config_config 相同结构"""
    return _load_spv_config_config(spv_id)


def compute_priority_from_risk_data(spv_id, risk_data, exchange_rate=1):
    """
    当 spv_initial_params 无数据时，从 risk_data + spv_config 计算优先级指标
    risk_data 中金额为 USD（已转换）
    Senior/Junior、斩仓线、平仓线从 spv_config.config 读取
    """
    if not risk_data:
        return None
    spv_cfg = _load_spv_config_config(spv_id)
    liq = spv_cfg.get("liquidation_line") or 1.02
    mc = spv_cfg.get("margin_call_line") or 1.15
    base = spv_cfg.get("baseline") or 1.43

    # 覆盖倍数：使用 V/L 公式计算
    cov_current, cov_breakdown = _compute_coverage_ratio({}, spv_cfg, risk_data, exchange_rate, base)

    # 杠杆：Senior:Junior 从 spv_config.config 读取
    lev_str = spv_cfg.get("senior_junior_ratio") or spv_cfg.get("leverage_ratio") or "5:1"
    lev_limit = _parse_ratio_to_limit(lev_str)
    lev_current = min(cov_current * 0.6, lev_limit * 0.7)  # 占位

    # 优先收益率、优先本金仅从 spv_initial_params 获取，compute 时无该表数据，故缺失
    return {
        "priority_principal": None,
        "leverage_ratio": {"current": round(lev_current, 1), "limit": lev_limit, "unit": "x"},
        "priority_yield": None,
        "coverage_ratio": {
            "current": round(cov_current, 2) if cov_current else base,
            "liquidation": liq,
            "margin_call": mc,
            "baseline": base,
            "unit": "x",
            "breakdown": cov_breakdown,
        },
        "margin_deposit": None,
        "guarantee_deposit": None,
    }
