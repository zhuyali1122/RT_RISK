"""
RT_RISK Web 应用 - 角色化资产管理平台
"""
import json
import os
from functools import wraps
from flask import (
    Flask, render_template, jsonify, request,
    session, redirect, url_for, send_from_directory
)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.urandom(24)

BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, "config", "users.json")
PORTFOLIO_PATH = os.path.join(BASE_DIR, "config", "portfolio.json")
DD_CHECKLIST_PATH = os.path.join(BASE_DIR, "config", "dd_checklist.json")
PARTNERS_PATH = os.path.join(BASE_DIR, "config", "partners.json")
TRANSACTIONS_PATH = os.path.join(BASE_DIR, "config", "transactions.json")
VINTAGE_PORTFOLIO_PATH = os.path.join(BASE_DIR, "config", "vintage_portfolio.json")
DPD_PORTFOLIO_PATH = os.path.join(BASE_DIR, "config", "dpd_portfolio.json")
MATURITY_PORTFOLIO_PATH = os.path.join(BASE_DIR, "config", "maturity_portfolio.json")
LOAN_DETAILS_PATH = os.path.join(BASE_DIR, "config", "loan_details.json")
DD_TEMPLATES_DIR = os.path.join(BASE_DIR, "dd_templates")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")

os.makedirs(DD_TEMPLATES_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)


def load_user_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_portfolio_data():
    with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_dd_checklist():
    with open(DD_CHECKLIST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_partners():
    with open(PARTNERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_transactions():
    with open(TRANSACTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_vintage_portfolio():
    if os.path.exists(VINTAGE_PORTFOLIO_PATH):
        with open(VINTAGE_PORTFOLIO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_dpd_portfolio():
    if os.path.exists(DPD_PORTFOLIO_PATH):
        with open(DPD_PORTFOLIO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_maturity_portfolio():
    if os.path.exists(MATURITY_PORTFOLIO_PATH):
        with open(MATURITY_PORTFOLIO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_loan_details():
    if os.path.exists(LOAN_DETAILS_PATH):
        with open(LOAN_DETAILS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _find_loan(partner_id, loan_id):
    """Find loan in vintage, dpd, or maturity portfolio."""
    v = load_vintage_portfolio()
    for month, loans in v.get(partner_id, {}).items():
        for loan in loans:
            if loan.get("loan_id") == loan_id:
                return loan
    d = load_dpd_portfolio()
    for bucket, loans in d.get(partner_id, {}).items():
        for loan in loans:
            if loan.get("loan_id") == loan_id:
                return loan
    m = load_maturity_portfolio()
    for month, loans in m.get(partner_id, {}).items():
        for loan in loans:
            if loan.get("loan_id") == loan_id:
                return loan
    return None


def _portfolio_stats(loans):
    """Compute customer_type, product_type stats and key KPIs for portfolio."""
    n = len(loans)
    if n == 0:
        return {
            "customer_stats": {"new_count": 0, "returning_count": 0, "new_pct": 0, "returning_pct": 0},
            "product_stats": [],
            "credit_ratings": [],
            "credit_stats": [],
            "customer_pie_gradient": "#E2E8F0 0% 100%",
            "product_pie_gradient": "#E2E8F0 0% 100%",
            "credit_pie_gradient": "#E2E8F0 0% 100%",
            "kpi_stats": {
                "total_disbursement": 0, "avg_term": 0, "total_overdue": 0,
                "outstanding_balance": 0, "dpd7_pct": 0, "mob1_pct": None,
            },
        }
    new_count = sum(1 for l in loans if l.get("customer_type") == "new")
    ret_count = n - new_count
    new_pct = round(new_count / n * 100, 1)
    ret_pct = round(ret_count / n * 100, 1)
    customer_pie = f"#2E7D6E 0% {new_pct}%, #4DB6AC {new_pct}% 100%"
    product_counts = {}
    for l in loans:
        pt = l.get("product_type") or "其他"
        product_counts[pt] = product_counts.get(pt, 0) + 1
    colors = ["#2E7D6E", "#4DB6AC", "#E8A838", "#3B4A6A", "#D8434F"]
    product_stats = []
    cum = 0
    pie_parts = []
    for i, (name, cnt) in enumerate(product_counts.items()):
        pct = round(cnt / n * 100, 1)
        product_stats.append({"name": name, "count": cnt, "pct": pct, "color": colors[i % len(colors)]})
        pie_parts.append(f"{colors[i % len(colors)]} {cum}% {cum + pct}%")
        cum += pct
    product_pie = ", ".join(pie_parts) if pie_parts else "#E2E8F0 0% 100%"

    credit_counts = {}
    for l in loans:
        cr = l.get("credit_rating") or "-"
        credit_counts[cr] = credit_counts.get(cr, 0) + 1
    credit_ratings = sorted(credit_counts.keys())
    credit_stats = []
    cum = 0
    credit_pie_parts = []
    cr_colors = ["#2E7D6E", "#4DB6AC", "#E8A838", "#3B4A6A", "#D8434F"]
    for i, cr in enumerate(credit_ratings):
        cnt = credit_counts[cr]
        pct = round(cnt / n * 100, 1)
        credit_stats.append({"name": cr, "count": cnt, "pct": pct, "color": cr_colors[i % len(cr_colors)]})
        credit_pie_parts.append(f"{cr_colors[i % len(cr_colors)]} {cum}% {cum + pct}%")
        cum += pct
    credit_pie = ", ".join(credit_pie_parts) if credit_pie_parts else "#E2E8F0 0% 100%"

    # Key KPIs
    total_disb = sum(float(l.get("disbursement_amount") or 0) for l in loans)
    terms = [float(l.get("term_month") or 0) for l in loans]
    avg_term = round(sum(terms) / n, 1) if terms else 0
    total_overdue = sum(
        float(l.get("overdue_principal") or 0) + float(l.get("overdue_interest") or 0) + float(l.get("overdue_penalty") or 0)
        for l in loans
    )
    outstanding = sum(float(l.get("outstanding_principal") or 0) for l in loans)
    dpd7_count = sum(1 for l in loans if (l.get("dpd") or 0) >= 7)
    dpd7_pct = round(dpd7_count / n * 100, 2) if n else 0
    mob1_rates = [float(l.get("mob1_rate") or 0) for l in loans if l.get("mob1_rate") is not None]
    mob1_pct = round(sum(mob1_rates) / len(mob1_rates) * 100, 2) if mob1_rates else None

    return {
        "customer_stats": {"new_count": new_count, "returning_count": ret_count, "new_pct": new_pct, "returning_pct": ret_pct},
        "product_stats": product_stats,
        "credit_ratings": credit_ratings,
        "credit_stats": credit_stats,
        "customer_pie_gradient": customer_pie,
        "product_pie_gradient": product_pie,
        "credit_pie_gradient": credit_pie,
        "kpi_stats": {
            "total_disbursement": total_disb,
            "avg_term": avg_term,
            "total_overdue": total_overdue,
            "outstanding_balance": outstanding,
            "dpd7_pct": dpd7_pct,
            "mob1_pct": mob1_pct,
        },
    }


def fmt_usd(n):
    if n >= 1e6:
        return f"${n/1e6:.1f}M"
    if n >= 1e3:
        return f"${n/1e3:.0f}K"
    return f"${n:,.0f}"


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            # API 请求返回 JSON，避免 fetch 解析 HTML 报错
            if request.path.startswith("/api/") or "application/json" in (request.headers.get("Accept") or ""):
                return jsonify({"error": "请先登录"}), 401
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


@app.route("/")
def index():
    if "user" in session:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login_page"))


@app.route("/login")
def login_page():
    if "user" in session:
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json()
    username = data.get("username", "").strip()
    password = data.get("password", "")

    config = load_user_config()

    for user in config["users"]:
        if user["username"] == username and user["password"] == password:
            role_info = config["roles"].get(user["role"], {})
            session["user"] = {
                "username": user["username"],
                "display_name": user["display_name"],
                "email": user["email"],
                "role": user["role"],
                "role_label": role_info.get("label", user["role"]),
                "permissions": role_info.get("permissions", []),
            }

            redirect_url = "/dashboard"
            return jsonify({"success": True, "redirect": redirect_url})

    return jsonify({"success": False, "message": "用户名或密码错误"})


@app.route("/dashboard")
@login_required
def dashboard():
    user = session["user"]
    return render_template("dashboard.html", user=user)


@app.route("/alert/panel")
@login_required
def alert_panel():
    user = session["user"]
    if "alert_panel" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    data = load_partners()
    partners_list = []
    for pid, p in data["partners"].items():
        alerts = p.get("alerts", [])
        pi = p.get("priority_indicators", {})
        cov = pi.get("coverage_ratio", {})
        partners_list.append({
            "id": p["id"],
            "name": p["name"],
            "country": p["country"],
            "product_type": p["product_type"],
            "alerts": alerts,
            "coverage": cov,
        })
    return render_template(
        "alert_panel.html",
        user=user,
        partners=partners_list,
    )


def _portfolio_cumulative_stats():
    """从 partners 汇总：累积放款总额、累积借款笔数、累积借款人数"""
    data = load_partners()
    partners = data.get("partners", {})
    cum_disb = 0
    cum_loans = 0
    cum_borrowers = 0
    for p in partners.values():
        # 累积放款：取 revenue_data 最后一期的 cumulative_disbursement
        rev = p.get("revenue_data", [])
        if rev:
            last = rev[-1]
            v = last.get("cumulative_disbursement")
            if v is not None:
                cum_disb += int(v) if isinstance(v, (int, float)) else int(str(v).replace(",", ""))
        # 累积借款笔数、人数：从 risk_data 最新一条的 vintage_data 汇总
        risk = p.get("risk_data", [])
        if risk:
            latest = sorted(risk, key=lambda r: r.get("stat_date", ""), reverse=True)[0]
            # 累积借款笔数：vintage_data 各月 disbursement_count 之和
            for vd in latest.get("vintage_data", []):
                cnt = vd.get("disbursement_count")
                if cnt is not None:
                    cum_loans += int(cnt) if isinstance(cnt, (int, float)) else int(str(cnt).replace(",", ""))
            # 累积借款人数：active_borrowers（当前在贷人数，作近似）
            ab = latest.get("active_borrowers")
            if ab is not None:
                cum_borrowers += int(ab) if isinstance(ab, (int, float)) else int(str(ab).replace(",", ""))
    return {
        "cumulative_disbursement": cum_disb,
        "cumulative_loan_count": cum_loans,
        "cumulative_borrower_count": cum_borrowers,
    }


# 国家名映射：中文 -> ECharts 世界地图用英文
_COUNTRY_NAME_MAP = {
    "中国": "China", "印度尼西亚": "Indonesia", "越南": "Vietnam", "泰国": "Thailand",
    "菲律宾": "Philippines", "马来西亚": "Malaysia", "新加坡": "Singapore",
    "印度": "India", "孟加拉国": "Bangladesh", "巴基斯坦": "Pakistan", "斯里兰卡": "Sri Lanka",
    "日本": "Japan", "韩国": "South Korea",
    "墨西哥": "Mexico", "巴西": "Brazil", "哥伦比亚": "Colombia", "阿根廷": "Argentina",
    "智利": "Chile", "秘鲁": "Peru", "尼日利亚": "Nigeria", "肯尼亚": "Kenya",
    "南非": "South Africa", "埃及": "Egypt", "摩洛哥": "Morocco",
    "美国": "United States", "英国": "United Kingdom", "德国": "Germany", "法国": "France",
}

def _aggregate_producer_data(records):
    """按地域、行业、成熟度、场景、类型、国家聚合"""
    from collections import defaultdict
    by_region = defaultdict(list)
    by_country = defaultdict(list)  # 国家级，用于世界地图
    by_industry = defaultdict(list)
    by_maturity = defaultdict(list)
    by_scenario = defaultdict(list)
    by_type = defaultdict(list)
    region_detail_map = defaultdict(set)
    for r in records:
        region = r.get("region") or r.get("地域") or "未分类"
        region_d = r.get("region_detail") or r.get("地区明细") or region
        industry = r.get("industry") or r.get("行业") or "未分类"
        maturity = r.get("maturity") or r.get("项目成熟程度") or "未分类"
        scenario = r.get("scenario") or r.get("场景") or "未分类"
        typ = r.get("type") or r.get("类型") or "未分类"
        name = r.get("name") or r.get("名称") or r.get("生产商") or "-"
        rec = {**r, "name": name, "region": region, "region_detail": region_d, "industry": industry, "maturity": maturity, "scenario": scenario, "type": typ}
        by_region[region].append(rec)
        country_en = _COUNTRY_NAME_MAP.get(region_d, region_d)
        by_country[country_en].append(rec)
        by_industry[industry].append(rec)
        by_maturity[maturity].append(rec)
        by_scenario[scenario].append(rec)
        by_type[typ].append(rec)
        region_detail_map[region].add(region_d)
    return {
        "by_region": {k: {"count": len(v), "items": v} for k, v in by_region.items()},
        "by_country": {k: {"count": len(v), "items": v} for k, v in by_country.items()},
        "by_industry": {k: {"count": len(v), "items": v} for k, v in by_industry.items()},
        "by_maturity": {k: {"count": len(v), "items": v} for k, v in by_maturity.items()},
        "by_scenario": {k: {"count": len(v), "items": v} for k, v in by_scenario.items()},
        "by_type": {k: {"count": len(v), "items": v} for k, v in by_type.items()},
        "region_details": {k: list(v) for k, v in region_detail_map.items()},
    }


@app.route("/api/producers/data")
@login_required
def api_producers_data():
    """生产商拓展列表数据（含聚合统计）"""
    if "view_portfolio" not in session.get("user", {}).get("permissions", []):
        return jsonify({"error": "权限不足"}), 403
    try:
        from feishu_producer import load_producer_data
        data = load_producer_data()
        records = data.get("records", [])
        agg = _aggregate_producer_data(records)
        return jsonify({
            "records": records,
            "aggregation": agg,
            "source": data.get("source", "unknown"),
            "total": len(records),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/portfolio")
@login_required
def portfolio():
    user = session["user"]
    if "view_portfolio" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    portfolio_data = load_portfolio_data()
    # 合并从 partners 汇总的累积指标
    cum = _portfolio_cumulative_stats()
    portfolio_data.setdefault("fund", {})["cumulative_disbursement"] = cum["cumulative_disbursement"]
    portfolio_data.setdefault("fund", {})["cumulative_loan_count"] = cum["cumulative_loan_count"]
    portfolio_data.setdefault("fund", {})["cumulative_borrower_count"] = cum["cumulative_borrower_count"]
    return render_template("portfolio.html", user=user, portfolio=portfolio_data)


@app.route("/partner/apply")
@login_required
def partner_apply():
    user = session["user"]
    if "apply_partner" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    dd_checklist = load_dd_checklist()
    return render_template("partner_apply.html", user=user, dd_checklist=dd_checklist)


@app.route("/partner/manage")
@login_required
def partner_manage():
    user = session["user"]
    if "manage_partners" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    data = load_partners()
    username = user["username"]
    partner_ids = data["assignments"].get(username, [])
    partners = []
    for pid in partner_ids:
        p = data["partners"].get(pid)
        if not p:
            continue
        latest = sorted(p["risk_data"], key=lambda r: r["stat_date"], reverse=True)[0]
        od1 = float(latest.get("overdue_1_plus_ratio", 0))
        m0 = float(latest.get("m0_ratio", 0))
        partners.append({
            **p,
            "monthly_volume_fmt": fmt_usd(p["monthly_volume"]),
            "latest": {
                "stat_date": latest["stat_date"],
                "current_balance_fmt": fmt_usd(float(latest["current_balance"])),
                "cum_disb_fmt": fmt_usd(float(latest["cumulative_disbursement"])),
                "m0_ratio_fmt": f"{m0*100:.2f}%",
                "m0_class": "good" if m0 >= 0.96 else "warn" if m0 >= 0.93 else "danger",
                "od1_fmt": f"{od1*100:.2f}%",
                "od1_class": "good" if od1 < 0.03 else "warn" if od1 < 0.05 else "danger",
                "active_borrowers": f"{int(latest['active_borrowers']):,}",
            }
        })
    partners_revenue = []
    for pid in partner_ids:
        p = data["partners"].get(pid)
        if not p:
            continue
        partners_revenue.append({
            "id": p["id"],
            "name": p["name"],
            "country": p["country"],
            "product_type": p["product_type"],
            "revenue_data": p.get("revenue_data", []),
        })
    return render_template(
        "partner_list.html", user=user, partners=partners,
        partners_revenue=partners_revenue,
    )


@app.route("/partner/<partner_id>/risk")
@login_required
def partner_risk(partner_id):
    user = session["user"]
    data = load_partners()
    username = user["username"]
    allowed = data["assignments"].get(username, [])
    if partner_id not in allowed and user["role"] not in ("admin", "risk"):
        return redirect(url_for("dashboard"))
    partner = data["partners"].get(partner_id)
    if not partner:
        return redirect(url_for("partner_manage"))
    return render_template(
        "partner_risk.html",
        user=user,
        partner=partner,
        risk_data=partner["risk_data"],
        alerts=partner.get("alerts", []),
        priority_indicators=partner.get("priority_indicators", {}),
        local_currency=partner.get("local_currency", "USD"),
        exchange_rate=partner.get("exchange_rate", 1),
    )


@app.route("/partner/<partner_id>/vintage/<disbursement_month>")
@login_required
def vintage_portfolio(partner_id, disbursement_month):
    user = session["user"]
    data = load_partners()
    username = user["username"]
    allowed = data["assignments"].get(username, [])
    if partner_id not in allowed and user["role"] not in ("admin", "risk"):
        return redirect(url_for("dashboard"))
    partner = data["partners"].get(partner_id)
    if not partner:
        return redirect(url_for("partner_manage"))
    portfolio_data = load_vintage_portfolio()
    partner_loans = portfolio_data.get(partner_id, {}).get(disbursement_month, [])
    stats = _portfolio_stats(partner_loans)
    return render_template(
        "portfolio_asset.html",
        user=user,
        partner=partner,
        page_title=f"Vintage {disbursement_month}",
        loans=partner_loans,
        **stats,
    )


@app.route("/partner/<partner_id>/dpd/<bucket>")
@login_required
def dpd_portfolio(partner_id, bucket):
    user = session["user"]
    data = load_partners()
    username = user["username"]
    allowed = data["assignments"].get(username, [])
    if partner_id not in allowed and user["role"] not in ("admin", "risk"):
        return redirect(url_for("dashboard"))
    partner = data["partners"].get(partner_id)
    if not partner:
        return redirect(url_for("partner_manage"))
    portfolio_data = load_dpd_portfolio()
    partner_loans = portfolio_data.get(partner_id, {}).get(bucket, [])
    stats = _portfolio_stats(partner_loans)
    return render_template(
        "portfolio_asset.html",
        user=user,
        partner=partner,
        page_title=f"DPD {bucket}",
        loans=partner_loans,
        **stats,
    )


@app.route("/partner/<partner_id>/maturity/<maturity_month>")
@login_required
def maturity_portfolio(partner_id, maturity_month):
    user = session["user"]
    data = load_partners()
    username = user["username"]
    allowed = data["assignments"].get(username, [])
    if partner_id not in allowed and user["role"] not in ("admin", "risk"):
        return redirect(url_for("dashboard"))
    partner = data["partners"].get(partner_id)
    if not partner:
        return redirect(url_for("partner_manage"))
    portfolio_data = load_maturity_portfolio()
    partner_loans = portfolio_data.get(partner_id, {}).get(maturity_month, [])
    stats = _portfolio_stats(partner_loans)
    return render_template(
        "portfolio_asset.html",
        user=user,
        partner=partner,
        page_title=f"到期月 {maturity_month}",
        loans=partner_loans,
        **stats,
    )


@app.route("/partner/<partner_id>/loan/<loan_id>")
@login_required
def loan_detail(partner_id, loan_id):
    user = session["user"]
    data = load_partners()
    username = user["username"]
    allowed = data["assignments"].get(username, [])
    if partner_id not in allowed and user["role"] not in ("admin", "risk"):
        return redirect(url_for("dashboard"))
    partner = data["partners"].get(partner_id)
    if not partner:
        return redirect(url_for("partner_manage"))
    loan = _find_loan(partner_id, loan_id)
    if not loan:
        return redirect(url_for("partner_risk", partner_id=partner_id))
    details = load_loan_details().get(loan_id, {})
    schedule = details.get("schedule", [])
    repayments = details.get("repayments", [])
    customer_info = details.get("customer_info", {})
    return render_template(
        "loan_detail.html",
        user=user,
        partner=partner,
        loan=loan,
        schedule=schedule,
        repayments=repayments,
        customer_info=customer_info,
    )


@app.route("/partner/<partner_id>/revenue")
@login_required
def partner_revenue(partner_id):
    user = session["user"]
    data = load_partners()
    username = user["username"]
    allowed = data["assignments"].get(username, [])
    if partner_id not in allowed and user["role"] not in ("admin", "risk"):
        return redirect(url_for("dashboard"))
    partner = data["partners"].get(partner_id)
    if not partner:
        return redirect(url_for("partner_manage"))
    return render_template(
        "partner_revenue.html",
        user=user,
        partner=partner,
        revenue_data=partner.get("revenue_data", []),
    )


@app.route("/transaction/apply")
@login_required
def transaction_apply():
    user = session["user"]
    if "apply_transactions" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    txn_data = load_transactions()
    apps = [a for a in txn_data["applications"] if a["applicant"] == user["username"]]
    partner_data = load_partners()
    partner_ids = partner_data["assignments"].get(user["username"], [])
    partners_list = [
        {"id": pid, "name": partner_data["partners"][pid]["name"]}
        for pid in partner_ids if pid in partner_data["partners"]
    ]
    return render_template(
        "transaction_apply.html",
        user=user,
        applications=apps,
        partners=partners_list,
    )


@app.route("/api/db/status")
@login_required
def api_db_status():
    """数据库连接状态检查（供数据查询等页面使用）"""
    try:
        from db_connect import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "message": "数据库连接正常"})
    except Exception as e:
        err = str(e)
        hint = ""
        if "translate host name" in err.lower() or "nodename" in err.lower():
            hint = "域名解析失败，请检查：1) 网络是否正常；2) 是否需连接 VPN；3) 在系统终端（非 IDE）运行 python3 app.py"
        elif "timeout" in err.lower() or "timed out" in err.lower():
            hint = "连接超时，请检查：1) RDS 白名单是否包含当前 IP；2) 是否需通过 VPN/内网访问；3) 公网地址是否已开启"
        elif "connection refused" in err.lower():
            hint = "连接被拒绝，请检查端口、防火墙及 RDS 是否运行"
        elif "password" in err.lower() or "authentication" in err.lower():
            hint = "认证失败，请检查 .env 中的 DB_USER 和 DB_PASSWORD"
        return jsonify({"ok": False, "message": err, "hint": hint})


@app.route("/risk/query")
@login_required
def risk_query_page():
    user = session["user"]
    if "data_query" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    return render_template("risk_query.html", user=user)


@app.route("/api/risk/query/loan/<loan_id>")
@login_required
def api_risk_query_loan(loan_id):
    if "data_query" not in session["user"].get("permissions", []):
        return jsonify({"error": "权限不足"}), 403
    from risk_query import query_loan_detail
    result = query_loan_detail(loan_id)
    return jsonify(result)


@app.route("/api/risk/query/disbursements", methods=["POST"])
@login_required
def api_risk_query_disbursements():
    if "data_query" not in session["user"].get("permissions", []):
        return jsonify({"error": "权限不足"}), 403
    data = request.get_json() or {}
    query_date = data.get("date", "").strip()
    from risk_query import query_daily_disbursements
    result = query_daily_disbursements(query_date)
    return jsonify(result)


@app.route("/transaction/review")
@login_required
def transaction_review():
    user = session["user"]
    if "approve_transactions" not in user.get("permissions", []):
        return redirect(url_for("dashboard"))
    txn_data = load_transactions()
    return render_template(
        "transaction_review.html",
        user=user,
        applications=txn_data["applications"],
    )


@app.route("/api/dd/upload", methods=["POST"])
@login_required
def dd_upload():
    if "file" not in request.files:
        return jsonify({"success": False, "message": "未选择文件"}), 400
    f = request.files["file"]
    item_id = request.form.get("item_id", "unknown")
    if f.filename:
        filename = secure_filename(f"{item_id}_{f.filename}")
        user_dir = os.path.join(UPLOAD_DIR, session["user"]["username"])
        os.makedirs(user_dir, exist_ok=True)
        f.save(os.path.join(user_dir, filename))
        return jsonify({"success": True, "filename": filename})
    return jsonify({"success": False, "message": "文件无效"}), 400


@app.route("/api/dd/template/<module_id>")
@login_required
def dd_template_download(module_id):
    template_file = f"{module_id}_checklist.xlsx"
    template_path = os.path.join(DD_TEMPLATES_DIR, template_file)
    if os.path.exists(template_path):
        return send_from_directory(DD_TEMPLATES_DIR, template_file, as_attachment=True)
    dd = load_dd_checklist()
    mod = next((m for m in dd["modules"] if m["id"] == module_id), None)
    if not mod:
        return jsonify({"error": "模块不存在"}), 404
    content = f"{mod['name']} ({mod['name_en']}) - 尽调清单\n{'='*50}\n\n"
    for i, item in enumerate(mod["items"], 1):
        req = "【必填】" if item["required"] else "【选填】"
        content += f"{i}. {req} {item['name']}\n   文件名：\n   备注：\n\n"
    from flask import Response
    return Response(
        content,
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={module_id}_checklist.txt"}
    )


@app.route("/api/user")
@login_required
def api_user():
    return jsonify(session["user"])


@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login_page"))


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=True)
