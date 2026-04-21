import os
import io
import csv
import random
from datetime import datetime, date, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, g, jsonify, Response
)
import bcrypt
import zcatalyst_sdk
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "expense-tracker-dev-key-do-not-use-in-prod")

UPLOAD_FOLDER = os.path.join(app.static_folder, "uploads")
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024

DEFAULT_CATEGORIES = [
    "Entertainment", "Food & Dining", "Health", "Home Rental",
    "Insurance", "Investments", "Travel", "Welfare", "Utilities",
    "Education", "Shopping", "Transportation", "Other"
]

INVESTMENT_TYPES = [
    "Stocks", "Mutual Funds", "Fixed Deposit", "Real Estate",
    "Gold", "Crypto", "Bonds", "PPF/EPF", "Other"
]

INCOME_SOURCES = [
    "Salary", "Freelance", "Business", "Rental",
    "Dividends", "Interest", "Bonus", "Gift", "Other"
]

SECURITY_QUESTIONS = [
    "What is your pet's name?",
    "What city were you born in?",
    "What is your favorite movie?",
    "What was the name of your first school?",
    "What is your mother's maiden name?",
    "What is your favorite food?",
]

AVATAR_COLORS = [
    "#6366f1", "#f43f5e", "#10b981", "#f59e0b", "#3b82f6",
    "#8b5cf6", "#ec4899", "#14b8a6", "#f97316", "#06b6d4",
]

RECURRENCE_OPTIONS = ["none", "weekly", "monthly", "yearly"]

_categories_seeded = False

# ── Catalyst DataStore helpers ────────────────────────────────────

def get_catalyst():
    if "catalyst" not in g:
        g.catalyst = zcatalyst_sdk.initialize(req=request)
    return g.catalyst


def ds_table(name):
    return get_catalyst().datastore().table(name)


def zcql_query(query):
    return get_catalyst().zcql().execute_query(query)


def qstr(val):
    return str(val).replace("'", "''")


def normalize(row):
    if row is None:
        return None
    d = dict(row)
    # ZCQL wraps each row as {table_name: {col: val, ...}} — unwrap it
    if len(d) == 1:
        inner = list(d.values())[0]
        if isinstance(inner, dict):
            d = inner
    # Normalize all keys to lowercase for consistent access
    d = {k.lower(): v for k, v in d.items()}
    rowid = d.get("rowid")
    if rowid is not None:
        d["ROWID"] = str(rowid)
        d["id"] = str(rowid)
    return d


def normalize_all(rows):
    return [normalize(r) for r in (rows or [])]


def fetchone(query):
    rows = zcql_query(query)
    return normalize(rows[0]) if rows else None


def fetchall(query):
    return normalize_all(zcql_query(query))


# ── Category seeding ──────────────────────────────────────────────

def ensure_categories():
    global _categories_seeded
    if _categories_seeded:
        return
    existing = fetchall("SELECT name FROM categories WHERE is_default = '1'")
    existing_names = {r["name"] for r in existing}
    tbl = ds_table("categories")
    for cat in DEFAULT_CATEGORIES:
        if cat not in existing_names:
            tbl.insert_row({"name": cat, "is_default": "1"})
    _categories_seeded = True


@app.before_request
def before_request_hook():
    if request.endpoint and not request.endpoint.startswith("static"):
        ensure_categories()


# ── Date helpers ──────────────────────────────────────────────────

def get_date_range(period):
    today = date.today()
    if period == "week":
        start = today - timedelta(days=today.weekday())
        return start.isoformat(), (start + timedelta(days=6)).isoformat()
    elif period == "month":
        start = today.replace(day=1)
        nxt = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
        return start.isoformat(), (nxt - timedelta(days=1)).isoformat()
    elif period == "year":
        return today.replace(month=1, day=1).isoformat(), today.replace(month=12, day=31).isoformat()
    return None, None


def date_clause(col, period):
    s, e = get_date_range(period)
    if s and e:
        return f" AND {col} BETWEEN '{s}' AND '{e}'"
    return ""


def generate_insights(expense_data, invest_data, tot_exp, tot_inv, tot_inc=0):
    insights = []
    if tot_inc > 0 and tot_exp > 0:
        sr = (tot_inc - tot_exp) / tot_inc * 100
        if sr >= 30:
            insights.append({"type": "success", "icon": "trending-up", "msg": f"Excellent! Saving {sr:.0f}% of income."})
        elif sr >= 10:
            insights.append({"type": "info", "icon": "piggy-bank", "msg": f"Saving {sr:.0f}% — aim for 20%+."})
        elif sr > 0:
            insights.append({"type": "warning", "icon": "alert-triangle", "msg": f"Only {sr:.0f}% savings. Cut discretionary spending."})
        else:
            insights.append({"type": "warning", "icon": "alert-circle", "msg": "Spending exceeds income this period!"})
    if tot_inv > 0 and tot_exp > 0 and tot_inv / tot_exp >= 0.5:
        insights.append({"type": "success", "icon": "rocket", "msg": "Great investment allocation!"})
    emap = {r["name"]: r["total"] for r in expense_data} if expense_data else {}
    disc = emap.get("Entertainment", 0) + emap.get("Food & Dining", 0) + emap.get("Shopping", 0)
    if tot_exp > 0 and disc / tot_exp > 0.5:
        insights.append({"type": "warning", "icon": "shopping-cart", "msg": f"Discretionary spending is {disc/tot_exp*100:.0f}%. Set budget limits."})
    if not insights and tot_exp > 0:
        insights.append({"type": "success", "icon": "check-circle", "msg": "Spending looks balanced!"})
    if not insights:
        insights.append({"type": "info", "icon": "info", "msg": "Add transactions to get insights."})
    return insights


# ── Auth helpers ──────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def wrapper(*a, **kw):
        if "user_id" not in session:
            flash("Please sign in first.", "warning")
            return redirect(url_for("signin"))
        user = current_user()
        if not user:
            session.clear()
            flash("Session expired. Please sign in again.", "warning")
            return redirect(url_for("signin"))
        return f(*a, **kw)
    return wrapper


def current_user():
    if "user_id" in session:
        return fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(session['user_id'])}'")
    return None


def get_circle_member_ids(uid):
    sent = fetchall(f"SELECT friend_id FROM circles WHERE user_id = '{qstr(uid)}' AND status = 'accepted'")
    received = fetchall(f"SELECT user_id FROM circles WHERE friend_id = '{qstr(uid)}' AND status = 'accepted'")
    ids = [str(uid)]
    ids += [str(r["friend_id"]) for r in sent]
    ids += [str(r["user_id"]) for r in received]
    return list(set(ids))


def get_circle_members(uid):
    member_ids = [m for m in get_circle_member_ids(uid) if str(m) != str(uid)]
    if not member_ids:
        return []
    in_clause = ",".join([f"'{qstr(m)}'" for m in member_ids])
    return fetchall(f"SELECT * FROM users WHERE ROWID IN ({in_clause})")


def can_view_user(viewer, target):
    return str(viewer) == str(target) or str(target) in get_circle_member_ids(viewer)


def resolve_view(uid):
    v = request.args.get("view_user", str(uid))
    if v == "overall":
        return "overall", True
    return v, False


def uid_name_map(uid, circle):
    m = {str(uid): session.get("display_name", "Me")}
    for c in circle:
        m[str(c["ROWID"])] = c["display_name"]
    return m


def fnum(val):
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


# ── Auth routes ───────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("dashboard") if "user_id" in session else url_for("signin"))


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        dn = request.form.get("display_name", "").strip()
        pw = request.form.get("password", "")
        c = request.form.get("confirm_password", "")
        sq = request.form.get("security_question", "")
        sa = request.form.get("security_answer", "").strip()
        errs = []
        if not u or not dn or not pw:
            errs.append("All fields required.")
        if len(u) < 3:
            errs.append("Username ≥ 3 chars.")
        if pw != c:
            errs.append("Passwords don't match.")
        if len(pw) < 6:
            errs.append("Password ≥ 6 chars.")
        if not sq or not sa:
            errs.append("Security question required.")
        if errs:
            for e in errs:
                flash(e, "danger")
            return render_template("signup.html", security_questions=SECURITY_QUESTIONS)
        if fetchone(f"SELECT ROWID FROM users WHERE username = '{qstr(u)}'"):
            flash("Username taken.", "danger")
            return render_template("signup.html", security_questions=SECURITY_QUESTIONS)
        ph = bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()
        ah = bcrypt.hashpw(sa.lower().encode(), bcrypt.gensalt()).decode()
        ds_table("users").insert_row({
            "username": u,
            "display_name": dn,
            "password_hash": ph,
            "avatar_color": random.choice(AVATAR_COLORS),
            "profile_pic": "",
            "security_question": sq,
            "security_answer_hash": ah,
            "created_at": datetime.now().isoformat(),
        })
        flash("Account created! Sign in.", "success")
        return redirect(url_for("signin"))
    return render_template("signup.html", security_questions=SECURITY_QUESTIONS)


@app.route("/signin", methods=["GET", "POST"])
def signin():
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        pw = request.form.get("password", "")
        user = fetchone(f"SELECT * FROM users WHERE username = '{qstr(u)}'")
        if user and bcrypt.checkpw(pw.encode(), user["password_hash"].encode()):
            session["user_id"] = user["ROWID"]
            session["username"] = user["username"]
            session["display_name"] = user["display_name"]
            flash(f"Welcome, {user['display_name']}!", "success")
            return redirect(url_for("dashboard"))
        flash("Invalid username or password.", "danger")
    return render_template("signin.html")


@app.route("/signout")
def signout():
    session.clear()
    flash("Signed out.", "info")
    return redirect(url_for("signin"))


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    step = request.form.get("step", "1")
    if request.method == "POST" and step == "1":
        u = request.form.get("username", "").strip()
        user = fetchone(f"SELECT * FROM users WHERE username = '{qstr(u)}'")
        if not user or not user.get("security_question"):
            flash("Username not found.", "danger")
            return render_template("forgot_password.html", step=1)
        return render_template("forgot_password.html", step=2, username=u, security_question=user["security_question"])
    elif request.method == "POST" and step == "2":
        u = request.form.get("username", "").strip()
        a = request.form.get("security_answer", "").strip()
        user = fetchone(f"SELECT * FROM users WHERE username = '{qstr(u)}'")
        if not user or not bcrypt.checkpw(a.lower().encode(), user["security_answer_hash"].encode()):
            flash("Incorrect answer.", "danger")
            return render_template("forgot_password.html", step=2, username=u,
                                   security_question=user["security_question"] if user else "")
        return render_template("forgot_password.html", step=3, username=u)
    elif request.method == "POST" and step == "3":
        u = request.form.get("username", "").strip()
        np = request.form.get("new_password", "")
        c = request.form.get("confirm_password", "")
        if len(np) < 6:
            flash("Password ≥ 6 chars.", "danger")
            return render_template("forgot_password.html", step=3, username=u)
        if np != c:
            flash("Passwords don't match.", "danger")
            return render_template("forgot_password.html", step=3, username=u)
        user = fetchone(f"SELECT ROWID FROM users WHERE username = '{qstr(u)}'")
        if user:
            ds_table("users").update_row({
                "ROWID": user["ROWID"],
                "password_hash": bcrypt.hashpw(np.encode(), bcrypt.gensalt()).decode(),
            })
        flash("Password reset! Sign in.", "success")
        return redirect(url_for("signin"))
    return render_template("forgot_password.html", step=1)


@app.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    if request.method == "POST":
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
        current = request.form.get("current_password", "")
        new_pw = request.form.get("new_password", "")
        confirm = request.form.get("confirm_password", "")
        user = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(session['user_id'])}'")
        if not user or not bcrypt.checkpw(current.encode(), user["password_hash"].encode()):
            msg = "Current password is incorrect."
            if is_ajax:
                return jsonify(success=False, message=msg), 200
            flash(msg, "danger")
            return render_template("change_password.html", user=current_user())
        if len(new_pw) < 6:
            msg = "New password must be at least 6 characters."
            if is_ajax:
                return jsonify(success=False, message=msg), 200
            flash(msg, "danger")
            return render_template("change_password.html", user=current_user())
        if new_pw != confirm:
            msg = "Passwords do not match."
            if is_ajax:
                return jsonify(success=False, message=msg), 200
            flash(msg, "danger")
            return render_template("change_password.html", user=current_user())
        ds_table("users").update_row({
            "ROWID": session["user_id"],
            "password_hash": bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode(),
        })
        if is_ajax:
            return jsonify(success=True, message="Password changed successfully!"), 200
        flash("Password changed successfully!", "success")
        return redirect(url_for("dashboard"))
    return render_template("change_password.html", user=current_user())


# ── Dashboard ─────────────────────────────────────────────────────

@app.route("/dashboard")
@login_required
def dashboard():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    circle = get_circle_members(uid)
    dc_e = date_clause("expense_date", period)
    dc_i = date_clause("invest_date", period)
    dc_inc = date_clause("income_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        in_ids = ",".join([f"'{qstr(i)}'" for i in ids])
        exp = fetchall(f"SELECT category_name as name, SUM(amount) as total FROM expenses WHERE user_id IN ({in_ids}){dc_e} GROUP BY category_name ORDER BY total DESC")
        inv = fetchall(f"SELECT type, SUM(amount) as total FROM investments WHERE user_id IN ({in_ids}){dc_i} GROUP BY type ORDER BY total DESC")
        inc = fetchall(f"SELECT source, SUM(amount) as total FROM income WHERE user_id IN ({in_ids}){dc_inc} GROUP BY source ORDER BY total DESC")
        recent = fetchall(f"SELECT * FROM expenses WHERE user_id IN ({in_ids}){dc_e} ORDER BY expense_date DESC LIMIT 8")
        nm = uid_name_map(uid, circle)
        for r in recent:
            r["owner_name"] = nm.get(str(r.get("user_id")), "Unknown")
        vu = None
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger")
            return redirect(url_for("dashboard"))
        exp = fetchall(f"SELECT category_name as name, SUM(amount) as total FROM expenses WHERE user_id = '{qstr(vid)}'{dc_e} GROUP BY category_name ORDER BY total DESC")
        inv = fetchall(f"SELECT type, SUM(amount) as total FROM investments WHERE user_id = '{qstr(vid)}'{dc_i} GROUP BY type ORDER BY total DESC")
        inc = fetchall(f"SELECT source, SUM(amount) as total FROM income WHERE user_id = '{qstr(vid)}'{dc_inc} GROUP BY source ORDER BY total DESC")
        recent = fetchall(f"SELECT * FROM expenses WHERE user_id = '{qstr(vid)}'{dc_e} ORDER BY expense_date DESC LIMIT 8")
        vu = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(vid)}'")

    te = sum(fnum(r.get("total")) for r in exp)
    ti = sum(fnum(r.get("total")) for r in inv)
    tinc = sum(fnum(r.get("total")) for r in inc)

    ms, me = get_date_range("month")
    buid = uid if is_all else vid
    raw_budgets = fetchall(f"SELECT * FROM budgets WHERE user_id = '{qstr(buid)}'")
    budgets = []
    for b in raw_budgets:
        cat_name = b.get("category_name", "")
        spent_rows = fetchall(f"SELECT SUM(amount) as spent FROM expenses WHERE user_id = '{qstr(buid)}' AND category_name = '{qstr(cat_name)}' AND expense_date BETWEEN '{ms}' AND '{me}'")
        spent = fnum(spent_rows[0].get("spent") if spent_rows else 0)
        budgets.append({**b, "name": cat_name, "monthly_limit": fnum(b.get("monthly_limit")), "spent": spent})

    insights = generate_insights(exp, inv, te, ti, tinc)

    return render_template("dashboard.html",
        expense_data=exp, invest_data=inv, income_data=inc,
        total_expenses=te, total_investments=ti, total_income=tinc,
        recent_expenses=recent, budgets=budgets, insights=insights,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_overall=is_all, period=period, user=current_user())


# ── Expenses ──────────────────────────────────────────────────────

@app.route("/expenses")
@login_required
def expenses():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    cats = fetchall("SELECT * FROM categories ORDER BY name")
    circle = get_circle_members(uid)
    dc = date_clause("expense_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        in_ids = ",".join([f"'{qstr(i)}'" for i in ids])
        rows = fetchall(f"SELECT * FROM expenses WHERE user_id IN ({in_ids}){dc} ORDER BY expense_date DESC")
        nm = uid_name_map(uid, circle)
        for r in rows:
            r["owner_name"] = nm.get(str(r.get("user_id")), "Unknown")
        vu, own = None, False
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger")
            return redirect(url_for("expenses"))
        rows = fetchall(f"SELECT * FROM expenses WHERE user_id = '{qstr(vid)}'{dc} ORDER BY expense_date DESC")
        vu = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(vid)}'")
        own = str(vid) == str(uid)

    return render_template("expenses.html", expenses=rows, categories=cats,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_own=own, is_overall=is_all, period=period,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())


@app.route("/expenses/add", methods=["POST"])
@login_required
def add_expense():
    uid = session["user_id"]
    cat_rowid = request.form.get("category_id", "").strip()
    amt = request.form.get("amount", type=float)
    desc = request.form.get("description", "").strip()
    dt = request.form.get("expense_date", "")
    rec = request.form.get("recurrence", "none")
    if not cat_rowid or not amt or not dt:
        flash("Category, amount, and date required.", "danger")
        return redirect(url_for("expenses"))
    cat = fetchone(f"SELECT name FROM categories WHERE ROWID = '{qstr(cat_rowid)}'")
    cat_name = cat["name"] if cat else "Other"
    ds_table("expenses").insert_row({
        "user_id": uid,
        "category_id": cat_rowid,
        "category_name": cat_name,
        "amount": amt,
        "description": desc,
        "expense_date": dt,
        "recurrence": rec,
        "created_at": datetime.now().isoformat(),
    })
    flash("Expense added.", "success")
    return redirect(url_for("expenses"))


@app.route("/expenses/delete/<expense_id>", methods=["POST"])
@login_required
def delete_expense(expense_id):
    exp = fetchone(f"SELECT user_id FROM expenses WHERE ROWID = '{qstr(expense_id)}'")
    if exp and str(exp.get("user_id")) == str(session["user_id"]):
        ds_table("expenses").delete_row(expense_id)
    flash("Expense deleted.", "info")
    return redirect(url_for("expenses"))


@app.route("/expenses/edit/<expense_id>", methods=["GET", "POST"])
@login_required
def edit_expense(expense_id):
    uid = session["user_id"]
    exp = fetchone(f"SELECT * FROM expenses WHERE ROWID = '{qstr(expense_id)}'")
    if not exp or str(exp.get("user_id")) != str(uid):
        flash("Expense not found.", "danger")
        return redirect(url_for("expenses"))
    if request.method == "POST":
        cat_rowid = request.form.get("category_id", "").strip()
        amt = request.form.get("amount", type=float)
        desc = request.form.get("description", "").strip()
        dt = request.form.get("expense_date", "")
        rec = request.form.get("recurrence", "none")
        if not cat_rowid or not amt or not dt:
            flash("Category, amount, and date required.", "danger")
            return redirect(url_for("edit_expense", expense_id=expense_id))
        cat = fetchone(f"SELECT name FROM categories WHERE ROWID = '{qstr(cat_rowid)}'")
        cat_name = cat["name"] if cat else "Other"
        ds_table("expenses").update_row({
            "ROWID": expense_id,
            "category_id": cat_rowid,
            "category_name": cat_name,
            "amount": amt,
            "description": desc,
            "expense_date": dt,
            "recurrence": rec,
        })
        flash("Expense updated.", "success")
        return redirect(url_for("expenses"))
    cats = fetchall("SELECT * FROM categories ORDER BY name")
    return render_template("edit_expense.html", expense=exp, categories=cats,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())


# ── Income ────────────────────────────────────────────────────────

@app.route("/income")
@login_required
def income():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    circle = get_circle_members(uid)
    dc = date_clause("income_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        in_ids = ",".join([f"'{qstr(i)}'" for i in ids])
        rows = fetchall(f"SELECT * FROM income WHERE user_id IN ({in_ids}){dc} ORDER BY income_date DESC")
        nm = uid_name_map(uid, circle)
        for r in rows:
            r["owner_name"] = nm.get(str(r.get("user_id")), "Unknown")
        vu, own = None, False
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger")
            return redirect(url_for("income"))
        rows = fetchall(f"SELECT * FROM income WHERE user_id = '{qstr(vid)}'{dc} ORDER BY income_date DESC")
        vu = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(vid)}'")
        own = str(vid) == str(uid)

    return render_template("income.html", incomes=rows, income_sources=INCOME_SOURCES,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_own=own, is_overall=is_all, period=period,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())


@app.route("/income/add", methods=["POST"])
@login_required
def add_income():
    uid = session["user_id"]
    src = request.form.get("source", "").strip()
    amt = request.form.get("amount", type=float)
    desc = request.form.get("description", "").strip()
    dt = request.form.get("income_date", "")
    rec = request.form.get("recurrence", "none")
    if not src or not amt or not dt:
        flash("Source, amount, and date required.", "danger")
        return redirect(url_for("income"))
    ds_table("income").insert_row({
        "user_id": uid,
        "source": src,
        "amount": amt,
        "description": desc,
        "income_date": dt,
        "recurrence": rec,
        "created_at": datetime.now().isoformat(),
    })
    flash("Income added.", "success")
    return redirect(url_for("income"))


@app.route("/income/delete/<income_id>", methods=["POST"])
@login_required
def delete_income(income_id):
    inc = fetchone(f"SELECT user_id FROM income WHERE ROWID = '{qstr(income_id)}'")
    if inc and str(inc.get("user_id")) == str(session["user_id"]):
        ds_table("income").delete_row(income_id)
    flash("Income deleted.", "info")
    return redirect(url_for("income"))


@app.route("/income/edit/<income_id>", methods=["GET", "POST"])
@login_required
def edit_income(income_id):
    uid = session["user_id"]
    inc = fetchone(f"SELECT * FROM income WHERE ROWID = '{qstr(income_id)}'")
    if not inc or str(inc.get("user_id")) != str(uid):
        flash("Income not found.", "danger")
        return redirect(url_for("income"))
    if request.method == "POST":
        src = request.form.get("source", "").strip()
        amt = request.form.get("amount", type=float)
        desc = request.form.get("description", "").strip()
        dt = request.form.get("income_date", "")
        rec = request.form.get("recurrence", "none")
        if not src or not amt or not dt:
            flash("Source, amount, and date required.", "danger")
            return redirect(url_for("edit_income", income_id=income_id))
        ds_table("income").update_row({
            "ROWID": income_id,
            "source": src,
            "amount": amt,
            "description": desc,
            "income_date": dt,
            "recurrence": rec,
        })
        flash("Income updated.", "success")
        return redirect(url_for("income"))
    return render_template("edit_income.html", income_item=inc, income_sources=INCOME_SOURCES,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())


# ── Investments ───────────────────────────────────────────────────

@app.route("/investments")
@login_required
def investments():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    circle = get_circle_members(uid)
    dc = date_clause("invest_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        in_ids = ",".join([f"'{qstr(i)}'" for i in ids])
        rows = fetchall(f"SELECT * FROM investments WHERE user_id IN ({in_ids}){dc} ORDER BY invest_date DESC")
        nm = uid_name_map(uid, circle)
        for r in rows:
            r["owner_name"] = nm.get(str(r.get("user_id")), "Unknown")
        vu, own = None, False
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger")
            return redirect(url_for("investments"))
        rows = fetchall(f"SELECT * FROM investments WHERE user_id = '{qstr(vid)}'{dc} ORDER BY invest_date DESC")
        vu = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(vid)}'")
        own = str(vid) == str(uid)

    return render_template("investments.html", investments=rows, investment_types=INVESTMENT_TYPES,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_own=own, is_overall=is_all, period=period, user=current_user())


@app.route("/investments/add", methods=["POST"])
@login_required
def add_investment():
    uid = session["user_id"]
    t = request.form.get("type", "").strip()
    amt = request.form.get("amount", type=float)
    desc = request.form.get("description", "").strip()
    dt = request.form.get("invest_date", "")
    if not t or not amt or not dt:
        flash("Type, amount, and date required.", "danger")
        return redirect(url_for("investments"))
    ds_table("investments").insert_row({
        "user_id": uid,
        "type": t,
        "amount": amt,
        "description": desc,
        "invest_date": dt,
        "created_at": datetime.now().isoformat(),
    })
    flash("Investment added.", "success")
    return redirect(url_for("investments"))


@app.route("/investments/delete/<inv_id>", methods=["POST"])
@login_required
def delete_investment(inv_id):
    inv = fetchone(f"SELECT user_id FROM investments WHERE ROWID = '{qstr(inv_id)}'")
    if inv and str(inv.get("user_id")) == str(session["user_id"]):
        ds_table("investments").delete_row(inv_id)
    flash("Investment deleted.", "info")
    return redirect(url_for("investments"))


@app.route("/investments/edit/<inv_id>", methods=["GET", "POST"])
@login_required
def edit_investment(inv_id):
    uid = session["user_id"]
    inv = fetchone(f"SELECT * FROM investments WHERE ROWID = '{qstr(inv_id)}'")
    if not inv or str(inv.get("user_id")) != str(uid):
        flash("Investment not found.", "danger")
        return redirect(url_for("investments"))
    if request.method == "POST":
        t = request.form.get("type", "").strip()
        amt = request.form.get("amount", type=float)
        desc = request.form.get("description", "").strip()
        dt = request.form.get("invest_date", "")
        if not t or not amt or not dt:
            flash("Type, amount, and date required.", "danger")
            return redirect(url_for("edit_investment", inv_id=inv_id))
        ds_table("investments").update_row({
            "ROWID": inv_id,
            "type": t,
            "amount": amt,
            "description": desc,
            "invest_date": dt,
        })
        flash("Investment updated.", "success")
        return redirect(url_for("investments"))
    return render_template("edit_investment.html", investment=inv, investment_types=INVESTMENT_TYPES, user=current_user())


# ── Budgets ───────────────────────────────────────────────────────

@app.route("/budgets", methods=["GET", "POST"])
@login_required
def budgets():
    uid = session["user_id"]
    if request.method == "POST":
        cat_rowid = request.form.get("category_id", "").strip()
        lim = request.form.get("monthly_limit", type=float)
        if cat_rowid and lim and lim > 0:
            cat = fetchone(f"SELECT name FROM categories WHERE ROWID = '{qstr(cat_rowid)}'")
            cat_name = cat["name"] if cat else "Other"
            existing = fetchone(f"SELECT ROWID FROM budgets WHERE user_id = '{qstr(uid)}' AND category_name = '{qstr(cat_name)}'")
            if existing:
                ds_table("budgets").update_row({"ROWID": existing["ROWID"], "monthly_limit": lim})
            else:
                ds_table("budgets").insert_row({"user_id": uid, "category_name": cat_name, "monthly_limit": lim})
            flash("Budget saved.", "success")
        return redirect(url_for("budgets"))

    cats = fetchall("SELECT * FROM categories ORDER BY name")
    ms, me = get_date_range("month")
    raw_budgets = fetchall(f"SELECT * FROM budgets WHERE user_id = '{qstr(uid)}'")
    budget_list = []
    for b in raw_budgets:
        cat_name = b.get("category_name", "")
        spent_rows = fetchall(f"SELECT SUM(amount) as spent FROM expenses WHERE user_id = '{qstr(uid)}' AND category_name = '{qstr(cat_name)}' AND expense_date BETWEEN '{ms}' AND '{me}'")
        spent = fnum(spent_rows[0].get("spent") if spent_rows else 0)
        budget_list.append({**b, "name": cat_name, "monthly_limit": fnum(b.get("monthly_limit")), "spent": spent})

    return render_template("budgets.html", budgets=budget_list, categories=cats, user=current_user())


@app.route("/budgets/delete/<budget_id>", methods=["POST"])
@login_required
def delete_budget(budget_id):
    b = fetchone(f"SELECT user_id FROM budgets WHERE ROWID = '{qstr(budget_id)}'")
    if b and str(b.get("user_id")) == str(session["user_id"]):
        ds_table("budgets").delete_row(budget_id)
    flash("Budget removed.", "info")
    return redirect(url_for("budgets"))


# ── Circle ────────────────────────────────────────────────────────

@app.route("/circle")
@login_required
def circle():
    uid = session["user_id"]
    members = get_circle_members(uid)
    sent_raw = fetchall(f"SELECT * FROM circles WHERE user_id = '{qstr(uid)}' AND status = 'pending'")
    received_raw = fetchall(f"SELECT * FROM circles WHERE friend_id = '{qstr(uid)}' AND status = 'pending'")
    sent = []
    for c in sent_raw:
        u = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(c['friend_id'])}'")
        if u:
            sent.append({**c, "username": u["username"], "display_name": u["display_name"]})
    received = []
    for c in received_raw:
        u = fetchone(f"SELECT * FROM users WHERE ROWID = '{qstr(c['user_id'])}'")
        if u:
            received.append({**c, "username": u["username"], "display_name": u["display_name"]})
    return render_template("circle.html", members=members, sent_requests=sent,
        received_requests=received, user=current_user())


@app.route("/circle/add", methods=["POST"])
@login_required
def add_to_circle():
    uid = session["user_id"]
    fu = request.form.get("username", "").strip()
    if not fu:
        flash("Enter a username.", "danger")
        return redirect(url_for("circle"))
    f = fetchone(f"SELECT * FROM users WHERE username = '{qstr(fu)}'")
    if not f:
        flash("User not found.", "danger")
        return redirect(url_for("circle"))
    friend_id = f["ROWID"]
    if str(friend_id) == str(uid):
        flash("Can't add yourself.", "warning")
        return redirect(url_for("circle"))
    ex1 = fetchone(f"SELECT status FROM circles WHERE user_id = '{qstr(uid)}' AND friend_id = '{qstr(friend_id)}'")
    ex2 = fetchone(f"SELECT status FROM circles WHERE user_id = '{qstr(friend_id)}' AND friend_id = '{qstr(uid)}'")
    ex = ex1 or ex2
    if ex:
        flash(f"Already {'connected' if ex.get('status') == 'accepted' else 'pending'}.", "info")
        return redirect(url_for("circle"))
    ds_table("circles").insert_row({
        "user_id": uid,
        "friend_id": friend_id,
        "status": "pending",
        "created_at": datetime.now().isoformat(),
    })
    flash(f"Request sent to {f['display_name']}.", "success")
    return redirect(url_for("circle"))


@app.route("/circle/accept/<circle_id>", methods=["POST"])
@login_required
def accept_circle(circle_id):
    c = fetchone(f"SELECT * FROM circles WHERE ROWID = '{qstr(circle_id)}'")
    if c and str(c.get("friend_id")) == str(session["user_id"]):
        ds_table("circles").update_row({"ROWID": circle_id, "status": "accepted"})
    flash("Accepted!", "success")
    return redirect(url_for("circle"))


@app.route("/circle/reject/<circle_id>", methods=["POST"])
@login_required
def reject_circle(circle_id):
    c = fetchone(f"SELECT * FROM circles WHERE ROWID = '{qstr(circle_id)}'")
    if c and str(c.get("friend_id")) == str(session["user_id"]):
        ds_table("circles").delete_row(circle_id)
    flash("Rejected.", "info")
    return redirect(url_for("circle"))


@app.route("/circle/remove/<member_id>", methods=["POST"])
@login_required
def remove_from_circle(member_id):
    uid = session["user_id"]
    c1 = fetchone(f"SELECT ROWID FROM circles WHERE user_id = '{qstr(uid)}' AND friend_id = '{qstr(member_id)}'")
    c2 = fetchone(f"SELECT ROWID FROM circles WHERE user_id = '{qstr(member_id)}' AND friend_id = '{qstr(uid)}'")
    for c in [c1, c2]:
        if c:
            ds_table("circles").delete_row(c["ROWID"])
    flash("Removed.", "info")
    return redirect(url_for("circle"))


# ── CSV Export ────────────────────────────────────────────────────

@app.route("/export/<dtype>")
@login_required
def export_csv(dtype):
    uid = session["user_id"]
    buf = io.StringIO()
    w = csv.writer(buf)
    if dtype == "expenses":
        w.writerow(["Date", "Category", "Amount", "Description", "Recurrence"])
        for r in fetchall(f"SELECT * FROM expenses WHERE user_id = '{qstr(uid)}' ORDER BY expense_date DESC"):
            w.writerow([r.get("expense_date"), r.get("category_name"), r.get("amount"), r.get("description") or "", r.get("recurrence")])
    elif dtype == "income":
        w.writerow(["Date", "Source", "Amount", "Description", "Recurrence"])
        for r in fetchall(f"SELECT * FROM income WHERE user_id = '{qstr(uid)}' ORDER BY income_date DESC"):
            w.writerow([r.get("income_date"), r.get("source"), r.get("amount"), r.get("description") or "", r.get("recurrence")])
    elif dtype == "investments":
        w.writerow(["Date", "Type", "Amount", "Description"])
        for r in fetchall(f"SELECT * FROM investments WHERE user_id = '{qstr(uid)}' ORDER BY invest_date DESC"):
            w.writerow([r.get("invest_date"), r.get("type"), r.get("amount"), r.get("description") or ""])
    else:
        flash("Invalid.", "danger")
        return redirect(url_for("dashboard"))
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment;filename={dtype}_{date.today().isoformat()}.csv"})


# ── Profile ───────────────────────────────────────────────────────

@app.route("/profile/avatar", methods=["POST"])
@login_required
def update_avatar():
    c = request.form.get("avatar_color", "#6366f1")
    if not c.startswith("#") or len(c) != 7:
        c = "#6366f1"
    ds_table("users").update_row({"ROWID": session["user_id"], "avatar_color": c})
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    if is_ajax:
        return jsonify(success=True, message="Avatar updated!"), 200
    flash("Avatar updated!", "success")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/profile/picture", methods=["POST"])
@login_required
def update_profile_pic():
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    f = request.files.get("profile_pic")
    if not f or f.filename == "":
        msg = "No file selected."
        if is_ajax:
            return jsonify(success=False, message=msg), 200
        flash(msg, "danger")
        return redirect(request.referrer or url_for("dashboard"))
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        msg = "Invalid file type. Use PNG, JPG, GIF, or WebP."
        if is_ajax:
            return jsonify(success=False, message=msg), 200
        flash(msg, "danger")
        return redirect(request.referrer or url_for("dashboard"))
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    filename = f"user_{session['user_id']}.{ext}"
    for old_ext in ALLOWED_EXTENSIONS:
        old_path = os.path.join(UPLOAD_FOLDER, f"user_{session['user_id']}.{old_ext}")
        if os.path.exists(old_path):
            os.remove(old_path)
    f.save(os.path.join(UPLOAD_FOLDER, filename))
    ds_table("users").update_row({"ROWID": session["user_id"], "profile_pic": filename})
    if is_ajax:
        pic_url = url_for("static", filename=f"uploads/{filename}")
        return jsonify(success=True, message="Profile picture updated!", pic_url=pic_url), 200
    flash("Profile picture updated!", "success")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/profile/picture/remove", methods=["POST"])
@login_required
def remove_profile_pic():
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    user = fetchone(f"SELECT profile_pic FROM users WHERE ROWID = '{qstr(session['user_id'])}'")
    if user and user.get("profile_pic"):
        pic_path = os.path.join(UPLOAD_FOLDER, user["profile_pic"])
        if os.path.exists(pic_path):
            os.remove(pic_path)
        ds_table("users").update_row({"ROWID": session["user_id"], "profile_pic": ""})
    if is_ajax:
        return jsonify(success=True, message="Profile picture removed."), 200
    flash("Profile picture removed.", "success")
    return redirect(request.referrer or url_for("dashboard"))


# ── API ───────────────────────────────────────────────────────────

@app.route("/api/search-users")
@login_required
def search_users():
    uid = session["user_id"]
    q = request.args.get("q", "").strip()
    if len(q) < 1:
        return jsonify([])
    c1 = fetchall(f"SELECT friend_id as cid FROM circles WHERE user_id = '{qstr(uid)}'")
    c2 = fetchall(f"SELECT user_id as cid FROM circles WHERE friend_id = '{qstr(uid)}'")
    exclude = {str(uid)} | {str(r["cid"]) for r in c1} | {str(r["cid"]) for r in c2}
    rows = fetchall(f"SELECT * FROM users WHERE username LIKE '%{qstr(q)}%' OR display_name LIKE '%{qstr(q)}%' LIMIT 20")
    result = []
    for r in rows:
        if str(r["ROWID"]) not in exclude:
            result.append({"username": r["username"], "display_name": r["display_name"],
                           "avatar_color": r.get("avatar_color") or "#6366f1"})
            if len(result) >= 10:
                break
    return jsonify(result)


@app.route("/debug/users")
def debug_users():
    users = fetchall("SELECT * FROM users")
    return jsonify({
        "total": len(users),
        "users": [{"id": u.get("ROWID"), "username": u.get("username"),
                   "display_name": u.get("display_name"), "created_at": u.get("created_at")} for u in users]
    })


@app.route("/debug/zcql")
def debug_zcql():
    import traceback
    results = {}
    queries = {
        "users_all": "SELECT * FROM users LIMIT 2",
        "expenses_sum": "SELECT SUM(amount) as total FROM expenses LIMIT 1",
        "expenses_group": "SELECT category_name, SUM(amount) FROM expenses GROUP BY category_name LIMIT 3",
        "expenses_alias": "SELECT category_name as name, SUM(amount) as total FROM expenses GROUP BY category_name LIMIT 3",
    }
    for key, q in queries.items():
        try:
            raw = zcql_query(q)
            results[key] = {"raw": raw, "normalized": normalize_all(raw)}
        except Exception as e:
            results[key] = {"error": str(e), "traceback": traceback.format_exc()}
    return jsonify(results)


@app.errorhandler(Exception)
def handle_exception(e):
    import traceback
    return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


# ── Startup ───────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("X_ZOHO_CATALYST_LISTEN_PORT", os.environ.get("PORT", 9000)))
    app.run(debug=False, host="0.0.0.0", port=port)
