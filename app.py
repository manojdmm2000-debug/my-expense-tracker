import os
import io
import csv
import secrets
import random
from datetime import datetime, date, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, g, jsonify, Response, send_from_directory
)
import bcrypt
from werkzeug.utils import secure_filename

# ── Database engine detection ─────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
IS_PG = DATABASE_URL.startswith("postgres")

if IS_PG:
    import psycopg2
    import psycopg2.extras
else:
    import sqlite3

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "expense-tracker-dev-key-do-not-use-in-prod")

UPLOAD_FOLDER = os.path.join(app.static_folder, "uploads")
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024  # 2MB max

DATABASE = os.environ.get("DATABASE_PATH", "expenses.db")

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

# ── Database ─────────────────────────────────────────────────────

class Database:
    """Thin wrapper providing a unified API for PostgreSQL and SQLite."""
    def __init__(self, conn, is_pg=False):
        self.conn = conn
        self.is_pg = is_pg

    def execute(self, query, params=None):
        if self.is_pg:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, params or ())
            return cur
        else:
            query = query.replace('%s', '?')
            return self.conn.execute(query, params or ())

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


def get_db():
    if "db" not in g:
        if IS_PG:
            conn = psycopg2.connect(DATABASE_URL)
            g.db = Database(conn, is_pg=True)
        else:
            conn = sqlite3.connect(DATABASE, timeout=20)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            g.db = Database(conn, is_pg=False)
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        if exc is None:
            try:
                db.commit()
            except Exception:
                pass
        db.close()


def init_db():
    if IS_PG:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            avatar_color TEXT DEFAULT '#6366f1',
            profile_pic TEXT DEFAULT NULL,
            security_question TEXT,
            security_answer_hash TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            is_default INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS budgets (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            category_id INTEGER NOT NULL REFERENCES categories(id),
            monthly_limit NUMERIC(12,2) NOT NULL,
            UNIQUE(user_id, category_id)
        );
        CREATE TABLE IF NOT EXISTS expenses (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            category_id INTEGER NOT NULL REFERENCES categories(id),
            amount NUMERIC(12,2) NOT NULL,
            description TEXT,
            expense_date DATE NOT NULL,
            recurrence TEXT DEFAULT 'none',
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS income (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            source TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            description TEXT,
            income_date DATE NOT NULL,
            recurrence TEXT DEFAULT 'none',
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS investments (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            type TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            description TEXT,
            invest_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS circles (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            friend_id INTEGER NOT NULL REFERENCES users(id),
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(user_id, friend_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users (LOWER(username));
        """)
        conn.commit()
        # Seed default categories
        cur.execute("SELECT COUNT(*) FROM categories WHERE is_default=1")
        if cur.fetchone()[0] == 0:
            for cat in DEFAULT_CATEGORIES:
                cur.execute("INSERT INTO categories (name, is_default) VALUES (%s, 1)", (cat,))
            conn.commit()
        # Migration: add profile_pic column if missing
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='profile_pic'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE users ADD COLUMN profile_pic TEXT DEFAULT NULL")
            conn.commit()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DATABASE, timeout=20)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE COLLATE NOCASE,
            display_name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            avatar_color TEXT DEFAULT '#6366f1',
            profile_pic TEXT DEFAULT NULL,
            security_question TEXT,
            security_answer_hash TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            is_default INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            category_id INTEGER NOT NULL REFERENCES categories(id),
            monthly_limit REAL NOT NULL,
            UNIQUE(user_id, category_id)
        );
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            category_id INTEGER NOT NULL REFERENCES categories(id),
            amount REAL NOT NULL,
            description TEXT,
            expense_date TEXT NOT NULL,
            recurrence TEXT DEFAULT 'none',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS income (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            source TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            income_date TEXT NOT NULL,
            recurrence TEXT DEFAULT 'none',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS investments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            invest_date TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS circles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            friend_id INTEGER NOT NULL REFERENCES users(id),
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, friend_id)
        );
        """)
        for cat in DEFAULT_CATEGORIES:
            existing = conn.execute("SELECT id FROM categories WHERE name=? AND is_default=1", (cat,)).fetchone()
            if not existing:
                conn.execute("INSERT INTO categories (name, is_default) VALUES (?, 1)", (cat,))
        conn.commit()
        # Migration: add profile_pic column if missing
        cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "profile_pic" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN profile_pic TEXT DEFAULT NULL")
            conn.commit()
        conn.close()

# ── Helpers ──────────────────────────────────────────────────────

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

def period_clause(col, period):
    s, e = get_date_range(period)
    if s and e:
        return f" AND {col} BETWEEN %s AND %s", [s, e]
    return "", []

def generate_insights(expense_data, invest_data, tot_exp, tot_inv, tot_inc=0):
    insights = []
    if tot_inc > 0 and tot_exp > 0:
        sr = (tot_inc - tot_exp) / tot_inc * 100
        if sr >= 30:
            insights.append({"type":"success","icon":"trending-up","msg":f"Excellent! Saving {sr:.0f}% of income."})
        elif sr >= 10:
            insights.append({"type":"info","icon":"piggy-bank","msg":f"Saving {sr:.0f}% — aim for 20%+."})
        elif sr > 0:
            insights.append({"type":"warning","icon":"alert-triangle","msg":f"Only {sr:.0f}% savings. Cut discretionary spending."})
        else:
            insights.append({"type":"warning","icon":"alert-circle","msg":"Spending exceeds income this period!"})
    if tot_inv > 0 and tot_exp > 0 and tot_inv / tot_exp >= 0.5:
        insights.append({"type":"success","icon":"rocket","msg":"Great investment allocation!"})
    emap = {r["name"]: r["total"] for r in expense_data} if expense_data else {}
    disc = emap.get("Entertainment", 0) + emap.get("Food & Dining", 0) + emap.get("Shopping", 0)
    if tot_exp > 0 and disc / tot_exp > 0.5:
        insights.append({"type":"warning","icon":"shopping-cart","msg":f"Discretionary spending is {disc/tot_exp*100:.0f}%. Set budget limits."})
    if not insights and tot_exp > 0:
        insights.append({"type":"success","icon":"check-circle","msg":"Spending looks balanced!"})
    if not insights:
        insights.append({"type":"info","icon":"info","msg":"Add transactions to get insights."})
    return insights

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
        return get_db().execute("SELECT * FROM users WHERE id=%s", (session["user_id"],)).fetchone()
    return None

def get_circle_member_ids(uid):
    rows = get_db().execute(
        "SELECT friend_id FROM circles WHERE user_id=%s AND status='accepted' UNION SELECT user_id FROM circles WHERE friend_id=%s AND status='accepted'",
        (uid, uid)).fetchall()
    return [uid] + [r[0] for r in rows]

def get_circle_members(uid):
    return get_db().execute("""
        SELECT u.* FROM users u WHERE u.id IN (
            SELECT friend_id FROM circles WHERE user_id=%s AND status='accepted'
            UNION SELECT user_id FROM circles WHERE friend_id=%s AND status='accepted'
        ) AND u.id != %s""", (uid, uid, uid)).fetchall()

def can_view_user(viewer, target):
    return viewer == target or target in get_circle_member_ids(viewer)

def resolve_view(uid):
    v = request.args.get("view_user", str(uid))
    if v == "overall":
        return "overall", True
    try:
        return int(v), False
    except ValueError:
        return uid, False

# ── Auth ─────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("dashboard") if "user_id" in session else url_for("signin"))

@app.route("/signup", methods=["GET","POST"])
def signup():
    if request.method == "POST":
        u = request.form.get("username","").strip()
        dn = request.form.get("display_name","").strip()
        pw = request.form.get("password","")
        c = request.form.get("confirm_password","")
        sq = request.form.get("security_question","")
        sa = request.form.get("security_answer","").strip()
        errs = []
        if not u or not dn or not pw: errs.append("All fields required.")
        if len(u) < 3: errs.append("Username ≥ 3 chars.")
        if pw != c: errs.append("Passwords don't match.")
        if len(pw) < 6: errs.append("Password ≥ 6 chars.")
        if not sq or not sa: errs.append("Security question required.")
        if errs:
            for e in errs: flash(e, "danger")
            return render_template("signup.html", security_questions=SECURITY_QUESTIONS)
        db = get_db()
        if db.execute("SELECT id FROM users WHERE username=%s", (u,)).fetchone():
            flash("Username taken.", "danger")
            return render_template("signup.html", security_questions=SECURITY_QUESTIONS)
        ph = bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()
        ah = bcrypt.hashpw(sa.lower().encode(), bcrypt.gensalt()).decode()
        db.execute("INSERT INTO users (username,display_name,password_hash,avatar_color,security_question,security_answer_hash) VALUES (%s,%s,%s,%s,%s,%s)",
                   (u, dn, ph, random.choice(AVATAR_COLORS), sq, ah))
        db.commit()
        flash("Account created! Sign in.", "success")
        return redirect(url_for("signin"))
    return render_template("signup.html", security_questions=SECURITY_QUESTIONS)

@app.route("/signin", methods=["GET","POST"])
def signin():
    if request.method == "POST":
        u = request.form.get("username","").strip()
        pw = request.form.get("password","")
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE username=%s", (u,)).fetchone()
        if user and bcrypt.checkpw(pw.encode(), user["password_hash"].encode()):
            session["user_id"] = user["id"]
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

@app.route("/forgot-password", methods=["GET","POST"])
def forgot_password():
    step = request.form.get("step", "1")
    if request.method == "POST" and step == "1":
        u = request.form.get("username","").strip()
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE username=%s", (u,)).fetchone()
        if not user or not user["security_question"]:
            flash("Username not found.", "danger")
            return render_template("forgot_password.html", step=1)
        return render_template("forgot_password.html", step=2, username=u, security_question=user["security_question"])
    elif request.method == "POST" and step == "2":
        u = request.form.get("username","").strip()
        a = request.form.get("security_answer","").strip()
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE username=%s", (u,)).fetchone()
        if not user or not bcrypt.checkpw(a.lower().encode(), user["security_answer_hash"].encode()):
            flash("Incorrect answer.", "danger")
            return render_template("forgot_password.html", step=2, username=u, security_question=user["security_question"] if user else "")
        return render_template("forgot_password.html", step=3, username=u)
    elif request.method == "POST" and step == "3":
        u = request.form.get("username","").strip()
        np = request.form.get("new_password","")
        c = request.form.get("confirm_password","")
        if len(np) < 6:
            flash("Password ≥ 6 chars.", "danger")
            return render_template("forgot_password.html", step=3, username=u)
        if np != c:
            flash("Passwords don't match.", "danger")
            return render_template("forgot_password.html", step=3, username=u)
        db = get_db()
        db.execute("UPDATE users SET password_hash=%s WHERE username=%s",
                   (bcrypt.hashpw(np.encode(), bcrypt.gensalt()).decode(), u))
        db.commit()
        flash("Password reset! Sign in.", "success")
        return redirect(url_for("signin"))
    return render_template("forgot_password.html", step=1)

# ── Change Password (logged-in) ──────────────────────────────────

@app.route("/change-password", methods=["GET","POST"])
@login_required
def change_password():
    if request.method == "POST":
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
        current = request.form.get("current_password","")
        new_pw = request.form.get("new_password","")
        confirm = request.form.get("confirm_password","")
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE id=%s", (session["user_id"],)).fetchone()
        if not user or not bcrypt.checkpw(current.encode(), user["password_hash"].encode()):
            if is_ajax:
                return jsonify(success=False, message="Current password is incorrect."), 200
            flash("Current password is incorrect.", "danger")
            return render_template("change_password.html", user=current_user())
        if len(new_pw) < 6:
            if is_ajax:
                return jsonify(success=False, message="New password must be at least 6 characters."), 200
            flash("New password must be at least 6 characters.", "danger")
            return render_template("change_password.html", user=current_user())
        if new_pw != confirm:
            if is_ajax:
                return jsonify(success=False, message="Passwords do not match."), 200
            flash("Passwords do not match.", "danger")
            return render_template("change_password.html", user=current_user())
        db.execute("UPDATE users SET password_hash=%s WHERE id=%s",
                   (bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode(), session["user_id"]))
        db.commit()
        if is_ajax:
            return jsonify(success=True, message="Password changed successfully!"), 200
        flash("Password changed successfully!", "success")
        return redirect(url_for("dashboard"))
    return render_template("change_password.html", user=current_user())

# ── Dashboard ────────────────────────────────────────────────────

@app.route("/dashboard")
@login_required
def dashboard():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    db = get_db()
    circle = get_circle_members(uid)
    pe, pp_e = period_clause("e.expense_date", period)
    pi_, pp_i = period_clause("i.invest_date", period)
    pinc, pp_inc = period_clause("inc.income_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        ph = ",".join(["%s"]*len(ids))
        exp = db.execute(f"SELECT c.name, SUM(e.amount) as total FROM expenses e JOIN categories c ON e.category_id=c.id WHERE e.user_id IN ({ph}){pe} GROUP BY c.name ORDER BY total DESC", ids+pp_e).fetchall()
        inv = db.execute(f"SELECT i.type, SUM(i.amount) as total FROM investments i WHERE i.user_id IN ({ph}){pi_} GROUP BY i.type ORDER BY total DESC", ids+pp_i).fetchall()
        inc = db.execute(f"SELECT inc.source, SUM(inc.amount) as total FROM income inc WHERE inc.user_id IN ({ph}){pinc} GROUP BY inc.source ORDER BY total DESC", ids+pp_inc).fetchall()
        recent = db.execute(f"SELECT e.*, c.name as category_name, u.display_name as owner_name FROM expenses e JOIN categories c ON e.category_id=c.id JOIN users u ON e.user_id=u.id WHERE e.user_id IN ({ph}){pe} ORDER BY e.expense_date DESC LIMIT 8", ids+pp_e).fetchall()
        vu = None
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger")
            return redirect(url_for("dashboard"))
        exp = db.execute(f"SELECT c.name, SUM(e.amount) as total FROM expenses e JOIN categories c ON e.category_id=c.id WHERE e.user_id=%s{pe} GROUP BY c.name ORDER BY total DESC", [vid]+pp_e).fetchall()
        inv = db.execute(f"SELECT i.type, SUM(i.amount) as total FROM investments i WHERE i.user_id=%s{pi_} GROUP BY i.type ORDER BY total DESC", [vid]+pp_i).fetchall()
        inc = db.execute(f"SELECT inc.source, SUM(inc.amount) as total FROM income inc WHERE inc.user_id=%s{pinc} GROUP BY inc.source ORDER BY total DESC", [vid]+pp_inc).fetchall()
        recent = db.execute(f"SELECT e.*, c.name as category_name FROM expenses e JOIN categories c ON e.category_id=c.id WHERE e.user_id=%s{pe} ORDER BY e.expense_date DESC LIMIT 8", [vid]+pp_e).fetchall()
        vu = db.execute("SELECT * FROM users WHERE id=%s", (vid,)).fetchone()

    te = sum(r["total"] for r in exp) if exp else 0
    ti = sum(r["total"] for r in inv) if inv else 0
    tinc = sum(r["total"] for r in inc) if inc else 0
    ms, me = get_date_range("month")
    buid = uid if is_all else vid
    budgets = db.execute("SELECT b.monthly_limit, c.name, COALESCE((SELECT SUM(e.amount) FROM expenses e WHERE e.user_id=b.user_id AND e.category_id=b.category_id AND e.expense_date BETWEEN %s AND %s),0) as spent FROM budgets b JOIN categories c ON b.category_id=c.id WHERE b.user_id=%s", (ms, me, buid)).fetchall()
    insights = generate_insights(exp, inv, te, ti, tinc)

    return render_template("dashboard.html",
        expense_data=exp, invest_data=inv, income_data=inc,
        total_expenses=te, total_investments=ti, total_income=tinc,
        recent_expenses=recent, budgets=budgets, insights=insights,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_overall=is_all, period=period, user=current_user())

# ── Expenses ─────────────────────────────────────────────────────

@app.route("/expenses")
@login_required
def expenses():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    db = get_db()
    cats = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    circle = get_circle_members(uid)
    pc, pp = period_clause("e.expense_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        ph = ",".join(["%s"]*len(ids))
        rows = db.execute(f"SELECT e.*, c.name as category_name, u.display_name as owner_name FROM expenses e JOIN categories c ON e.category_id=c.id JOIN users u ON e.user_id=u.id WHERE e.user_id IN ({ph}){pc} ORDER BY e.expense_date DESC", ids+pp).fetchall()
        vu, own = None, False
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger"); return redirect(url_for("expenses"))
        rows = db.execute(f"SELECT e.*, c.name as category_name FROM expenses e JOIN categories c ON e.category_id=c.id WHERE e.user_id=%s{pc} ORDER BY e.expense_date DESC", [vid]+pp).fetchall()
        vu = db.execute("SELECT * FROM users WHERE id=%s", (vid,)).fetchone()
        own = vid == uid

    return render_template("expenses.html", expenses=rows, categories=cats,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_own=own, is_overall=is_all, period=period,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())

@app.route("/expenses/add", methods=["POST"])
@login_required
def add_expense():
    uid = session["user_id"]
    cid = request.form.get("category_id", type=int)
    amt = request.form.get("amount", type=float)
    desc = request.form.get("description","").strip()
    dt = request.form.get("expense_date","")
    rec = request.form.get("recurrence","none")
    if not cid or not amt or not dt:
        flash("Category, amount, and date required.", "danger")
        return redirect(url_for("expenses"))
    db = get_db()
    db.execute("INSERT INTO expenses (user_id,category_id,amount,description,expense_date,recurrence) VALUES (%s,%s,%s,%s,%s,%s)",
               (uid, cid, amt, desc, dt, rec))
    db.commit()
    flash("Expense added.", "success")
    return redirect(url_for("expenses"))

@app.route("/expenses/delete/<int:expense_id>", methods=["POST"])
@login_required
def delete_expense(expense_id):
    db = get_db()
    db.execute("DELETE FROM expenses WHERE id=%s AND user_id=%s", (expense_id, session["user_id"]))
    db.commit()
    flash("Expense deleted.", "info")
    return redirect(url_for("expenses"))

@app.route("/expenses/edit/<int:expense_id>", methods=["GET","POST"])
@login_required
def edit_expense(expense_id):
    uid = session["user_id"]
    db = get_db()
    exp = db.execute("SELECT * FROM expenses WHERE id=%s AND user_id=%s", (expense_id, uid)).fetchone()
    if not exp:
        flash("Expense not found.", "danger"); return redirect(url_for("expenses"))
    if request.method == "POST":
        cid = request.form.get("category_id", type=int)
        amt = request.form.get("amount", type=float)
        desc = request.form.get("description","").strip()
        dt = request.form.get("expense_date","")
        rec = request.form.get("recurrence","none")
        if not cid or not amt or not dt:
            flash("Category, amount, and date required.", "danger")
            return redirect(url_for("edit_expense", expense_id=expense_id))
        db.execute("UPDATE expenses SET category_id=%s, amount=%s, description=%s, expense_date=%s, recurrence=%s WHERE id=%s AND user_id=%s",
                   (cid, amt, desc, dt, rec, expense_id, uid))
        db.commit()
        flash("Expense updated.", "success")
        return redirect(url_for("expenses"))
    cats = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    return render_template("edit_expense.html", expense=exp, categories=cats,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())

# ── Income / Salary ──────────────────────────────────────────────

@app.route("/income")
@login_required
def income():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    db = get_db()
    circle = get_circle_members(uid)
    pc, pp = period_clause("inc.income_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        ph = ",".join(["%s"]*len(ids))
        rows = db.execute(f"SELECT inc.*, u.display_name as owner_name FROM income inc JOIN users u ON inc.user_id=u.id WHERE inc.user_id IN ({ph}){pc} ORDER BY inc.income_date DESC", ids+pp).fetchall()
        vu, own = None, False
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger"); return redirect(url_for("income"))
        rows = db.execute(f"SELECT * FROM income inc WHERE user_id=%s{pc} ORDER BY income_date DESC", [vid]+pp).fetchall()
        vu = db.execute("SELECT * FROM users WHERE id=%s", (vid,)).fetchone()
        own = vid == uid

    return render_template("income.html", incomes=rows, income_sources=INCOME_SOURCES,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_own=own, is_overall=is_all, period=period,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())

@app.route("/income/add", methods=["POST"])
@login_required
def add_income():
    uid = session["user_id"]
    src = request.form.get("source","").strip()
    amt = request.form.get("amount", type=float)
    desc = request.form.get("description","").strip()
    dt = request.form.get("income_date","")
    rec = request.form.get("recurrence","none")
    if not src or not amt or not dt:
        flash("Source, amount, and date required.", "danger")
        return redirect(url_for("income"))
    db = get_db()
    db.execute("INSERT INTO income (user_id,source,amount,description,income_date,recurrence) VALUES (%s,%s,%s,%s,%s,%s)",
               (uid, src, amt, desc, dt, rec))
    db.commit()
    flash("Income added.", "success")
    return redirect(url_for("income"))

@app.route("/income/delete/<int:income_id>", methods=["POST"])
@login_required
def delete_income(income_id):
    db = get_db()
    db.execute("DELETE FROM income WHERE id=%s AND user_id=%s", (income_id, session["user_id"]))
    db.commit()
    flash("Income deleted.", "info")
    return redirect(url_for("income"))

@app.route("/income/edit/<int:income_id>", methods=["GET","POST"])
@login_required
def edit_income(income_id):
    uid = session["user_id"]
    db = get_db()
    inc = db.execute("SELECT * FROM income WHERE id=%s AND user_id=%s", (income_id, uid)).fetchone()
    if not inc:
        flash("Income not found.", "danger"); return redirect(url_for("income"))
    if request.method == "POST":
        src = request.form.get("source","").strip()
        amt = request.form.get("amount", type=float)
        desc = request.form.get("description","").strip()
        dt = request.form.get("income_date","")
        rec = request.form.get("recurrence","none")
        if not src or not amt or not dt:
            flash("Source, amount, and date required.", "danger")
            return redirect(url_for("edit_income", income_id=income_id))
        db.execute("UPDATE income SET source=%s, amount=%s, description=%s, income_date=%s, recurrence=%s WHERE id=%s AND user_id=%s",
                   (src, amt, desc, dt, rec, income_id, uid))
        db.commit()
        flash("Income updated.", "success")
        return redirect(url_for("income"))
    return render_template("edit_income.html", income_item=inc, income_sources=INCOME_SOURCES,
        recurrence_options=RECURRENCE_OPTIONS, user=current_user())

# ── Investments ──────────────────────────────────────────────────

@app.route("/investments")
@login_required
def investments():
    uid = session["user_id"]
    vid, is_all = resolve_view(uid)
    period = request.args.get("period", "month")
    db = get_db()
    circle = get_circle_members(uid)
    pc, pp = period_clause("i.invest_date", period)

    if is_all:
        ids = get_circle_member_ids(uid)
        ph = ",".join(["%s"]*len(ids))
        rows = db.execute(f"SELECT i.*, u.display_name as owner_name FROM investments i JOIN users u ON i.user_id=u.id WHERE i.user_id IN ({ph}){pc} ORDER BY i.invest_date DESC", ids+pp).fetchall()
        vu, own = None, False
    else:
        if not can_view_user(uid, vid):
            flash("Access denied.", "danger"); return redirect(url_for("investments"))
        rows = db.execute(f"SELECT * FROM investments i WHERE user_id=%s{pc} ORDER BY invest_date DESC", [vid]+pp).fetchall()
        vu = db.execute("SELECT * FROM users WHERE id=%s", (vid,)).fetchone()
        own = vid == uid

    return render_template("investments.html", investments=rows, investment_types=INVESTMENT_TYPES,
        circle_members=circle, view_user_id=vid, viewed_user=vu,
        is_own=own, is_overall=is_all, period=period, user=current_user())

@app.route("/investments/add", methods=["POST"])
@login_required
def add_investment():
    uid = session["user_id"]
    t = request.form.get("type","").strip()
    amt = request.form.get("amount", type=float)
    desc = request.form.get("description","").strip()
    dt = request.form.get("invest_date","")
    if not t or not amt or not dt:
        flash("Type, amount, and date required.", "danger")
        return redirect(url_for("investments"))
    db = get_db()
    db.execute("INSERT INTO investments (user_id,type,amount,description,invest_date) VALUES (%s,%s,%s,%s,%s)",
               (uid, t, amt, desc, dt))
    db.commit()
    flash("Investment added.", "success")
    return redirect(url_for("investments"))

@app.route("/investments/delete/<int:inv_id>", methods=["POST"])
@login_required
def delete_investment(inv_id):
    db = get_db()
    db.execute("DELETE FROM investments WHERE id=%s AND user_id=%s", (inv_id, session["user_id"]))
    db.commit()
    flash("Investment deleted.", "info")
    return redirect(url_for("investments"))

@app.route("/investments/edit/<int:inv_id>", methods=["GET","POST"])
@login_required
def edit_investment(inv_id):
    uid = session["user_id"]
    db = get_db()
    inv = db.execute("SELECT * FROM investments WHERE id=%s AND user_id=%s", (inv_id, uid)).fetchone()
    if not inv:
        flash("Investment not found.", "danger"); return redirect(url_for("investments"))
    if request.method == "POST":
        t = request.form.get("type","").strip()
        amt = request.form.get("amount", type=float)
        desc = request.form.get("description","").strip()
        dt = request.form.get("invest_date","")
        if not t or not amt or not dt:
            flash("Type, amount, and date required.", "danger")
            return redirect(url_for("edit_investment", inv_id=inv_id))
        db.execute("UPDATE investments SET type=%s, amount=%s, description=%s, invest_date=%s WHERE id=%s AND user_id=%s",
                   (t, amt, desc, dt, inv_id, uid))
        db.commit()
        flash("Investment updated.", "success")
        return redirect(url_for("investments"))
    return render_template("edit_investment.html", investment=inv, investment_types=INVESTMENT_TYPES, user=current_user())

# ── Budgets ──────────────────────────────────────────────────────

@app.route("/budgets", methods=["GET","POST"])
@login_required
def budgets():
    uid = session["user_id"]
    db = get_db()
    if request.method == "POST":
        cid = request.form.get("category_id", type=int)
        lim = request.form.get("monthly_limit", type=float)
        if cid and lim and lim > 0:
            if IS_PG:
                db.execute("""INSERT INTO budgets (user_id,category_id,monthly_limit) VALUES (%s,%s,%s)
                    ON CONFLICT (user_id,category_id) DO UPDATE SET monthly_limit=EXCLUDED.monthly_limit""", (uid, cid, lim))
            else:
                db.execute("INSERT OR REPLACE INTO budgets (user_id,category_id,monthly_limit) VALUES (%s,%s,%s)", (uid, cid, lim))
            db.commit()
            flash("Budget saved.", "success")
        return redirect(url_for("budgets"))
    cats = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    ms, me = get_date_range("month")
    bl = db.execute("SELECT b.*, c.name as category_name, COALESCE((SELECT SUM(e.amount) FROM expenses e WHERE e.user_id=%s AND e.category_id=b.category_id AND e.expense_date BETWEEN %s AND %s),0) as spent FROM budgets b JOIN categories c ON b.category_id=c.id WHERE b.user_id=%s ORDER BY c.name",
                    (uid, ms, me, uid)).fetchall()
    return render_template("budgets.html", budgets=bl, categories=cats, user=current_user())

@app.route("/budgets/delete/<int:budget_id>", methods=["POST"])
@login_required
def delete_budget(budget_id):
    db = get_db()
    db.execute("DELETE FROM budgets WHERE id=%s AND user_id=%s", (budget_id, session["user_id"]))
    db.commit()
    flash("Budget removed.", "info")
    return redirect(url_for("budgets"))

# ── Circle ───────────────────────────────────────────────────────

@app.route("/circle")
@login_required
def circle():
    uid = session["user_id"]
    db = get_db()
    members = get_circle_members(uid)
    sent = db.execute("SELECT c.*, u.username, u.display_name FROM circles c JOIN users u ON c.friend_id=u.id WHERE c.user_id=%s AND c.status='pending'", (uid,)).fetchall()
    received = db.execute("SELECT c.*, u.username, u.display_name FROM circles c JOIN users u ON c.user_id=u.id WHERE c.friend_id=%s AND c.status='pending'", (uid,)).fetchall()
    return render_template("circle.html", members=members, sent_requests=sent, received_requests=received, user=current_user())

@app.route("/circle/add", methods=["POST"])
@login_required
def add_to_circle():
    uid = session["user_id"]
    fu = request.form.get("username","").strip()
    if not fu:
        flash("Enter a username.", "danger"); return redirect(url_for("circle"))
    db = get_db()
    f = db.execute("SELECT * FROM users WHERE username=%s", (fu,)).fetchone()
    if not f:
        flash("User not found.", "danger"); return redirect(url_for("circle"))
    if f["id"] == uid:
        flash("Can't add yourself.", "warning"); return redirect(url_for("circle"))
    ex = db.execute("SELECT * FROM circles WHERE (user_id=%s AND friend_id=%s) OR (user_id=%s AND friend_id=%s)",
                    (uid, f["id"], f["id"], uid)).fetchone()
    if ex:
        flash(f"Already {'connected' if ex['status']=='accepted' else 'pending'}.", "info")
        return redirect(url_for("circle"))
    db.execute("INSERT INTO circles (user_id,friend_id,status) VALUES (%s,%s,'pending')", (uid, f["id"]))
    db.commit()
    flash(f"Request sent to {f['display_name']}.", "success")
    return redirect(url_for("circle"))

@app.route("/circle/accept/<int:circle_id>", methods=["POST"])
@login_required
def accept_circle(circle_id):
    db = get_db()
    db.execute("UPDATE circles SET status='accepted' WHERE id=%s AND friend_id=%s", (circle_id, session["user_id"]))
    db.commit()
    flash("Accepted!", "success")
    return redirect(url_for("circle"))

@app.route("/circle/reject/<int:circle_id>", methods=["POST"])
@login_required
def reject_circle(circle_id):
    db = get_db()
    db.execute("DELETE FROM circles WHERE id=%s AND friend_id=%s", (circle_id, session["user_id"]))
    db.commit()
    flash("Rejected.", "info")
    return redirect(url_for("circle"))

@app.route("/circle/remove/<int:member_id>", methods=["POST"])
@login_required
def remove_from_circle(member_id):
    uid = session["user_id"]
    db = get_db()
    db.execute("DELETE FROM circles WHERE (user_id=%s AND friend_id=%s) OR (user_id=%s AND friend_id=%s)", (uid, member_id, member_id, uid))
    db.commit()
    flash("Removed.", "info")
    return redirect(url_for("circle"))

# ── CSV Export ───────────────────────────────────────────────────

@app.route("/export/<dtype>")
@login_required
def export_csv(dtype):
    uid = session["user_id"]
    db = get_db()
    buf = io.StringIO()
    w = csv.writer(buf)
    if dtype == "expenses":
        w.writerow(["Date","Category","Amount","Description","Recurrence"])
        for r in db.execute("SELECT e.expense_date,c.name,e.amount,e.description,e.recurrence FROM expenses e JOIN categories c ON e.category_id=c.id WHERE e.user_id=%s ORDER BY e.expense_date DESC", (uid,)):
            w.writerow([r["expense_date"],r["name"],r["amount"],r["description"] or "",r["recurrence"]])
    elif dtype == "income":
        w.writerow(["Date","Source","Amount","Description","Recurrence"])
        for r in db.execute("SELECT income_date,source,amount,description,recurrence FROM income WHERE user_id=%s ORDER BY income_date DESC", (uid,)):
            w.writerow([r["income_date"],r["source"],r["amount"],r["description"] or "",r["recurrence"]])
    elif dtype == "investments":
        w.writerow(["Date","Type","Amount","Description"])
        for r in db.execute("SELECT invest_date,type,amount,description FROM investments WHERE user_id=%s ORDER BY invest_date DESC", (uid,)):
            w.writerow([r["invest_date"],r["type"],r["amount"],r["description"] or ""])
    else:
        flash("Invalid.", "danger"); return redirect(url_for("dashboard"))
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment;filename={dtype}_{date.today().isoformat()}.csv"})

# ── APIs ─────────────────────────────────────────────────────────

@app.route("/api/search-users")
@login_required
def search_users():
    uid = session["user_id"]
    q = request.args.get("q","").strip()
    if len(q) < 1: return jsonify([])
    rows = get_db().execute("""
        SELECT id, username, display_name, avatar_color FROM users
        WHERE id != %s AND (username LIKE %s OR display_name LIKE %s)
        AND id NOT IN (SELECT friend_id FROM circles WHERE user_id=%s UNION SELECT user_id FROM circles WHERE friend_id=%s)
        LIMIT 10""", (uid, f"%{q}%", f"%{q}%", uid, uid)).fetchall()
    return jsonify([{"username":r["username"],"display_name":r["display_name"],"avatar_color":r["avatar_color"] or "#6366f1"} for r in rows])

@app.route("/profile/avatar", methods=["POST"])
@login_required
def update_avatar():
    c = request.form.get("avatar_color","#6366f1")
    if not c.startswith("#") or len(c) != 7: c = "#6366f1"
    db = get_db()
    db.execute("UPDATE users SET avatar_color=%s WHERE id=%s", (c, session["user_id"]))
    db.commit()
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
    # Remove old profile pics with different extensions
    for old_ext in ALLOWED_EXTENSIONS:
        old_path = os.path.join(UPLOAD_FOLDER, f"user_{session['user_id']}.{old_ext}")
        if os.path.exists(old_path):
            os.remove(old_path)
    f.save(os.path.join(UPLOAD_FOLDER, filename))
    db = get_db()
    db.execute("UPDATE users SET profile_pic=%s WHERE id=%s", (filename, session["user_id"]))
    db.commit()
    if is_ajax:
        pic_url = url_for("static", filename=f"uploads/{filename}")
        return jsonify(success=True, message="Profile picture updated!", pic_url=pic_url), 200
    flash("Profile picture updated!", "success")
    return redirect(request.referrer or url_for("dashboard"))

@app.route("/profile/picture/remove", methods=["POST"])
@login_required
def remove_profile_pic():
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    db = get_db()
    user = db.execute("SELECT profile_pic FROM users WHERE id=%s", (session["user_id"],)).fetchone()
    if user and user["profile_pic"]:
        pic_path = os.path.join(UPLOAD_FOLDER, user["profile_pic"])
        if os.path.exists(pic_path):
            os.remove(pic_path)
        db.execute("UPDATE users SET profile_pic=NULL WHERE id=%s", (session["user_id"],))
        db.commit()
    if is_ajax:
        return jsonify(success=True, message="Profile picture removed."), 200
    flash("Profile picture removed.", "success")
    return redirect(request.referrer or url_for("dashboard"))

# ── Startup ──────────────────────────────────────────────────────

with app.app_context():
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
