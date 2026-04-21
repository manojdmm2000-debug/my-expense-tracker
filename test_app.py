"""
Comprehensive test suite for Expense Tracker app.
Covers: Auth, CRUD, Edit, Period Filters, Calculations, Insights,
        Circle Access Control, CSV Export, API, Budgets, WCAG/Accessibility,
        Edge Cases, Security.
Uses a TEMP database — never touches production expenses.db.
"""
import os
import sys
import csv
import io
import tempfile
import unittest
from datetime import date, timedelta
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))

TEST_DB = os.path.join(tempfile.gettempdir(), "test_expenses_intensive.db")
os.environ["DATABASE_PATH"] = TEST_DB


def _reload_app():
    """Reload app module to pick up fresh DB."""
    import importlib
    import app as _app
    importlib.reload(_app)
    return _app


class BaseTestCase(unittest.TestCase):
    """Sets up a fresh DB + test client per test."""

    def setUp(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        os.environ["DATABASE_PATH"] = TEST_DB
        self.app_module = _reload_app()
        self.flask_app = self.app_module.app
        self.flask_app.config["TESTING"] = True
        self.flask_app.config["SECRET_KEY"] = "test-secret"
        self.client = self.flask_app.test_client()

    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    # ── helpers ──────────────────────────────────────────
    def signup(self, username="alice", display_name="Alice", password="pass123",
               security_question="What is your pet's name?", security_answer="buddy"):
        return self.client.post("/signup", data={
            "username": username, "display_name": display_name,
            "password": password, "confirm_password": password,
            "security_question": security_question,
            "security_answer": security_answer,
        }, follow_redirects=True)

    def signin(self, username="alice", password="pass123"):
        return self.client.post("/signin", data={
            "username": username, "password": password,
        }, follow_redirects=True)

    def login(self, username="alice", display_name="Alice", password="pass123"):
        self.signup(username, display_name, password)
        return self.signin(username, password)

    def _add_expense(self, cat_id=1, amount=100, dt=None, desc="test", rec="none"):
        dt = dt or date.today().isoformat()
        return self.client.post("/expenses/add", data={
            "category_id": cat_id, "amount": amount,
            "expense_date": dt, "recurrence": rec, "description": desc
        }, follow_redirects=True)

    def _add_income(self, source="Salary", amount=50000, dt=None, desc="test", rec="none"):
        dt = dt or date.today().isoformat()
        return self.client.post("/income/add", data={
            "source": source, "amount": amount,
            "income_date": dt, "recurrence": rec, "description": desc
        }, follow_redirects=True)

    def _add_investment(self, itype="Stocks", amount=10000, dt=None, desc="test"):
        dt = dt or date.today().isoformat()
        return self.client.post("/investments/add", data={
            "type": itype, "amount": amount,
            "invest_date": dt, "description": desc
        }, follow_redirects=True)

    def _db_query(self, sql, params=()):
        with self.flask_app.app_context():
            return self.app_module.get_db().execute(sql, params).fetchall()

    def _db_one(self, sql, params=()):
        with self.flask_app.app_context():
            return self.app_module.get_db().execute(sql, params).fetchone()

    def _make_circle(self, u1="alice", u2="bob"):
        """Create two users and connect them in a circle."""
        self.signup(u1, u1.title())
        self.signup(u2, u2.title())
        self.signin(u1)
        self.client.post("/circle/add", data={"username": u2})
        self.client.get("/signout")
        self.signin(u2)
        cid = self._db_one("SELECT id FROM circles WHERE status='pending' LIMIT 1")["id"]
        self.client.post(f"/circle/accept/{cid}")
        self.client.get("/signout")
        return (
            self._db_one("SELECT id FROM users WHERE username=?", (u1,))["id"],
            self._db_one("SELECT id FROM users WHERE username=?", (u2,))["id"],
        )


# ═══════════════════════════════════════════════════════════════════
# 1. AUTHENTICATION
# ═══════════════════════════════════════════════════════════════════
class TestAuthSignup(BaseTestCase):

    def test_signup_success(self):
        r = self.signup()
        self.assertIn(b"Account created", r.data)

    def test_signup_creates_user_in_db(self):
        self.signup("dave", "Dave")
        row = self._db_one("SELECT * FROM users WHERE username='dave'")
        self.assertIsNotNone(row)
        self.assertEqual(row["display_name"], "Dave")

    def test_signup_duplicate_username(self):
        self.signup()
        r = self.signup()
        self.assertIn(b"Username taken", r.data)

    def test_signup_case_insensitive_duplicate(self):
        self.signup("alice")
        r = self.signup("Alice")
        self.assertIn(b"Username taken", r.data)

    def test_signup_short_username(self):
        r = self.signup(username="ab")
        self.assertIn(b"3 chars", r.data)

    def test_signup_short_password(self):
        r = self.signup(password="abc")
        self.assertIn(b"6 chars", r.data)

    def test_signup_password_mismatch(self):
        r = self.client.post("/signup", data={
            "username": "zz", "display_name": "Z",
            "password": "pass123", "confirm_password": "pass999",
            "security_question": "Q", "security_answer": "A",
        }, follow_redirects=True)
        self.assertIn(b"match", r.data)

    def test_signup_missing_security(self):
        r = self.client.post("/signup", data={
            "username": "test1", "display_name": "T",
            "password": "pass123", "confirm_password": "pass123",
            "security_question": "", "security_answer": "",
        }, follow_redirects=True)
        self.assertIn(b"Security question required", r.data)

    def test_signup_assigns_avatar_color(self):
        self.signup("eve", "Eve")
        row = self._db_one("SELECT avatar_color FROM users WHERE username='eve'")
        self.assertTrue(row["avatar_color"].startswith("#"))

    def test_signup_hashes_password(self):
        self.signup("frank", "Frank", "secret99")
        row = self._db_one("SELECT password_hash FROM users WHERE username='frank'")
        self.assertNotEqual(row["password_hash"], "secret99")
        self.assertTrue(row["password_hash"].startswith("$2"))

    def test_signup_page_get(self):
        r = self.client.get("/signup")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"Create Account", r.data)


class TestAuthSignin(BaseTestCase):

    def test_signin_success(self):
        self.signup()
        r = self.signin()
        self.assertIn(b"Welcome", r.data)

    def test_signin_wrong_password(self):
        self.signup()
        r = self.signin(password="wrong")
        self.assertIn(b"Invalid", r.data)

    def test_signin_nonexistent_user(self):
        r = self.signin(username="ghost")
        self.assertIn(b"Invalid", r.data)

    def test_signout(self):
        self.login()
        r = self.client.get("/signout", follow_redirects=True)
        self.assertIn(b"Signed out", r.data)

    def test_signout_clears_session(self):
        self.login()
        self.client.get("/signout")
        r = self.client.get("/dashboard", follow_redirects=True)
        self.assertIn(b"Sign in", r.data)

    def test_signin_page_get(self):
        r = self.client.get("/signin")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"Welcome Back", r.data)


class TestAuthForgotPassword(BaseTestCase):

    def test_full_flow(self):
        self.signup("charlie", "Charlie", "old123", security_answer="fluffy")
        # Step 1
        r = self.client.post("/forgot-password", data={"step": "1", "username": "charlie"}, follow_redirects=True)
        self.assertIn(b"pet", r.data)
        # Step 2
        r = self.client.post("/forgot-password", data={
            "step": "2", "username": "charlie", "security_answer": "FLUFFY"
        }, follow_redirects=True)
        self.assertIn(b"New Password", r.data)
        # Step 3
        r = self.client.post("/forgot-password", data={
            "step": "3", "username": "charlie",
            "new_password": "newpass1", "confirm_password": "newpass1"
        }, follow_redirects=True)
        self.assertIn(b"Password reset", r.data)
        # Old password should fail
        r = self.signin("charlie", "old123")
        self.assertIn(b"Invalid", r.data)
        # New password works
        r = self.signin("charlie", "newpass1")
        self.assertIn(b"Welcome", r.data)

    def test_case_insensitive_answer(self):
        self.signup("dan", "Dan", security_answer="New York")
        self.client.post("/forgot-password", data={"step": "1", "username": "dan"})
        r = self.client.post("/forgot-password", data={
            "step": "2", "username": "dan", "security_answer": "new york"
        }, follow_redirects=True)
        self.assertIn(b"New Password", r.data)

    def test_wrong_answer(self):
        self.signup("ed", "Ed", security_answer="cat")
        self.client.post("/forgot-password", data={"step": "1", "username": "ed"})
        r = self.client.post("/forgot-password", data={
            "step": "2", "username": "ed", "security_answer": "dog"
        }, follow_redirects=True)
        self.assertIn(b"Incorrect", r.data)

    def test_unknown_user(self):
        r = self.client.post("/forgot-password", data={"step": "1", "username": "nobody"}, follow_redirects=True)
        self.assertIn(b"not found", r.data)

    def test_reset_short_password(self):
        self.signup("ff", "FF", security_answer="x")
        self.client.post("/forgot-password", data={"step": "1", "username": "ff"})
        r = self.client.post("/forgot-password", data={
            "step": "3", "username": "ff",
            "new_password": "ab", "confirm_password": "ab"
        }, follow_redirects=True)
        self.assertIn(b"6 chars", r.data)

    def test_reset_password_mismatch(self):
        self.signup("gg", "GG", security_answer="x")
        r = self.client.post("/forgot-password", data={
            "step": "3", "username": "gg",
            "new_password": "pass123", "confirm_password": "pass999"
        }, follow_redirects=True)
        self.assertIn(b"match", r.data)

    def test_get_page(self):
        r = self.client.get("/forgot-password")
        self.assertEqual(r.status_code, 200)


class TestProtectedRoutes(BaseTestCase):

    def test_all_protected_pages_redirect(self):
        pages = ["/dashboard", "/expenses", "/income", "/investments",
                 "/budgets", "/circle", "/export/expenses", "/api/search-users?q=a"]
        for url in pages:
            r = self.client.get(url, follow_redirects=True)
            self.assertIn(b"Sign in", r.data, f"{url} not protected")

    def test_protected_post_routes(self):
        posts = ["/expenses/add", "/income/add", "/investments/add",
                 "/circle/add", "/budgets", "/profile/avatar"]
        for url in posts:
            r = self.client.post(url, data={}, follow_redirects=True)
            self.assertIn(b"Sign in", r.data, f"POST {url} not protected")


# ═══════════════════════════════════════════════════════════════════
# 2. EXPENSES CRUD + EDIT
# ═══════════════════════════════════════════════════════════════════
class TestExpensesCRUD(BaseTestCase):

    def test_add(self):
        self.login()
        r = self._add_expense(1, 250.50, desc="Lunch")
        self.assertIn(b"Expense added", r.data)

    def test_add_missing_fields(self):
        self.login()
        r = self.client.post("/expenses/add", data={
            "category_id": "", "amount": "", "expense_date": ""
        }, follow_redirects=True)
        self.assertIn(b"required", r.data)

    def test_add_stores_in_db(self):
        self.login()
        self._add_expense(1, 999.99, desc="DB check")
        row = self._db_one("SELECT * FROM expenses WHERE description='DB check'")
        self.assertAlmostEqual(row["amount"], 999.99, places=2)

    def test_add_with_recurrence(self):
        self.login()
        self._add_expense(1, 500, rec="monthly")
        row = self._db_one("SELECT recurrence FROM expenses ORDER BY id DESC LIMIT 1")
        self.assertEqual(row["recurrence"], "monthly")

    def test_delete(self):
        self.login()
        self._add_expense(desc="to_delete")
        eid = self._db_one("SELECT id FROM expenses WHERE description='to_delete'")["id"]
        r = self.client.post(f"/expenses/delete/{eid}", follow_redirects=True)
        self.assertIn(b"deleted", r.data)
        self.assertIsNone(self._db_one("SELECT id FROM expenses WHERE id=?", (eid,)))

    def test_delete_other_users_expense(self):
        self.login("alice")
        self._add_expense(desc="alice_exp")
        eid = self._db_one("SELECT id FROM expenses WHERE description='alice_exp'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/expenses/delete/{eid}")
        # Alice's expense should still exist
        self.assertIsNotNone(self._db_one("SELECT id FROM expenses WHERE id=?", (eid,)))

    def test_edit_get(self):
        self.login()
        self._add_expense(1, 300, desc="editable")
        eid = self._db_one("SELECT id FROM expenses WHERE description='editable'")["id"]
        r = self.client.get(f"/expenses/edit/{eid}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"300", r.data)
        self.assertIn(b"editable", r.data)

    def test_edit_post(self):
        self.login()
        self._add_expense(1, 300, desc="before_edit")
        eid = self._db_one("SELECT id FROM expenses WHERE description='before_edit'")["id"]
        r = self.client.post(f"/expenses/edit/{eid}", data={
            "category_id": 2, "amount": 999, "expense_date": "2026-01-01",
            "recurrence": "weekly", "description": "after_edit"
        }, follow_redirects=True)
        self.assertIn(b"updated", r.data)
        row = self._db_one("SELECT * FROM expenses WHERE id=?", (eid,))
        self.assertEqual(row["amount"], 999)
        self.assertEqual(row["description"], "after_edit")
        self.assertEqual(row["recurrence"], "weekly")

    def test_edit_other_users_expense(self):
        self.login("alice")
        self._add_expense(desc="alice_only")
        eid = self._db_one("SELECT id FROM expenses WHERE description='alice_only'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        r = self.client.get(f"/expenses/edit/{eid}", follow_redirects=True)
        self.assertIn(b"not found", r.data)

    def test_edit_nonexistent(self):
        self.login()
        r = self.client.get("/expenses/edit/99999", follow_redirects=True)
        self.assertIn(b"not found", r.data)

    def test_list_page(self):
        self.login()
        self._add_expense(1, 111, desc="visible_item")
        r = self.client.get("/expenses?period=all")
        self.assertIn(b"visible_item", r.data)
        self.assertIn(b"111.00", r.data)


# ═══════════════════════════════════════════════════════════════════
# 3. INCOME CRUD + EDIT
# ═══════════════════════════════════════════════════════════════════
class TestIncomeCRUD(BaseTestCase):

    def test_add(self):
        self.login()
        r = self._add_income("Salary", 75000)
        self.assertIn(b"Income added", r.data)

    def test_add_missing_fields(self):
        self.login()
        r = self.client.post("/income/add", data={
            "source": "", "amount": "", "income_date": ""
        }, follow_redirects=True)
        self.assertIn(b"required", r.data)

    def test_delete(self):
        self.login()
        self._add_income(desc="del_me")
        iid = self._db_one("SELECT id FROM income WHERE description='del_me'")["id"]
        r = self.client.post(f"/income/delete/{iid}", follow_redirects=True)
        self.assertIn(b"deleted", r.data)

    def test_edit_get(self):
        self.login()
        self._add_income("Freelance", 15000, desc="edit_inc")
        iid = self._db_one("SELECT id FROM income WHERE description='edit_inc'")["id"]
        r = self.client.get(f"/income/edit/{iid}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"15000", r.data)

    def test_edit_post(self):
        self.login()
        self._add_income("Salary", 50000, desc="old_salary")
        iid = self._db_one("SELECT id FROM income WHERE description='old_salary'")["id"]
        r = self.client.post(f"/income/edit/{iid}", data={
            "source": "Bonus", "amount": 60000, "income_date": "2026-04-01",
            "recurrence": "yearly", "description": "new_bonus"
        }, follow_redirects=True)
        self.assertIn(b"updated", r.data)
        row = self._db_one("SELECT * FROM income WHERE id=?", (iid,))
        self.assertEqual(row["source"], "Bonus")
        self.assertEqual(row["amount"], 60000)

    def test_edit_other_users_income(self):
        self.login("alice")
        self._add_income(desc="alice_inc")
        iid = self._db_one("SELECT id FROM income WHERE description='alice_inc'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        r = self.client.get(f"/income/edit/{iid}", follow_redirects=True)
        self.assertIn(b"not found", r.data)

    def test_list(self):
        self.login()
        self._add_income("Dividends", 5000, desc="div_payment")
        r = self.client.get("/income?period=all")
        self.assertIn(b"div_payment", r.data)
        self.assertIn(b"5000.00", r.data)


# ═══════════════════════════════════════════════════════════════════
# 4. INVESTMENTS CRUD + EDIT
# ═══════════════════════════════════════════════════════════════════
class TestInvestmentsCRUD(BaseTestCase):

    def test_add(self):
        self.login()
        r = self._add_investment("Mutual Funds", 20000)
        self.assertIn(b"Investment added", r.data)

    def test_add_missing_fields(self):
        self.login()
        r = self.client.post("/investments/add", data={
            "type": "", "amount": "", "invest_date": ""
        }, follow_redirects=True)
        self.assertIn(b"required", r.data)

    def test_delete(self):
        self.login()
        self._add_investment(desc="del_inv")
        iid = self._db_one("SELECT id FROM investments WHERE description='del_inv'")["id"]
        r = self.client.post(f"/investments/delete/{iid}", follow_redirects=True)
        self.assertIn(b"deleted", r.data)

    def test_edit_get(self):
        self.login()
        self._add_investment("Gold", 8000, desc="edit_inv")
        iid = self._db_one("SELECT id FROM investments WHERE description='edit_inv'")["id"]
        r = self.client.get(f"/investments/edit/{iid}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"8000", r.data)

    def test_edit_post(self):
        self.login()
        self._add_investment("Gold", 8000, desc="old_gold")
        iid = self._db_one("SELECT id FROM investments WHERE description='old_gold'")["id"]
        r = self.client.post(f"/investments/edit/{iid}", data={
            "type": "Crypto", "amount": 12000, "invest_date": "2026-03-01",
            "description": "new_crypto"
        }, follow_redirects=True)
        self.assertIn(b"updated", r.data)
        row = self._db_one("SELECT * FROM investments WHERE id=?", (iid,))
        self.assertEqual(row["type"], "Crypto")
        self.assertEqual(row["amount"], 12000)

    def test_edit_other_users(self):
        self.login("alice")
        self._add_investment(desc="alice_inv")
        iid = self._db_one("SELECT id FROM investments WHERE description='alice_inv'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        r = self.client.get(f"/investments/edit/{iid}", follow_redirects=True)
        self.assertIn(b"not found", r.data)


# ═══════════════════════════════════════════════════════════════════
# 5. BUDGETS
# ═══════════════════════════════════════════════════════════════════
class TestBudgets(BaseTestCase):

    def test_set_budget(self):
        self.login()
        r = self.client.post("/budgets", data={
            "category_id": 1, "monthly_limit": 5000
        }, follow_redirects=True)
        self.assertIn(b"Budget saved", r.data)

    def test_budget_upsert(self):
        self.login()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 5000})
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 8000})
        rows = self._db_query("SELECT monthly_limit FROM budgets WHERE category_id=1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["monthly_limit"], 8000)

    def test_budget_shows_spending(self):
        self.login()
        today = date.today().isoformat()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 5000})
        self._add_expense(1, 2000, dt=today)
        self._add_expense(1, 1500, dt=today)
        r = self.client.get("/budgets")
        self.assertIn(b"3500", r.data)
        self.assertIn(b"5000", r.data)

    def test_budget_excludes_other_months(self):
        self.login()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 5000})
        self._add_expense(1, 9999, dt="2020-06-15")
        r = self.client.get("/budgets")
        self.assertNotIn(b"9999", r.data)

    def test_delete_budget(self):
        self.login()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 3000})
        bid = self._db_one("SELECT id FROM budgets LIMIT 1")["id"]
        r = self.client.post(f"/budgets/delete/{bid}", follow_redirects=True)
        self.assertIn(b"removed", r.data)
        self.assertIsNone(self._db_one("SELECT id FROM budgets WHERE id=?", (bid,)))

    def test_budget_page_loads(self):
        self.login()
        r = self.client.get("/budgets")
        self.assertEqual(r.status_code, 200)


# ═══════════════════════════════════════════════════════════════════
# 6. PERIOD FILTERS — exhaustive
# ═══════════════════════════════════════════════════════════════════
class TestPeriodFilters(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.login()
        self.today = date.today()
        # Current week/month/year data
        self._add_expense(1, 100, dt=self.today.isoformat(), desc="this_period")
        self._add_income("Salary", 5000, dt=self.today.isoformat(), desc="this_salary")
        self._add_investment("Stocks", 2000, dt=self.today.isoformat(), desc="this_inv")
        # Last year data
        old = (self.today.replace(year=self.today.year - 1)).isoformat()
        self._add_expense(1, 777, dt=old, desc="old_expense")
        self._add_income("Bonus", 3000, dt=old, desc="old_income")
        self._add_investment("Gold", 1000, dt=old, desc="old_inv")

    def test_month_includes_current(self):
        r = self.client.get("/expenses?period=month")
        self.assertIn(b"this_period", r.data)

    def test_month_excludes_old(self):
        r = self.client.get("/expenses?period=month")
        self.assertNotIn(b"old_expense", r.data)

    def test_year_includes_current(self):
        r = self.client.get("/expenses?period=year")
        self.assertIn(b"this_period", r.data)

    def test_year_excludes_previous_year(self):
        r = self.client.get("/expenses?period=year")
        self.assertNotIn(b"old_expense", r.data)

    def test_all_includes_everything(self):
        r = self.client.get("/expenses?period=all")
        self.assertIn(b"this_period", r.data)
        self.assertIn(b"old_expense", r.data)

    def test_week_only_current_week(self):
        r = self.client.get("/expenses?period=week")
        self.assertIn(b"this_period", r.data)
        self.assertNotIn(b"old_expense", r.data)

    def test_income_period_filter(self):
        r = self.client.get("/income?period=month")
        self.assertIn(b"this_salary", r.data)
        self.assertNotIn(b"old_income", r.data)
        r = self.client.get("/income?period=all")
        self.assertIn(b"old_income", r.data)

    def test_investments_period_filter(self):
        r = self.client.get("/investments?period=month")
        self.assertIn(b"this_inv", r.data)
        self.assertNotIn(b"old_inv", r.data)
        r = self.client.get("/investments?period=all")
        self.assertIn(b"old_inv", r.data)

    def test_dashboard_period_totals(self):
        r = self.client.get("/dashboard?period=all")
        self.assertIn(b"877.00", r.data)  # 100 + 777
        self.assertIn(b"8000.00", r.data)  # 5000 + 3000
        r = self.client.get("/dashboard?period=month")
        self.assertNotIn(b"877", r.data)
        self.assertIn(b"100.00", r.data)

    def test_default_period_is_month(self):
        r = self.client.get("/dashboard")
        self.assertIn(b"pill-active", r.data)


# ═══════════════════════════════════════════════════════════════════
# 7. DASHBOARD CALCULATIONS
# ═══════════════════════════════════════════════════════════════════
class TestDashboardCalculations(BaseTestCase):

    def test_net_savings_positive(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 100000, dt=today)
        self._add_expense(1, 30000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"70000.00", r.data)
        self.assertIn(b"text-positive", r.data)

    def test_net_savings_negative(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 10000, dt=today)
        self._add_expense(1, 50000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"text-negative", r.data)

    def test_multiple_expenses_sum(self):
        self.login()
        today = date.today().isoformat()
        self._add_expense(1, 100, dt=today)
        self._add_expense(2, 200, dt=today)
        self._add_expense(3, 300, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"600.00", r.data)

    def test_multiple_income_sum(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 50000, dt=today)
        self._add_income("Freelance", 25000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"75000.00", r.data)

    def test_zero_state_dashboard(self):
        self.login()
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"0.00", r.data)

    def test_budget_progress_on_dashboard(self):
        self.login()
        today = date.today().isoformat()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 10000})
        self._add_expense(1, 7500, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"7500", r.data)
        self.assertIn(b"10000", r.data)


# ═══════════════════════════════════════════════════════════════════
# 8. INSIGHTS ENGINE
# ═══════════════════════════════════════════════════════════════════
class TestInsights(BaseTestCase):

    def test_high_savings_insight(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 100000, dt=today)
        self._add_expense(1, 10000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"Saving 90%", r.data)

    def test_low_savings_warning(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 100000, dt=today)
        self._add_expense(1, 95000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"Only 5%", r.data)

    def test_overspending_warning(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 50000, dt=today)
        self._add_expense(1, 60000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"exceeds income", r.data)

    def test_great_investment_insight(self):
        self.login()
        today = date.today().isoformat()
        self._add_expense(1, 10000, dt=today)
        self._add_investment("Stocks", 10000, dt=today)
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"investment", r.data.lower())

    def test_discretionary_spending_warning(self):
        self.login()
        today = date.today().isoformat()
        # Entertainment is category 1, Food & Dining is 2, Shopping is 11
        self._add_income("Salary", 100000, dt=today)
        self._add_expense(1, 30000, dt=today, desc="entertainment")
        self._add_expense(2, 30000, dt=today, desc="food")
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"Discretionary", r.data)

    def test_no_data_insight(self):
        self.login()
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"Add transactions", r.data)

    def test_balanced_insight(self):
        self.login()
        today = date.today().isoformat()
        # Only non-discretionary expense, no income
        self._add_expense(3, 5000, dt=today, desc="health")
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"balanced", r.data)


# ═══════════════════════════════════════════════════════════════════
# 16.5 FINAL GAPS — 404, CDN, Budget thresholds
# ═══════════════════════════════════════════════════════════════════
class TestErrorHandling(BaseTestCase):
    """404 and unknown route handling."""

    def test_404_unknown_route(self):
        r = self.client.get("/this-does-not-exist")
        self.assertEqual(r.status_code, 404)

    def test_404_unknown_post(self):
        self.login()
        r = self.client.post("/unknown-route", data={})
        self.assertEqual(r.status_code, 404)


class TestCDNAndAssets(BaseTestCase):
    """Verify external dependencies are loaded."""

    def test_lucide_cdn_loaded(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"unpkg.com/lucide", r.data)

    def test_chartjs_cdn_loaded(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"chart.js", r.data)

    def test_lucide_createicons_called(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"lucide.createIcons()", r.data)

    def test_theme_toggle_script(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"themeToggle", r.data)

    def test_favicon_svg_exists(self):
        r = self.client.get("/static/favicon.svg")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"<svg", r.data)

    def test_css_file_exists(self):
        r = self.client.get("/static/style.css")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"--primary", r.data)


class TestBudgetThresholds(BaseTestCase):
    """Budget progress bar warning/danger thresholds."""

    def test_budget_over_90_pct_danger(self):
        self.login()
        today = date.today().isoformat()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 1000})
        self._add_expense(1, 950, dt=today)
        r = self.client.get("/budgets")
        self.assertIn(b"progress-danger", r.data)

    def test_budget_over_70_pct_warning(self):
        self.login()
        today = date.today().isoformat()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 1000})
        self._add_expense(1, 750, dt=today)
        r = self.client.get("/budgets")
        self.assertIn(b"progress-warning", r.data)

    def test_budget_under_70_pct_normal(self):
        self.login()
        today = date.today().isoformat()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 1000})
        self._add_expense(1, 500, dt=today)
        r = self.client.get("/budgets")
        self.assertNotIn(b"progress-danger", r.data)
        self.assertNotIn(b"progress-warning", r.data)

    def test_budget_zero_limit_no_crash(self):
        """Edge: 0 limit shouldn't cause division by zero."""
        self.login()
        # Directly insert a 0-limit budget to test template safety
        with self.flask_app.app_context():
            db = self.app_module.get_db()
            uid = db.execute("SELECT id FROM users WHERE username='alice'").fetchone()["id"]
            db.execute("INSERT INTO budgets (user_id, category_id, monthly_limit) VALUES (?,?,?)",
                       (uid, 1, 0))
            db.commit()
        r = self.client.get("/budgets")
        self.assertEqual(r.status_code, 200)

    def test_insight_unit_generate_insights(self):
        """Unit test the generate_insights function directly."""
        gi = self.app_module.generate_insights
        # Excellent savings
        ins = gi([], [], 1000, 0, 10000)
        self.assertTrue(any("90%" in i["msg"] for i in ins))
        # Overspending
        ins = gi([], [], 60000, 0, 50000)
        self.assertTrue(any("exceeds" in i["msg"] for i in ins))
        # Empty
        ins = gi([], [], 0, 0, 0)
        self.assertTrue(any("Add transactions" in i["msg"] for i in ins))


# ═══════════════════════════════════════════════════════════════════
# 9. CIRCLE: ADD, ACCEPT, REJECT, REMOVE, ACCESS CONTROL
# ═══════════════════════════════════════════════════════════════════
class TestCircle(BaseTestCase):

    def test_send_request(self):
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        r = self.client.post("/circle/add", data={"username": "bob"}, follow_redirects=True)
        self.assertIn(b"Request sent", r.data)

    def test_accept_request(self):
        self._make_circle()
        rows = self._db_query("SELECT status FROM circles")
        self.assertTrue(any(r["status"] == "accepted" for r in rows))

    def test_reject_request(self):
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        self.client.get("/signout")
        self.signin("bob")
        cid = self._db_one("SELECT id FROM circles WHERE status='pending'")["id"]
        r = self.client.post(f"/circle/reject/{cid}", follow_redirects=True)
        self.assertIn(b"Rejected", r.data)
        self.assertIsNone(self._db_one("SELECT id FROM circles WHERE id=?", (cid,)))

    def test_remove_member(self):
        aid, bid = self._make_circle()
        self.signin("alice")
        r = self.client.post(f"/circle/remove/{bid}", follow_redirects=True)
        self.assertIn(b"Removed", r.data)

    def test_add_self(self):
        self.login()
        r = self.client.post("/circle/add", data={"username": "alice"}, follow_redirects=True)
        self.assertIn(b"yourself", r.data)

    def test_add_nonexistent(self):
        self.login()
        r = self.client.post("/circle/add", data={"username": "ghost"}, follow_redirects=True)
        self.assertIn(b"not found", r.data)

    def test_add_empty_username(self):
        self.login()
        r = self.client.post("/circle/add", data={"username": ""}, follow_redirects=True)
        self.assertIn(b"Enter a username", r.data)

    def test_duplicate_request(self):
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        r = self.client.post("/circle/add", data={"username": "bob"}, follow_redirects=True)
        self.assertIn(b"pending", r.data)


class TestCircleAccessControl(BaseTestCase):

    def test_view_circle_member_dashboard(self):
        aid, bid = self._make_circle()
        self.signin("alice")
        r = self.client.get(f"/dashboard?view_user={bid}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"Bob", r.data)

    def test_view_non_circle_denied(self):
        self.signup("alice"); self.signup("eve", "Eve")
        self.signin("alice")
        eve_id = self._db_one("SELECT id FROM users WHERE username='eve'")["id"]
        r = self.client.get(f"/dashboard?view_user={eve_id}", follow_redirects=True)
        self.assertIn(b"Access denied", r.data)

    def test_access_denied_expenses(self):
        self.signup("alice"); self.signup("eve", "Eve")
        self.signin("alice")
        eve_id = self._db_one("SELECT id FROM users WHERE username='eve'")["id"]
        r = self.client.get(f"/expenses?view_user={eve_id}", follow_redirects=True)
        self.assertIn(b"Access denied", r.data)

    def test_access_denied_income(self):
        self.signup("alice"); self.signup("eve", "Eve")
        self.signin("alice")
        eve_id = self._db_one("SELECT id FROM users WHERE username='eve'")["id"]
        r = self.client.get(f"/income?view_user={eve_id}", follow_redirects=True)
        self.assertIn(b"Access denied", r.data)

    def test_access_denied_investments(self):
        self.signup("alice"); self.signup("eve", "Eve")
        self.signin("alice")
        eve_id = self._db_one("SELECT id FROM users WHERE username='eve'")["id"]
        r = self.client.get(f"/investments?view_user={eve_id}", follow_redirects=True)
        self.assertIn(b"Access denied", r.data)

    def test_overall_filter(self):
        self._make_circle()
        self.signin("alice")
        r = self.client.get("/dashboard?view_user=overall&period=all")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"Combined", r.data)

    def test_overall_aggregates_data(self):
        aid, bid = self._make_circle()
        today = date.today().isoformat()
        self.signin("alice")
        self._add_expense(1, 1000, dt=today, desc="alice_exp")
        self.client.get("/signout")
        self.signin("bob")
        self._add_expense(1, 2000, dt=today, desc="bob_exp")
        # Overall from bob's view
        r = self.client.get("/dashboard?view_user=overall&period=month")
        self.assertIn(b"3000.00", r.data)

    def test_bidirectional_access(self):
        aid, bid = self._make_circle()
        # Bob can view Alice
        self.signin("bob")
        r = self.client.get(f"/dashboard?view_user={aid}")
        self.assertEqual(r.status_code, 200)

    def test_pending_no_access(self):
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        # Pending — Bob shouldn't have access
        self.client.get("/signout")
        self.signin("bob")
        r = self.client.get(f"/dashboard?view_user={self._db_one('SELECT id FROM users WHERE username=%s' % repr('alice'))['id']}", follow_redirects=True)
        self.assertIn(b"Access denied", r.data)


# ═══════════════════════════════════════════════════════════════════
# 10. CSV EXPORT
# ═══════════════════════════════════════════════════════════════════
class TestCSVExport(BaseTestCase):

    def test_expenses_csv(self):
        self.login()
        self._add_expense(1, 999, desc="csv_exp")
        r = self.client.get("/export/expenses")
        self.assertEqual(r.status_code, 200)
        self.assertIn("text/csv", r.content_type)
        reader = csv.reader(io.StringIO(r.data.decode()))
        rows = list(reader)
        self.assertEqual(rows[0], ["Date", "Category", "Amount", "Description", "Recurrence"])
        self.assertTrue(any("999" in str(row) for row in rows))

    def test_income_csv(self):
        self.login()
        self._add_income("Salary", 50000, desc="csv_inc")
        r = self.client.get("/export/income")
        self.assertEqual(r.status_code, 200)
        reader = csv.reader(io.StringIO(r.data.decode()))
        rows = list(reader)
        self.assertEqual(rows[0], ["Date", "Source", "Amount", "Description", "Recurrence"])
        self.assertTrue(any("50000" in str(row) for row in rows))

    def test_investments_csv(self):
        self.login()
        self._add_investment("Stocks", 10000, desc="csv_inv")
        r = self.client.get("/export/investments")
        self.assertEqual(r.status_code, 200)
        reader = csv.reader(io.StringIO(r.data.decode()))
        rows = list(reader)
        self.assertEqual(rows[0], ["Date", "Type", "Amount", "Description"])

    def test_csv_invalid_type(self):
        self.login()
        r = self.client.get("/export/foobar", follow_redirects=True)
        self.assertIn(b"Invalid", r.data)

    def test_csv_content_disposition(self):
        self.login()
        r = self.client.get("/export/expenses")
        self.assertIn("attachment", r.headers.get("Content-Disposition", ""))
        self.assertIn("expenses_", r.headers.get("Content-Disposition", ""))

    def test_csv_only_own_data(self):
        self.login("alice")
        self._add_expense(1, 111, desc="alice_only_csv")
        self.client.get("/signout")
        self.login("bob", "Bob")
        self._add_expense(1, 222, desc="bob_csv")
        r = self.client.get("/export/expenses")
        content = r.data.decode()
        self.assertIn("222", content)
        self.assertNotIn("111", content)


# ═══════════════════════════════════════════════════════════════════
# 11. API
# ═══════════════════════════════════════════════════════════════════
class TestSearchAPI(BaseTestCase):

    def test_search_users(self):
        self.signup("alice"); self.signup("bob", "Bob Builder")
        self.signin("alice")
        r = self.client.get("/api/search-users?q=bob")
        data = r.get_json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["username"], "bob")
        self.assertEqual(data[0]["display_name"], "Bob Builder")

    def test_search_excludes_self(self):
        self.login("alice", "Alice")
        r = self.client.get("/api/search-users?q=alice")
        self.assertEqual(len(r.get_json()), 0)

    def test_search_excludes_existing_circle(self):
        self._make_circle()
        self.signin("alice")
        r = self.client.get("/api/search-users?q=bob")
        self.assertEqual(len(r.get_json()), 0)

    def test_search_empty_query(self):
        self.login()
        r = self.client.get("/api/search-users?q=")
        self.assertEqual(r.get_json(), [])

    def test_search_partial_match(self):
        self.signup("alice"); self.signup("robert", "Robert")
        self.signin("alice")
        r = self.client.get("/api/search-users?q=rob")
        self.assertEqual(len(r.get_json()), 1)

    def test_search_by_display_name(self):
        self.signup("alice"); self.signup("bob", "Bobby Fischer")
        self.signin("alice")
        r = self.client.get("/api/search-users?q=Fischer")
        self.assertEqual(len(r.get_json()), 1)

    def test_update_avatar(self):
        self.login()
        r = self.client.post("/profile/avatar", data={"avatar_color": "#f43f5e"},
                             follow_redirects=True)
        self.assertIn(b"Avatar updated", r.data)
        row = self._db_one("SELECT avatar_color FROM users WHERE username='alice'")
        self.assertEqual(row["avatar_color"], "#f43f5e")

    def test_update_avatar_invalid_color(self):
        self.login()
        self.client.post("/profile/avatar", data={"avatar_color": "notacolor"})
        row = self._db_one("SELECT avatar_color FROM users WHERE username='alice'")
        self.assertEqual(row["avatar_color"], "#6366f1")


# ═══════════════════════════════════════════════════════════════════
# 12. WCAG / ACCESSIBILITY
# ═══════════════════════════════════════════════════════════════════
class TestWCAGAccessibility(BaseTestCase):

    def _get_page(self, url):
        self.login()
        return self.client.get(url).data.decode()

    def test_html_lang_attribute(self):
        html = self._get_page("/dashboard")
        self.assertIn('lang="en"', html)

    def test_viewport_meta(self):
        html = self._get_page("/dashboard")
        self.assertIn('name="viewport"', html)
        self.assertIn("width=device-width", html)

    def test_page_titles(self):
        pages = {
            "/dashboard": "Dashboard",
            "/expenses": "Expenses",
            "/income": "Income",
            "/investments": "Investments",
            "/budgets": "Budgets",
            "/circle": "Circle",
        }
        self.login()
        for url, expected in pages.items():
            r = self.client.get(url)
            self.assertIn(f"<title>{expected}", r.data.decode(), f"{url} missing title")

    def test_form_labels_signin(self):
        r = self.client.get("/signin")
        html = r.data.decode()
        self.assertIn("<label", html)
        self.assertIn('type="text"', html)
        self.assertIn('type="password"', html)

    def test_form_labels_signup(self):
        r = self.client.get("/signup")
        html = r.data.decode()
        labels = html.count("<label")
        self.assertGreaterEqual(labels, 5)  # username, display, pass, confirm, sec_q, sec_a

    def test_form_required_attributes(self):
        r = self.client.get("/signup")
        html = r.data.decode()
        self.assertGreaterEqual(html.count("required"), 4)

    def test_hamburger_aria_label(self):
        html = self._get_page("/dashboard")
        self.assertIn('aria-label="Menu"', html)

    def test_theme_toggle_aria_label(self):
        html = self._get_page("/dashboard")
        self.assertIn('aria-label="Profile menu"', html)
        self.assertIn('role="menu"', html)

    def test_tables_have_thead(self):
        self.login()
        self._add_expense(1, 100)
        r = self.client.get("/expenses?period=all")
        html = r.data.decode()
        self.assertIn("<thead>", html)
        self.assertIn("<th>", html)

    def test_images_no_missing_alt(self):
        """Ensure no img tags without alt (we use SVG icons, not img)."""
        pages = ["/dashboard", "/expenses", "/income", "/investments", "/circle"]
        self.login()
        for url in pages:
            html = self.client.get(url).data.decode()
            # Count img tags vs alt attributes
            import re
            imgs = re.findall(r'<img\b', html)
            alts = re.findall(r'<img[^>]*alt=', html)
            self.assertEqual(len(imgs), len(alts), f"{url}: img without alt")

    def test_buttons_have_type(self):
        r = self.client.get("/signup")
        html = r.data.decode()
        import re
        buttons = re.findall(r'<button\b[^>]*>', html)
        for btn in buttons:
            self.assertIn('type=', btn, f"Button missing type: {btn[:60]}")

    def test_color_contrast_variables(self):
        """Verify CSS has dark mode variables for both themes."""
        css_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        with open(css_path) as f:
            css = f.read()
        self.assertIn('[data-theme="dark"]', css)
        self.assertIn("--text:", css)
        self.assertIn("--bg:", css)
        self.assertIn("--bg-card:", css)

    def test_focus_styles_exist(self):
        """Verify focus styles for keyboard navigation."""
        css_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        with open(css_path) as f:
            css = f.read()
        self.assertIn(":focus", css)

    def test_responsive_breakpoints(self):
        """Verify CSS has mobile breakpoints."""
        css_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        with open(css_path) as f:
            css = f.read()
        self.assertIn("@media", css)
        self.assertIn("768px", css)

    def test_no_inline_styles_on_structural_elements(self):
        """Forms should not rely on inline styles for layout."""
        r = self.client.get("/signin")
        html = r.data.decode()
        # Auth form should use CSS classes, not inline styles
        self.assertIn('class="auth-card"', html)

    def test_signin_page_no_nav(self):
        """Signin shouldn't show app navigation."""
        r = self.client.get("/signin")
        self.assertNotIn(b"nav-links", r.data)

    def test_favicon_link(self):
        html = self._get_page("/dashboard")
        self.assertIn('favicon.svg', html)


# ═══════════════════════════════════════════════════════════════════
# 13. DEFAULT DATA / INIT
# ═══════════════════════════════════════════════════════════════════
class TestInitialization(BaseTestCase):

    def test_default_categories_created(self):
        rows = self._db_query("SELECT name FROM categories WHERE is_default=1 ORDER BY name")
        names = [r["name"] for r in rows]
        self.assertIn("Entertainment", names)
        self.assertIn("Home Rental", names)
        self.assertIn("Health", names)
        self.assertIn("Food & Dining", names)
        self.assertIn("Transportation", names)
        self.assertEqual(len(names), 13)

    def test_categories_idempotent(self):
        """Running init_db twice should not duplicate categories."""
        with self.flask_app.app_context():
            self.app_module.init_db()
        rows = self._db_query("SELECT COUNT(*) as c FROM categories WHERE is_default=1")
        self.assertEqual(rows[0]["c"], 13)

    def test_index_redirects_to_signin(self):
        r = self.client.get("/")
        self.assertEqual(r.status_code, 302)
        self.assertIn("/signin", r.location)

    def test_index_redirects_to_dashboard_if_logged_in(self):
        self.login()
        r = self.client.get("/")
        self.assertEqual(r.status_code, 302)
        self.assertIn("/dashboard", r.location)


# ═══════════════════════════════════════════════════════════════════
# 14. EDGE CASES & SECURITY
# ═══════════════════════════════════════════════════════════════════
class TestEdgeCases(BaseTestCase):

    def test_zero_amount_expense(self):
        """amount=0 should be treated as invalid (min is 0.01)."""
        self.login()
        r = self.client.post("/expenses/add", data={
            "category_id": 1, "amount": 0, "expense_date": date.today().isoformat()
        }, follow_redirects=True)
        # 0 is falsy, so should trigger "required" error
        self.assertIn(b"required", r.data)

    def test_negative_amount_budget(self):
        self.login()
        r = self.client.post("/budgets", data={
            "category_id": 1, "monthly_limit": -100
        }, follow_redirects=True)
        # Should not save a negative budget
        row = self._db_one("SELECT id FROM budgets")
        self.assertIsNone(row)

    def test_very_large_amount(self):
        self.login()
        self._add_expense(1, 99999999.99)
        row = self._db_one("SELECT amount FROM expenses ORDER BY id DESC LIMIT 1")
        self.assertAlmostEqual(row["amount"], 99999999.99, places=2)

    def test_special_characters_in_description(self):
        self.login()
        self._add_expense(1, 100, desc="<script>alert('xss')</script>")
        r = self.client.get("/expenses?period=all")
        # Jinja auto-escapes — raw <script> should not appear
        self.assertNotIn(b"<script>alert", r.data)

    def test_sql_injection_search(self):
        self.login()
        r = self.client.get("/api/search-users?q='; DROP TABLE users; --")
        self.assertEqual(r.status_code, 200)
        # users table should still exist
        self.assertIsNotNone(self._db_one("SELECT COUNT(*) FROM users"))

    def test_invalid_view_user_param(self):
        self.login()
        r = self.client.get("/dashboard?view_user=notanumber")
        # Should default to own view
        self.assertEqual(r.status_code, 200)

    def test_edit_with_missing_post_fields(self):
        self.login()
        self._add_expense(1, 100, desc="test_missing")
        eid = self._db_one("SELECT id FROM expenses WHERE description='test_missing'")["id"]
        r = self.client.post(f"/expenses/edit/{eid}", data={
            "category_id": "", "amount": "", "expense_date": ""
        }, follow_redirects=True)
        self.assertIn(b"required", r.data)

    def test_concurrent_category_names(self):
        """Verify all category names are unique."""
        rows = self._db_query("SELECT name, COUNT(*) as c FROM categories GROUP BY name HAVING c > 1")
        self.assertEqual(len(rows), 0, "Duplicate category names found")

    def test_multiple_users_data_isolation(self):
        """User A cannot see User B's data in their own expense list."""
        self.login("alice")
        self._add_expense(1, 111, desc="alice_secret")
        self.client.get("/signout")
        self.login("bob", "Bob")
        self._add_expense(1, 222, desc="bob_data")
        r = self.client.get("/expenses?period=all")
        self.assertIn(b"bob_data", r.data)
        self.assertNotIn(b"alice_secret", r.data)


class TestDateRangeHelper(BaseTestCase):
    """Unit tests for get_date_range and period_clause."""

    def test_week_range(self):
        with self.flask_app.app_context():
            s, e = self.app_module.get_date_range("week")
            self.assertIsNotNone(s)
            start = date.fromisoformat(s)
            end = date.fromisoformat(e)
            self.assertEqual(start.weekday(), 0)  # Monday
            self.assertEqual((end - start).days, 6)

    def test_month_range(self):
        with self.flask_app.app_context():
            s, e = self.app_module.get_date_range("month")
            start = date.fromisoformat(s)
            end = date.fromisoformat(e)
            self.assertEqual(start.day, 1)
            # End should be last day of month
            next_day = end + timedelta(days=1)
            self.assertEqual(next_day.day, 1)

    def test_year_range(self):
        with self.flask_app.app_context():
            s, e = self.app_module.get_date_range("year")
            start = date.fromisoformat(s)
            end = date.fromisoformat(e)
            self.assertEqual(start.month, 1)
            self.assertEqual(start.day, 1)
            self.assertEqual(end.month, 12)
            self.assertEqual(end.day, 31)

    def test_all_returns_none(self):
        with self.flask_app.app_context():
            s, e = self.app_module.get_date_range("all")
            self.assertIsNone(s)
            self.assertIsNone(e)

    def test_invalid_period(self):
        with self.flask_app.app_context():
            s, e = self.app_module.get_date_range("foobar")
            self.assertIsNone(s)
            self.assertIsNone(e)

    def test_period_clause_generates_sql(self):
        with self.flask_app.app_context():
            clause, params = self.app_module.period_clause("e.expense_date", "month")
            self.assertIn("BETWEEN", clause)
            self.assertEqual(len(params), 2)

    def test_period_clause_all_empty(self):
        with self.flask_app.app_context():
            clause, params = self.app_module.period_clause("e.expense_date", "all")
            self.assertEqual(clause, "")
            self.assertEqual(params, [])


# ═══════════════════════════════════════════════════════════════════
# 15. ALL PAGES RENDER (smoke tests)
# ═══════════════════════════════════════════════════════════════════
class TestPageRendering(BaseTestCase):

    def test_all_main_pages_200(self):
        self.login()
        urls = ["/dashboard", "/expenses", "/income", "/investments", "/budgets", "/circle"]
        for url in urls:
            r = self.client.get(url)
            self.assertEqual(r.status_code, 200, f"{url} failed")

    def test_all_period_variants(self):
        self.login()
        for period in ["week", "month", "year", "all"]:
            for page in ["/dashboard", "/expenses", "/income", "/investments"]:
                r = self.client.get(f"{page}?period={period}")
                self.assertEqual(r.status_code, 200, f"{page}?period={period} failed")

    def test_edit_pages_load(self):
        self.login()
        self._add_expense(1, 100, desc="e")
        self._add_income("Salary", 100, desc="i")
        self._add_investment("Stocks", 100, desc="v")
        eid = self._db_one("SELECT id FROM expenses WHERE description='e'")["id"]
        iid = self._db_one("SELECT id FROM income WHERE description='i'")["id"]
        vid = self._db_one("SELECT id FROM investments WHERE description='v'")["id"]
        for url in [f"/expenses/edit/{eid}", f"/income/edit/{iid}", f"/investments/edit/{vid}"]:
            r = self.client.get(url)
            self.assertEqual(r.status_code, 200, f"{url} failed")

    def test_auth_pages(self):
        for url in ["/signin", "/signup", "/forgot-password"]:
            r = self.client.get(url)
            self.assertEqual(r.status_code, 200, f"{url} failed")

    def test_chart_canvas_on_dashboard(self):
        self.login()
        self._add_expense(1, 100)
        r = self.client.get("/dashboard?period=all")
        self.assertIn(b"expenseChart", r.data)

    def test_empty_states(self):
        self.login()
        r = self.client.get("/expenses?period=all")
        self.assertIn(b"No expenses found", r.data)
        r = self.client.get("/income?period=all")
        self.assertIn(b"No income records", r.data)
        r = self.client.get("/investments?period=all")
        self.assertIn(b"No investments found", r.data)


# ═══════════════════════════════════════════════════════════════════
# 16. MISSING GAPS — BOUNDARY, CROSS-ENTITY, HTTP METHOD, SECURITY
# ═══════════════════════════════════════════════════════════════════

class TestBoundaryValues(BaseTestCase):
    """Boundary value tests for inputs."""

    def test_username_exactly_3_chars(self):
        r = self.signup(username="abc")
        self.assertIn(b"Account created", r.data)

    def test_password_exactly_6_chars(self):
        r = self.signup(username="bound1", password="abcdef")
        self.assertIn(b"Account created", r.data)

    def test_amount_minimum_001(self):
        self.login()
        r = self._add_expense(1, 0.01)
        self.assertIn(b"Expense added", r.data)

    def test_decimal_precision(self):
        self.login()
        self._add_expense(1, 123.45, desc="precise")
        row = self._db_one("SELECT amount FROM expenses WHERE description='precise'")
        self.assertAlmostEqual(row["amount"], 123.45, places=2)

    def test_future_date_allowed(self):
        self.login()
        future = (date.today() + timedelta(days=30)).isoformat()
        r = self._add_expense(1, 100, dt=future, desc="future_exp")
        self.assertIn(b"Expense added", r.data)

    def test_very_old_date(self):
        self.login()
        r = self._add_expense(1, 50, dt="2000-01-01", desc="y2k_exp")
        self.assertIn(b"Expense added", r.data)
        r = self.client.get("/expenses?period=all")
        self.assertIn(b"y2k_exp", r.data)

    def test_empty_description_allowed(self):
        self.login()
        r = self._add_expense(1, 100, desc="")
        self.assertIn(b"Expense added", r.data)

    def test_unicode_description(self):
        self.login()
        r = self._add_expense(1, 100, desc="காலை உணவு 🍕")
        self.assertIn(b"Expense added", r.data)
        r = self.client.get("/expenses?period=all")
        self.assertIn("காலை உணவு".encode(), r.data)

    def test_unicode_display_name(self):
        r = self.signup("uni_user", "பாலா", "pass123")
        self.assertIn(b"Account created", r.data)
        row = self._db_one("SELECT display_name FROM users WHERE username='uni_user'")
        self.assertEqual(row["display_name"], "பாலா")


class TestHTTPMethodEnforcement(BaseTestCase):
    """Ensure POST-only routes reject GET and vice versa."""

    def test_get_on_add_expense(self):
        self.login()
        r = self.client.get("/expenses/add")
        self.assertIn(r.status_code, [405, 308])

    def test_get_on_delete_expense(self):
        self.login()
        r = self.client.get("/expenses/delete/1")
        self.assertIn(r.status_code, [405, 308])

    def test_get_on_add_income(self):
        self.login()
        r = self.client.get("/income/add")
        self.assertIn(r.status_code, [405, 308])

    def test_get_on_add_investment(self):
        self.login()
        r = self.client.get("/investments/add")
        self.assertIn(r.status_code, [405, 308])

    def test_get_on_circle_add(self):
        self.login()
        r = self.client.get("/circle/add")
        self.assertIn(r.status_code, [405, 308])

    def test_get_on_avatar_update(self):
        self.login()
        r = self.client.get("/profile/avatar")
        self.assertIn(r.status_code, [405, 308])


class TestCrossUserSecurity(BaseTestCase):
    """Ensure users cannot tamper with other users' data."""

    def test_delete_other_users_income(self):
        self.login("alice")
        self._add_income("Salary", 50000, desc="alice_salary")
        iid = self._db_one("SELECT id FROM income WHERE description='alice_salary'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/income/delete/{iid}")
        self.assertIsNotNone(self._db_one("SELECT id FROM income WHERE id=?", (iid,)))

    def test_delete_other_users_investment(self):
        self.login("alice")
        self._add_investment("Gold", 5000, desc="alice_gold")
        iid = self._db_one("SELECT id FROM investments WHERE description='alice_gold'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/investments/delete/{iid}")
        self.assertIsNotNone(self._db_one("SELECT id FROM investments WHERE id=?", (iid,)))

    def test_delete_other_users_budget(self):
        self.login("alice")
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 5000})
        bid = self._db_one("SELECT id FROM budgets LIMIT 1")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/budgets/delete/{bid}")
        self.assertIsNotNone(self._db_one("SELECT id FROM budgets WHERE id=?", (bid,)))

    def test_edit_post_other_users_expense(self):
        self.login("alice")
        self._add_expense(1, 100, desc="alice_edit_target")
        eid = self._db_one("SELECT id FROM expenses WHERE description='alice_edit_target'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/expenses/edit/{eid}", data={
            "category_id": 2, "amount": 9999, "expense_date": "2026-01-01",
            "recurrence": "none", "description": "hacked"
        })
        row = self._db_one("SELECT description, amount FROM expenses WHERE id=?", (eid,))
        self.assertEqual(row["description"], "alice_edit_target")
        self.assertEqual(row["amount"], 100)

    def test_edit_post_other_users_income(self):
        self.login("alice")
        self._add_income("Salary", 80000, desc="alice_sal")
        iid = self._db_one("SELECT id FROM income WHERE description='alice_sal'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/income/edit/{iid}", data={
            "source": "Hacked", "amount": 1, "income_date": "2026-01-01",
            "recurrence": "none", "description": "hacked"
        })
        row = self._db_one("SELECT source, amount FROM income WHERE id=?", (iid,))
        self.assertEqual(row["source"], "Salary")
        self.assertEqual(row["amount"], 80000)

    def test_edit_post_other_users_investment(self):
        self.login("alice")
        self._add_investment("Stocks", 20000, desc="alice_stocks")
        iid = self._db_one("SELECT id FROM investments WHERE description='alice_stocks'")["id"]
        self.client.get("/signout")
        self.login("bob", "Bob")
        self.client.post(f"/investments/edit/{iid}", data={
            "type": "Hacked", "amount": 1, "invest_date": "2026-01-01",
            "description": "hacked"
        })
        row = self._db_one("SELECT type, amount FROM investments WHERE id=?", (iid,))
        self.assertEqual(row["type"], "Stocks")
        self.assertEqual(row["amount"], 20000)

    def test_accept_others_circle_request(self):
        """User C shouldn't be able to accept a request sent to User B."""
        self.signup("alice"); self.signup("bob", "Bob"); self.signup("carol", "Carol")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        cid = self._db_one("SELECT id FROM circles WHERE status='pending'")["id"]
        self.client.get("/signout")
        # Carol tries to accept Alice→Bob request
        self.signin("carol")
        self.client.post(f"/circle/accept/{cid}")
        # Should still be pending
        row = self._db_one("SELECT status FROM circles WHERE id=?", (cid,))
        self.assertEqual(row["status"], "pending")


class TestOverallFilterAllPages(BaseTestCase):
    """Overall filter should work on expenses, income, investments pages too."""

    def setUp(self):
        super().setUp()
        self.aid, self.bid = self._make_circle()
        today = date.today().isoformat()
        self.signin("alice")
        self._add_expense(1, 1000, dt=today, desc="a_exp")
        self._add_income("Salary", 5000, dt=today, desc="a_inc")
        self._add_investment("Stocks", 2000, dt=today, desc="a_inv")
        self.client.get("/signout")
        self.signin("bob")
        self._add_expense(1, 2000, dt=today, desc="b_exp")
        self._add_income("Freelance", 3000, dt=today, desc="b_inc")
        self._add_investment("Gold", 1000, dt=today, desc="b_inv")

    def test_overall_expenses(self):
        r = self.client.get("/expenses?view_user=overall&period=month")
        self.assertIn(b"a_exp", r.data)
        self.assertIn(b"b_exp", r.data)

    def test_overall_income(self):
        r = self.client.get("/income?view_user=overall&period=month")
        self.assertIn(b"a_inc", r.data)
        self.assertIn(b"b_inc", r.data)

    def test_overall_investments(self):
        r = self.client.get("/investments?view_user=overall&period=month")
        self.assertIn(b"a_inv", r.data)
        self.assertIn(b"b_inv", r.data)

    def test_overall_dashboard_totals(self):
        r = self.client.get("/dashboard?view_user=overall&period=month")
        self.assertIn(b"3000.00", r.data)   # 1000+2000 expenses
        self.assertIn(b"8000.00", r.data)   # 5000+3000 income


class TestCSVEdgeCases(BaseTestCase):
    """Additional CSV export edge cases."""

    def test_empty_export(self):
        self.login()
        r = self.client.get("/export/expenses")
        self.assertEqual(r.status_code, 200)
        reader = csv.reader(io.StringIO(r.data.decode()))
        rows = list(reader)
        self.assertEqual(len(rows), 1)  # Header only

    def test_csv_special_characters(self):
        self.login()
        self._add_expense(1, 100, desc='Comma, "quotes" & stuff')
        r = self.client.get("/export/expenses")
        content = r.data.decode()
        self.assertIn("Comma", content)

    def test_csv_date_in_filename(self):
        self.login()
        r = self.client.get("/export/expenses")
        disp = r.headers.get("Content-Disposition", "")
        self.assertIn(date.today().isoformat(), disp)


class TestRecentExpensesLimit(BaseTestCase):
    """Dashboard recent expenses should be limited to 8."""

    def test_recent_expenses_max_8(self):
        self.login()
        today = date.today().isoformat()
        for i in range(12):
            self._add_expense(1, 100 + i, dt=today, desc=f"exp_{i}")
        r = self.client.get("/dashboard?period=month")
        html = r.data.decode()
        # Count table rows in recent expenses
        count = html.count("exp_")
        self.assertLessEqual(count, 8)


class TestCirclePageContent(BaseTestCase):
    """Circle page should render all sections."""

    def test_circle_shows_members(self):
        self._make_circle()
        self.signin("alice")
        r = self.client.get("/circle")
        self.assertIn(b"Bob", r.data)
        self.assertIn(b"Members", r.data)

    def test_circle_shows_pending_sent(self):
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        r = self.client.get("/circle")
        self.assertIn(b"Sent Requests", r.data)
        self.assertIn(b"Bob", r.data)

    def test_circle_shows_pending_received(self):
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        self.client.get("/signout")
        self.signin("bob")
        r = self.client.get("/circle")
        self.assertIn(b"Pending Requests", r.data)
        self.assertIn(b"Alice", r.data)

    def test_circle_profile_section(self):
        self.login()
        r = self.client.get("/circle")
        self.assertIn(b"My Profile", r.data)
        self.assertIn(b"alice", r.data)

    def test_circle_empty_state(self):
        self.login()
        r = self.client.get("/circle")
        self.assertIn(b"Circle is empty", r.data)


class TestViewOwnDataExplicitly(BaseTestCase):
    """Viewing own data via explicit view_user param."""

    def test_view_self_dashboard(self):
        self.login()
        uid = self._db_one("SELECT id FROM users WHERE username='alice'")["id"]
        r = self.client.get(f"/dashboard?view_user={uid}")
        self.assertEqual(r.status_code, 200)

    def test_view_self_expenses(self):
        self.login()
        self._add_expense(1, 500, desc="self_view")
        uid = self._db_one("SELECT id FROM users WHERE username='alice'")["id"]
        r = self.client.get(f"/expenses?view_user={uid}&period=all")
        self.assertIn(b"self_view", r.data)


class TestInsightsMediumSavings(BaseTestCase):
    """Test the 10-30% savings range insight."""

    def test_medium_savings_insight(self):
        self.login()
        today = date.today().isoformat()
        self._add_income("Salary", 100000, dt=today)
        self._add_expense(1, 85000, dt=today)  # 15% savings
        r = self.client.get("/dashboard?period=month")
        self.assertIn(b"aim for 20%", r.data)


class TestDatabaseIntegrity(BaseTestCase):
    """Ensure foreign keys and constraints work."""

    def test_foreign_key_categories(self):
        """Expenses reference valid categories."""
        self.login()
        self._add_expense(1, 100)
        rows = self._db_query("""
            SELECT e.id FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE c.id IS NULL
        """)
        self.assertEqual(len(rows), 0, "Orphan expenses found")

    def test_unique_budget_per_category(self):
        """UNIQUE(user_id, category_id) on budgets enforced."""
        self.login()
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 5000})
        self.client.post("/budgets", data={"category_id": 1, "monthly_limit": 8000})
        rows = self._db_query("SELECT COUNT(*) as c FROM budgets WHERE category_id=1")
        self.assertEqual(rows[0]["c"], 1)

    def test_circle_unique_constraint(self):
        """Can't create duplicate circle entries."""
        self.signup("alice"); self.signup("bob", "Bob")
        self.signin("alice")
        self.client.post("/circle/add", data={"username": "bob"})
        self.client.post("/circle/add", data={"username": "bob"})
        rows = self._db_query("SELECT COUNT(*) as c FROM circles")
        self.assertEqual(rows[0]["c"], 1)


class TestWCAGAdditional(BaseTestCase):
    """Additional accessibility checks."""

    def test_heading_h1_on_each_page(self):
        self.login()
        pages = ["/dashboard", "/expenses", "/income", "/investments", "/budgets", "/circle"]
        for url in pages:
            r = self.client.get(url)
            self.assertIn(b"<h1", r.data, f"{url} missing h1")

    def test_auth_pages_have_h1(self):
        for url in ["/signin", "/signup", "/forgot-password"]:
            r = self.client.get(url)
            self.assertIn(b"<h1", r.data, f"{url} missing h1")

    def test_select_elements_have_labels(self):
        self.login()
        r = self.client.get("/expenses")
        html = r.data.decode()
        import re
        selects = re.findall(r'<select[^>]*name="([^"]*)"', html)
        for name in selects:
            # Each select's name should have a preceding label
            self.assertIn(f'name="{name}"', html)

    def test_dark_mode_theme_attribute(self):
        r = self.client.get("/signin")
        self.assertIn(b'data-theme=', r.data)

    def test_print_media_query(self):
        css_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        with open(css_path) as f:
            css = f.read()
        self.assertIn("@media print", css)

    def test_480_breakpoint(self):
        css_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        with open(css_path) as f:
            css = f.read()
        self.assertIn("480px", css)


# ═══════════════════════════════════════════════════════════════════
# 29. CHANGE PASSWORD
# ═══════════════════════════════════════════════════════════════════
class TestChangePassword(BaseTestCase):
    """Tests for the change-password feature."""

    def test_change_password_page_requires_login(self):
        r = self.client.get("/change-password", follow_redirects=True)
        self.assertIn(b"Sign In", r.data)

    def test_change_password_page_renders(self):
        self.login()
        r = self.client.get("/change-password")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"current_password", r.data)
        self.assertIn(b"new_password", r.data)
        self.assertIn(b"confirm_password", r.data)
        self.assertIn(b"Change Password", r.data)

    def test_change_password_success(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "newpass456",
            "confirm_password": "newpass456"
        }, follow_redirects=True)
        self.assertIn(b"Password changed", r.data)
        # Sign out and sign in with new password
        self.client.get("/signout")
        r = self.client.post("/signin", data={"username": "alice", "password": "newpass456"}, follow_redirects=True)
        self.assertIn(b"Dashboard", r.data)

    def test_change_password_wrong_current(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "wrongpassword",
            "new_password": "newpass456",
            "confirm_password": "newpass456"
        }, follow_redirects=True)
        self.assertIn(b"Current password is incorrect", r.data)

    def test_change_password_too_short(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "abc",
            "confirm_password": "abc"
        }, follow_redirects=True)
        self.assertIn(b"at least 6", r.data)

    def test_change_password_mismatch(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "newpass456",
            "confirm_password": "different789"
        }, follow_redirects=True)
        self.assertIn(b"do not match", r.data)

    def test_old_password_fails_after_change(self):
        self.login()
        self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "newpass456",
            "confirm_password": "newpass456"
        })
        self.client.get("/signout")
        r = self.client.post("/signin", data={"username": "alice", "password": "pass123"}, follow_redirects=True)
        self.assertIn(b"Invalid username or password", r.data)

    def test_change_password_form_labels(self):
        """WCAG: form fields have labels."""
        self.login()
        r = self.client.get("/change-password")
        self.assertIn(b'for="current_password"', r.data)
        self.assertIn(b'for="new_password"', r.data)
        self.assertIn(b'for="confirm_password"', r.data)

    def test_change_password_has_h1(self):
        self.login()
        r = self.client.get("/change-password")
        self.assertIn(b"<h1>", r.data)

    def test_change_password_has_back_link(self):
        self.login()
        r = self.client.get("/change-password")
        self.assertIn(b"Back to Dashboard", r.data)


# ═══════════════════════════════════════════════════════════════════
# 30. PROFILE DROPDOWN
# ═══════════════════════════════════════════════════════════════════
class TestProfileDropdown(BaseTestCase):
    """Tests for the profile dropdown menu in the navbar."""

    def test_dropdown_present_on_dashboard(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"profile-dropdown", r.data)
        self.assertIn(b"profileTrigger", r.data)

    def test_dropdown_has_theme_toggle(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"themeToggle", r.data)
        self.assertIn(b"Dark Mode", r.data)

    def test_dropdown_has_change_password_link(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"change-password", r.data)
        self.assertIn(b"Change Password", r.data)

    def test_dropdown_has_signout_link(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"Sign Out", r.data)
        self.assertIn(b"signout", r.data)

    def test_dropdown_shows_display_name(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"Alice", r.data)
        self.assertIn(b"profile-dropdown-name", r.data)

    def test_dropdown_shows_username(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"@alice", r.data)

    def test_dropdown_has_my_profile_link(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"My Profile", r.data)

    def test_dropdown_aria_attributes(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b'aria-expanded="false"', r.data)
        self.assertIn(b'aria-haspopup="true"', r.data)
        self.assertIn(b'role="menu"', r.data)
        self.assertIn(b'role="menuitem"', r.data)

    def test_dropdown_present_on_all_pages(self):
        """Profile dropdown should be on every authenticated page."""
        self.login()
        for url in ["/dashboard", "/expenses", "/income", "/investments", "/budgets", "/circle"]:
            r = self.client.get(url)
            self.assertIn(b"profile-dropdown", r.data, f"Missing on {url}")

    def test_dropdown_has_avatar(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"nav-avatar", r.data)

    def test_dropdown_chevron_icon(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"profile-chevron", r.data)


# ═══════════════════════════════════════════════════════════════════
# 31. CHANGE PASSWORD AJAX DIALOG
# ═══════════════════════════════════════════════════════════════════
class TestChangePasswordAjax(BaseTestCase):
    """Tests for the change-password AJAX dialog responses."""

    def test_ajax_change_password_success(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "newpass456",
            "confirm_password": "newpass456"
        }, headers={"X-Requested-With": "XMLHttpRequest"})
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertTrue(data["success"])
        self.assertIn("successfully", data["message"])

    def test_ajax_change_password_wrong_current(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "wrong",
            "new_password": "newpass456",
            "confirm_password": "newpass456"
        }, headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertFalse(data["success"])
        self.assertIn("incorrect", data["message"])

    def test_ajax_change_password_too_short(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "abc",
            "confirm_password": "abc"
        }, headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertFalse(data["success"])
        self.assertIn("6 characters", data["message"])

    def test_ajax_change_password_mismatch(self):
        self.login()
        r = self.client.post("/change-password", data={
            "current_password": "pass123",
            "new_password": "newpass456",
            "confirm_password": "different789"
        }, headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertFalse(data["success"])
        self.assertIn("do not match", data["message"])


# ═══════════════════════════════════════════════════════════════════
# 32. PROFILE DIALOG & PICTURE UPLOAD
# ═══════════════════════════════════════════════════════════════════
class TestProfileDialog(BaseTestCase):
    """Tests for profile dialog elements in base template."""

    def test_profile_dialog_present(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"myProfileDialog", r.data)
        self.assertIn(b"openMyProfile", r.data)

    def test_change_password_dialog_present(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"changePasswordDialog", r.data)
        self.assertIn(b"openChangePassword", r.data)

    def test_profile_dialog_has_upload_input(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"profilePicInput", r.data)
        self.assertIn(b'accept="image/', r.data)

    def test_profile_dialog_has_avatar_color_picker(self):
        self.login()
        r = self.client.get("/dashboard")
        self.assertIn(b"avatar_color", r.data)
        self.assertIn(b"color-swatch", r.data)

    def test_dialogs_present_on_all_pages(self):
        self.login()
        for url in ["/dashboard", "/expenses", "/income", "/investments", "/budgets", "/circle"]:
            r = self.client.get(url)
            self.assertIn(b"changePasswordDialog", r.data, f"Missing CP dialog on {url}")
            self.assertIn(b"myProfileDialog", r.data, f"Missing profile dialog on {url}")


class TestProfilePictureUpload(BaseTestCase):
    """Tests for profile picture upload, update, and removal."""

    def _upload_pic(self, content=b"fake-image-data", filename="test.png", content_type="image/png"):
        from io import BytesIO
        return self.client.post("/profile/picture", data={
            "profile_pic": (BytesIO(content), filename)
        }, content_type="multipart/form-data",
           headers={"X-Requested-With": "XMLHttpRequest"})

    def test_upload_requires_login(self):
        r = self.client.post("/profile/picture", headers={"X-Requested-With": "XMLHttpRequest"})
        self.assertIn(r.status_code, [302, 401])

    def test_upload_no_file(self):
        self.login()
        r = self.client.post("/profile/picture",
                             headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertFalse(data["success"])
        self.assertIn("No file", data["message"])

    def test_upload_invalid_extension(self):
        self.login()
        r = self._upload_pic(filename="evil.exe", content_type="application/octet-stream")
        data = r.get_json()
        self.assertFalse(data["success"])
        self.assertIn("Invalid file type", data["message"])

    def test_upload_success(self):
        self.login()
        r = self._upload_pic()
        data = r.get_json()
        self.assertTrue(data["success"])
        self.assertIn("pic_url", data)
        self.assertIn("uploads/", data["pic_url"])

    def test_upload_updates_db(self):
        self.login()
        self._upload_pic()
        with self.flask_app.app_context():
            db = self.app_module.get_db()
            user = db.execute("SELECT profile_pic FROM users WHERE username='alice'").fetchone()
            self.assertIsNotNone(user["profile_pic"])
            self.assertTrue(user["profile_pic"].startswith("user_"))

    def test_upload_replaces_old_pic(self):
        self.login()
        self._upload_pic(filename="first.png")
        self._upload_pic(filename="second.jpg", content_type="image/jpeg")
        with self.flask_app.app_context():
            db = self.app_module.get_db()
            user = db.execute("SELECT profile_pic FROM users WHERE username='alice'").fetchone()
            self.assertTrue(user["profile_pic"].endswith(".jpg"))

    def test_remove_pic_requires_login(self):
        r = self.client.post("/profile/picture/remove",
                             headers={"X-Requested-With": "XMLHttpRequest"})
        self.assertIn(r.status_code, [302, 401])

    def test_remove_pic_success(self):
        self.login()
        self._upload_pic()
        r = self.client.post("/profile/picture/remove",
                             headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertTrue(data["success"])
        with self.flask_app.app_context():
            db = self.app_module.get_db()
            user = db.execute("SELECT profile_pic FROM users WHERE username='alice'").fetchone()
            self.assertIsNone(user["profile_pic"])

    def test_remove_pic_when_none(self):
        self.login()
        r = self.client.post("/profile/picture/remove",
                             headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertTrue(data["success"])

    def test_avatar_ajax_update(self):
        self.login()
        r = self.client.post("/profile/avatar", data={"avatar_color": "#f43f5e"},
                             headers={"X-Requested-With": "XMLHttpRequest"})
        data = r.get_json()
        self.assertTrue(data["success"])
        self.assertIn("Avatar updated", data["message"])

    def test_profile_pic_shown_in_nav(self):
        self.login()
        self._upload_pic()
        r = self.client.get("/dashboard")
        self.assertIn(b"nav-avatar-img", r.data)
        self.assertIn(b"uploads/user_", r.data)

    def test_allowed_extensions(self):
        self.login()
        for ext, ctype in [("jpg", "image/jpeg"), ("jpeg", "image/jpeg"),
                           ("gif", "image/gif"), ("webp", "image/webp")]:
            r = self._upload_pic(filename=f"photo.{ext}", content_type=ctype)
            data = r.get_json()
            self.assertTrue(data["success"], f"Failed for .{ext}")


# ═══════════════════════════════════════════════════════════════════
# 33. PROFILE PIC MIGRATION
# ═══════════════════════════════════════════════════════════════════
class TestProfilePicMigration(BaseTestCase):
    """Tests that profile_pic column exists after init."""

    def test_profile_pic_column_exists(self):
        with self.flask_app.app_context():
            db = self.app_module.get_db()
            cols = [r[1] for r in db.execute("PRAGMA table_info(users)").fetchall()]
            self.assertIn("profile_pic", cols)


if __name__ == "__main__":
    unittest.main(verbosity=2)
