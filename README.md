# Expense Tracker

A full-stack expense tracker built with Flask & PostgreSQL — manage expenses, income, investments, budgets, and shared circles.

## Live Demo

🌐 [expense-tracker-uvel.onrender.com](https://expense-tracker-uvel.onrender.com)

## Features

- 📊 **Dashboard** — Overview with spending insights, charts, and budget alerts
- 💸 **Expenses** — Track spending by category with recurrence support
- 💰 **Income** — Log salary, freelance, and other income sources
- 📈 **Investments** — Monitor stocks, mutual funds, FD, crypto, and more
- 🎯 **Budgets** — Set monthly category limits with progress tracking
- 👥 **Circles** — Share and view finances with trusted friends/family
- 📤 **CSV Export** — Download all data for external analysis
- 🔒 **Security** — Password hashing (bcrypt), security questions, session auth

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Flask (Python) |
| Database | PostgreSQL (production) / SQLite (development) |
| Server | Gunicorn (4 workers) |
| Styling | Custom CSS with responsive design |

## Setup

### Local Development (SQLite)

```bash
pip install -r requirements.txt
python app.py
```

### Production (PostgreSQL)

Set the `DATABASE_URL` environment variable:

```bash
export DATABASE_URL=postgresql://user:password@host:5432/expense_tracker
gunicorn app:app --bind 0.0.0.0:8000 --workers 4 --threads 2
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | _(falls back to SQLite)_ |
| `SECRET_KEY` | Flask session secret | dev key |
| `PORT` | Server port | 5000 |

## Tests

```bash
pip install pytest
python -m pytest test_app.py -q
```

256 tests covering auth, expenses, income, investments, budgets, circles, CSV export, profile management, and AJAX dialogs.

## Deployment

Configured for Railway/Render with:
- `Procfile` — Gunicorn start command
- `railway.json` — Railway platform config
- `requirements.txt` — Python dependencies

Recommended free hosting: **Render** (app) + **Neon** (PostgreSQL database)
