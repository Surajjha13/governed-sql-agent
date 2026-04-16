# SQL Agent

SQL Agent is a FastAPI application with a lightweight web frontend that turns natural-language questions into safe, read-only SQL queries. It supports schema discovery, LLM-based SQL generation, query execution, result summarization, and a browser UI for both solo and enterprise-style workflows.

This README is tailored to the current repository and a local Windows PowerShell setup.

## What This Project Does

- Connects to PostgreSQL and MySQL databases
- Inspects schema metadata and builds semantic context
- Uses an LLM provider to generate SQL from plain-English questions
- Applies validation and guardrails before execution
- Returns table results, summaries, and chart-friendly output
- Includes authentication, admin flows, and RBAC-oriented enterprise features

## Tech Stack

- Backend: FastAPI, Uvicorn
- Frontend: static HTML, CSS, JavaScript
- Data access: SQLAlchemy, asyncpg, psycopg2, pymysql
- LLM providers: Groq, OpenAI, Gemini
- Semantic search: sentence-transformers, FAISS, Torch

## Current Project Layout

- `main.py`: app startup, CORS, health route, frontend routing
- `app/auth`: authentication, JWT handling, bootstrap admin setup, RBAC
- `app/connection_service`: database connection lifecycle
- `app/schema_service`: schema discovery and normalization
- `app/query_service`: prompt building, validation, execution, formatting
- `app/llm_service`: provider adapters, SQL generation, retries, summaries
- `app/semantic_service`: embedding model loading and vector search
- `frontend`: landing page, workspace UI, login page, admin page
- `tests`: automated test coverage
- `download_models.py`: optional model pre-download helper

## Requirements

Before starting, make sure you have:

- Python 3.10 or newer
- `pip`
- PowerShell
- Access to a PostgreSQL or MySQL database
- At least one LLM API key:
  - `GROQ_API_KEY`
  - `OPENAI_API_KEY`
  - `GEMINI_API_KEY`

## Local Setup on Windows

### 1. Create and activate a virtual environment

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, run this once in a PowerShell window:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### 2. Install dependencies

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Note: `requirements.txt` includes Torch, FAISS, and sentence-transformers, so the first install can take a while.

### 3. Create your environment file

```powershell
Copy-Item .env.example .env
```

Then edit `.env` with your real values.

## Environment Variables

The app currently expects these important values:

- `MASTER_KEY`: required for token signing and encrypted secret handling
- `BOOTSTRAP_ADMIN_PASSWORD`: required on first startup when no admin exists yet
- One provider key such as `GROQ_API_KEY`, `OPENAI_API_KEY`, or `GEMINI_API_KEY`
- Optional `ALLOWED_ORIGINS`: comma-separated CORS origins

Database values in `.env.example` are placeholders for your own PostgreSQL or MySQL server:

- `DB_ENGINE`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

Important: `.env.example` currently uses `GOOGLE_API_KEY`, but the runtime code reads `GEMINI_API_KEY`. Use `GEMINI_API_KEY` in your real `.env` file unless you also update the application code.

## Run the App Locally

Start the API from the repository root:

```powershell
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

Then open:

- `http://127.0.0.1:8000/` for the landing page
- `http://127.0.0.1:8000/workspace` for the main app
- `http://127.0.0.1:8000/login` for login
- `http://127.0.0.1:8000/admin` for the admin page

Health check:

- `http://127.0.0.1:8000/health`

## Optional: Pre-download Embedding Models

The app preloads the embedding model on startup. If you want to download it ahead of time:

```powershell
python .\download_models.py
```

This stores the model under a local `models/` folder.

## Docker Option

This repo includes a production-oriented container setup for the FastAPI app.

### 1. Create your environment file

```powershell
Copy-Item .env.example .env
```

Fill in at least:

- `MASTER_KEY`
- `BOOTSTRAP_ADMIN_PASSWORD`
- one of `GROQ_API_KEY`, `OPENAI_API_KEY`, or `GEMINI_API_KEY`

If your database is running on your host machine, keep:

- `DB_HOST=host.docker.internal`

### 2. Build and run the container

```powershell
docker compose up --build
```

The API is exposed on port `8000`.

The compose file persists downloaded embedding models and logs in named Docker volumes so the container does not need to re-download them every time.

### 3. Open the app

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/health`

### 4. Stop the stack

```powershell
docker compose down
```

## First-Time Startup Notes

- On first boot, the app uses `BOOTSTRAP_ADMIN_PASSWORD` to create the initial admin account if one does not already exist.
- The app starts a background task for idle connection cleanup.
- The app also starts loading the embedding model during startup, so the first launch may take longer than later runs.

## Supported Database Engines

- PostgreSQL
- MySQL

## Common Development Commands

Install dependencies:

```powershell
pip install -r requirements.txt
```

Run tests:

```powershell
pytest
```

Start the development server:

```powershell
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

## Known Repository Notes

- The README previously referenced a `docs/` directory, but that directory is not present in this workspace right now.
- `.env.example` and runtime config are not fully aligned for Gemini naming.
- The application appears to support enterprise and solo flows through the frontend routes, but startup is still a single FastAPI service.

## Troubleshooting

If the app does not start:

- Confirm your virtual environment is activated
- Confirm `.env` exists and contains `MASTER_KEY`
- Confirm you set `BOOTSTRAP_ADMIN_PASSWORD` for first boot
- Confirm at least one LLM API key is set
- Re-run `pip install -r requirements.txt` if Torch or FAISS dependencies failed during install

If PowerShell cannot activate the environment:

- Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

If model loading feels slow on first run:

- Run `python .\download_models.py` before starting the server
