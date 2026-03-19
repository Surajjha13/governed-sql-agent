# SQL Agent: Natural Language to SQL Interface

![SQL Agent Architecture](https://img.shields.io/badge/Architecture-FastAPI%20%7C%20Vanilla%20JS%20%7C%20Multi--LLM-blue)
![Security](https://img.shields.io/badge/Security-SQLGlot%20%7C%20RBAC-green)

SQL Agent is an intelligent, AI-powered tool designed to bridge the gap between human language and relational databases. It allows users to query databases (Postgres, SQL Server) using plain English, automatically translating questions into valid SQL, executing them, and providing natural language insights along with dynamic visualizations.

---

## 🚀 Key Features

- **Text-to-SQL Conversion:** Converts complex natural language questions into optimized SQL queries.
- **Multi-LLM Integration:** Supports multiple providers including **Groq (Llama-3)**, **OpenAI (GPT-4o)**, **Google Gemini**, **Anthropic**, and **DeepSeek**.
- **Self-Correcting SQL:** Automatically detects and repairs common SQL syntax errors or schema mismatches via an internal "SQL Repair" loop.
- **Dynamic Visualization:** Suggests and renders appropriate charts (Bar, Line, Pie, Area) using **Chart.js**.
- **Enterprise-Grade Security (RBAC):** Implements Role-Based Access Control to filter database schemas, ensuring users only see data they are authorized to access.
- **SQL Injection Prevention:** Uses **AST (Abstract Syntax Tree)** parsing via `sqlglot` to block destructive commands (`DROP`, `DELETE`, `UPDATE`) and prevent `SELECT *`.
- **Semantic Search:** Uses **FAISS** and `sentence-transformers` for intelligent schema discovery in large databases.

---

## 🛠️ Technology Stack

- **Backend:** [FastAPI](https://fastapi.tiangolo.com/) (Python)
- **Frontend:** HTML5, CSS3, Vanilla JavaScript (Refined UI)
- **Database Support:** PostgreSQL (`psycopg2`), SQL Server (`pyodbc`)
- **LLM Processing:** Groq, OpenAI, Google AI, Anthropic APIs
- **Core Libraries:**
  - `sqlglot`: SQL Parsing and Dialect Translation
  - `pandas`: Data manipulation and reporting
  - `FAISS` & `sentence-transformers`: Vector search for schema metadata
  - `fpdf2` & `xlsxwriter`: PDF and Excel report generation

---

## 🏗️ Technical Architecture

```mermaid
graph TD
    User((User)) -->|Question| UI[Frontend UI]
    UI -->|API Request| FastAPI Backend
    
    subgraph "Backend Processing"
        FastAPI[FastAPI Backend] --> RBAC[RBAC Schema Filter]
        RBAC --> Intent[Intent Analysis]
        Intent --> Prompt[Prompt Builder]
        Prompt --> LLM[LLM Generator]
        LLM -->|SQL Query| Validator[SQLGlot Validator]
        Validator -->|Safe Query| Exec[Query Executor]
    end
    
    Exec -->|SQL| DB[(Postgres/SQL Server)]
    DB -->|Results| Exec
    Exec -->|Data| UI
    UI -->|Charts/Summary| User
```

1. **User Query:** The user submits a question in the UI.
2. **Schema Introspection:** The backend retrieves the database schema (filtered by RBAC).
3. **Intent Analysis:** A small LLM call identifies the type of analysis (Trend, Comparison, etc.) to suggest visualizations.
4. **Prompt Construction:** A structured prompt is built, including schema, history, and results from semantic search.
5. **SQL Generation:** The LLM generates the SQL query.
6. **Validation & Optimization:** `sqlglot` validates the query against security rules.
7. **Execution:** The query is executed on the target database via Pandas.
8. **Insights & Visuals:** The system generates a natural language summary and suggests charts.

---

## ⚙️ Installation & Setup

### 1. Prerequisites
- Python 3.9+
- A valid API Key for Groq or OpenAI.

### 2. Setup
```bash
# Clone the repository
git clone <your-repo-link>
cd SQL_Agent

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the root directory:
```env
GROQ_API_KEY=your_groq_key
OPENAI_API_KEY=your_openai_key
# Optional
GEMINI_API_KEY=your_gemini_key
ANTHROPIC_API_KEY=your_anthropic_key
```

### 4. Running the Application
```bash
python main.py
```
Then, open `frontend/landing.html` in your web browser.

---

## 🛡️ Security Design

SQL Agent is designed with a "Security-First" approach:
- **Restricted Operations:** Only `SELECT` statements are permitted.
- **AST Validation:** Every query is parsed before execution. If a non-select or forbidden pattern (like `SELECT *`) is detected, the system rejects it or asks for repair.
- **RBAC Schema Masking:** The LLM only receives metadata for tables and columns authorized for the current user's role.

---

## 🔮 Future Enhancements
- **Agentic Multi-Step Reasoning:** Supporting multi-turn data analysis.
- **Advanced Caching:** Redis-based result caching for performance.
- **Deeper Integration:** Direct Slack/Teams integration for query alerts.
