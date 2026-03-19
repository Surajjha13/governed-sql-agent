import sys
import os
import json
import logging
import datetime

# Add project root to path
sys.path.append(os.getcwd())

from app.query_service.api import run_query, QueryRequest
from app.schema_service.models import SchemaResponse, MetricDefinition
import app.app_state as app_state

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASET_PATH = "tests/dataset.json"
RESULTS_PATH = "tests/eval_results.json"

def setup_mock_state():
    # Load metrics from config for realistic testing
    metrics_config_path = "app/schema_service/metrics_config.json"
    metrics = []
    if os.path.exists(metrics_config_path):
        with open(metrics_config_path, 'r') as f:
            data = json.load(f)
            metrics = [MetricDefinition(**m) for m in data]

    app_state.normalized_schema = SchemaResponse(
        engine="postgres",
        database="northwind",
        tables=[],
        metrics=metrics
    )
    app_state.vector_index = None
    app_state.vector_metadata = []
    app_state.chat_history = []
    app_state.current_connection = {"connected": False}

def calculate_score(generated_sql, keywords):
    if not generated_sql: return 0
    sql_upper = generated_sql.upper()
    matches = [k for k in keywords if k.upper() in sql_upper]
    return len(matches) / len(keywords) if keywords else 1.0

def run_eval():
    print("\n" + "="*50)
    print("RUNNING SQL AGENT v2 BENCHMARK")
    print("="*50 + "\n")
    
    setup_mock_state()
    
    if not os.path.exists(DATASET_PATH):
        print(f"Error: Dataset not found at {DATASET_PATH}")
        return

    with open(DATASET_PATH, 'r') as f:
        dataset = json.load(f)

    results = []
    total_score = 0

    for test in dataset:
        print(f"Question: {test['question']}")
        
        try:
            req = QueryRequest(question=test['question'])
            resp = run_query(req)
            
            sql = resp.get("sql")
            score = calculate_score(sql, test.get("keywords", []))
            
            print(f"Generated SQL: {sql}")
            print(f"Score: {score:.2f}")
            
            results.append({
                "question": test["question"],
                "sql": sql,
                "score": score,
                "timestamp": datetime.datetime.now().isoformat()
            })
            total_score += score
            
        except Exception as e:
            print(f"Error: {e}")
            results.append({
                "question": test["question"],
                "error": str(e),
                "score": 0
            })
        print("-" * 30)

    avg_score = total_score / len(dataset) if dataset else 0
    print(f"\nFINAL BENCHMARK SCORE: {avg_score:.2f}")

    # Persist results
    with open(RESULTS_PATH, 'w') as f:
        json.dump({
            "summary": {
                "avg_score": avg_score,
                "total_cases": len(dataset),
                "date": datetime.datetime.now().isoformat()
            },
            "details": results
        }, f, indent=2)
    print(f"Results saved to {RESULTS_PATH}")

if __name__ == "__main__":
    run_eval()
