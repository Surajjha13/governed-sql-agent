import os
import json
import datetime
import logging

logger = logging.getLogger(__name__)

SOLO_AUDIT_LOG = "logs/solo_audit.log"
ENTERPRISE_AUDIT_LOG = "logs/audit.log"

def audit_query(username: str, role: str, question: str, sql: str, results: dict, summary: str):
    log_file = ENTERPRISE_AUDIT_LOG
    if role == "SOLO_VIRTUAL":
        log_file = SOLO_AUDIT_LOG
        
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "username": username,
        "role": role,
        "question": question,
        "sql": sql,
        "success": not bool(results.get("error")),
        "error": results.get("error"),
        "latency_sec": results.get("latency_sec"),
        "row_count": len(results.get("rows", []))
    }
    
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        logger.error(f"Failed to write to audit log: {e}")
