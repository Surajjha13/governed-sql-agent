import re
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

def classify_intent(question: str) -> Dict[str, str]:
    """
    Classify the user's natural language question into a suggested SQL logical pattern.
    This provides GUIDANCE to the LLM prompt — it is NOT enforced as a hard gate.
    """
    q = question.lower()
    
    # 1. Global Absence — strict: requires "never rented", "no rentals ever", "zero orders"
    #    Do NOT trigger on simple "not" or "no" which are common filters
    if re.search(r'\b(never)\b', q) or re.search(r'\b(zero)\s+(orders?|rentals?|sales?|transactions?|purchases?|records?)\b', q):
        return {
            "pattern_name": "Global Absence",
            "instruction": "The user may be asking about universal absence. Consider using `NOT EXISTS`, `LEFT JOIN ... IS NULL`, or `HAVING COUNT(...) = 0` if appropriate.",
            "required_sql_keywords": []  # Non-enforced — just guidance
        }
    
    # 2. Universal Condition (Quantifiers like "every category has", "all students who passed each exam")
    #    Only trigger for true relational division, NOT simple "list all X" or "show all Y"
    if re.search(r'\bevery\b.*\b(has|have|had|contain|contains|include|includes)\b', q):
        return {
            "pattern_name": "Universal Condition",
            "instruction": "The user may be asking for a universal relationship ('every X has Y'). Consider using double NOT EXISTS or GROUP BY ... HAVING COUNT(DISTINCT x) = (SELECT COUNT(*) ...) if appropriate.",
            "required_sql_keywords": []  # Non-enforced
        }
        
    # 3. Existence (At least one)
    if re.search(r'\b(at least one|at least \d+)\b', q):
        return {
            "pattern_name": "Existence",
            "instruction": "The user is asking for existence. An INNER JOIN, EXISTS, or HAVING COUNT(...) >= N clause is appropriate.",
            "required_sql_keywords": []  # Non-enforced
        }
        
    # 4. Count Intent
    if re.search(r'\b(how many|count of|number of|total number)\b', q):
        return {
            "pattern_name": "Count Aggregation",
            "instruction": "The user is asking for a count. Use the COUNT() aggregation function.",
            "required_sql_keywords": []  # Non-enforced
        }
        
    # 5. Sum Intent
    if re.search(r'\b(total amounts?|sum of|how much)\b', q):
        return {
            "pattern_name": "Sum Aggregation",
            "instruction": "The user is asking for a total numeric sum. Use the SUM() aggregation function.",
            "required_sql_keywords": []  # Non-enforced
        }
        
    # 6. Top-K
    if re.search(r'\b(top \d+|highest|lowest|best|worst|most popular|least popular)\b', q):
        return {
            "pattern_name": "Top-K Ordering",
            "instruction": "The user is asking for ranked results. Include an ORDER BY clause to sort the results.",
            "required_sql_keywords": []  # Non-enforced
        }
        
    # 7. Distribution / Grouping
    if re.search(r'\b(distribution|broken down by|per category|per group|by each)\b', q):
        return {
            "pattern_name": "Distribution",
            "instruction": "The user requires a distribution of data. Aggregate and group the data using a GROUP BY clause.",
            "required_sql_keywords": []  # Non-enforced
        }
        
    # Default (Standard Selection/Aggregation)
    return {
        "pattern_name": "Standard Query",
        "instruction": "Analyze the request and build standard selection and join logic.",
        "required_sql_keywords": []
    }

def verify_intent_match(sql: str, intent_data: Dict[str, str]) -> Optional[str]:
    """
    Intent verification is now a NO-OP.
    
    The intent classifier provides guidance to the LLM prompt, but we do NOT 
    hard-block SQL that doesn't match the regex-classified intent. The LLM 
    understands the user's question better than regex patterns and should be 
    trusted to pick the right SQL constructs.
    
    Always returns None (no error).
    """
    return None
