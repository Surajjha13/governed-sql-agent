"""
Test script to demonstrate input validation for the /query endpoint.
"""
import pytest
import sys
from pydantic import ValidationError
from app.query_service.api import QueryRequest

# Valid cases
VALID_TEST_CASES = [
    ("Valid question - normal length", "What are the total sales for last month?"),
    ("Valid question - minimum length (3 chars)", "Why"),
    ("Valid question - with special characters", "Show me products where price > $100 and category = 'Electronics'"),
    ("Question with leading/trailing whitespace (should be trimmed)", "   What are the sales?   "),
    ("Question with legitimate SQL keywords", "How many customers have orders where total > 1000?"),
    # Regression tests for safe usage of keywords
    ("Valid question containing drop word", "Can I drop off the package here?"),
    ("Valid question containing update word", "Please update me on the status"),
    ("Valid question containing delete word", "I did not delete the file"),
]

@pytest.mark.parametrize("description,question", VALID_TEST_CASES)
def test_valid_questions(description, question):
    """Test that valid questions are accepted."""
    req = QueryRequest(question=question)
    # Verify whitespace was stripped if applicable
    assert req.question == question.strip()

# Invalid cases
INVALID_TEST_CASES = [
    ("Empty string", ""),
    ("Only whitespace", "   \t\n   "),
    # Length checks
    ("Too short - 2 characters", "Hi"),
    ("Too short - 1 character", "a"),
    ("Too long - over 1000 characters", "A" * 1001),
    # Dangerous SQL patterns (Original)
    ("SQL Injection - DROP TABLE with semicolon", "Show me users; DROP TABLE users;"),
    ("SQL Injection - DELETE with semicolon", "Get all data; DELETE FROM customers;"),
    ("SQL Injection - UPDATE with semicolon", "List products; UPDATE products SET price = 0;"),
    ("SQL Injection - SQL comment", "Show me data --"),
    ("SQL Injection - Block comment", "Get users /* malicious code */"),
    ("SQL Injection - xp_cmdshell", "EXEC xp_cmdshell 'dir'"),
    # Dangerous SQL patterns (New - covering the fix)
    ("SQL Injection - Standalone DROP", "drop table city"),
    ("SQL Injection - Standalone DELETE", "delete from customers"),
    ("SQL Injection - Standalone UPDATE", "update users set password='123'"),
    ("SQL Injection - Standalone INSERT", "insert into logs values(1)"),
    ("SQL Injection - Standalone TRUNCATE", "truncate table logs"),
    ("SQL Injection - Standalone ALTER", "alter table users drop column"),
    ("SQL Injection - Semicolon at start", "; DROP TABLE users"),
]

@pytest.mark.parametrize("description,question", INVALID_TEST_CASES)
def test_invalid_questions(description, question):
    """Test that invalid questions raise ValidationError."""
    with pytest.raises(ValidationError) as excinfo:
        QueryRequest(question=question)
    
if __name__ == "__main__":
    # Allow running as a script too
    sys.exit(pytest.main([__file__]))
