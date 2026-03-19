import sqlglot
from sqlglot import exp, parse_one

sqls = [
    "SELECT * FROM categories",
    "SELECT c.* FROM categories c",
    "SELECT id, * FROM categories",
    "SELECT id, name FROM categories"
]

for sql in sqls:
    parsed = parse_one(sql)
    stars = list(parsed.find_all(exp.Star))
    print(f"SQL: {sql}")
    print(f"Found Star: {len(stars) > 0}")
    for s in stars:
        print(f"  - {type(s)}")
    print("-" * 20)
