from app.query_service.context_builder import build_context
from app.schema_service.models import SchemaResponse, TableMeta, ColumnMeta

def test_name_prioritization_context():
    print("--- Testing Name Prioritization in Context Building ---")
    
    # Setup mock schema where CategoryName is NOT initially scored high (no 'category' in question)
    table_categories = TableMeta(
        schema_name="public",
        table="Categories",
        columns=[
            ColumnMeta(name="CategoryID", data_type="integer", is_primary_key=True, nullable=False),
            ColumnMeta(name="CategoryName", data_type="text", is_primary_key=False, nullable=False),
            ColumnMeta(name="Description", data_type="text", is_primary_key=False, nullable=True)
        ]
    )
    table_sales = TableMeta(
        schema_name="public",
        table="Sales",
        columns=[
            ColumnMeta(name="SalesID", data_type="integer", is_primary_key=True, nullable=False),
            ColumnMeta(name="Quantity", data_type="integer", semantic_type="metric", is_primary_key=False, nullable=False),
            ColumnMeta(name="CategoryID", data_type="integer", foreign_key="public.Categories.CategoryID", is_primary_key=False, nullable=False)
        ]
    )
    schema = SchemaResponse(engine="postgres", database="test_db", tables=[table_categories, table_sales])

    # Case 1: Question doesn't mention "name" but mentions the entity
    question = "show top 5 categories by sales quantity"
    print(f"Question: {question}")
    
    context = build_context(question, schema)
    
    print(f"Tables selected: {context['tables']}")
    print(f"Columns for Categories: {context['columns'].get('Categories', [])}")
    
    assert "Categories" in context["tables"]
    assert "CategoryName" in context["columns"]["Categories"], "CategoryName should be included even if not explicitly asked for"
    print("[SUCCESS] CategoryName included in context!")

    # Case 2: Multi-table context
    question = "which category has the most sales"
    context = build_context(question, schema)
    assert "CategoryName" in context["columns"]["Categories"]
    print("[SUCCESS] CategoryName included in multi-table context!")

if __name__ == "__main__":
    test_name_prioritization_context()
