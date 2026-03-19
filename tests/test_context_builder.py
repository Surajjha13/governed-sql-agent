from app.query_service.context_builder import build_context
from app.schema_service.models import SchemaResponse, TableMeta, ColumnMeta


def test_context_builder_sales_by_country():
    schema = SchemaResponse(
        engine="postgres",
        database="test_db",
        tables=[
            TableMeta(
                schema="public",
                table="sales",
                columns=[
                    ColumnMeta(
                        name="SalesID",
                        data_type="bigint",
                        nullable=False,
                        is_primary_key=True,
                        foreign_key=None,
                        semantic_type="id"
                    ),
                    ColumnMeta(
                        name="CustomerID",
                        data_type="bigint",
                        nullable=True,
                        is_primary_key=False,
                        foreign_key="public.customers.CustomerID",
                        semantic_type="foreign_key"
                    ),
                    ColumnMeta(
                        name="TotalPrice",
                        data_type="double precision",
                        nullable=True,
                        is_primary_key=False,
                        foreign_key=None,
                        semantic_type="metric"
                    ),
                    ColumnMeta(
                        name="SalesDate",
                        data_type="text",
                        nullable=True,
                        is_primary_key=False,
                        foreign_key=None,
                        semantic_type="time"
                    ),
                ]
            ),
            TableMeta(
                schema="public",
                table="customers",
                columns=[
                    ColumnMeta(
                        name="CustomerID",
                        data_type="bigint",
                        nullable=False,
                        is_primary_key=True,
                        foreign_key=None,
                        semantic_type="id"
                    ),
                    ColumnMeta(
                        name="CityID",
                        data_type="bigint",
                        nullable=True,
                        is_primary_key=False,
                        foreign_key="public.cities.CityID",
                        semantic_type="foreign_key"
                    ),
                ]
            ),
        ]
    )

    context = build_context(
        question="total sales by customer",
        schema=schema
    )

    assert "sales" in context["tables"]
    assert "customers" in context["tables"]

    assert "TotalPrice" in context["columns"]["sales"]
    assert "SalesDate" in context["columns"]["sales"]

    assert any("CustomerID" in j for j in context["joins"])
