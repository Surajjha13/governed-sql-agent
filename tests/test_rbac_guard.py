from app.query_service.rbac_guard import validate_sql_against_rbac


def test_rbac_guard_parses_mysql_identifier_quoting():
    denial = validate_sql_against_rbac(
        "SELECT `orders`.`secret_note` FROM `orders`",
        {"blocked_columns": ["orders.secret_note"]},
        engine="mysql",
    )

    assert denial == "Access denied: administrator restricted column 'orders.secret_note'."
