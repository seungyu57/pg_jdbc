def _import_pg_jdbc():
    try:
        from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient
    except Exception:
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient


FIXED_HOST = "localhost"
FIXED_PORT = 5432


def _build_client(config, database):
    try:
        PgJdbcConfig, PgJdbcClient = _import_pg_jdbc()
    except Exception:
        return None

    jar_path = config.get("jar_path")
    user = config.get("user")
    password = config.get("password")

    if not jar_path or not user or not password or not database:
        return None

    cfg = PgJdbcConfig(
        jar_path=jar_path,
        host=FIXED_HOST,
        port=FIXED_PORT,
        database=database,
        user=user,
        password=password
    )
    return PgJdbcClient(cfg)


def do(payload, config, plugin_config, inputs):
    param = (payload or {}).get("parameterName")

    # 1️⃣ Database 목록
    if param == "database":
        admin_db = config.get("admin_db") or "postgres"
        client = _build_client(config, admin_db)
        if client is None:
            return {"choices": []}

        try:
            rows = client.query("""
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                ORDER BY datname
            """)
            dbs = [r[0] for r in rows]
        except Exception:
            dbs = []

        return {"choices": [{"value": d, "label": d} for d in dbs]}

    # 2️⃣ Schema 목록
    if param == "schema":
        database = config.get("database")
        client = _build_client(config, database)
        if client is None:
            return {"choices": []}

        schemas = client.list_schemas()
        schemas = [s for s in schemas if not s.startswith("pg_") and s != "information_schema"]

        return {"choices": [{"value": s, "label": s} for s in schemas]}

    # 3️⃣ Table 목록
    if param == "table":
        database = config.get("database")
        schema = config.get("schema")

        client = _build_client(config, database)
        if client is None or not schema:
            return {"choices": []}

        tables = client.list_tables(schema)
        return {"choices": [{"value": t, "label": t} for t in tables]}

    return {"choices": []}