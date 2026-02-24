# resource/params.py

def _import_pg_jdbc():
    try:
        from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient
    except Exception:
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient


def _build_client(config, database):
    try:
        PgJdbcConfig, PgJdbcClient = _import_pg_jdbc()
    except Exception:
        return None

    jar_path = config.get("jar_path")
    if not jar_path:
        return None

    host = config.get("host") or "localhost"
    port = int(config.get("port") or 5432)

    user = config.get("user")
    password = config.get("password")
    if not user or not password:
        return None

    if not database:
        return None

    cfg = PgJdbcConfig(
        jar_path=jar_path,
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    return PgJdbcClient(cfg)


def do(payload, config, plugin_config, inputs):
    param = (payload or {}).get("parameterName")

    # 1) Database dropdown
    if param == "database":
        admin_db = config.get("admin_db") or "postgres"
        client = _build_client(config, admin_db)
        if client is None:
            return {"choices": []}

        # pg_database에서 목록 뽑기 (템플릿/접속불가 제외)
        # pg-jdbc-lib에 "list_databases"가 있으면 그걸 쓰는 게 베스트인데,
        # 없다면 query 메서드가 있어야 함.
        # 여기서는 client.query(sql) 가 있다고 가정하고, 없으면 아래 Q2에서 맞춰줄게.
        try:
            rows = client.query("""
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                ORDER BY datname
            """)
            dbs = [r[0] for r in rows]
        except Exception:
            # 혹시 라이브러리 구현이 다르면 빈 리스트로
            dbs = []

        return {"choices": [{"value": d, "label": d} for d in dbs]}

    # 2) Schema dropdown (선택한 DB로 접속)
    if param == "schema":
        database = config.get("database")
        client = _build_client(config, database)
        if client is None:
            return {"choices": []}

        schemas = client.list_schemas()

        hidden_prefixes = ("pg_",)
        hidden_exact = {"information_schema"}
        schemas = [
            s for s in schemas
            if not s.startswith(hidden_prefixes) and s not in hidden_exact
        ]

        return {"choices": [{"value": s, "label": s} for s in schemas]}

    # 3) Table dropdown
    if param == "table":
        database = config.get("database")
        schema = config.get("schema")
        if not database or not schema:
            return {"choices": []}

        client = _build_client(config, database)
        if client is None:
            return {"choices": []}

        tables = client.list_tables(schema)
        return {"choices": [{"value": t, "label": t} for t in tables]}

    return {"choices": []}