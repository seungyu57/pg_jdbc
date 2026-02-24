# resource/params.py

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
    PgJdbcConfig, PgJdbcClient = _import_pg_jdbc()

    jar_path = config.get("jar_path")
    user = config.get("user")
    password = config.get("password")

    if not jar_path:
        raise Exception("jar_path is missing in dataset config (hidden param).")
    if not user or not password:
        return None
    if not database:
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


def _choices(items):
    return {"choices": [{"value": x, "label": x} for x in items]}


def do(payload, config, plugin_config, inputs):
    param = (payload or {}).get("parameterName")

    # 디버그: config가 뭐 들고 들어오는지 보고 싶으면 아래 주석 풀기
    # print("PARAM:", param)
    # print("CONFIG_KEYS:", sorted(list((config or {}).keys())))

    # 1) Database 목록
    if param == "database":
        admin_db = config.get("admin_db") or "postgres"
        client = _build_client(config, admin_db)
        if client is None:
            return {"choices": []}

        # ✅ 1순위: 라이브러리에 list_databases가 있으면 그걸 사용
        if hasattr(client, "list_databases"):
            dbs = client.list_databases()
            return _choices(dbs)

        # ✅ 2순위: query/execute 류 메서드 탐색
        sql = """
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
              AND datallowconn = true
            ORDER BY datname
        """

        if hasattr(client, "query"):
            rows = client.query(sql)
            dbs = [r[0] for r in rows]
            return _choices(dbs)

        if hasattr(client, "execute"):
            rows = client.execute(sql)
            dbs = [r[0] for r in rows]
            return _choices(dbs)

        # 여기까지 왔으면 라이브러리가 SQL 실행을 지원 안 함
        raise Exception(
            "PgJdbcClient has no method to list databases. "
            "Implement one of: list_databases(), query(sql), execute(sql) in pg-jdbc-lib."
        )

    # 2) Schema 목록 (선택한 DB로 접속)
    if param == "schema":
        database = config.get("database")
        client = _build_client(config, database)
        if client is None:
            return {"choices": []}

        schemas = client.list_schemas()
        schemas = [s for s in schemas if not s.startswith("pg_") and s != "information_schema"]
        return _choices(schemas)

    # 3) Table 목록
    if param == "table":
        database = config.get("database")
        schema = config.get("schema")
        if not database or not schema:
            return {"choices": []}

        client = _build_client(config, database)
        if client is None:
            return {"choices": []}

        tables = client.list_tables(schema)
        return _choices(tables)

    return {"choices": []}