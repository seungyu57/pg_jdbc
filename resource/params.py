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
        raise Exception("jar_path missing (hidden param).")
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


def do(payload, config, plugin_config, inputs):
    param = (payload or {}).get("parameterName")

    if param == "database":
        admin_db = config.get("admin_db") or "postgres"

        # ✅ 여기서 접속 실패하면 에러를 띄워서 원인 확인
        client = _build_client(config, admin_db)
        if client is None:
            return {"choices": []}

        sql = """
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
              AND datallowconn = true
            ORDER BY datname
        """

        # ✅ client가 SQL 실행 메서드를 제공해야 함
        if hasattr(client, "query"):
            rows = client.query(sql)
        elif hasattr(client, "execute"):
            rows = client.execute(sql)
        else:
            raise Exception(
                "PgJdbcClient에 SQL 실행 메서드가 없음. "
                "pg-jdbc-lib에 query(sql) 또는 execute(sql) 추가해야 DB 목록 조회 가능"
            )

        dbs = [r[0] for r in rows]
        return {"choices": [{"value": d, "label": d} for d in dbs]}

    # schema/table은 너 기존 코드 유지해도 됨
    return {"choices": []}