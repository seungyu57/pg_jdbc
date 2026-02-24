# resource/params.py

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"
DEFAULT_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"  # 서버 고정값으로 박아두기

def _build_client(config, plugin_config):
    # 함수 내부 import: Dataiku customui 컨텍스트에서 가장 안전함
    from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient

    jar_path = (
        config.get("jar_path")
        or plugin_config.get("jar_path")
        or DEFAULT_JAR_PATH
    )
    user = config.get("user")
    password = config.get("password")

    # user/password 없으면 client 만들 이유가 없음
    if not user or not password:
        return None

    cfg = PgJdbcConfig(
        jar_path=jar_path,
        host=FIXED_HOST,
        port=FIXED_PORT,
        database=FIXED_DB,
        user=user,
        password=password
    )
    return PgJdbcClient(cfg)

def do(payload, config, plugin_config, inputs):
    """
    MUST return JSON-serializable data only.
    """
    param = (payload or {}).get("parameterName")

    # user/password 없으면 드롭다운 비워두기 (에러 대신 조용히)
    client = _build_client(config, plugin_config)
    if client is None:
        return {"choices": []}

    if param == "schema":
        schemas = client.list_schemas()

        # (선택) 시스템 스키마 숨기기
        hidden_prefixes = ("pg_",)
        hidden_exact = {"information_schema"}
        schemas = [
            s for s in schemas
            if not s.startswith(hidden_prefixes) and s not in hidden_exact
        ]

        return {"choices": [{"value": s, "label": s} for s in schemas]}

    if param == "table":
        schema = config.get("schema")
        if not schema:
            return {"choices": []}
        tables = client.list_tables(schema)
        return {"choices": [{"value": t, "label": t} for t in tables]}

    return {"choices": []}