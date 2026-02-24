# resource/params.py

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"
DEFAULT_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"

def _build_client(config, plugin_config):
    # Dataiku custom UI 컨텍스트에서 안전하게: 함수 내부 import
    try:
        from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
    except Exception:
        # 라이브러리가 코드환경에 설치 안 된 상태면 choices 비움
        return None

    jar_path = (
        config.get("jar_path")
        or plugin_config.get("jar_path")
        or DEFAULT_JAR_PATH
    )
    user = config.get("user")
    password = config.get("password")

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
    param = (payload or {}).get("parameterName")

    client = _build_client(config, plugin_config)
    if client is None:
        return {"choices": []}

    if param == "schema":
        schemas = client.list_schemas()

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