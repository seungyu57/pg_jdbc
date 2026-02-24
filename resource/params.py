# resource/params.py

# ---- 고정 접속정보(원래대로) ----
FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"

# ---- jar path는 plugin.json의 config.jar_path "단 한 곳" ----
JAR_PLUGIN_KEY = "jar_path"


def _import_pg_jdbc():
    # custom UI 컨텍스트에서 안전하게: 함수 내부 import + 구조 흡수
    try:
        from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient
    except Exception:
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient


def _get_jar_path(plugin_config):
    return (plugin_config or {}).get(JAR_PLUGIN_KEY)


def _build_client(config, plugin_config):
    try:
        PgJdbcConfig, PgJdbcClient = _import_pg_jdbc()
    except Exception:
        return None

    jar_path = _get_jar_path(plugin_config)
    if not jar_path:
        return None

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
    """
    MUST return JSON-serializable data only.
    """
    param = (payload or {}).get("parameterName")

    client = _build_client(config, plugin_config)
    if client is None:
        return {"choices": []}

    if param == "schema":
        schemas = client.list_schemas()

        # 시스템 스키마 숨기기(원하면 조정)
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