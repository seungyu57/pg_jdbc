# resource/params.py

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"
FALLBACK_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"


def _import_pg_jdbc():
    try:
        from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient
    except Exception:
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient


def _get_basic_auth_from_preset(config):
    preset = (config or {}).get("pg_credentials") or {}
    user = preset.get("user")
    password = preset.get("password")
    if not user or not password:
        return None, None
    return user, password


def _get_jar_path(config, plugin_config):
    return (
        (config or {}).get("jar_path")
        or (plugin_config or {}).get("jar_path")
        or FALLBACK_JAR_PATH
    )


def _build_client(config, plugin_config):
    try:
        PgJdbcConfig, PgJdbcClient = _import_pg_jdbc()
    except Exception:
        return None

    jar_path = _get_jar_path(config, plugin_config)
    if not jar_path:
        return None

    user, password = _get_basic_auth_from_preset(config)
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
        # 시스템 스키마 숨김(원하면 풀어도 됨)
        hidden_prefixes = ("pg_",)
        hidden_exact = {"information_schema"}
        schemas = [s for s in schemas if not s.startswith(hidden_prefixes) and s not in hidden_exact]
        return {"choices": [{"value": s, "label": s} for s in schemas]}

    if param == "table":
        schema = (config or {}).get("schema")
        if not schema:
            return {"choices": []}
        tables = client.list_tables(schema)
        return {"choices": [{"value": t, "label": t} for t in tables]}

    return {"choices": []}