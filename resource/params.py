# resource/params.py

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"


def _import_pg_jdbc():
    try:
        from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient
    except Exception:
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
        return PgJdbcConfig, PgJdbcClient


def _build_client(config):
    try:
        PgJdbcConfig, PgJdbcClient = _import_pg_jdbc()
    except Exception:
        return None

    # ✅ jar_path는 dataset config(숨김 파라미터)에서만 가져온다
    jar_path = config.get("jar_path")
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
    param = (payload or {}).get("parameterName")

    client = _build_client(config)
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