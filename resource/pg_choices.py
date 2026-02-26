from __future__ import annotations

import glob
import os

import jaydebeapi


CANDIDATE_JAR_RELATIVE_PATHS = [
    os.path.join("resource1", "jdbc", "postgresql-42.7.10.jar"),
]
LEGACY_JAR_PATH = "/data/jdbc/postgresql-42.7.10.jar"
PLUGIN_ID = "pg-jdbc"


def _candidate_plugin_roots():
    roots = []

    this_file = globals().get("__file__")
    if this_file:
        # .../resource/pg_choices.py -> plugin root is parent of resource/
        roots.append(os.path.abspath(os.path.join(os.path.dirname(this_file), "..")))

    cwd = os.getcwd()
    roots.extend(
        [
            cwd,
            os.path.abspath(os.path.join(cwd, "..")),
            os.path.abspath(os.path.join(cwd, "..", "..")),
        ]
    )

    dedup = []
    for root in roots:
        if root not in dedup:
            dedup.append(root)
    return dedup


def _resolve_jar_path() -> str:
    # 1) Try runtime-relative candidates first.
    for plugin_root in _candidate_plugin_roots():
        for relative_path in CANDIDATE_JAR_RELATIVE_PATHS:
            candidate = os.path.join(plugin_root, relative_path)
            if os.path.isfile(candidate):
                return candidate

    # 2) Try known DSS plugin locations (dev, then installed).
    known_patterns = [
        f"/data/dataiku/DATA_DIR/plugins/dev/{PLUGIN_ID}/resource/jdbc/postgresql-42.7.10.jar",
        f"/data/dataiku/DATA_DIR/plugins/installed/{PLUGIN_ID}/resource/jdbc/postgresql-42.7.10.jar",
    ]
    for pattern in known_patterns:
        for candidate in glob.glob(pattern):
            if os.path.isfile(candidate):
                return candidate

    # 3) Final legacy fallback.
    if os.path.isfile(LEGACY_JAR_PATH):
        return LEGACY_JAR_PATH

    expected = " or ".join(CANDIDATE_JAR_RELATIVE_PATHS)
    raise ValueError(
        "PostgreSQL JDBC jar not found. "
        f"Expected plugin resource jar ({expected}) or {LEGACY_JAR_PATH}"
    )


def _extract_user_password(config: dict):
    """
    Extract user/password from PRESET -> BASIC credential structure.
    """
    preset_block = config.get("pg_credentials") or {}
    cred_block = preset_block.get("pg") or {}

    user = cred_block.get("user") or cred_block.get("login")
    password = cred_block.get("password")
    return user, password


def _connect(config: dict):
    host = config.get("host", "localhost")
    port = int(config.get("port", 5432))
    database = config.get("database", "dataiku")

    user, password = _extract_user_password(config)

    if not host:
        raise ValueError("Missing host")
    if not user:
        raise ValueError("Missing user")

    jar_path = _resolve_jar_path()

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    jdbc_props = {"user": user, "password": password or ""}

    return jaydebeapi.connect(
        "org.postgresql.Driver",
        jdbc_url,
        jdbc_props,
        jar_path,
    )


def _list_schemas(config: dict):
    sql = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schema_name
    """
    with _connect(config) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return [r[0] for r in rows]
        finally:
            try:
                cur.close()
            except Exception:
                pass


def _list_tables(config: dict, schema: str):
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = ?
      AND table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name
    """
    with _connect(config) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, [schema])
            rows = cur.fetchall()
            return [r[0] for r in rows]
        finally:
            try:
                cur.close()
            except Exception:
                pass


def do(payload, config, plugin_config, inputs):

    target_param = (
        payload.get("parameterName")
        or payload.get("paramName")
        or payload.get("name")
        or ""
    )

    try:
        if target_param == "schema":
            schemas = _list_schemas(config)
            return {
                "choices": [{"value": s, "label": s} for s in schemas]
            }

        if target_param == "table":
            schema = config.get("schema")
            if not schema:
                return {"choices": []}
            tables = _list_tables(config, schema)
            return {
                "choices": [{"value": t, "label": t} for t in tables]
            }

        if config.get("schema") and not config.get("table"):
            tables = _list_tables(config, config["schema"])
            return {"choices": [{"value": t, "label": t} for t in tables]}

        schemas = _list_schemas(config)
        return {"choices": [{"value": s, "label": s} for s in schemas]}

    except Exception as e:
        return {
            "choices": [
                {"value": "", "label": f"[ERROR] {str(e)}"}
            ]
        }
