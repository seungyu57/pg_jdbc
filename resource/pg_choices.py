from __future__ import annotations

import os
import jaydebeapi


JAR_PATH = "/data/jdbc/postgresql-42.7.10.jar"  # 고정 경로


def _extract_user_password(config: dict):
    """
    PRESET -> BASIC credential 구조에서 user/password 꺼내기
    config 구조 예시(버전에 따라 약간 다름):
    {
      "pg_credentials": {
        "pg": {"user": "...", "password": "..."}
      }
    }
    """
    preset_block = config.get("pg_credentials") or {}
    cred_block = preset_block.get("pg") or {}

    user = cred_block.get("user") or cred_block.get("login")
    password = cred_block.get("password")
    return user, password


def _connect(config: dict):
    host = config.get("host")
    port = int(config.get("port", 5432))
    database = config.get("database", "dataiku")

    user, password = _extract_user_password(config)

    if not host:
        raise ValueError("Missing host")
    if not user:
        raise ValueError("Missing user")
    if not os.path.isfile(JAR_PATH):
        raise ValueError(f"PostgreSQL JDBC jar not found: {JAR_PATH}")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    jdbc_props = {"user": user, "password": password or ""}

    return jaydebeapi.connect(
        "org.postgresql.Driver",
        jdbc_url,
        jdbc_props,
        JAR_PATH
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

        # 혹시 target을 못 잡는 경우를 대비해 payload에 schema/table 힌트가 있으면 분기
        if config.get("schema") and not config.get("table"):
            tables = _list_tables(config, config["schema"])
            return {"choices": [{"value": t, "label": t} for t in tables]}

        schemas = _list_schemas(config)
        return {"choices": [{"value": s, "label": s} for s in schemas]}

    except Exception as e:
        # UI가 완전히 죽지 않게 빈 리스트 반환 + 에러 표시용 라벨
        return {
            "choices": [
                {"value": "", "label": f"[ERROR] {str(e)}"}
            ]
        }