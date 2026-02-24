from dataiku.connector import Connector

# ---- pg-jdbc-lib==0.1.0 import 안정화 (패키지 구조 차이를 흡수) ----
PgJdbcConfig = None
PgJdbcClient = None
_IMPORT_ERR = None

try:
    # 케이스 1) pg_jdbc_lib/client.py 구조
    from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
except Exception as e1:
    try:
        # 케이스 2) pg_jdbc_lib/__init__.py에서 export 하는 구조
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
    except Exception as e2:
        _IMPORT_ERR = (e1, e2)

# ---- 고정 접속정보(원래대로) ----
FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"

# ---- jar path는 plugin.json의 config.jar_path "단 한 곳" ----
JAR_PLUGIN_KEY = "jar_path"


def _get_plugin_config(connector) -> dict:
    # DSS 컨텍스트에 따라 속성명이 다를 수 있어 안전하게 흡수
    cfg = getattr(connector, "plugin_config", None)
    if isinstance(cfg, dict):
        return cfg
    cfg = getattr(connector, "pluginConfig", None)
    if isinstance(cfg, dict):
        return cfg
    return {}


def _get_jar_path(connector) -> str:
    plugin_cfg = _get_plugin_config(connector)
    return plugin_cfg.get(JAR_PLUGIN_KEY)


class PgJdbcConnector(Connector):
    def get_read_schema(self):
        return None

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=None
    ):
        if PgJdbcClient is None:
            raise Exception(
                "pg-jdbc-lib==0.1.0 is not available in this code env. "
                f"Import errors: {_IMPORT_ERR}"
            )

        jar_path = _get_jar_path(self)
        if not jar_path:
            raise Exception(
                "Missing jar_path in plugin config. "
                "Set plugin.json -> config.jar_path"
            )

        user = self.config.get("user")
        password = self.config.get("password")
        schema = self.config.get("schema")
        table = self.config.get("table")

        # schema/table 선택 전 호출돼도 조용히 종료
        if not schema or not table:
            return

        cfg_limit = int(self.config.get("limit", 1000))
        limit = records_limit if records_limit is not None else cfg_limit
        if limit == 0:
            limit = 10_000_000  # 무제한은 위험해서 큰 값으로 타협

        cfg = PgJdbcConfig(
            jar_path=jar_path,
            host=FIXED_HOST,
            port=FIXED_PORT,
            database=FIXED_DB,
            user=user,
            password=password
        )

        cli = PgJdbcClient(cfg)
        cols, rows = cli.fetch_table(schema=schema, table=table, limit=limit)

        for r in rows:
            yield dict(zip(cols, r))