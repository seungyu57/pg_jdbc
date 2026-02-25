from dataiku.connector import Connector

try:
    from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
except Exception:
    # 일부 패키징에서 루트 export를 쓰는 경우 대비
    from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient


# 🔒 고정값 (UI에서 숨김)
FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"

# 최후의 안전장치 (plugin.json에 jar_path 넣었으면 보통 이거 쓸 일 없음)
FALLBACK_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"


def _get_basic_auth_from_preset(cfg: dict):
    preset = (cfg or {}).get("pg_credentials") or {}
    user = preset.get("user")
    password = preset.get("password")
    if not user or not password:
        raise Exception("Missing credentials in preset: pg_credentials.user/password")
    return user, password


def _get_jar_path(connector_self, dataset_cfg: dict):
    # 1) dataset config (숨김 param) 우선
    jar_path = (dataset_cfg or {}).get("jar_path")
    if jar_path:
        return jar_path

    # 2) plugin config가 있으면 거기서
    plugin_cfg = getattr(connector_self, "plugin_config", None) or {}
    jar_path = (plugin_cfg or {}).get("jar_path")
    if jar_path:
        return jar_path

    # 3) 최후 fallback
    return FALLBACK_JAR_PATH


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
        jar_path = _get_jar_path(self, self.config)
        if not jar_path:
            raise Exception("Missing jar_path (plugin.json config.jar_path recommended)")

        user, password = _get_basic_auth_from_preset(self.config)

        schema = self.config.get("schema")
        table = self.config.get("table")

        # schema/table 선택 전 호출되면 조용히 종료
        if not schema or not table:
            return

        cfg_limit = int(self.config.get("limit", 1000))
        limit = records_limit if records_limit is not None else cfg_limit
        if limit == 0:
            limit = 10_000_000

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