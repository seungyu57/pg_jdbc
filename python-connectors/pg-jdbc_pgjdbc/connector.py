from dataiku.connector import Connector

# pg-jdbc-lib==0.1.0 설치 기준: import 모듈명은 보통 pg_jdbc_lib
PgJdbcConfig = None
PgJdbcClient = None
_IMPORT_ERR = None

try:
    # 케이스 1) pg_jdbc_lib/client.py 구조
    from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
except Exception as e1:
    try:
        # 케이스 2) pg_jdbc_lib/__init__.py에서 export 해주는 구조
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
    except Exception as e2:
        _IMPORT_ERR = (e1, e2)

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"
DEFAULT_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"


class PgJdbcConnector(Connector):
    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        if PgJdbcClient is None:
            raise Exception(
                "pg-jdbc-lib==0.1.0 is not available in this code env. "
                f"Import errors: {_IMPORT_ERR}"
            )

        jar_path = self.config.get("jar_path") or DEFAULT_JAR_PATH
        user = self.config.get("user")
        password = self.config.get("password")
        schema = self.config.get("schema")
        table = self.config.get("table")

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