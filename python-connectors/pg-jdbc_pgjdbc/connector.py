from dataiku.connector import Connector

try:
    from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
except Exception as e:
    PgJdbcConfig = None
    PgJdbcClient = None
    _IMPORT_ERR = e

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"
DEFAULT_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"

class PgJdbcConnector(Connector):
    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        if PgJdbcClient is None:
            raise Exception(f"pg_jdbc_lib is not available in the code env: {_IMPORT_ERR}")

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