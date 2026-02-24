from dataiku.connector import Connector

PgJdbcConfig = None
PgJdbcClient = None
_IMPORT_ERR = None

try:
    from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient
except Exception as e1:
    try:
        from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient
    except Exception as e2:
        _IMPORT_ERR = (e1, e2)

# 🔒 고정
FIXED_HOST = "localhost"
FIXED_PORT = 5432


class PgJdbcConnector(Connector):
    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        if PgJdbcClient is None:
            raise Exception(
                "pg-jdbc-lib==0.1.0 is not available in this code env. "
                f"Import errors: {_IMPORT_ERR}"
            )

        jar_path = self.config.get("jar_path")
        if not jar_path:
            raise Exception("Missing jar_path")

        user = self.config.get("user")
        password = self.config.get("password")
        database = self.config.get("database")
        schema = self.config.get("schema")
        table = self.config.get("table")

        if not database or not schema or not table:
            return

        limit = int(self.config.get("limit", 1000))
        if limit == 0:
            limit = 10_000_000

        cfg = PgJdbcConfig(
            jar_path=jar_path,
            host=FIXED_HOST,
            port=FIXED_PORT,
            database=database,
            user=user,
            password=password
        )

        cli = PgJdbcClient(cfg)
        cols, rows = cli.fetch_table(schema=schema, table=table, limit=limit)

        for r in rows:
            yield dict(zip(cols, r))