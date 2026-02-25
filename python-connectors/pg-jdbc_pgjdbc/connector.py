from dataiku.connector import Connector
from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"
FIXED_JAR = "/data/jdbc/postgresql-42.7.10.jar"


class PgJdbcConnector(Connector):

    def get_read_schema(self):
        cfg = self._make_cfg()
        client = PgJdbcClient(cfg)
        cols = client.infer_schema()

        return {"columns": cols}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        cfg = self._build_cfg()
        client = PgJdbcClient(cfg)
        for row in client.read_rows(limit=records_limit):
            yield row

    def _build_cfg(self):
        schema = self.config.get("schema")
        table = self.config.get("table")

        creds = self.config.get("pg_credentials") or {}
        user = creds.get("user")
        password = creds.get("password")

        if not user or not password:
            raise Exception("Missing credentials preset")

        return PgJdbcConfig(
            host=FIXED_HOST,
            port=FIXED_PORT,
            database=FIXED_DB,
            user=user,
            password=password,
            schema=schema,
            table=table,
            jar_path=FIXED_JAR
        )