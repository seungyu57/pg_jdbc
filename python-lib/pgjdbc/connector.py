from dataiku.connector import Connector

# ✅ 라이브러리 분리: code env에 설치된 wheel에서 import
from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient


DEFAULT_PG_JAR = "/data/jdbc/postgresql.jar"  # ✅ 버전 선택 제거: 고정 jar 경로


class PgJdbcConnector(Connector):
    """
    Thin Dataiku connector wrapper.
    Business logic lives in pg_jdbc_lib (installed in code env).
    """

    def get_read_schema(self):
        # 선택: 라이브러리에서 schema infer 기능이 있으면 여기서 호출해도 됨.
        # Dataiku가 비어있는 schema를 허용하지 않는 경우가 있어 infer_schema를 쓰는 걸 추천.
        cfg = self._build_cfg()
        client = PgJdbcClient(cfg)
        return client.infer_schema()

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        cfg = self._build_cfg()
        client = PgJdbcClient(cfg)

        for row in client.read_rows(limit=records_limit):
            yield row

    def _build_cfg(self) -> PgJdbcConfig:
        host = self.config.get("host")
        port = int(self.config.get("port", 5432))

        schema = self.config.get("schema", "public")
        table = self.config.get("table")

        fixed_db = self.config.get("fixed_db", "dataiku")

        jar_path = self.config.get("jar_path") or DEFAULT_PG_JAR

        creds = self.config.get("pg_credentials") or {}
        user = creds.get("user")
        password = creds.get("password")

        if not host:
            raise Exception("Missing host")
        if not table:
            raise Exception("Missing table")
        if not user or not password:
            raise Exception("Missing credentials preset (user/password).")

        return PgJdbcConfig(
            host=host,
            port=port,
            database=fixed_db,
            user=user,
            password=password,
            schema=schema,
            table=table,
            jar_path=jar_path
        )