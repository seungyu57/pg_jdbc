from dataiku.connector import Connector

from pg_jdbc_lib.client import PgJdbcConfig, PgJdbcClient

FIXED_HOST = "localhost"
FIXED_PORT = 5432
FIXED_DB = "dataiku"

# Hidden (UI 미노출) 기본 JDBC 드라이버 경로
DEFAULT_JAR_PATH = "/data/test_ssg/postgresql-42.7.10.jar"


class PgJdbcConnector(Connector):
    def get_read_schema(self):
        # 스키마는 첫 rows로 infer 가능. (DSS가 필요하면 알아서 추론)
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        jar_path = self.config.get("jar_path") or DEFAULT_JAR_PATH
        user = self.config.get("user")
        password = self.config.get("password")
        schema = self.config.get("schema")
        table = self.config.get("table")

        # ✅ 핵심: schema/table 선택 전이면 read_rows 호출돼도 예외 내지 말고 빈 결과로 종료
        if not schema or not table:
            return

        cfg_limit = int(self.config.get("limit", 1000))
        limit = records_limit if records_limit is not None else cfg_limit
        if limit == 0:
            limit = 10_000_000  # 무제한은 위험해서 일단 큰 값 타협

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