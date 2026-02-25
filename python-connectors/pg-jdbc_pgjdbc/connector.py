from __future__ import annotations

from dataiku.connector import Connector

from pg_jdbc_lib import PgJdbcClient, PgJdbcConfig


class PgJdbcConnector(Connector):
    """
    Dataiku Custom Python Dataset Connector (PostgreSQL via JDBC)
    - get_read_schema(): Dataiku가 테스트/스키마 조회 시 호출
    - generate_rows(): 실제 데이터 읽을 때 호출
    """

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)

    # =========================
    # 내부: config -> PgJdbcConfig 생성
    # =========================
    def make_cfg(self) -> PgJdbcConfig:
        jar_path = "/data/jdbc/postgresql-42.7.3.jar"
        host = self.config.get("host")
        port = int(self.config.get("port", 5432))
        database = self.config.get("database", "postgres")
        user = self.config.get("user")
        password = self.config.get("password", "")
        schema = self.config.get("schema", "public")
        table = self.config.get("table")

        fetch_size = int(self.config.get("fetch_size", 1000))
        default_limit = self.config.get("limit")
        default_limit = int(default_limit) if default_limit not in (None, "", "null") else None

        if not jar_path:
            raise ValueError("Missing jar_path (PostgreSQL JDBC driver .jar)")
        if not host:
            raise ValueError("Missing host")
        if not user:
            raise ValueError("Missing user")
        if not table:
            raise ValueError("Missing table")

        return PgJdbcConfig(
            jar_path=jar_path,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            schema=schema,
            table=table,
            fetch_size=fetch_size,
            default_limit=default_limit,
        )

    # =========================
    # Dataiku가 스키마를 요구할 때
    # =========================
    def get_read_schema(self):
        cfg = self.make_cfg()
        client = PgJdbcClient(cfg)

        cols = client.infer_schema()   # [{"name":..,"type":..}, ...]
        return {"columns": cols}       # ✅ Dataiku가 안정적으로 받는 포맷

    # =========================
    # Dataiku가 실제 데이터를 읽을 때
    # =========================
    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=None
    ):
        cfg = self.make_cfg()
        client = PgJdbcClient(cfg)

        sch = cfg.schema
        tbl = cfg.table

        # Dataiku가 limit을 주면 그걸 우선 적용
        lim = records_limit if records_limit is not None else cfg.default_limit

        sql = f'SELECT * FROM "{sch}"."{tbl}"'
        for row in client.iter_rows(sql, limit=lim):
            yield row