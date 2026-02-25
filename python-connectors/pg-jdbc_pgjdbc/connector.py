from __future__ import annotations

from dataiku.connector import Connector

from pg_jdbc_lib import PgJdbcClient, PgJdbcConfig


class PgJdbcConnector(Connector):

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)

    def make_cfg(self) -> PgJdbcConfig:

        jar_path = "/data/jdbc/postgresql-42.7.10.jar"

        host = self.config.get("host")
        port = int(self.config.get("port", 5432))
        database = self.config.get("database", "dataiku")
        schema = self.config.get("schema", "public")
        table = self.config.get("table")

        # ✅ preset credentials (중첩 구조)
        preset_block = self.config.get("pg_credentials") or {}
        cred_block = preset_block.get("pg") or {}
        user = cred_block.get("user") or cred_block.get("login")
        password = cred_block.get("password", "")

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
        )

    # =========================
    # Dataiku가 스키마를 요구할 때
    # =========================
    def get_read_schema(self):
        cfg = self.make_cfg()
        client = PgJdbcClient(cfg)

        cols = client.infer_schema()
        return {"columns": cols}
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

        lim = records_limit if records_limit is not None else cfg.default_limit

        sql = f'SELECT * FROM "{sch}"."{tbl}"'
        for row in client.iter_rows(sql, limit=lim):
            yield row