# -*- coding: utf-8 -*-
from dataiku.connector import Connector

# 외부 wheel에서 가져옴 (Code Env에 설치되어 있어야 함)
from pg_jdbc_lib import PgJdbcConfig, PgJdbcClient


class PgJdbcConnector(Connector):

    def _build_client(self) -> PgJdbcClient:
        creds = self.config.get("dbCreds") or {}
        user = creds.get("user")
        password = creds.get("password")
        if not user or not password:
            raise Exception("Missing DB credentials preset (dbCreds.user/password)")

        cfg = PgJdbcConfig(
            jar_path=self.config.get("jar_path"),
            host=self.config.get("host"),
            port=int(self.config.get("port", 5432)),
            database=self.config.get("database"),
            user=user,
            password=password
        )
        return PgJdbcClient(cfg)

    def get_read_schema(self):
        client = self._build_client()
        schema = self.config.get("schema", "public")
        table = self.config.get("table")

        cols = client.fetch_columns(schema, table)
        # 타입은 기본 string으로 (원하면 추후 타입 매핑 추가 가능)
        return [{"name": c, "type": "string"} for c in cols]

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=None):
        client = self._build_client()
        schema = self.config.get("schema", "public")
        table = self.config.get("table")

        cfg_limit = int(self.config.get("limit", 0))
        effective_limit = None
        if records_limit is not None:
            effective_limit = int(records_limit)
        elif cfg_limit > 0:
            effective_limit = cfg_limit

        for row in client.fetch_rows(schema, table, limit=effective_limit):
            yield row