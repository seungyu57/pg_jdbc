from __future__ import annotations

import glob
import os

from dataiku.connector import Connector

from pg_jdbc_lib import PgJdbcClient, PgJdbcConfig


CANDIDATE_JAR_RELATIVE_PATHS = [
    os.path.join("resource", "jdbc1", "postgresql-42.7.10.jar"),
    os.path.join("resource", "postgresql-42.7.10.jar"),
]
LEGACY_JAR_PATH = "/data/jdbc/postgresql-42.7.10.jar"
PLUGIN_ID = "pg-jdbc"


class PgJdbcConnector(Connector):

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)

    def _resolve_jar_path(self) -> str:
        # 1) Runtime-relative candidates.
        plugin_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        for relative_path in CANDIDATE_JAR_RELATIVE_PATHS:
            candidate = os.path.join(plugin_root, relative_path)
            if os.path.isfile(candidate):
                return candidate

        # 2) Known DSS plugin locations (dev, then installed).
        known_patterns = [
            f"/data/dataiku/DATA_DIR/plugins/dev/{PLUGIN_ID}/resource/jdbc/postgresql-42.7.10.jar",
            f"/data/dataiku/DATA_DIR/plugins/dev/{PLUGIN_ID}/resource/postgresql-42.7.10.jar",
            f"/data/dataiku/DATA_DIR/plugins/installed/{PLUGIN_ID}/resource/jdbc/postgresql-42.7.10.jar",
            f"/data/dataiku/DATA_DIR/plugins/installed/{PLUGIN_ID}/resource/postgresql-42.7.10.jar",
        ]
        for pattern in known_patterns:
            for candidate in glob.glob(pattern):
                if os.path.isfile(candidate):
                    return candidate

        # 3) Final legacy fallback.
        if os.path.isfile(LEGACY_JAR_PATH):
            return LEGACY_JAR_PATH

        expected = " or ".join(os.path.join(plugin_root, p) for p in CANDIDATE_JAR_RELATIVE_PATHS)
        raise ValueError(
            "PostgreSQL JDBC jar not found. "
            f"Expected one of: {expected} or {LEGACY_JAR_PATH}"
        )

    def make_cfg(self) -> PgJdbcConfig:

        jar_path = self._resolve_jar_path()

        host = self.config.get("host", "localhost")
        port = int(self.config.get("port", 5432))
        database = self.config.get("database", "dataiku")
        schema = self.config.get("schema", "public")
        table = self.config.get("table")

        # preset credentials (nested structure)
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

    def get_read_schema(self):
        cfg = self.make_cfg()
        client = PgJdbcClient(cfg)

        cols = client.infer_schema()
        return {"columns": cols}

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=None,
    ):
        cfg = self.make_cfg()
        client = PgJdbcClient(cfg)

        sch = cfg.schema
        tbl = cfg.table

        lim = records_limit if records_limit is not None else cfg.default_limit

        sql = f'SELECT * FROM "{sch}"."{tbl}"'
        for row in client.iter_rows(sql, limit=lim):
            yield row
