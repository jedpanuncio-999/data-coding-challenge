"""Dagster Resources — ClickHouse connection wrapper."""

import os
from contextlib import contextmanager
from typing import Generator

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from dagster import ConfigurableResource


class ClickHouseResource(ConfigurableResource):
    """Configurable ClickHouse connection resource using clickhouse-connect."""

    host: str = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    user: str = os.environ.get("CLICKHOUSE_USER", "default")
    password: str = os.environ.get("CLICKHOUSE_PASSWORD", "")

    @contextmanager
    def get_client(self) -> Generator[Client, None, None]:
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
        )
        try:
            yield client
        finally:
            client.close()

    def execute(self, query: str) -> None:
        with self.get_client() as client:
            client.command(query)

    def query(self, query: str) -> list:
        with self.get_client() as client:
            return client.query(query).result_rows
