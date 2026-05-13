"""Dagster Definitions — entry point for the Venatus data pipeline."""

from dagster import Definitions

from dagster_project.assets.ingest import (
    raw_ad_events,
    raw_ad_units,
    raw_campaigns,
    raw_publishers,
)
from dagster_project.resources import ClickHouseResource

defs = Definitions(
    assets=[raw_publishers, raw_ad_units, raw_campaigns, raw_ad_events],
    resources={
        "clickhouse": ClickHouseResource(),
    },
)
