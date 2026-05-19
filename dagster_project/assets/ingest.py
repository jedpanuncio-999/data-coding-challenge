"""
Ingestion Assets — load raw files from data/raw/ into ClickHouse.

The previous engineer started these assets. Dim CSV loaders are partly
wired up; the events asset is a stub that just lists the files it finds.

Notes left by the previous engineer:
    - Dim CSVs (publishers, ad_units, campaigns_export) load into their
      respective raw.* tables. I have NOT looked at the redelivery folder
      yet — assumed dimensions don't change between deliveries.
    - Event files live in `data/raw/events/`. There is also a
      `data/raw/diagnostics/` folder — assumed internal-only, not part
      of the analytical contract.
    - Did not run idempotency tests yet. Re-running these assets twice
      may or may not produce the same downstream numbers.
"""

from datetime import datetime
import csv
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import numpy as np

from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_project.resources import ClickHouseResource

RAW_DATA_DIR = Path("data/raw")
REDELIVERY_DIR = RAW_DATA_DIR / "redelivery"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str):
    if not value or value.strip() == "":
        return None

    try:
        return datetime.fromisoformat(value.strip())
    except ValueError:
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d")
        except ValueError:
            return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Dimension Assets
# ---------------------------------------------------------------------------


@asset(group_name="ingestion")
def raw_publishers(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load publishers.csv into raw.publishers."""
    rows = _read_csv(RAW_DATA_DIR / "publishers.csv")

    clickhouse.execute("TRUNCATE TABLE raw.publishers")

    data = [
        (
            int(r["publisher_id"]),
            r["publisher_name"],
            r["publisher_category"],
            r["primary_domain"],
            r["account_manager"],
            r["country"],
            r.get("timezone", ""),
            _parse_date(r["created_at"]),
            _parse_date(r["updated_at"]),
        )
        for r in rows
    ]

    with clickhouse.get_client() as client:
        client.insert(
            "raw.publishers",
            data,
            column_names=[
                "publisher_id",
                "publisher_name",
                "publisher_category",
                "primary_domain",
                "account_manager",
                "country",
                "timezone",
                "created_at",
                "updated_at",
            ],
        )

    context.log.info("Loaded %d publishers", len(data))

    context.add_output_metadata({
        "row_count": MetadataValue.int(len(data))
    })


@asset(group_name="ingestion")
def raw_campaigns(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load campaigns_export.csv into raw.campaigns."""

    rows = _read_csv(RAW_DATA_DIR / "campaigns" / "campaigns_export.csv")

    clickhouse.execute("TRUNCATE TABLE raw.campaigns")

    data = [
        (
            int(r["campaign_id"]),
            r["campaign_name"],
            int(r["advertiser_id"]),
            r["advertiser_name"],
            _parse_date(r["start_date"]),
            _parse_date(r["end_date"]),
            float(r["budget_usd"]),
            r["status"],
            r.get("device_targeting", ""),
            r.get("country_targeting", ""),
            _parse_date(r["created_at"]),
        )
        for r in rows
    ]

    with clickhouse.get_client() as client:
        client.insert(
            "raw.campaigns",
            data,
            column_names=[
                "campaign_id",
                "campaign_name",
                "advertiser_id",
                "advertiser_name",
                "campaign_start_date",
                "campaign_end_date",
                "campaign_budget_usd",
                "campaign_status",
                "targeting_device_types",
                "targeting_countries",
                "created_at",
            ],
        )

    context.log.info("Loaded %d campaigns", len(data))

    context.add_output_metadata({
        "row_count": MetadataValue.int(len(data))
    })


@asset(group_name="ingestion")
def raw_ad_units(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load ad_units.csv into raw.ad_units."""

    rows = _read_csv(RAW_DATA_DIR / "ad_units.csv")

    clickhouse.execute("TRUNCATE TABLE raw.ad_units")

    data = [
        (
            r["ad_unit_id"],
            int(r["publisher_id"]),
            r["ad_unit_name"],
            r["ad_format"],
            r["ad_size"],
            r["placement_type"],
            int(r["is_active"]),
            _parse_date(r["created_at"]),
        )
        for r in rows
    ]

    with clickhouse.get_client() as client:
        client.insert(
            "raw.ad_units",
            data,
            column_names=[
                "ad_unit_id",
                "publisher_id",
                "ad_unit_name",
                "ad_format",
                "ad_size",
                "placement_type",
                "is_active",
                "created_at",
            ],
        )

    context.log.info("Loaded %d ad units", len(data))

    context.add_output_metadata({
        "row_count": MetadataValue.int(len(data))
    })


# ---------------------------------------------------------------------------
# Fact Asset
# ---------------------------------------------------------------------------


@asset(
    group_name="ingestion",
    deps=[raw_publishers, raw_ad_units, raw_campaigns],
)
def raw_ad_events(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load event parquet files into raw.ad_events."""

    # added: load original delivery parquet files
    initial_files = sorted(
        (RAW_DATA_DIR / "events").glob("*.parquet")
    )

    # added: load replay/redelivery parquet files
    redelivery_files = sorted(
        (REDELIVERY_DIR / "events").glob("*.parquet")
    )

    # added: combine both deliveries
    event_files = initial_files + redelivery_files

    context.log.info(
        "Found %d parquet files",
        len(event_files),
    )

    # added: full reload for deterministic reruns
    clickhouse.execute("TRUNCATE TABLE raw.ad_events")

    rows_loaded = 0

    with clickhouse.get_client() as client:

        for file_path in event_files:

            context.log.info(
                "Loading file: %s",
                file_path,
            )

            # added: read parquet file
            table = pq.read_table(file_path)

            # added: convert parquet table into pandas dataframe
            df = table.to_pandas()

            # added: normalize event timestamp
            df["event_timestamp"] = pd.to_datetime(
                df["timestamp"],
                errors="coerce",
                utc=True,
            )
            # added: preserve source lineage
            df["_source_file"] = str(file_path)

            # added: ingestion timestamp metadata
            df["_loaded_at"] = datetime.utcnow()

            df = df.astype(object)

            # added: handle the NaN issue
            df = df.replace({
                np.nan: None,
                pd.NaT: None,
                np.inf: None,
                -np.inf: None,
            })

            # added: convert NaN values to None for ClickHouse
            df = df.where(pd.notnull(df), None)

            # added: exact raw.ad_events column order
            columns = [
                "event_id",
                "event_type",
                "event_timestamp",
                "publisher_id",
                "ad_unit_id",
                "campaign_id",
                "advertiser_id",
                "device_type",
                "browser",
                "country_code",
                "region",
                "placement_quality_score",
                "revenue_usd",
                "bid_floor_usd",
                "is_filled",
                "site_domain",
                "_source_file",
                "_loaded_at",
            ]

            # added: dataframe rows into tuple format
            data = list(
                df[columns].itertuples(
                    index=False,
                    name=None,
                )
            )

            client.insert(
                "raw.ad_events",
                data,
                column_names=columns,
            )

            rows_loaded += len(data)

    context.log.info(
        "Loaded %d event rows",
        rows_loaded,
    )

    context.add_output_metadata({
        "files_found": MetadataValue.int(len(event_files)),
        "rows_loaded": MetadataValue.int(rows_loaded),
    })