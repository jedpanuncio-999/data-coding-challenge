-- =============================================================================
-- Venatus Data Engineering Challenge — Database Setup
-- =============================================================================
-- This file is executed once on first ClickHouse startup.
-- It creates the databases and empty target tables.
--
-- The candidate's Dagster pipeline is responsible for loading data from
-- the raw files in data/raw/ into these tables.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Databases
-- -------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

-- -------------------------------------------------------------------------
-- raw.publishers  (target for file ingestion)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.publishers
(
    publisher_id       UInt32,
    publisher_name     String,
    publisher_category String,
    primary_domain     String,
    account_manager    String,
    country            String,
    timezone           String,
    created_at         DateTime,
    updated_at         DateTime,
    _loaded_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY publisher_id;

-- -------------------------------------------------------------------------
-- raw.campaigns  (target for file ingestion)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.campaigns
(
    campaign_id            UInt32,
    campaign_name          String,
    advertiser_id          UInt32,
    advertiser_name        String,
    campaign_start_date    Date,
    campaign_end_date      Date,
    campaign_budget_usd    Decimal64(2),
    campaign_status        String,
    targeting_device_types String,
    targeting_countries    String,
    created_at             DateTime,
    _loaded_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY campaign_id;

-- -------------------------------------------------------------------------
-- raw.ad_units  (target for file ingestion)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ad_units
(
    ad_unit_id     String,
    publisher_id   UInt32,
    ad_unit_name   String,
    ad_format      String,
    ad_size        String,
    placement_type String,
    is_active      UInt8,
    created_at     DateTime,
    _loaded_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY (publisher_id, ad_unit_id);

-- -------------------------------------------------------------------------
-- raw.ad_events  (target for file ingestion)
--
-- This schema is the analytical load contract for ad_events parquet
-- files. Diagnostic fields (internal trace identifiers, user hashes,
-- etc.) are streamed by upstream to a separate `data/raw/diagnostics/`
-- partition and are not part of the analytical contract.
--
-- The single most important quality signal is `placement_quality_score`,
-- a 0.0-1.0 score emitted by the SSP's quality model. Treat it as the
-- canonical quality input for downstream filtering.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ad_events
(
    event_id                  String,
    event_type                String,
    event_timestamp           DateTime64(3),
    publisher_id              UInt32,
    site_domain               String,
    ad_unit_id                String,
    campaign_id               Nullable(UInt32),
    advertiser_id             Nullable(UInt32),
    device_type               String,
    country_code              String,
    region                    String DEFAULT '',
    browser                   String,
    placement_quality_score   Nullable(Float32),
    revenue_usd               Nullable(Decimal64(6)),
    bid_floor_usd             Nullable(Decimal64(6)),
    is_filled                 UInt8,
    _loaded_at                DateTime DEFAULT now(),
    _source_file              String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (event_timestamp, publisher_id);
