# Design Document

> Replace each section with your own analysis. Keep it concise — quality over quantity.

## 1) Pipeline — What You Changed and Why

Completed the raw_ad_events ingestion asset

Original behaviour

The original pipeline had the dimension ingestion assets wired correctly, but the raw_ad_events asset was incomplete. Event parquet files were not being fully loaded into ClickHouse and the pipeline did not correctly handle replay/redelivery files.

How I discovered the problem

I compared the raw parquet files against the records loaded into ClickHouse and found:

- Missing event records from the redelivery batch
- Row count mismatches between parquet files and raw.ad_events
- Duplicate event_id values after replay ingestion
- Downstream metrics changing after re-running the pipeline

I also encountered a ClickHouse error during ingestion:
- ProgrammingError: Unrecognized column 'timestamp' in table raw.ad_events

## 2) dbt — Design Choices
What I changed

I updated raw_ad_events to:

- Load both the initial delivery and replay/redelivery parquet files
- Normalize timestamp into event_timestamp
- Convert null-like pandas values into ClickHouse-compatible None
- Enforce exact column ordering before insert
- Truncate the raw table before reload to guarantee deterministic reruns

added:
clickhouse.execute("TRUNCATE TABLE raw.ad_events")
and
row_number() over (
    partition by event_id
    order by _loaded_at desc
)
was later used downstream for replay-aware deduplication

How I verified the fix:

- Row counts per parquet file vs ClickHouse
- Revenue totals before and after replay ingestion
- Distinct event_id counts
- Re-running Dagster ingestion twice produced identical downstream dbt aggregates

After the fix:

- Replay records correctly replaced older versions downstream
- Re-running ingestion did not increase revenue totals
- Final mart metrics remained stable across reruns

Fixed schema mismatch between pandas dataframe and ClickHouse

Original behaviour

The pipeline attempted to insert a timestamp column into ClickHouse even though the raw table schema used event_timestamp.

Dagster failed with:
ProgrammingError: Unrecognized column 'timestamp' in table raw.ad_events

I renamed the dataframe field from timestamp to event_timestamp before insertion

Dagster assets materialized successfully and ClickHouse accepted inserts without schema errors.

Added replay-aware deduplication

Original behaviour

The raw layer contained multiple copies of the same event_id because replay/redelivery files intentionally resent corrected events.

This caused:

- Duplicate impressions and clicks
- Inflated revenue
- Failed dbt uniqueness tests

the dbt test unique_stg_ad_events_event_id failed with over 9000 records

I intentionally kept the raw ingestion layer append-only for lineage purposes, then implemented replay-aware deduplication in dbt staging using:

row_number() over (partition by event_id order by _loaded_at desc) as rn 
and 
where rn=1

This will keep the newest replayed version of each event

To verify
I compared 
select count(*)
from analytics.stg_ad_events;

and

select count(distinct event_id)
from raw.ad_events;

counts are aligned after the deduplication


### 2a) Materialisation

for stg_ad_events I used materialized='view'
because:

- The staging layer is primarily transformation logic
- Replay deduplication needs to reflect the latest raw state immediately
- Views avoid stale intermediate copies during reruns

for fct_ad_events_daily i used

materialized='incremental'
incremental_strategy='delete+insert'
unique_key='event_date'

because replay files can resend corrected rows for recent dates.

This strategy:

Rebuilds recent partitions safely
Prevents duplicate aggregates
Preserves stable downstream metrics after reruns

I validated indempotency by rerunning the full flow multiple times and checking

select round(sum(revenue_usd), 2)
from analytics.fct_ad_events_daily;

and 

select sum(impressions), sum(clicks)
from analytics.fct_ad_events_daily;

Totals are the same even after the re-runs


### 2b) Daily aggregation boundary

event_date is derived from toDate(event_timestamp)

I chose UTC because:

Raw event files were partitioned by UTC delivery day
Replay batches referenced UTC event timestamps
Daily revenue reconciliation is more reliable when aligned with ingestion partitions

based on my experience timezones used always should be on local timezone of the client

### 2c) Dimension handling

I used a latest-value strategy for dimensions.

Replay dimension files contained attribute changes for:

- Campaign metadata
- Publisher attributes
- Ad unit attributes

The marts reflect the latest available dimension values instead of historical SCD snapshots.

I chose this because:

- The test primarily focused on replay-safe revenue reporting
- The provided dimensions behaved more like operational lookup tables
- Daily reporting typically uses the latest corrected metadata in ad operations workflows

### 2d) Tests

Unique + not_null tests

Applied to:

event_id
dimension primary keys
fact grain columns

These validate replay dedup correctness and dimensional integrity.

accepted_values:
  values: ['impression', 'click', 'viewable_impression']

Business invariant:

Only known ad event types should exist.

A failure would indicate:

upstream corruption
malformed replay files
unsupported event categories
Relationships tests

Used to verify:

campaign IDs exist
publisher IDs exist
ad unit IDs exist

A failure would indicate disconnected fact records.


## 3) Revenue Integrity Investigation

For each anomaly you found, fill in:

Finding 1: Replay ingestion created duplicate event IDs

What

Replay/redelivery files resent existing event_id values with corrected attributes.

Before deduplication:

- More than 9,000 duplicated event IDs existed
- Revenue totals increased after rerunning ingestion
- Click and impression counts inflated downstream

Why it matters

Duplicate replay handling directly impacts:

- advertiser billing
- publisher payout
- revenue reporting accuracy
- KPI trustworthiness

Without replay-aware logic, the same ad event could be monetized multiple times

query used to look for duplicate_revenue
select
    sum(revenue_usd) as duplicated_revenue
from raw.ad_events
where event_id in (
    select event_id
    from raw.ad_events
    group by event_id
    having count(*) > 1
)

I implemented replay-aware deduplication using:
row_number() over (
    partition by event_id
    order by _loaded_at desc
)

query to find event duplicates:
select
event_id,
count(*) as versions
from raw.ad_events
group by event_id
having versions > 1
order by versions desc

after applying where rn = 1 duplicates no longer affects the computation downstream

Finding 2: CTR anomalies greater than 100%
What
Some publisher/campaign combinations produced:

meaning clicks exceeded impressions.

This is commercially suspicious because:
clicks should normally be less than or equal to impressions
abnormal CTR can indicate click fraud
replay duplication can inflate clicks disproportionately

How I handled it
I added a business invariant test and investigated high-CTR cohorts separately

select
publisher_id,
campaign_id,
sum(clicks) as total_clicks,
sum(impressions) as total_impressions,
round(sum(clicks) / nullif(sum(impressions), 0), 4) as ctr,
round(sum(revenue_usd), 2) as revenue_usd
from analytics.fct_ad_events_daily
group by publisher_id, campaign_id
having total_impressions > 100
and ctr > 1
order by ctr desc


Finding 3: Replay corrections changed revenue values
What
Some replayed events contained corrected revenue_usd values compared to the original delivery.

Why it matters

Revenue correction handling is critical because:
- advertisers are billed on finalized values
- publisher payout calculations depend on corrected revenue
- duplicate corrections can inflate total revenue materially

How I handled it
The latest replayed event version replaced earlier copies through _loaded_at DESC ordering.

Detection query:
select
event_id,
min(revenue_usd) as min_revenue,
max(revenue_usd) as max_revenue,
count(*) as versions
from raw.ad_events
group by event_id
having versions > 1
and min_revenue != max_revenue
order by versions desc

## 4) Trade-offs

What I intentionally did not implement
I did not fully productionize:
Type 2 SCD dimensions because no required historical records
automated anomaly alerting
partition-level incremental replay processing
orchestration retries and observability

Shortcuts taken
I used:
truncate table raw.ad_events before reloads instead of implementing partition-aware incremental ingestion.

This simplified deterministic reruns and ensured idempotency during development.
In production I would:
ingest incrementally by partition
maintain immutable raw storage
use merge semantics instead of full truncation this is the normal way we load in production on my past experience

With another four hours
I would:
Implement dbt snapshots for historical dimension tracking
Build anomaly monitoring dashboards
Add automated replay reconciliation reports
Add data quality alerting into Dagster
Benchmark ClickHouse partitioning and ordering strategies
Build fraud-oriented distribution analysis for CTR and fill-rate anomalies


