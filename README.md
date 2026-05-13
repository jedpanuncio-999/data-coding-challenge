# Data Engineering Take-Home Challenge

## Overview

You are joining the data team at **Venatus**, a programmatic advertising company. Raw data from our ad-serving platform lands as files in a data lake landing zone. A previous engineer started building the ingestion pipeline and some dbt staging models, but the work has not been validated against the real data.

Your task is to:

1. **Get the pipeline working end-to-end** — the Python ingestion is partly stubbed and partly wired up; do not assume the output is correct. Validate.
2. **Build the dbt mart layer** — staging is started; you build the marts. Several design choices are load-bearing.
3. **Investigate revenue integrity** — there are anomalies in the data. They will not be obvious from per-row inspection. They emerge from aggregation, cross-referencing, and distributional analysis. We are not telling you what they are. The goal is to find them, quantify them, and decide what to do about them.
4. **Document your design decisions** and findings in `DESIGN.md`. **IMPORTANT** This file should be written entirely in your own language, not with AI.
5. **Commit frequently** — we want to observe how you work, not just the final state.

> **AI use is permitted.** This challenge is designed so that AI assistance does not replace engineering judgement: the planted issues are not pattern-matchable to obvious bugs, the materialisation choices have right and wrong answers, and the investigation task rewards running queries and observing distributions rather than reading code.

---

## The Stack

| Tool | Purpose | Access |
|------|---------|--------|
| **ClickHouse** | Columnar data warehouse (empty tables pre-created) | `http://localhost:8123/play` |
| **Dagster** | Pipeline orchestration | Runs locally via `make dagster` |
| **dbt** (dbt-clickhouse) | Data transformation | Runs via Docker |
| **Python** | Ingestion pipeline code | Local environment |

Everything runs locally or in Docker.

> **New to ClickHouse or Dagster?** That's fine — we don't expect prior experience with either.
>
> - **ClickHouse** is similar to PostgreSQL for SQL purposes. The dialect is nearly standard. The main difference is table engines (already set up for you in `clickhouse/init-db.sql`). See [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference) if you get stuck.
> - **Dagster** is a Python-first orchestration framework. Instead of DAGs and operators, you define **assets** (decorated Python functions). The [Dagster tutorial](https://docs.dagster.io/tutorial) covers the basics in ~20 minutes.
> - The Dagster wiring (`resources.py`, `definitions.py`) is working. The dim ingestion in `dagster_project/assets/ingest.py` runs and loads CSVs; the events asset is a stub. Verifying correctness is **your** responsibility.

---

## Getting Started

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (v2+)
- Python 3.10+
- `make`
- Git
- [DBeaver](https://dbeaver.io) optional UI tool for ClickHouse

### 1. Clone and set up

```bash
git clone <this-repo>
cd data-coding-challenge
make up           # start ClickHouse
make setup        # install Python deps
```

### 2. Inspect the raw data BEFORE writing code

```bash
ls data/raw/
ls data/raw/events/ | head
ls data/raw/diagnostics/ | head
head -3 data/raw/publishers.csv
head -3 data/raw/campaigns/campaigns_export.csv
head -3 data/raw/ad_units.csv
ls data/raw/redelivery/
ls data/external/
```

> Event files are **parquet**. Use `python -c "import pyarrow.parquet as pq; print(pq.read_table('data/raw/events/2026-03-01.parquet').schema)"` (or DBeaver / clickhouse-local) to peek.

Verify the ClickHouse tables exist and are empty:
```sql
-- http://localhost:8123/play
SHOW TABLES FROM raw;
SELECT count() FROM raw.ad_events;
```

### 3. Run the pipeline

```bash
make dagster
# Open http://localhost:3000 and materialise the assets.
# When the run completes, do not move on until you have validated the
# loaded data against the raw files.
```

### 4. Run dbt

```bash
make dbt-deps
make dbt-run
make dbt-test
```

Tests will pass trivially against the stubbed events table on first run. Once you finish the events loader, real failures will appear — those are signal, not noise, and point to decisions you need to make.

---

## Raw Data

Files live in `data/raw/` and `data/external/`.

### `data/raw/events/YYYY-MM-DD.parquet`

Daily ad-serving event files in Apache Parquet format, one file per UTC delivery date over ~30 days.

**Schema contract (load-bearing fields).** The fields below are the agreed schema with our upstream SSP, and the ones the pre-created `raw.ad_events` table loads:

| Field | Type | Notes |
|-------|------|-------|
| `event_id` | string | UUID. Unique per ad-server emission. |
| `event_type` | string | One of: `impression`, `click`, `viewable_impression`. |
| `timestamp` | string | ISO 8601, **UTC**. |
| `publisher_id` | int | FK to publishers. |
| `ad_unit_id` | string | FK to ad units. |
| `campaign_id` | int? | absent on unfilled requests. |
| `advertiser_id` | int? | absent on unfilled requests. |
| `device_type` | string | `desktop`, `mobile`, `tablet`, `ctv`. |
| `browser` | string | |
| `country_code` | string | ISO 3166-1 alpha-2. |
| `region` | string | sub-region code. |
| `placement_quality_score` | float | **Canonical quality signal** emitted by the SSP. 0.0–1.0, low scores indicate low-value inventory. Treat this as the primary input to any quality filtering. |
| `revenue_usd` | float? | Publisher payout. Always in **USD** — the SSP normalises currency upstream before export. |
| `bid_floor_usd` | float | Floor price, USD. Every filled impression carries a floor; unfilled rows may be NULL. |
| `is_filled` | bool | Whether an ad was served. |
| `site_domain` | string | Domain where served. |

### `data/raw/diagnostics/YYYY-MM-DD.parquet`

Internal trace partition emitted alongside `events/`. Contains opaque debug identifiers used by the upstream SSP for forensic tracing (`user_id_hash`, `ip_prefix`, `user_agent`, `bid_request_id`). **Not part of the analytical contract** — these are pass-through trace strings, not load-bearing for reporting. The starter `raw.ad_events` schema does not load them.

### `data/raw/publishers.csv`

Publisher dimension (~20 publishers). Inspect the columns before joining.

### `data/raw/campaigns/campaigns_export.csv`

Campaign dimension with flight dates, status, and targeting fields.

### `data/raw/ad_units.csv`

Ad unit / placement configuration.

### `data/external/partner_weekly_export.csv`

Third-party partner sample export retained for legal records. **Not part of any pipeline.** Included in the repo for completeness only; the schema is opaque (`partner_account_id`, `partner_amount`, `period_end`, ...) and the format is not stable.

### `data/raw/redelivery/` — Second delivery batch

In ad-tech, revenue figures for recent days are **not final** when first reported. SSP/DSP reconciliation means the last 2–3 days routinely get corrected as final figures settle. Our production pipeline re-ingests recent days every run.

`redelivery/` contains a second batch that arrived one day later. The last 3 days of events have been re-delivered with corrections; dimension files have also been re-issued.

Inspect both deliveries before deciding how to handle them. The choices you make here are core design decisions — **document them.**

> **Note**: Treat this data as you would data from a production system. Explore it thoroughly. Not all of it is clean.

---

## Your Tasks

### Part 1: Pipeline (30%)

The dimension loaders in `dagster_project/assets/ingest.py` are wired up. The events loader is a stub — the previous engineer didn't finish it. **Finishing the load is your job; verifying what landed against the raw files is non-negotiable.**

**Required:**
- All four ingestion assets materialise successfully
- Data is loaded correctly — verify counts AND values, not just absence of errors
- The pipeline handles both the initial delivery and the redelivery batch correctly
- Running the pipeline twice (idempotency) produces the same final downstream numbers as running it once
- Document what you changed and why in `DESIGN.md`

**Expectations:**
- Validate loaded data against the raw files (row counts per file, revenue sums, distinct IDs)
- Clean, readable Python with proper typing
- Idempotency: re-running the pipeline must not corrupt downstream marts

### Part 2: dbt (30%)

Staging models in `dbt/models/staging/` are started but minimal. Build the marts.

**Required mart:**
- `fct_ad_events_daily` — daily aggregated metrics. **You choose the grain — and justify it.**
  - Metrics required: `impressions`, `clicks`, `revenue_usd`, `fill_rate`

**Design choices that are graded (not just "is there a model"):**

1. **Materialisation strategy.** This pipeline re-ingests recent days every run. Your model must produce correct numbers under that flow. Pick a materialisation strategy and justify it — and show numbers (sums, counts) that prove your choice behaves correctly when the redelivery batch is in play.

2. **Daily aggregation grain.** Decide what an "event day" means for your fact table, and justify it. The right choice in ad-tech is not always the most obvious one.

3. **Dimension change handling.** Some dimension attributes differ between the initial and redelivery files. Slowly-changing-dimension handling is a choice. Pick one, justify it, and demonstrate the impact on at least one downstream metric.

**Required tests:**
- `unique` and `not_null` on primary / surrogate keys (after your dedup)
- At least one `accepted_values` or `relationships` test
- At least one custom singular test (or `dbt_utils.expression_is_true`) that asserts a business invariant of your choosing

**Documentation:**
- Model descriptions and key column docs in `schema.yml`
- Grain of each fact / dim, explicitly stated
- Any business rules you applied

**Nice to have:**
- `dim_publishers`, `dim_campaigns`, `dim_ad_units`
- `fct_publisher_performance` — publisher-day rollup
- Additional metrics: `viewable_impressions`, `ctr`, `viewability_rate`

### Part 3: Revenue Integrity Investigation (40%)

While you build the pipeline you should notice **anomalies that don't show up at the row level.** They emerge from:
- Aggregation across the right grouping dimensions
- Cross-referencing between tables
- Distributional analysis (comparing like-for-like cohorts)

We are **not** giving you a checklist. The goal of this section is to demonstrate that you can look at unfamiliar data, form hypotheses about what's normal, and detect what isn't.

For **each anomaly you find**, document in `DESIGN.md`:

1. **What** the issue is — *quantified*. Identifiers, dates, counts, revenue impact in $ or %.
2. **Why it matters** from a business or revenue-integrity perspective.
3. **How you handled it** in your pipeline (or how you would handle it in production).
4. **The query you used to detect it** — include the actual SQL.

Vague descriptions ("the numbers look weird for some advertisers") will not be credited. Quantification is required: how many events, attributed to whom, costing how much.

> **Hint:** Inspecting individual rows will not find these. Look at distributions. Compare like-for-like cohorts. Ask whether what you're seeing makes physical or commercial sense.

---

## Timebox

**3–5 hours.** Quality over quantity. We don't reward line count.

---

## Evaluation Criteria

| Area | Weight | What we look for |
|------|--------|------------------|
| **Pipeline (Python/Dagster)** | 30% | Correctness of loaded data verified against raw, idempotency, clean fixes |
| **dbt** | 30% | Design choices justified with numbers, dimension handling, business-aware tests |
| **Investigation** | 40% | Number and significance of anomalies found, **quantification**, detection queries, business reasoning |

The investigation section carries the most weight because it is the part hardest to do well without engineering judgement.

### What we don't reward

- Reflexive `unique`/`not_null` tests that don't catch anything specific to this dataset
- Vague observations without dollar/count impact
- "I fixed the bug" without an explanation of how you verified it's actually fixed
- Cleaning the data without showing what you cleaned

### Followup

If your submission is strong, we will book a 30-minute followup to walk through one of your investigation findings live — extending the analysis, looking at edge cases, and discussing how you'd productionise the detection.

---

## Suggested Repo Structure

```
.
├── Makefile
├── README.md
├── DESIGN.md                ← your design document
├── pyproject.toml
├── docker-compose.yml
├── clickhouse/
│   ├── init-db.sql
│   └── users.xml
├── data/
│   ├── raw/                 # raw event + dimension files
│   │   ├── events/          # parquet, one file per UTC day
│   │   ├── diagnostics/     # internal trace partition (parquet)
│   │   ├── publishers.csv
│   │   ├── ad_units.csv
│   │   ├── campaigns/
│   │   └── redelivery/      # second-delivery batch
│   └── external/            # third-party reference exports
├── dagster_project/         ← started pipeline (validate it)
│   ├── definitions.py
│   ├── resources.py
│   └── assets/ingest.py
└── dbt/                     ← dbt project
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── sources.yml
        ├── staging/         ← started
        └── marts/           ← you build these
```

## How we will run it

```bash
make up                       # ClickHouse
make setup                    # Python deps
make dagster                  # materialise ingestion assets
make dbt-deps
make dbt-run
make dbt-test

# We will then re-run the full flow a second time end-to-end to verify
# idempotency — downstream numbers should not change.
```

We verify:
- Raw tables in ClickHouse contain plausible counts (cross-check against the raw files)
- dbt models build and your tests pass
- Re-running the pipeline does not change downstream mart numbers
- Dimension attribute changes are handled per your stated strategy
- Code is clean and the design document tracks your reasoning
