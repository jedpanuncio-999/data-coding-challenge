# Design Document

> Replace each section with your own analysis. Keep it concise — quality over quantity.

## 1) Pipeline — What You Changed and Why

For each change to `dagster_project/assets/ingest.py` (or any other infrastructure code):

- What was the original behaviour?
- How did you discover the problem? (e.g. row count mismatch, distribution check, value comparison against the raw file)
- What did you change?
- How did you verify the fix?

Be specific. "I made it idempotent" is not enough. "Re-running ingestion previously caused `raw.ad_events` to contain N copies of each redelivered event; I changed X to Y; running twice now produces the same downstream `fct_ad_events_daily.revenue_usd` sum (verified: $XXX)" is the level of specificity we expect.

## 2) dbt — Design Choices

### 2a) Materialisation

Which strategy did you pick for `fct_ad_events_daily` and the staging events model? Why?

- What does your choice do when an event is re-delivered with a corrected revenue value?
- What does your choice do when an event is re-delivered with `revenue_usd = NULL`?
- What does your choice do when ingestion is run twice on identical data?

Show the numbers (sums, counts) that prove your choice produces the right behaviour.

### 2b) Daily aggregation boundary

How does your `fct_ad_events_daily` define "day"? Why?

If your `event_date` differs by publisher timezone, show one publisher where it changes the daily total vs. the naive UTC choice.

### 2c) Dimension handling

How are you handling publisher and campaign attribute changes between the initial and redelivery dimension files?

- Which fields change?
- Does your join from facts to dimensions reflect the value at event-time, or the latest value?
- Why is that the right choice for daily ad-revenue reporting?

### 2d) Tests

For each test you wrote that isn't a basic `unique` / `not_null`: what business invariant does it assert, and what would a failure mean?

## 3) Revenue Integrity Investigation

For each anomaly you found, fill in:

### Finding N: [short title]

- **What:** Quantified description. Specific IDs, date ranges, counts, revenue impact.
- **Why it matters:** Business / commercial implication.
- **How you handled it:** What did you do in the pipeline / what would you do in production.
- **Detection query:**

```sql
-- the actual SQL you ran
```

- **Result table / distribution that confirms it:** (paste the rows or a summary)

Repeat for each finding. There is no fixed number of findings expected — but each one must be quantified, evidenced, and traced to a query. Anything described in vague terms or without numbers will not be credited.

## 4) Trade-offs

- What did you intentionally not implement, and why?
- Where did you take shortcuts that you would not take in production?
- With another four hours, what would you do next?
