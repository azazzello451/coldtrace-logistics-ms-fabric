# ColdTrace Logistics — Technical Reference

Companion to the [README](../README.md): the README is the pitch, this is the lookup reference — schemas, pipeline mechanics, runbook. ColdTrace deliberately covers a broad (not exhaustive) slice of Fabric concepts, built as a portfolio project by a career-changer without prior on-the-job Fabric experience.

## Lakehouse layout

```
Files/
  landing/                    nightly drop zone, emptied by Copy Data at 00:00
  bronze/orders/               raw CSVs + archive/
Tables/
  orders_pending_routing       silver_orders            silver_vehicle_movements
  silver_pipeline_audit        quarantine_orders
  gold.Fact_Sales               gold.Dim_Customer / Dim_Product / Dim_Date
  gold.fact_vehicle_movements   gold.dim_route
```

`orders_pending_routing` is a **staging table**, overwritten in full on every run by `Orders_Bronze_to_Silver` — it holds every row of the current file, valid and invalid alike, tagged `IsValid`/`FailureReason`. It's the handoff point both downstream notebooks read from and is never queried directly by reports.

## Schema reference

| Table | Layer | Grain | Key columns |
|---|---|---|---|
| `bronze/orders/*.csv` | Bronze | one row per order line, as received | schema-on-read, inferred |
| `orders_pending_routing` | Silver (staging) | one row per line of the *current* file, tagged | all source columns + `IsValid`, `FailureReason` — overwritten every run |
| `silver_orders` | Silver | one row per order (deduped, merged) | `OrderId` (PK), `CustomerId`, `Quantity`, … — partitioned by `DeliveryRegion` |
| `silver_pipeline_audit` | Silver | one row per run per source file | `SourceFile`, `RunTimestamp`, `InitialRows`, `CleanRows`, `NullsRemoved`, `OutliersRemoved`, `DuplicatesRemoved`, `QualityScore`, `PassedThreshold` |
| `quarantine_orders` | Silver (quarantine) | one row per line of a rejected file (append-only) | all source columns + `OverallReason`, `QuarantinedAt` |
| `gold.Fact_Sales` | Gold | one row per order | `OrderId`, `CustomerId`, `ProductId`, `OrderDate`, `Quantity`, `UnitPrice`, `TotalAmount`, `Status`, `DeliveryRegion` |
| `gold.Dim_Customer` / `Dim_Product` | Gold | one row per customer / product | passthrough from the shortcut reference tables |
| `gold.Dim_Date` | Gold | one row per calendar day, spanning the order-date range ±7 days | `Date` (PK), `Year`, `Quarter`, `Month`, `MonthName`, `Weekday`, `IsWeekend` |
| `silver_vehicle_movements` | Silver | one row per movement event, deduped | `MovementId` (PK, SHA-256 hash), `VehicleTripId`, `RouteId`, `CheckpointId`, `EventTimestamp`, `PlannedArrival`, `VehicleState` |
| `gold.fact_vehicle_movements` | Gold | one row per movement | `MovementId`, `RouteId`, `EventDate`, `DelayMinutes`, `IsDelayed`, `IsArrival` |
| `gold.dim_route` | Gold | one row per route | `RouteId` (PK), `TripCount`, `CheckpointCount` |

`MovementId` is deterministic, not a random UUID: `sha2(concat_ws("|", VehicleTripId, CheckpointId, EventTimestamp, coalesce(VehicleState,"")), 256)` — the same event always hashes to the same ID, which is what makes the Silver merge idempotent.

## Pipelines

| Pipeline | Trigger | Steps | On failure |
|---|---|---|---|
| `PL_ColdTrace_Master` | nightly, ~00:00 | Copy Data (landing→Bronze) → `Orders_Bronze_to_Silver` (cleanse, score, stage) → If Condition on quality score → `Quarantine_And_Alert_Execution` *or* `Silver_Merge_Execution` → `Build_Gold_Sales_Tables` | no auto-retry; rerun the failed notebook manually (no "rerun from failed activity" in this environment) |
| `PL_ColdTrace_Vehicle_Telemetry` | continuous via Eventstream | `Merge_Vehicle_Movements_To_Silver` → `Build_Gold_Vehicle_Tables` | same manual-rerun caveat |
| `PL_ColdTrace_Storage_Maintenance` | weekly, Sun 02:00 | `OPTIMIZE ... ZORDER BY` + `VACUUM RETAIN 168 HOURS` across Silver/Gold | idempotent, safe to rerun |

## Notebooks

| Notebook | Reads | Writes | Does |
|---|---|---|---|
| `Orders_Bronze_to_Silver` | Bronze CSVs | `orders_pending_routing` (overwrite), `silver_pipeline_audit` (append) | watermark-based new-file detection, schema-drift handling, row-level validation, quality score, archives the raw file |
| `Silver_Merge_Execution` | `orders_pending_routing` (valid rows only) | `silver_orders` | idempotent upsert on `OrderId` — runs only when the quality gate passes |
| `Quarantine_And_Alert_Execution` | `orders_pending_routing` (all rows) | `quarantine_orders` (append) | quarantines the *entire* staged file — runs only when the quality gate fails |
| `Build_Gold_Sales_Tables` | `silver_orders`, shortcut reference tables | `gold.Fact_Sales`, `gold.Dim_Customer`, `gold.Dim_Product`, `gold.Dim_Date` | builds the sales star schema, `Dim_Date` regenerated to span the order-date range |
| `Merge_Vehicle_Movements_To_Silver` | `bronze_streaming.vehicle_movements` | `silver_vehicle_movements` | parse timestamps, extract vehicle state, validate, dedupe, merge by `MovementId` (insert-only — events are immutable) |
| `Build_Gold_Vehicle_Tables` | `silver_vehicle_movements` | `gold.fact_vehicle_movements`, `gold.dim_route` | delay calc (event time vs. planned arrival), `IsDelayed`/`IsArrival` flags, full rebuild each run |
| `Maintain_Delta_Tables` | — | in place | weekly `OPTIMIZE ZORDER` on the large fact tables + `VACUUM` across Silver and Gold |

## Quality gate

`QualityScore = CleanRows / InitialRows × 100`, threshold **70**. `Orders_Bronze_to_Silver` tags every row `IsValid`/`FailureReason` and stages the full file (valid and invalid rows together) in `orders_pending_routing` — nothing is dropped at this step. The pipeline's If Condition then branches on the score:

- **≥ 70** → `Silver_Merge_Execution` keeps only the valid rows and merges them into `silver_orders`
- **< 70** → `Quarantine_And_Alert_Execution` treats the whole staged file as untrusted and appends *every* row (valid and invalid alike) to `quarantine_orders`, plus an email alert

Either branch runs after `orders_pending_routing` already reflects the current file; every run — pass or fail — logs one row to `silver_pipeline_audit`, feeding `Rpt_Data_Health`.

## Security

`SM_ColdTrace_Sales` — 4 RLS roles, one per Regional Manager, each filtered to their own region. Defined once at the model, inherited by every report built on it.

## Runbook

- **`PowerBIEntityNotFound`** on a notebook activity → the notebook lost its default Lakehouse attachment (can happen after bulk content edits) → reconnect the Lakehouse in notebook settings, run manually.
- **`Parquet file ... not found in OneLake`** after dropping/recreating a table → Direct Lake is pointing at a stale file version → manual "Refresh now" on the semantic model.
- **No "View As" for RLS testing** on trial capacity → add the test account as a workspace Member temporarily instead.
- **Empty `landing/`** → pipeline still runs safely, 0 rows processed (idempotent by design).

## Setup from scratch

1. Fabric workspace (trial or paid capacity)
2. Lakehouse with `landing/`, `bronze/` folders
3. OneLake Shortcut to the reference-data source
4. Notebooks, each attached to the Lakehouse
5. Pipelines: Copy Data → Notebook → If Condition → Email
6. Eventstream + Eventhouse for telemetry
7. Semantic models + RLS roles
8. Power BI reports
9. Schedule triggers (nightly master, weekly maintenance)

## Limitations

See README → *What a production rollout would still need*.
