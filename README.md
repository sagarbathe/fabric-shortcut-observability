# Fabric Shortcut Access Observability

Provides an end-to-end Fabric-native solution to track and explain:

  -  Who accessed or copied shortcut-backed data, when, from where, and the true underlying source.**.

  -  Built entirely on native Fabric capabilities:

| Layer | Native capability |
|---|---|
| Raw access events | OneLake diagnostics (JSON, hourly) |
| Shortcut target metadata | Fabric REST `OneLake Shortcuts` API |
| Storage | Delta tables in a dedicated Observability Lakehouse |
| Compute | Fabric Spark notebooks |
| Orchestration | Fabric Data Pipeline (schedule the four notebooks) |
| Auth | `notebookutils.credentials.getToken("pbi")` (run-as-user) |

## Architecture

```
Workspace A (Lakehouse A, shortcuts) ─┐
Workspace B (Lakehouse B, real data) ─┼─► OneLake diag JSON (Files/DiagnosticLogs/...)
Workspace N (other target lakehouses) ┘            │
                                                   ▼
              ┌──────────────────────────────────────────────────┐
              │ Observability Workspace (this project)            │
              │  Lakehouse: ObservabilityLH                       │
              │   ├── bronze_onelake_raw_events     (Delta)       │
              │   ├── dim_shortcut_map              (Delta)       │
              │   ├── silver_onelake_enriched_access (Delta)      │
              │   └── gold_onelake_enriched_access   (Delta)      │
              │  Notebooks:                                       │
              │   01_setup                                        │
              │   02_bronze_union_diagnostics                     │
              │   03_dim_shortcuts_map                            │
              │   04_silver_enriched_access                       │
              │   05_gold_queries                                 │
              └──────────────────────────────────────────────────┘
```

## Setup

1. **Create / pick a workspace** that will host this observability project (separate from monitored workspaces, locked-down access).
2. Open [config.json](config.json) and fill in:
   - `observability_workspace_name` — where the notebooks + lakehouse live
   - `observability_lakehouse_name`
   - `monitored_workspaces[]` — list of **workspace display names** whose OneLake diagnostics you've enabled. The bronze notebook auto-discovers every lakehouse in those workspaces that has diagnostics enabled (i.e. has a `Files/DiagnosticLogs/OneLake/Workspaces/` folder) — you don't need to name lakehouses.
3. **Enable OneLake diagnostics** on every monitored workspace (Workspace settings → OneLake → "Add diagnostic events to a lakehouse"). Critical: enable on **target** workspaces (where shortcut data physically lives) — that is where shortcut access events are emitted. See [`setup/enable-onelake-diagnostics.md`](setup/enable-onelake-diagnostics.md).
4. Import all `.ipynb` files into the observability workspace.
5. **Attach the observability lakehouse as the default lakehouse on every notebook.** In each of the five notebooks (`01_setup` … `05_gold_queries`): open the notebook → in the **Lakehouses** pane on the left, click **Add** → **Existing lakehouse** → pick `ObservabilityLH` (the one named in `observability_lakehouse_name`) → set it as the **default** (pin/star icon). All `spark.table(...)` and `saveAsTable(...)` calls in the notebooks resolve against the default lakehouse, so this step is required — there is no API to set it programmatically today.
6. Upload [config.json](config.json) to the observability lakehouse at `Files/config.json` (or attach it to each notebook as a notebook resource under `builtin/config.json`).
7. Run `01_setup.ipynb` once to create the Delta tables.
8. Schedule `02`, `03`, `04`, `05` hourly via a Fabric Data Pipeline (in this order). `05` rebuilds the `gold_onelake_enriched_access` aggregate table that the sample queries use.
9. Attach the SQL analytics endpoint of `ObservabilityLH` to Power BI for dashboards (or run the sample queries in `05_gold_queries.ipynb` directly).

## What you get

For every read/copy of a shortcut-backed file, a row in `silver_onelake_enriched_access` with:

- `executingUPN`, `executingPrincipalType`, `callerIPAddress`
- `accessStartTime`, `originatingApp` (azcopy / Spark / Pipeline / Storage Explorer / …)
- `operationName`, `operationCategory`
- `shortcut_path_consumer` (full OneLake path the consumer hit, including `_delta_log`/parquet files)
- `target_workspace_name`, `target_item_name`, `target_path` (resolved Lakehouse B coords)
- `target_type` (`OneLake` | `AdlsGen2` | `S3` | `GCS` | `AzureBlob` | `Dataverse` | …)
- `correlationId` (join key to Spark / Warehouse engine logs)

### `gold_onelake_enriched_access`

OneLake diagnostics is **per-file**: a single logical SELECT or COPY can produce dozens of silver rows (one per `_delta_log` entry, per parquet file, per stats file). The **05_gold_queries** notebook collapses silver to one row per unique logical operation by `GROUP BY` on the user/operation/timestamp/target dimensions, and adds:

- `shortcut_consumer_path` — the user-meaningful path (`Tables/<table>` or `Files/<folder>`) extracted from `shortcut_path_consumer`, so you can group/filter by the table being touched without the underlying `_delta_log/...` noise.

Use `gold_onelake_enriched_access` for dashboards and ad-hoc analysis; use `silver_onelake_enriched_access` only when you need raw per-file granularity.

## Separating reads (SELECT) from copies

OneLake diagnostics records the underlying storage-API operation, not the SQL/Spark statement. Two columns on `silver_onelake_enriched_access` let you split user intent:

- `operationCategory` — coarse bucket: `Read` / `Write` / `Delete` / `Other`.
- `operationName` — exact storage op: `GetBlob`, `ReadFile`, `PutBlob`, `PutBlobFromURL`, `CopyBlob`, `AbortCopyBlob`, `DeleteBlob`, …

Combined with `originatingApp` (azcopy / Spark / Storage Explorer / pipeline / …) you can cleanly distinguish a `SELECT` from a bulk download from a server-side copy:

```sql
-- 1) Reads (SELECT-class: Spark reads, T-SQL SELECT, DirectLake refresh, lakehouse preview)
SELECT *
FROM   silver_onelake_enriched_access
WHERE  operationCategory = 'Read'
  AND  operationName NOT IN ('PutBlobFromURL','CopyBlob');

-- 2) Server-side copies (highest-signal exfiltration events)
SELECT *
FROM   silver_onelake_enriched_access
WHERE  operationName IN ('PutBlobFromURL','CopyBlob','AbortCopyBlob');

-- 3) External-tool downloads (azcopy, Storage Explorer, rclone, curl, custom scripts)
SELECT *
FROM   silver_onelake_enriched_access
WHERE  operationCategory = 'Read'
  AND  originatingApp RLIKE '(?i)(azcopy|storage.explorer|rclone|curl|python-requests)';
```

To recover the actual `SELECT … FROM …` text behind a row, join on `correlationId` to Spark Log Analytics / Workspace Monitoring `SparkLogs` (notebook + SJD queries) or Warehouse `queryinsights.exec_requests_history` (T-SQL).

## `config.json` reference

| Key | Purpose |
|---|---|
| `observability_workspace_name` | Workspace that hosts these notebooks and the central lakehouse. Keep separate from monitored workspaces; restrict membership. |
| `observability_lakehouse_name` | Lakehouse inside the observability workspace where bronze / dim / silver Delta tables are written. |
| `monitored_workspaces[]` | Array of workspace **display names** (strings) you want visibility into. Both `02_bronze_union_diagnostics` and `03_dim_shortcuts_map` enumerate **every lakehouse** in each workspace via `GET /v1/workspaces/{ws}/items?type=Lakehouse`. Bronze keeps only those lakehouses whose `Files/DiagnosticLogs/OneLake/Workspaces/` folder exists (i.e. OneLake diagnostics is enabled and pointing at that lakehouse). No per-lakehouse config required. |

Legacy shape `[{ "workspace_name": "..." }]` is still accepted by the notebooks for backwards compatibility, but plain strings are preferred.

| Key | Purpose |
|---|---|
| `tables.bronze` / `tables.dim_shortcuts` / `tables.silver` | Delta table names. Change only if you have a naming convention. The gold aggregate table (`gold_onelake_enriched_access`) is created directly by `05_gold_queries.ipynb`. |
| `ingestion.lookback_hours` | How many hours back the bronze notebook scans diagnostic JSON on each run. See **Tuning `lookback_hours`** below. |
| `ingestion.diagnostics_root_path` | Folder layout under each diagnostics lakehouse. The OneLake diagnostics feature writes to `Files/DiagnosticLogs/OneLake/Workspaces/<wsId>/y=…/m=…/d=…/h=…/m=…/PT1H.json`. Don't change unless Microsoft alters the layout. |
| `fabric_api.base_url` | Fabric REST endpoint (`https://api.fabric.microsoft.com/v1`). |
| `fabric_api.token_audience` | Audience passed to `notebookutils.credentials.getToken()`. `pbi` is the correct value for Fabric REST. |

### Tuning `ingestion.lookback_hours`

`lookback_hours` controls how far back the **02_bronze_union_diagnostics** notebook scans OneLake diagnostic JSON files on each run.

- The notebook builds an explicit list of hourly partition paths (`y=YYYY/m=MM/d=DD/h=HH/m=*/PT1H.json`) covering `now − lookback_hours … now`, instead of recursively listing the whole tree (which would be slow at scale).
- Re-ingesting the same hour is **safe**: bronze uses a `MERGE` keyed on `(correlationId, accessStartTime, operationName, Resource)`, so files seen on a previous run produce zero duplicates.
- OneLake diagnostics has up to ~1h of ingestion delay (occasionally more), so the lookback window must comfortably exceed your schedule interval.

**Rule of thumb:** `lookback_hours ≥ schedule_interval_hours + 2`.

| Pipeline schedule | Recommended `lookback_hours` |
|---|---|
| Every 15 min | `2` |
| Hourly (default) | `6` |
| Every 6 hours | `10` |
| Daily | `30` |
| One-time backfill | Temporarily raise (e.g. `720` = 30 days), run `02` once, then restore. |

## Why this design

- **OneLake diagnostics emits shortcut events from the *target* workspace**, not the consumer — so you must enable diagnostics on workspaces that own data (Lakehouse B), not just where the shortcut appears (Lakehouse A). The bronze notebook unions both.
- **`dim_shortcut_map` is overwritten on every run** with the current snapshot of all shortcuts returned by the Fabric REST API. There is no history retained — the silver join always resolves against today's shortcut mapping.
- **MERGE-based ingest** instead of append makes the bronze notebook idempotent and re-runnable inside the lookback window without producing duplicates.
- **Run-as-user auth** via `notebookutils.credentials.getToken('pbi')` avoids storing secrets. The executing identity needs at least `Contributor` on each monitored workspace to call the Shortcuts REST API.
- **`correlationId` is preserved end-to-end** so you can later join silver to Spark Log Analytics or Warehouse `queryinsights.exec_requests_history` to turn a single OneLake `FabricWorkloadAccess` event into per-statement lineage.
- **Notebooks are generated from `build_notebooks.py`** so cell logic lives in reviewable Python, not opaque ipynb JSON. Re-run `python build_notebooks.py` after any edit there.

## Operational notes

- Turn on **Immutable diagnostic logs** for each monitored workspace's diagnostics lakehouse (WORM, irreversible — choose retention carefully).
- Set up an alert on the M365 unified audit event `ModifyOneLakeDiagnosticSettings` — anyone disabling diagnostics is your single biggest blind spot.
- Tenant setting **"Users can include end-user identifiers in OneLake diagnostic logs"** must be **On** to receive `executingUPN` and `callerIPAddress`. If it's Off (compliance reasons), those columns will be `null` in silver.
- For external shortcuts (S3 / ADLS Gen2 / GCS), pair this dataset with the target system's native audit log, keyed by `target_connection_id` from `dim_shortcut_map`.

## References

- OneLake diagnostics: https://learn.microsoft.com/fabric/onelake/onelake-diagnostics-overview
- Shortcuts REST API: https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts
- Workspace monitoring (complementary): https://learn.microsoft.com/fabric/fundamentals/workspace-monitoring-overview
