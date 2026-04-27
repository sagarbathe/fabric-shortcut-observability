# Enabling OneLake diagnostics (one-time, per monitored workspace)

OneLake diagnostics is enabled per-workspace from the Fabric portal. There is no public REST API for this toggle yet (April 2026), so this is a manual / portal step.

## Steps

1. In the Fabric portal, open the workspace whose OneLake activity you want to capture.
2. **Workspace settings → OneLake → Add diagnostic events to a lakehouse** → toggle **On**.
3. Pick a lakehouse **in the same capacity** as the workspace. Recommended: a dedicated `DiagnosticsLH` per monitored workspace, accessible (read) from the central Observability workspace via a OneLake shortcut.
4. (Optional but recommended) Enable **Immutable diagnostic logs** for tamper-proof WORM retention.
5. Wait up to 1 hour for first events to appear under:
   `<DiagnosticsLH>/Files/DiagnosticLogs/OneLake/Workspaces/<WorkspaceId>/y=YYYY/m=MM/d=DD/h=HH/m=00/PT1H.json`

## Critical behavior — shortcut events

Cross-workspace shortcut access events are emitted in the **target/source-of-truth workspace** (the one that owns the data), **not** the workspace where the shortcut appears. Therefore:

- Always enable diagnostics on workspaces that **own data accessed by shortcuts** (Lakehouse B's workspace).
- Also enable on consumer workspaces (Lakehouse A's workspace) to capture native-path access and shortcut create/delete operations.

## How the central project consumes it

In `config.json → monitored_workspaces[]` list only the **workspace display names**. The bronze notebook (`02_bronze_union_diagnostics`) does the rest:

1. Calls `GET /v1/workspaces/{ws}/items?type=Lakehouse` for each monitored workspace.
2. For each lakehouse, probes `abfss://{wsId}@onelake.dfs.fabric.microsoft.com/{itemId}/Files/DiagnosticLogs/OneLake/Workspaces/` via `notebookutils.fs.ls`.
3. Treats every lakehouse where that probe succeeds as a diagnostics-enabled lakehouse and reads its hourly `PT1H.json` partitions directly over OneLake — no shortcut wiring required.

The identity running the notebook only needs read access to those workspaces (Viewer or higher on the workspace + at least Read on each lakehouse); the OneLake ABFSS reads use that same identity automatically.

## Tenant settings to verify

- `Users can include end-user identifiers in OneLake diagnostic logs` — must be **On** to receive `executingUPN` and `callerIPAddress`.
- Audit-of-audit: `ModifyOneLakeDiagnosticSettings` events are written to the M365 unified audit log — set up an alert on this event so anyone disabling diagnostics is detected.
