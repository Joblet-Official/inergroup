# Inergroup Careers MCP server

The server answers searches and widget resource reads from one validated,
read-only SQLite snapshot. Feed refreshes run in a separate child process,
build a new database in the snapshot directory, validate it, and atomically
promote it. A refresh failure never replaces the last-good snapshot.

The catalog is the Inergroup Insourcing Solutions job feed (on-site, full-time
U.S. roles: warehouse and general labor, electricians and skilled trades,
maintenance technicians, and onsite staffing/supervisory positions).

## Search location behavior

Searches cover the full Inergroup catalog by default. The catalog is U.S.-based,
so filter by a U.S. state or region only when explicitly requested in the
current user message (for example "electrician jobs in Texas"). For "near me",
ask for a state or city.

## Build and start

Compile and verify changes with development dependencies installed:

```text
npm ci
npm run build
npm run typecheck
npm test
```

Production starts the checked-in compiled entry point and does not require the
development-only `tsx` or TypeScript packages:

```text
npm ci --omit=dev
npm start
```

Rebuild and commit `dist/index.js` whenever `server/src/index.ts` changes.

## Configuration

The feed source and apply host default to Inergroup's production values and can
be overridden with environment variables:

```text
INERGROUP_FEED_URL=https://joveo-outbound-feeds-prod.s3-accelerate.amazonaws.com/joveo-1a48b557/3b5ca64b.xml
INERGROUP_APPLY_HOST=tnl2.jometer.com
```

`INERGROUP_APPLY_HOST` locks every application link to a single host: any listing
whose apply URL is not HTTPS on that host is rejected. The widget origin used
for the ChatGPT App CSP is fixed to `https://mcp.inergroup.joveo.com`.

## Persistent production storage

On the production host, point both database settings at mounted persistent storage. For
example:

```text
SQLITE_DB_PATH=/var/data/inergroup/jobs.db
SQLITE_SNAPSHOT_DIR=/var/data/inergroup/snapshots
```

`SQLITE_DB_PATH` is retained as a backward-compatible legacy snapshot. New
snapshots and `active-snapshot.json` are stored in `SQLITE_SNAPSHOT_DIR`.
Without a persistent disk, a brand-new instance cannot retain the last-good
snapshot from the previous instance. Run one refresh-owning service instance
per snapshot directory.

## Freshness and validation settings

- `SYNC_INTERVAL_MS`: refresh interval; default 1 hour.
- `INERGROUP_STALE_AFTER_MS`: health becomes `degraded`; default is at least 2 hours.
- `INERGROUP_MAX_STALE_MS`: health becomes `expired` and search stops using the old data; default 24 hours.
- `FEED_FETCH_TIMEOUT_MS`: worker feed timeout; default 10 minutes.
- `INERGROUP_REFRESH_WORKER_TIMEOUT_MS`: hard worker timeout; default feed timeout plus 5 minutes.
- `INERGROUP_SNAPSHOT_RETENTION`: generated snapshots to retain; default 3, minimum 2.
- `INERGROUP_MIN_SNAPSHOT_RETENTION_PERCENT`: minimum size of a refresh relative to the last-good snapshot; default 50. The Inergroup feed is small, so a lower floor tolerates ordinary churn; raise it toward 80 if the feed grows large and stable.
- `INERGROUP_MAX_INVALID_JOB_PERCENT`: maximum malformed/oversized/unsafe job-element percentage; default 1. Intentional content exclusions do not count as invalid.
- `INERGROUP_MIN_PROMOTED_JOBS`: absolute floor for every newly downloaded production snapshot; default 25 for the current small feed. Raise it only after confirming an intentional feed-size increase.
- `MIN_VALID_JOBS`: absolute minimum accepted snapshot size; default 25.
- `MAX_FEED_MB`: maximum downloaded feed size; default 512 MB.
- `INERGROUP_MCP_BODY_LIMIT_BYTES`: maximum MCP JSON request body; default 65536 bytes.
- `INERGROUP_MCP_MAX_CONCURRENT_REQUESTS`: maximum in-flight MCP requests; default 64.

`/health` returns HTTP 200 for fresh or degraded-but-usable snapshots and HTTP
503 only when no valid snapshot exists or the configured maximum age is
exceeded. Widget resources remain available in every state.

Every failed refresh emits a structured JSON log event named
`inergroup_feed_refresh_failed`. Configure a platform log alert for that event
(and for repeated `degraded` health checks) so an operator is notified while the
service continues using its last-good snapshot.

Run the regression checks with:

```text
npm run typecheck
npm test
```
