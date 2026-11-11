-- Per-issue cost rollups filter on (company_id, issue_id), which no existing index
-- covered: cost_events_company_heartbeat_run_idx supplied company_id alone and
-- issue_id fell through to a heap filter, so every lookup read the entire table.
-- Measured in prod over a 293s window: 408 sequential scans reading 4,835,245 rows
-- = 11,851 rows per scan against a live count of 11,856, making cost_events the #1
-- table in the database by sequential tuple rate. Plan cost for the dominant tenant
-- drops 599.33 -> 8.30 and both columns become a single index condition.
-- IF NOT EXISTS: this index was applied out-of-band on prod (2026-08-22) with
-- CREATE INDEX CONCURRENTLY to stop the scans immediately; sequential scans on
-- cost_events went to zero and stayed there. Migrations here run inside a
-- transaction (client.ts applyPendingMigrationsManually), so CONCURRENTLY is
-- unavailable -- on a 509-page table the plain build is milliseconds.
CREATE INDEX IF NOT EXISTS "cost_events_company_issue_idx"
  ON "cost_events" ("company_id", "issue_id");
