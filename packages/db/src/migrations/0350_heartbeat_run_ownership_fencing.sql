-- Monotonic fence source for active-active run ownership (plan 003). One
-- global sequence, not per-run: a claim/renewal/takeover mints the next
-- value here, so any two fence values are comparable across every run,
-- which is sufficient for a strict-greater-than takeover check.
CREATE SEQUENCE "heartbeat_run_fence_seq" AS bigint;--> statement-breakpoint
ALTER TABLE "heartbeat_run_events" ADD COLUMN "fence" bigint;--> statement-breakpoint
ALTER TABLE "heartbeat_runs" ADD COLUMN "owner_token" text;--> statement-breakpoint
ALTER TABLE "heartbeat_runs" ADD COLUMN "fence" bigint;--> statement-breakpoint
ALTER TABLE "heartbeat_runs" ADD COLUMN "lease_expires_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "heartbeat_runs" ADD COLUMN "lease_renewed_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "heartbeat_runs" ADD COLUMN "claim_attempt" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
CREATE INDEX "heartbeat_runs_status_lease_expires_idx" ON "heartbeat_runs" USING btree ("status","lease_expires_at");