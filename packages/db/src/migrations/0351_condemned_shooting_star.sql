-- Monotonic fence source for active-active plugin-job occurrence claims
-- (plan 004), mirroring heartbeat_run_fence_seq from plan 003: one global
-- sequence, not per-occurrence, so any two fence values are comparable
-- across every occurrence and a stale holder's writes can always be
-- detected by comparing against the fence currently on the row.
CREATE SEQUENCE "plugin_job_occurrence_fence_seq" AS bigint;--> statement-breakpoint
CREATE TABLE "plugin_job_occurrences" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"job_id" uuid NOT NULL,
	"plugin_id" uuid NOT NULL,
	"kind" text NOT NULL,
	"scheduled_for" timestamp with time zone,
	"owner_token" text,
	"fence" bigint,
	"lease_expires_at" timestamp with time zone,
	"lease_renewed_at" timestamp with time zone,
	"claim_attempt" integer DEFAULT 0 NOT NULL,
	"acknowledged_at" timestamp with time zone,
	"status" text DEFAULT 'pending' NOT NULL,
	"resolved_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "plugin_job_runs" ADD COLUMN "occurrence_id" uuid;--> statement-breakpoint
ALTER TABLE "plugin_job_occurrences" ADD CONSTRAINT "plugin_job_occurrences_job_id_plugin_jobs_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."plugin_jobs"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "plugin_job_occurrences" ADD CONSTRAINT "plugin_job_occurrences_plugin_id_plugins_id_fk" FOREIGN KEY ("plugin_id") REFERENCES "public"."plugins"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "plugin_job_occurrences_job_idx" ON "plugin_job_occurrences" USING btree ("job_id");--> statement-breakpoint
CREATE INDEX "plugin_job_occurrences_plugin_idx" ON "plugin_job_occurrences" USING btree ("plugin_id");--> statement-breakpoint
CREATE INDEX "plugin_job_occurrences_status_lease_expires_idx" ON "plugin_job_occurrences" USING btree ("status","lease_expires_at");--> statement-breakpoint
CREATE UNIQUE INDEX "plugin_job_occurrences_scheduled_unique_idx" ON "plugin_job_occurrences" USING btree ("job_id","scheduled_for") WHERE "plugin_job_occurrences"."kind" = 'scheduled';--> statement-breakpoint
ALTER TABLE "plugin_job_runs" ADD CONSTRAINT "plugin_job_runs_occurrence_id_plugin_job_occurrences_id_fk" FOREIGN KEY ("occurrence_id") REFERENCES "public"."plugin_job_occurrences"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "plugin_job_runs_occurrence_idx" ON "plugin_job_runs" USING btree ("occurrence_id");