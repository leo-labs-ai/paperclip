CREATE TABLE "durable_objects" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"company_id" uuid,
	"kind" text NOT NULL,
	"provider" text NOT NULL,
	"backend_id" text NOT NULL,
	"object_key" text NOT NULL,
	"content_type" text NOT NULL,
	"byte_size" bigint NOT NULL,
	"sha256" text NOT NULL,
	"version" text,
	"etag" text,
	"status" text DEFAULT 'committed' NOT NULL,
	"corruption_reason" text,
	"verified_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "durable_objects_kind_check" CHECK ("durable_objects"."kind" in ('asset', 'run_log', 'database_backup')),
	CONSTRAINT "durable_objects_status_check" CHECK ("durable_objects"."status" in ('committed', 'corrupt', 'deleted')),
	CONSTRAINT "durable_objects_scope_check" CHECK ((("durable_objects"."company_id" is null and "durable_objects"."object_key" like 'system/%') or ("durable_objects"."company_id" is not null and "durable_objects"."object_key" like "durable_objects"."company_id"::text || '/%'))),
	CONSTRAINT "durable_objects_corruption_check" CHECK ((("durable_objects"."status" in ('committed', 'deleted') and "durable_objects"."corruption_reason" is null) or ("durable_objects"."status" = 'corrupt' and "durable_objects"."corruption_reason" is not null))),
	CONSTRAINT "durable_objects_sha256_check" CHECK ("durable_objects"."sha256" ~ '^[0-9a-f]{64}$'),
	CONSTRAINT "durable_objects_byte_size_check" CHECK ("durable_objects"."byte_size" >= 0)
);
--> statement-breakpoint
ALTER TABLE "durable_objects" ADD CONSTRAINT "durable_objects_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "durable_objects_object_key_uq" ON "durable_objects" USING btree ("backend_id","object_key");--> statement-breakpoint
CREATE INDEX "durable_objects_company_kind_created_idx" ON "durable_objects" USING btree ("company_id","kind","created_at");--> statement-breakpoint
CREATE INDEX "durable_objects_status_updated_idx" ON "durable_objects" USING btree ("status","updated_at");
