-- Better Auth 1.7.3 removed the required "issuer" account field that 1.7.0-1.7.2
-- added (see 0237). New rows no longer write it, so the NOT NULL constraint
-- would reject every sign-up and account link. Relax it and drop the
-- (issuer, account_id) unique index, per the Better Auth 1.7 upgrade guide.
-- The column itself stays so existing values are kept.
DROP INDEX IF EXISTS "account_issuer_account_id_uq";--> statement-breakpoint
ALTER TABLE "account" ALTER COLUMN "issuer" DROP NOT NULL;
