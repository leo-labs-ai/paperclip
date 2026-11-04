-- Better Auth 1.7.3 removed the required "issuer" account field that 1.7.0-1.7.2
-- added (see 0237). New rows no longer write it, so the NOT NULL constraint
-- would reject every sign-up and account link. Relax it and drop the
-- (issuer, account_id) unique index, per the Better Auth 1.7 upgrade guide.
-- The column itself stays so existing values are kept.
--
-- Better Auth 1.7.3+ looks accounts up by (provider_id, account_id) and rejects
-- ambiguous matches. The old index allowed duplicates of that pair under
-- different issuers, so stop the upgrade if any exist instead of silently
-- breaking sign-in for those accounts. Resolve them per the upgrade guide.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM "account"
    GROUP BY "provider_id", "account_id"
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'account has duplicate (provider_id, account_id) pairs; resolve them before upgrading Better Auth (https://www.better-auth.com/docs/guides/1-7-upgrade-guide)';
  END IF;
END $$;--> statement-breakpoint
DROP INDEX IF EXISTS "account_issuer_account_id_uq";--> statement-breakpoint
ALTER TABLE "account" ALTER COLUMN "issuer" DROP NOT NULL;
