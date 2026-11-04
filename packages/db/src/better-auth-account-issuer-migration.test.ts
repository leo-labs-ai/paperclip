/**
 * `account.issuer` history. Migration 0237 added it NOT NULL for Better Auth
 * 1.7.0–1.7.2, with a total backfill (a row the UPDATE misses aborts the
 * upgrade). Better Auth 1.7.3 dropped the field and no longer writes it, so
 * 0287 relaxes the constraint and drops the unique index; otherwise every
 * sign-up fails. This suite rewinds both migrations, seeds pre-upgrade rows,
 * and re-applies them.
 */

import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { getTableConfig } from "drizzle-orm/pg-core";
import postgres from "postgres";
import { afterEach, describe, expect, it } from "vitest";
import { applyPendingMigrations } from "./client.js";
import { authAccounts } from "./schema/auth.js";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./test-embedded-postgres.js";

const MIGRATION_FILES = [
  "0237_better_auth_account_issuer.sql",
  "0287_better_auth_account_issuer_nullable.sql",
];
const UNIQUE_INDEX = "account_issuer_account_id_uq";

// Better Auth's own issuer helpers: `createLocalAccountIssuer("credential")`
// for email/password and `createOAuthAccountIssuer(providerId)` for a social
// provider that declares no issuer of its own. Sign-in looks the credential
// account up by this exact value.
const CREDENTIAL_ISSUER = "local:credential";
const OAUTH_ISSUER_PREFIX = "local:oauth:";

const cleanups: Array<() => Promise<void>> = [];
const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

async function migrationHash(file: string) {
  const content = await fs.promises.readFile(
    new URL(`./migrations/${file}`, import.meta.url),
    "utf8",
  );
  return createHash("sha256").update(content).digest("hex");
}

describe("account issuer schema", () => {
  it("keeps issuer only as a nullable legacy column with no unique index", () => {
    // Better Auth 1.7.3+ never writes issuer; a NOT NULL column rejects sign-up.
    const config = getTableConfig(authAccounts);
    const issuer = config.columns.find((column) => column.name === "issuer");
    expect(issuer).toBeDefined();
    expect(issuer?.notNull).toBe(false);
    expect(config.indexes.find((index) => index.config.name === UNIQUE_INDEX)).toBeUndefined();
  });
});

describeEmbeddedPostgres("account issuer migration", () => {
  // Reverse registration order, one at a time. The raw `postgres` client is not
  // registered with the module's client registry, so the cluster teardown does
  // not close it. Stopping the cluster while that client is still draining kills
  // the backend socket under a queued write and can crash the runner.
  afterEach(async () => {
    for (const cleanup of cleanups.splice(0).reverse()) {
      await cleanup();
    }
  });

  it("backfills pre-upgrade rows, then relaxes issuer for Better Auth 1.7.3+", async () => {
    const database = await startEmbeddedPostgresTestDatabase("paperclip-account-issuer-");
    cleanups.push(database.cleanup);
    const sql = postgres(database.connectionString, { max: 1 });
    cleanups.push(async () => sql.end());

    // Rewind to the pre-upgrade shape: no issuer column, no unique index.
    for (const file of MIGRATION_FILES) {
      await sql`DELETE FROM "drizzle"."__drizzle_migrations" WHERE "hash" = ${await migrationHash(file)}`;
    }
    await sql`DROP INDEX IF EXISTS ${sql(UNIQUE_INDEX)}`;
    await sql`ALTER TABLE "account" DROP COLUMN IF EXISTS "issuer"`;

    const credentialUserId = `user-${randomUUID()}`;
    const socialUserId = `user-${randomUUID()}`;
    const credentialAccountId = `account-${randomUUID()}`;
    const githubAccountId = `account-${randomUUID()}`;
    const googleAccountId = `account-${randomUUID()}`;

    await sql`
      INSERT INTO "user" ("id", "name", "email", "created_at", "updated_at")
      VALUES
        (${credentialUserId}, 'Credential User', 'credential@example.com', now(), now()),
        (${socialUserId}, 'Social User', 'social@example.com', now(), now())
    `;
    await sql`
      INSERT INTO "account"
        ("id", "account_id", "provider_id", "user_id", "password", "created_at", "updated_at")
      VALUES
        (${credentialAccountId}, ${credentialUserId}, 'credential', ${credentialUserId}, 'hashed', now(), now()),
        (${githubAccountId}, 'github-subject-1', 'github', ${socialUserId}, NULL, now(), now()),
        (${googleAccountId}, 'google-subject-1', 'google', ${socialUserId}, NULL, now(), now())
    `;

    await applyPendingMigrations(database.connectionString);

    const rows = await sql<{ id: string; issuer: string }[]>`
      SELECT "id", "issuer" FROM "account" ORDER BY "id"
    `;
    const issuerById = new Map(rows.map((row) => [row.id, row.issuer]));
    expect(issuerById.get(credentialAccountId)).toBe(CREDENTIAL_ISSUER);
    // A backfill restricted to `provider_id = 'credential'` leaves these NULL,
    // and SET NOT NULL then aborts the upgrade.
    expect(issuerById.get(githubAccountId)).toBe(`${OAUTH_ISSUER_PREFIX}github`);
    expect(issuerById.get(googleAccountId)).toBe(`${OAUTH_ISSUER_PREFIX}google`);

    const [issuerColumn] = await sql<{ is_nullable: string }[]>`
      SELECT "is_nullable" FROM "information_schema"."columns"
      WHERE "table_name" = 'account' AND "column_name" = 'issuer'
    `;
    expect(issuerColumn?.is_nullable).toBe("YES");

    const indexes = await sql<{ indexname: string }[]>`
      SELECT "indexname" FROM "pg_indexes"
      WHERE "tablename" = 'account' AND "indexname" = ${UNIQUE_INDEX}
    `;
    expect(indexes).toHaveLength(0);

    // Better Auth 1.7.3+ inserts accounts without an issuer.
    await sql`
      INSERT INTO "account"
        ("id", "account_id", "provider_id", "user_id", "created_at", "updated_at")
      VALUES (${`account-${randomUUID()}`}, 'github-subject-2', 'github', ${socialUserId}, now(), now())
    `;
  }, 30_000);

  it("refuses to drop the issuer index while (provider_id, account_id) pairs collide", async () => {
    const database = await startEmbeddedPostgresTestDatabase("paperclip-account-issuer-dup-");
    cleanups.push(database.cleanup);
    const sql = postgres(database.connectionString, { max: 1 });
    cleanups.push(async () => sql.end());

    // Rewind only 0287 and restore the 1.7.2 shape it relaxes.
    await sql`DELETE FROM "drizzle"."__drizzle_migrations" WHERE "hash" = ${await migrationHash(MIGRATION_FILES[1]!)}`;
    await sql`CREATE UNIQUE INDEX IF NOT EXISTS ${sql(UNIQUE_INDEX)} ON "account" ("issuer", "account_id")`;

    const userId = `user-${randomUUID()}`;
    await sql`
      INSERT INTO "user" ("id", "name", "email", "created_at", "updated_at")
      VALUES (${userId}, 'Dup User', 'dup@example.com', now(), now())
    `;
    // Valid under the issuer index, ambiguous for Better Auth 1.7.3+.
    await sql`
      INSERT INTO "account"
        ("id", "issuer", "account_id", "provider_id", "user_id", "created_at", "updated_at")
      VALUES
        (${`account-${randomUUID()}`}, 'https://a.example', 'subject-1', 'oidc', ${userId}, now(), now()),
        (${`account-${randomUUID()}`}, 'https://b.example', 'subject-1', 'oidc', ${userId}, now(), now())
    `;

    await expect(applyPendingMigrations(database.connectionString)).rejects.toThrow();
    const indexes = await sql<{ indexname: string }[]>`
      SELECT "indexname" FROM "pg_indexes"
      WHERE "tablename" = 'account' AND "indexname" = ${UNIQUE_INDEX}
    `;
    expect(indexes).toHaveLength(1);
  }, 30_000);
});
