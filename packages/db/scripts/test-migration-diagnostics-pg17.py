"""Run with the path to the bundled migrate.mjs. Requires local Docker."""
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], text=True, timeout=60, **kwargs)


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    shutil.copyfile(sys.argv[1], root / "migrate.mjs")
    migrations = root / "migrations"
    (migrations / "meta").mkdir(parents=True)
    tag = "0000_diagnostic_probe"
    entry = dict(idx=0, version="7", when=1771300567463, tag=tag, breakpoints=True)
    (migrations / "meta" / "_journal.json").write_text(json.dumps(dict(version="7", dialect="postgresql", entries=[entry])))
    sql = migrations / (tag + ".sql")
    sql.write_text("SELECT 1;\n--> statement-breakpoint\nDO $$ BEGIN RAISE EXCEPTION USING ERRCODE='08P01', MESSAGE='PRIVATE_CANARY'; END $$;")
    pg = docker("run", "--rm", "-d", "--network", "none", "--tmpfs", "/var/lib/postgresql/data", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17-alpine", capture_output=True, check=True).stdout.strip()
    try:
        docker("exec", pg, "sh", "-ec", "until pg_isready -q -h 127.0.0.1 -U postgres; do sleep 0.2; done", check=True)
        version = docker("exec", pg, "psql", "-U", "postgres", "-Atc", "SHOW server_version_num", capture_output=True, check=True).stdout.strip()
        assert version.startswith("17"), version

        def migrate():
            return docker("run", "--rm", "--network", "container:" + pg, "-v", tmp + ":/app:ro", "-e", "DATABASE_URL=postgres://postgres@127.0.0.1/postgres", "-e", "PAPERCLIP_MIGRATION_DIAGNOSTICS=1", "node:24-bookworm-slim", "node", "/app/migrate.mjs", capture_output=True)

        failed = migrate()
        assert failed.returncode == 1
        assert "PRIVATE_CANARY" not in failed.stderr + failed.stdout
        events = [json.loads(line) for line in failed.stderr.splitlines()]
        context = dict(phase="statement", file=sql.name, statement=2)
        assert events[-2] == dict(event="migration-progress", **context), events
        assert events[-1] == dict(event="migration-failed", last=context, sqlstate="08P01", reason="protocol violation"), events
        sql.write_text("SELECT 1;")
        passed = migrate()
        assert passed.returncode == 0 and "Migrations complete" in passed.stdout, passed.stderr
        repeated = migrate()
        assert repeated.returncode == 0 and "No pending migrations" in repeated.stdout, repeated.stderr
        print(f"PASS PostgreSQL {version}: injected 08P01, redaction, success, no-op rerun")
    finally:
        docker("rm", "-f", pg, check=True, stdout=subprocess.DEVNULL)
