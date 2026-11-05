# Database backup image proof

Run this check against the exact immutable image produced by the normal Docker
workflow. It uses a disposable PostgreSQL 17 server. It must not use a production
connection string, volume, or application settings. The script checks the native
client version, gzip integrity, private final publication, and a native restore.

For a Docker runner, set `image` to the verified `ghcr.io/...@sha256:...` image:

```sh
set -euo pipefail
network="backup-proof-$(date +%s)-$$"
database="${network}-db"
trap 'docker rm -fv "$database" >/dev/null 2>&1 || true; docker network rm "$network" >/dev/null 2>&1 || true' EXIT
docker network create --internal "$network" >/dev/null
docker run -d --name "$database" --network "$network" --network-alias backup-db \
  -e POSTGRES_PASSWORD=synthetic-proof-password postgres:17-alpine >/dev/null
ready=false
for attempt in $(seq 1 60); do
  if docker exec "$database" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
test "$ready" = true
docker run --rm --user node --network "$network" --entrypoint sh "$image" -ec '
  set -- /app/node_modules/.pnpm/tsx@*/node_modules/tsx/dist/loader.mjs
  test "$#" -eq 1 && test -f "$1"
  exec node --import "$1" /app/scripts/assert-database-backup.mjs
'
```

For a Kubernetes canary, use the same immutable app image and script with a
PostgreSQL 17 sidecar in one disposable pod. Set `PAPERCLIP_BACKUP_PROOF_HOST` to
`127.0.0.1`. Wait for TCP readiness before the script starts. Disable service
account token mounting; use only ephemeral storage and a bounded deadline. Do not
mount production secrets or PVCs. Delete the pod and confirm deletion after the
check. Save the image digest, exit status, and the script's summary line. Do not
save SQL contents or connection credentials in the proof report.

This check is separate from normal required CI. Do not treat a successful image
build as a successful backup/restore check.

Native libpq connection arguments retain their established behavior. This change does not add credential-transport hardening or replace libpq URL parsing.
