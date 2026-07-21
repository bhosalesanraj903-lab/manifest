# Deploy runbook (R4)

Target: any always-on box with Docker (Linux server, Mac mini, home server).

## First deploy

```bash
git clone https://github.com/bhosalesanraj903-lab/manifest.git /opt/manifest
cd /opt/manifest
cat > .env <<EOF
AISSTREAM_API_KEY=<key from aisstream.io>
SLACK_WEBHOOK_URL=<incoming-webhook from Slack app config>
AIRFLOW_ADMIN_PASSWORD=<pick one>
EOF
docker compose -f deploy/docker-compose.prod.yml --env-file .env up -d
```

Linux: also install the systemd unit so the stack starts at boot:

```bash
sudo cp deploy/manifest.service /etc/systemd/system/
sudo systemctl enable --now manifest
```

Mac: enable "Start Docker Desktop when you sign in" instead; `restart:
unless-stopped` brings all containers back once the daemon is up.

## Acceptance check (run after every deploy AND after a reboot)

1. `docker compose -f deploy/docker-compose.prod.yml ps` — five services Up.
2. Airflow http://<box>:8080 — `carrier_normalize` unpaused, next run scheduled.
3. `curl -s localhost:9091/metrics | grep manifest_` — metrics from last run.
4. Grafana http://<box>:3000 — "Manifest Ops" dashboard populated.
5. AIS: `wc -l data/bronze/ais/$(date -u +%F)/positions.ndjson` twice a minute
   apart — count grows (consumer reconnected after the reboot).
6. Pull the power once (game day R11 rehearses this): repeat 1-5 unattended.

## Morning triage

See `triage.md` — 5 checks, < 5 minutes.

## Upgrading

```bash
cd /opt/manifest && git pull
docker compose -f deploy/docker-compose.prod.yml up -d --build
```
