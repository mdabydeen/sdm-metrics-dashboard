# SDM Metrics Dashboard

A lightweight metrics dashboard for tracking software delivery velocity, quality, and team health across a technology organization.

**Stack:** Python (ingestion) + SQLite (data) + Grafana (dashboards) + FastAPI (capacity input)

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Git
- (Optional) JIRA, GitHub, ServiceNow API tokens

### 1. Clone & Setup

```bash
cd sdm-metrics-dashboard
cp .env.example .env
```

Edit `.env` with your API credentials:
- `JIRA_BASE_URL`: Your JIRA instance URL
- `JIRA_EMAIL` & `JIRA_API_TOKEN`: JIRA service account
- `GITHUB_TOKEN` & `GITHUB_ORG`: GitHub access token and org
- `SNOW_*`: ServiceNow credentials (Phase 2)

Edit `config/teams.yaml` with your team structure, board IDs, and repos.

### 2. Run

```bash
docker-compose up
```

This starts:
- **Grafana** on http://localhost:3000 (admin/admin)
- **Capacity Input** on http://localhost:8000
- **Ingestion scheduler** (fetches data every 15 min from JIRA/GitHub)

### 3. First Run

The database is initialized on startup. You should see:
```
Initializing database...
[ingestion] Starting ingestion: JiraSprintIngestor
[ingestion] Upserted X sprints into sprints
...
```

### 4. Enter Sprint Capacity

Visit http://localhost:8000 to record engineer availability for the current sprint. This data is used for:
- Capacity utilization trends
- ETA predictions
- Identifying over/under-loading

## Architecture

```
ingestion/
  ├── JIRA poller (sprints, issues, epics) → SQLite
  ├── GitHub poller (PRs, deployments) → SQLite
  └── ServiceNow poller (incidents, SLAs) → Phase 2

capacity/
  └── FastAPI web form + CSV import → SQLite

db/
  └── metrics.db (SQLite, single file)

grafana/
  └── Dashboards + datasource config
```

## Data Sources

### JIRA
- Sprints, velocity, story points
- Issues, cycle time, status transitions
- Epics, burndown, scope creep

### GitHub
- Pull requests, review turnaround, deployments
- Build status, failed CI runs

### ServiceNow (Phase 2)
- Incidents, MTTR, on-call load
- SLA breach rate

### Confluence (Phase 3)
- Documentation coverage

## Metrics

### Delivery Velocity
- Sprint velocity (rolling avg)
- Commitment accuracy
- Epic burndown + ETA confidence
- Deployment frequency

### Quality
- Defect escape rate
- PR review turnaround
- Test coverage trends

### Reliability
- Incident rate, MTTR
- Change failure rate
- On-call load

### Team Health
- WIP per engineer
- Capacity utilization
- Knowledge silos (bus factor)

### Backlog
- Tech debt backlog + aging
- Security vulns (Dependabot)
- Dependency staleness

## Dashboards

### SDM View (Primary)
- Team velocity + trend
- Capacity dashboard
- Gantt timeline (epics)
- Health radar (WIP, PR age, bus factor)

### Director View (Rollup, Phase 2)
- All SDM teams comparison
- Portfolio Gantt (all epics)
- Aggregate metrics

### SVP View (Phase 3)
- DORA metrics
- Investment efficiency
- Security posture

## Development

### Add a New Ingestor

1. Create a class in `src/ingestion/`:
```python
from src.ingestion.base import BaseIngestor

class MySourceIngestor(BaseIngestor):
    table_name = "my_table"

    def fetch_raw(self):
        # Call external API
        pass

    def normalize(self, raw):
        # Map to DB schema
        pass
```

2. Register in `src/scheduler.py`:
```python
scheduler.add_job(
    lambda: MySourceIngestor(config).run(),
    IntervalTrigger(minutes=15),
    id="my_source",
)
```

3. Restart: `docker-compose restart ingestion`

### Query Data Directly

```bash
# Connect to SQLite
sqlite3 data/metrics.db

# Example: Team velocity
SELECT s.sprint_name, sm.velocity, sm.commitment_accuracy
FROM sprint_metrics sm
JOIN sprints s ON s.sprint_id = sm.sprint_id
WHERE sm.team_id = 'payments-backend'
ORDER BY s.start_date DESC;
```

## Phased Rollout

### Phase 1 (MVP, 4–6 weeks)
- ✅ JIRA + GitHub ingestion
- ✅ SQLite schema + seed
- ✅ Capacity input form
- ✅ Basic Grafana dashboard (SDM view)
- ✅ Docker Compose local setup

### Phase 2 (7–12 weeks)
- [ ] ServiceNow integration
- [ ] Tech debt + security metrics
- [ ] Team health signals
- [ ] Director rollup dashboard
- [ ] Grafana RBAC (per-team visibility)

### Phase 3 (3–6 months)
- [ ] SQLite → PostgreSQL migration
- [ ] Cloud/server deployment
- [ ] Business metrics adapters
- [ ] ETA prediction model
- [ ] SVP/CTO executive dashboard
- [ ] Automated alerting

## Troubleshooting

### Ingestion logs
```bash
docker-compose logs -f ingestion
```

### Grafana won't connect to SQLite
- Check that `data/metrics.db` exists and is readable
- Verify datasource config: http://localhost:3000/admin/datasources
- Plugin may need reload: restart Grafana container

### JIRA API rate limits
- The scheduler uses delta sync (fetches only changed issues)
- Full refresh happens weekly to catch drift
- For testing, temporarily reduce polling interval in `src/scheduler.py`

### No data showing in dashboard
- Ensure capacity data is entered (http://localhost:8000)
- Check sync logs: `docker-compose logs ingestion | grep -i "upserted"`
- Verify team_id in config matches data: `sqlite3 data/metrics.db "SELECT DISTINCT team_id FROM sprints"`

## Phase 3: Migration to PostgreSQL

When ready to move to cloud:

1. Set up PostgreSQL instance
2. Apply `db/schema.sql` to PostgreSQL (auto-converts AUTOINCREMENT → SERIAL)
3. Run migration script:
   ```bash
   python scripts/export_to_postgres.py \
     --sqlite data/metrics.db \
     --postgres postgresql://user:pass@host:5432/sdm_metrics
   ```
4. Update `.env`: `DATABASE_URL=postgresql://...`
5. Restart Docker Compose

The schema is 100% compatible. No data loss, only a one-time export.

## Key Assumptions

1. **Team hierarchy is stable** — defined in `config/teams.yaml`, not dynamically loaded
2. **JIRA boards map to teams** — board ID → team ID in config
3. **GitHub repos map to teams** — repo owner/name → team ID in config
4. **Capacity must be entered manually** — at sprint start (2 min per team)
5. **Metrics are computed nightly** — no real-time guarantees, 15-min ingestion lag is acceptable

## Contributing

- **Add metrics:** Update schema, then add Grafana panels
- **Add sources:** Create new ingestor, register in scheduler
- **Fix dashboards:** Export JSON, edit, re-import to Grafana

## Support

For questions or issues:
1. Check the logs: `docker-compose logs`
2. Verify `.env` and `config/teams.yaml` match your org
3. Ensure API tokens are valid and have necessary scopes
4. Open an issue on GitHub with logs and config (sanitized)

---

**Built with ❤️ for SDM metrics. Designed to scale from local MVP to enterprise dashboard.**
