-- ============================================================
-- Reference / Configuration Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS teams (
    team_id         TEXT PRIMARY KEY,
    team_name       TEXT NOT NULL,
    sdm_name        TEXT,
    director_name   TEXT,
    department      TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS engineers (
    engineer_id     TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    team_id         TEXT NOT NULL REFERENCES teams(team_id),
    github_username TEXT,
    is_active       INTEGER DEFAULT 1,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Sprint & Velocity
-- ============================================================

CREATE TABLE IF NOT EXISTS sprints (
    sprint_id       INTEGER PRIMARY KEY,
    board_id        INTEGER NOT NULL,
    team_id         TEXT NOT NULL REFERENCES teams(team_id),
    sprint_name     TEXT NOT NULL,
    state           TEXT,
    start_date      TIMESTAMP,
    end_date        TIMESTAMP,
    goal            TEXT,
    synced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sprints_team ON sprints(team_id);
CREATE INDEX IF NOT EXISTS idx_sprints_state ON sprints(state);

CREATE TABLE IF NOT EXISTS issues (
    issue_id        TEXT PRIMARY KEY,
    issue_type      TEXT NOT NULL,
    summary         TEXT,
    status          TEXT,
    priority        TEXT,
    story_points    REAL,
    assignee_id     TEXT REFERENCES engineers(engineer_id),
    team_id         TEXT REFERENCES teams(team_id),
    sprint_id       INTEGER REFERENCES sprints(sprint_id),
    epic_key        TEXT,
    labels          TEXT,
    created_at      TIMESTAMP,
    resolved_at     TIMESTAMP,
    started_at      TIMESTAMP,
    is_unplanned    INTEGER DEFAULT 0,
    synced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_issues_sprint ON issues(sprint_id);
CREATE INDEX IF NOT EXISTS idx_issues_team ON issues(team_id);
CREATE INDEX IF NOT EXISTS idx_issues_epic ON issues(epic_key);
CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);

CREATE TABLE IF NOT EXISTS issue_changelog (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id        TEXT NOT NULL REFERENCES issues(issue_id),
    field           TEXT NOT NULL,
    from_value      TEXT,
    to_value        TEXT,
    changed_at      TIMESTAMP NOT NULL,
    synced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_changelog_issue ON issue_changelog(issue_id);

-- ============================================================
-- Epics & Timelines (Gantt source)
-- ============================================================

CREATE TABLE IF NOT EXISTS epics (
    epic_key        TEXT PRIMARY KEY,
    epic_name       TEXT NOT NULL,
    team_id         TEXT REFERENCES teams(team_id),
    status          TEXT,
    total_points    REAL,
    completed_points REAL DEFAULT 0,
    planned_start   TIMESTAMP,
    planned_end     TIMESTAMP,
    predicted_end   TIMESTAMP,
    confidence      REAL,
    synced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_epics_team ON epics(team_id);

-- ============================================================
-- Capacity (Manual Input)
-- ============================================================

CREATE TABLE IF NOT EXISTS sprint_capacity (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sprint_id       INTEGER NOT NULL REFERENCES sprints(sprint_id),
    engineer_id     TEXT NOT NULL REFERENCES engineers(engineer_id),
    available_days  REAL NOT NULL,
    total_days      REAL NOT NULL DEFAULT 10,
    capacity_points REAL,
    notes           TEXT,
    entered_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sprint_id, engineer_id)
);

CREATE INDEX IF NOT EXISTS idx_capacity_sprint ON sprint_capacity(sprint_id);

-- ============================================================
-- GitHub Metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS pull_requests (
    pr_id           TEXT PRIMARY KEY,
    repo            TEXT NOT NULL,
    team_id         TEXT REFERENCES teams(team_id),
    author          TEXT,
    title           TEXT,
    state           TEXT,
    additions       INTEGER,
    deletions       INTEGER,
    review_count    INTEGER DEFAULT 0,
    opened_at       TIMESTAMP,
    first_review_at TIMESTAMP,
    merged_at       TIMESTAMP,
    closed_at       TIMESTAMP,
    synced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prs_team ON pull_requests(team_id);
CREATE INDEX IF NOT EXISTS idx_prs_repo ON pull_requests(repo);
CREATE INDEX IF NOT EXISTS idx_prs_state ON pull_requests(state);

CREATE TABLE IF NOT EXISTS deployments (
    deployment_id   TEXT PRIMARY KEY,
    repo            TEXT NOT NULL,
    team_id         TEXT REFERENCES teams(team_id),
    environment     TEXT,
    sha             TEXT,
    deployed_at     TIMESTAMP NOT NULL,
    caused_incident INTEGER DEFAULT 0,
    synced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_deployments_team ON deployments(team_id);

-- ============================================================
-- Computed / Snapshot Metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS sprint_metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sprint_id           INTEGER NOT NULL REFERENCES sprints(sprint_id),
    team_id             TEXT NOT NULL REFERENCES teams(team_id),
    velocity            REAL,
    committed_points    REAL,
    commitment_accuracy REAL,
    scope_creep_rate    REAL,
    bug_count           INTEGER,
    story_count         INTEGER,
    avg_cycle_time_hrs  REAL,
    capacity_total_days REAL,
    utilization         REAL,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sprint_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_sprint_metrics_team ON sprint_metrics(team_id);

-- ============================================================
-- Sync State (for incremental ingestion)
-- ============================================================

CREATE TABLE IF NOT EXISTS sync_state (
    source          TEXT PRIMARY KEY,
    last_sync_at    TIMESTAMP,
    last_full_sync  TIMESTAMP,
    record_count    INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
