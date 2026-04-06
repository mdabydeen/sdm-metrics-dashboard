"""Common SQL queries used across the application."""

# Teams and Engineers
INSERT_OR_REPLACE_TEAM = """
    INSERT OR REPLACE INTO teams (team_id, team_name, sdm_name, director_name, department)
    VALUES (?, ?, ?, ?, ?)
"""

INSERT_OR_REPLACE_ENGINEER = """
    INSERT OR REPLACE INTO engineers (engineer_id, display_name, team_id, github_username, is_active)
    VALUES (?, ?, ?, ?, ?)
"""

# Sprints
INSERT_OR_REPLACE_SPRINT = """
    INSERT OR REPLACE INTO sprints (sprint_id, board_id, team_id, sprint_name, state, start_date, end_date, goal)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

SELECT_SPRINTS_BY_TEAM = """
    SELECT * FROM sprints
    WHERE team_id = ?
    ORDER BY start_date DESC
"""

# Issues
INSERT_OR_REPLACE_ISSUE = """
    INSERT OR REPLACE INTO issues (
        issue_id, issue_type, summary, status, priority, story_points,
        assignee_id, team_id, sprint_id, epic_key, labels,
        created_at, resolved_at, started_at, is_unplanned
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SELECT_ISSUES_BY_SPRINT = """
    SELECT * FROM issues
    WHERE sprint_id = ?
    ORDER BY created_at
"""

INSERT_CHANGELOG = """
    INSERT INTO issue_changelog (issue_id, field, from_value, to_value, changed_at)
    VALUES (?, ?, ?, ?, ?)
"""

# Epics
INSERT_OR_REPLACE_EPIC = """
    INSERT OR REPLACE INTO epics (
        epic_key, epic_name, team_id, status, total_points,
        completed_points, planned_start, planned_end, predicted_end, confidence
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SELECT_EPICS_BY_TEAM = """
    SELECT * FROM epics
    WHERE team_id = ?
    ORDER BY planned_start
"""

# Capacity — uses ON CONFLICT to preserve row id and entered_at timestamp
INSERT_OR_REPLACE_CAPACITY = """
    INSERT INTO sprint_capacity (
        sprint_id, engineer_id, available_days, total_days, capacity_points, notes
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(sprint_id, engineer_id) DO UPDATE SET
        available_days = excluded.available_days,
        total_days = excluded.total_days,
        capacity_points = excluded.capacity_points,
        notes = excluded.notes
"""

SELECT_CAPACITY_BY_SPRINT = """
    SELECT sc.*, e.display_name, e.github_username
    FROM sprint_capacity sc
    JOIN engineers e ON e.engineer_id = sc.engineer_id
    WHERE sc.sprint_id = ?
    ORDER BY e.display_name
"""

# Pull Requests
INSERT_OR_REPLACE_PR = """
    INSERT OR REPLACE INTO pull_requests (
        pr_id, repo, team_id, author, title, state,
        additions, deletions, review_count,
        opened_at, first_review_at, merged_at, closed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SELECT_PRS_BY_TEAM = """
    SELECT * FROM pull_requests
    WHERE team_id = ?
    ORDER BY opened_at DESC
    LIMIT 100
"""

# Deployments
INSERT_OR_REPLACE_DEPLOYMENT = """
    INSERT OR REPLACE INTO deployments (
        deployment_id, repo, team_id, environment, sha, deployed_at, caused_incident
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
"""

SELECT_DEPLOYMENTS_BY_TEAM = """
    SELECT * FROM deployments
    WHERE team_id = ?
    ORDER BY deployed_at DESC
    LIMIT 100
"""

# Sprint Metrics — uses ON CONFLICT to preserve row id and computed_at timestamp
INSERT_OR_REPLACE_SPRINT_METRICS = """
    INSERT INTO sprint_metrics (
        sprint_id, team_id, velocity, committed_points, commitment_accuracy,
        scope_creep_rate, bug_count, story_count, avg_cycle_time_hrs,
        capacity_total_days, utilization
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sprint_id, team_id) DO UPDATE SET
        velocity = excluded.velocity,
        committed_points = excluded.committed_points,
        commitment_accuracy = excluded.commitment_accuracy,
        scope_creep_rate = excluded.scope_creep_rate,
        bug_count = excluded.bug_count,
        story_count = excluded.story_count,
        avg_cycle_time_hrs = excluded.avg_cycle_time_hrs,
        capacity_total_days = excluded.capacity_total_days,
        utilization = excluded.utilization,
        computed_at = CURRENT_TIMESTAMP
"""

SELECT_SPRINT_METRICS_BY_TEAM = """
    SELECT sm.*, s.sprint_name, s.start_date
    FROM sprint_metrics sm
    JOIN sprints s ON s.sprint_id = sm.sprint_id
    WHERE sm.team_id = ?
    ORDER BY s.start_date DESC
    LIMIT 12
"""

# Sync State
INSERT_OR_UPDATE_SYNC_STATE = """
    INSERT OR REPLACE INTO sync_state (source, last_sync_at, record_count)
    VALUES (?, ?, ?)
"""

SELECT_SYNC_STATE = """
    SELECT * FROM sync_state WHERE source = ?
"""
