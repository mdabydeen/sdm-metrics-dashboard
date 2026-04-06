"""Tests for src/metrics/compute.py – MetricsComputer."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

import src.config as cfg_module
from src.metrics.compute import MetricsComputer, compute_metrics

SCHEMA_SQL = Path(__file__).parent.parent / "db" / "schema.sql"


@pytest.fixture(autouse=True)
def reset_config():
    cfg_module._config = None
    yield
    cfg_module._config = None


@pytest.fixture
def db_config(tmp_path):
    db_file = tmp_path / "metrics_test.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(SCHEMA_SQL.read_text())
    # Seed required FK data
    conn.execute("INSERT INTO teams (team_id, team_name) VALUES ('t1', 'Team 1')")
    conn.execute("INSERT INTO engineers (engineer_id, display_name, team_id) VALUES ('e1', 'Eng 1', 't1')")
    conn.execute(
        "INSERT INTO sprints (sprint_id, board_id, team_id, sprint_name, state) "
        "VALUES (1, 10, 't1', 'Sprint 1', 'closed')"
    )
    conn.commit()
    conn.close()
    return {
        "db": {"url": str(db_file)},
        "jira": {"api_token": "t"},
        "github": {"token": "t"},
    }


def _insert_issue(conn, issue_id, team_id="t1", sprint_id=1, status="Done",
                   issue_type="Story", story_points=5, is_unplanned=0,
                   started_at=None, resolved_at=None):
    conn.execute(
        "INSERT INTO issues (issue_id, issue_type, summary, status, story_points, "
        "team_id, sprint_id, is_unplanned, started_at, resolved_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (issue_id, issue_type, f"Summary {issue_id}", status, story_points,
         team_id, sprint_id, is_unplanned, started_at, resolved_at),
    )


def _insert_capacity(conn, sprint_id=1, engineer_id="e1", available_days=8.0):
    conn.execute(
        "INSERT INTO sprint_capacity (sprint_id, engineer_id, available_days, total_days) "
        "VALUES (?, ?, ?, 10)",
        (sprint_id, engineer_id, available_days),
    )


# ---------------------------------------------------------------------------
# compute_sprint_metrics
# ---------------------------------------------------------------------------

class TestComputeSprintMetrics:
    def test_velocity_sums_done_story_points(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=5)
        _insert_issue(conn, "T-2", status="Done", story_points=3)
        _insert_issue(conn, "T-3", status="In Progress", story_points=8)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["velocity"] == 8.0  # 5 + 3

    def test_committed_points_excludes_unplanned(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", story_points=5, is_unplanned=0)
        _insert_issue(conn, "T-2", story_points=3, is_unplanned=1)  # unplanned
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["committed_points"] == 5.0

    def test_commitment_accuracy(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=8, is_unplanned=0)
        _insert_issue(conn, "T-2", status="Done", story_points=2, is_unplanned=0)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        # velocity=10, committed=10 → accuracy=1.0
        assert result["commitment_accuracy"] == 1.0

    def test_scope_creep_rate(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", is_unplanned=0)
        _insert_issue(conn, "T-2", is_unplanned=0)
        _insert_issue(conn, "T-3", is_unplanned=1)
        _insert_issue(conn, "T-4", is_unplanned=1)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["scope_creep_rate"] == 0.5  # 2/4

    def test_bug_and_story_counts(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", issue_type="Story")
        _insert_issue(conn, "T-2", issue_type="Story")
        _insert_issue(conn, "T-3", issue_type="Bug")
        _insert_issue(conn, "T-4", issue_type="Task")
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["story_count"] == 2
        assert result["bug_count"] == 1

    def test_avg_cycle_time(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        # 24 hours cycle time
        _insert_issue(conn, "T-1", started_at="2024-01-05T10:00:00+00:00",
                       resolved_at="2024-01-06T10:00:00+00:00")
        # 48 hours cycle time
        _insert_issue(conn, "T-2", started_at="2024-01-05T10:00:00+00:00",
                       resolved_at="2024-01-07T10:00:00+00:00")
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["avg_cycle_time_hrs"] == 36.0  # (24+48)/2

    def test_utilization_with_capacity(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=20)
        _insert_capacity(conn, 1, "e1", 10.0)  # 10 days × 4 pts/day = 40 capacity
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["utilization"] == 0.5  # 20 / 40

    def test_utilization_zero_when_no_capacity(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=20)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["utilization"] == 0.0

    def test_returns_none_for_empty_sprint(self, db_config):
        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")
        assert result is None

    def test_handles_null_story_points(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=None)
        _insert_issue(conn, "T-2", status="Done", story_points=5)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            result = mc.compute_sprint_metrics(1, "t1")

        assert result["velocity"] == 5.0  # None treated as 0


# ---------------------------------------------------------------------------
# compute_all / compute_metrics
# ---------------------------------------------------------------------------

class TestComputeAll:
    def test_computes_and_upserts_metrics(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=5)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            count = mc.compute_all()

        assert count == 1

        # Verify persisted
        conn = sqlite3.connect(db_config["db"]["url"])
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM sprint_metrics WHERE sprint_id=1 AND team_id='t1'").fetchone()
        conn.close()
        assert row is not None
        assert row["velocity"] == 5.0

    def test_compute_metrics_entrypoint(self, db_config):
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=3)
        conn.commit()
        conn.close()

        with patch("src.db.connection.get_config", return_value=db_config):
            count = compute_metrics()
        assert count == 1

    def test_skips_unknown_teams(self, db_config):
        """Issues with team_id='unknown' should be skipped."""
        conn = sqlite3.connect(db_config["db"]["url"])
        conn.execute(
            "INSERT INTO issues (issue_id, issue_type, summary, status, story_points, "
            "team_id, sprint_id, is_unplanned) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("T-99", "Story", "s", "Done", 5, "unknown", 1, 0),
        )
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            count = mc.compute_all()
        assert count == 0

    def test_idempotent_compute(self, db_config):
        """Running compute twice should upsert, not duplicate."""
        conn = sqlite3.connect(db_config["db"]["url"])
        _insert_issue(conn, "T-1", status="Done", story_points=5)
        conn.commit()
        conn.close()

        mc = MetricsComputer()
        with patch("src.db.connection.get_config", return_value=db_config):
            mc.compute_all()
            mc.compute_all()

        conn = sqlite3.connect(db_config["db"]["url"])
        count = conn.execute("SELECT COUNT(*) FROM sprint_metrics WHERE sprint_id=1 AND team_id='t1'").fetchone()[0]
        conn.close()
        assert count == 1
