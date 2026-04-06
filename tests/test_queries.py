"""Tests for src/db/queries.py – ON CONFLICT upsert behavior for capacity and metrics."""

import sqlite3
from pathlib import Path
import pytest

from src.db import queries

SCHEMA_SQL = Path(__file__).parent.parent / "db" / "schema.sql"


@pytest.fixture
def conn(tmp_path):
    """Return a connection with schema applied and prerequisite FK rows."""
    db_file = tmp_path / "queries_test.db"
    c = sqlite3.connect(str(db_file))
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA_SQL.read_text())
    c.execute("INSERT INTO teams (team_id, team_name) VALUES ('t1', 'Team 1')")
    c.execute("INSERT INTO engineers (engineer_id, display_name, team_id) VALUES ('e1', 'Eng 1', 't1')")
    c.execute(
        "INSERT INTO sprints (sprint_id, board_id, team_id, sprint_name, state) VALUES (1, 10, 't1', 'Sprint 1', 'active')"
    )
    c.commit()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# sprint_capacity ON CONFLICT
# ---------------------------------------------------------------------------

class TestCapacityUpsert:
    def test_insert_new_capacity(self, conn):
        conn.execute(queries.INSERT_OR_REPLACE_CAPACITY, (1, "e1", 8.0, 10.0, 32.0, "note"))
        conn.commit()
        row = conn.execute("SELECT * FROM sprint_capacity WHERE sprint_id=1 AND engineer_id='e1'").fetchone()
        assert row["available_days"] == 8.0
        assert row["notes"] == "note"

    def test_upsert_preserves_row_id(self, conn):
        """Fix verification: ON CONFLICT should update in-place, preserving the row id."""
        conn.execute(queries.INSERT_OR_REPLACE_CAPACITY, (1, "e1", 8.0, 10.0, 32.0, "first"))
        conn.commit()
        row1 = conn.execute("SELECT id FROM sprint_capacity WHERE sprint_id=1 AND engineer_id='e1'").fetchone()
        original_id = row1["id"]

        # Upsert the same (sprint_id, engineer_id) with new values
        conn.execute(queries.INSERT_OR_REPLACE_CAPACITY, (1, "e1", 9.0, 10.0, 36.0, "updated"))
        conn.commit()
        row2 = conn.execute("SELECT id, available_days, notes FROM sprint_capacity WHERE sprint_id=1 AND engineer_id='e1'").fetchone()

        assert row2["id"] == original_id  # ID preserved
        assert row2["available_days"] == 9.0
        assert row2["notes"] == "updated"

    def test_upsert_preserves_entered_at(self, conn):
        """Fix verification: ON CONFLICT should not reset the entered_at timestamp."""
        conn.execute(queries.INSERT_OR_REPLACE_CAPACITY, (1, "e1", 8.0, 10.0, 32.0, "first"))
        conn.commit()
        row1 = conn.execute("SELECT entered_at FROM sprint_capacity WHERE sprint_id=1 AND engineer_id='e1'").fetchone()
        original_ts = row1["entered_at"]

        # Upsert again
        conn.execute(queries.INSERT_OR_REPLACE_CAPACITY, (1, "e1", 9.0, 10.0, 36.0, "second"))
        conn.commit()
        row2 = conn.execute("SELECT entered_at FROM sprint_capacity WHERE sprint_id=1 AND engineer_id='e1'").fetchone()

        assert row2["entered_at"] == original_ts


# ---------------------------------------------------------------------------
# sprint_metrics ON CONFLICT
# ---------------------------------------------------------------------------

class TestMetricsUpsert:
    def test_insert_new_metrics(self, conn):
        conn.execute(
            queries.INSERT_OR_REPLACE_SPRINT_METRICS,
            (1, "t1", 20.0, 25.0, 0.8, 0.1, 2, 8, 12.5, 40.0, 0.5),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM sprint_metrics WHERE sprint_id=1 AND team_id='t1'").fetchone()
        assert row["velocity"] == 20.0
        assert row["committed_points"] == 25.0

    def test_upsert_preserves_row_id(self, conn):
        """Fix verification: ON CONFLICT should update in-place, preserving the row id."""
        conn.execute(
            queries.INSERT_OR_REPLACE_SPRINT_METRICS,
            (1, "t1", 20.0, 25.0, 0.8, 0.1, 2, 8, 12.5, 40.0, 0.5),
        )
        conn.commit()
        row1 = conn.execute("SELECT id FROM sprint_metrics WHERE sprint_id=1 AND team_id='t1'").fetchone()
        original_id = row1["id"]

        # Upsert with updated velocity
        conn.execute(
            queries.INSERT_OR_REPLACE_SPRINT_METRICS,
            (1, "t1", 22.0, 25.0, 0.88, 0.1, 2, 8, 11.0, 40.0, 0.55),
        )
        conn.commit()
        row2 = conn.execute("SELECT id, velocity FROM sprint_metrics WHERE sprint_id=1 AND team_id='t1'").fetchone()

        assert row2["id"] == original_id  # ID preserved
        assert row2["velocity"] == 22.0

    def test_no_duplicate_rows_on_upsert(self, conn):
        """Upserting the same sprint/team should not create a second row."""
        for v in [10.0, 15.0, 20.0]:
            conn.execute(
                queries.INSERT_OR_REPLACE_SPRINT_METRICS,
                (1, "t1", v, 25.0, v / 25.0, 0.1, 2, 8, 12.5, 40.0, 0.5),
            )
            conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM sprint_metrics WHERE sprint_id=1 AND team_id='t1'").fetchone()[0]
        assert count == 1
