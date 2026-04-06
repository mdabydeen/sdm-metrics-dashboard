"""Tests for src/ingestion/base.py – BaseIngestor."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

import src.config as cfg_module
from src.ingestion.base import BaseIngestor
from tests.conftest import TEAMS_CONFIG

SCHEMA_SQL = Path(__file__).parent.parent / "db" / "schema.sql"


@pytest.fixture(autouse=True)
def reset_config():
    cfg_module._config = None
    yield
    cfg_module._config = None


# ---------------------------------------------------------------------------
# Concrete subclass for testing
# ---------------------------------------------------------------------------


class ConcreteIngestor(BaseIngestor):
    table_name = "sprints"

    def fetch_raw(self):
        return [
            {
                "sprint_id": 1,
                "board_id": 42,
                "team_id": "payments-backend",
                "sprint_name": "Sprint 1",
                "state": "active",
                "start_date": "2024-01-01",
                "end_date": "2024-01-14",
                "goal": "Test goal",
                "synced_at": "2024-01-01T00:00:00",
            }
        ]

    def normalize(self, raw):
        return raw


class EmptyFetchIngestor(BaseIngestor):
    table_name = "sprints"

    def fetch_raw(self):
        return []

    def normalize(self, raw):
        return raw


class FailingFetchIngestor(BaseIngestor):
    table_name = "sprints"

    def fetch_raw(self):
        raise ConnectionError("API down")

    def normalize(self, raw):
        return raw


class NoTableNameIngestor(BaseIngestor):
    table_name = None

    def fetch_raw(self):
        return [{"id": 1}]

    def normalize(self, raw):
        return raw


# ---------------------------------------------------------------------------
# DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def db_config(tmp_path):
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(SCHEMA_SQL.read_text())
    # Insert required team reference for FK constraint
    conn.execute("INSERT INTO teams (team_id, team_name) VALUES ('payments-backend', 'Payments')")
    conn.commit()
    conn.close()
    return {
        "db": {"url": str(db_file)},
        "jira": {"api_token": "t"},
        "github": {"token": "t"},
        "teams": TEAMS_CONFIG,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBaseIngestorInit:
    def test_stores_config(self, app_config):
        ing = ConcreteIngestor(app_config)
        assert ing.config is app_config

    def test_logger_named_after_class(self, app_config):
        ing = ConcreteIngestor(app_config)
        assert ing.logger.name == "ConcreteIngestor"


class TestBaseIngestorRun:
    def test_run_returns_upserted_count(self, app_config, db_config):
        ing = ConcreteIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            count = ing.run()
        assert count == 1

    def test_run_with_empty_fetch_returns_zero(self, app_config, db_config):
        ing = EmptyFetchIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            count = ing.run()
        assert count == 0

    def test_run_raises_on_fetch_failure(self, app_config, db_config):
        ing = FailingFetchIngestor(app_config)
        with (
            patch("src.db.connection.get_config", return_value=db_config),
            pytest.raises(ConnectionError, match="API down"),
        ):
            ing.run()

    def test_run_logs_start_info(self, app_config, db_config, caplog):
        import logging

        ing = ConcreteIngestor(app_config)
        with (
            patch("src.db.connection.get_config", return_value=db_config),
            caplog.at_level(logging.INFO, logger="ConcreteIngestor"),
        ):
            ing.run()
        assert any("Starting ingestion" in m for m in caplog.messages)


class TestBaseIngestorUpsert:
    def test_upsert_empty_records_returns_zero(self, app_config, db_config):
        ing = ConcreteIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.upsert([])
        assert result == 0

    def test_upsert_raises_when_no_table_name(self, app_config, db_config):
        ing = NoTableNameIngestor(app_config)
        with (
            patch("src.db.connection.get_config", return_value=db_config),
            pytest.raises(ValueError, match="must define table_name"),
        ):
            ing.upsert([{"id": 1}])

    def test_upsert_inserts_records(self, app_config, db_config):
        ing = ConcreteIngestor(app_config)
        records = [
            {
                "sprint_id": 99,
                "board_id": 42,
                "team_id": "payments-backend",
                "sprint_name": "Sprint 99",
                "state": "closed",
                "start_date": "2024-01-01",
                "end_date": "2024-01-14",
                "goal": None,
                "synced_at": "2024-01-01T00:00:00",
            }
        ]
        with patch("src.db.connection.get_config", return_value=db_config):
            count = ing.upsert(records)
        assert count == 1

    def test_upsert_replaces_existing_record(self, app_config, db_config):
        """INSERT OR REPLACE should overwrite an existing row."""
        ing = ConcreteIngestor(app_config)
        record = {
            "sprint_id": 100,
            "board_id": 42,
            "team_id": "payments-backend",
            "sprint_name": "Sprint 100",
            "state": "active",
            "start_date": "2024-01-01",
            "end_date": "2024-01-14",
            "goal": None,
            "synced_at": "2024-01-01T00:00:00",
        }
        with patch("src.db.connection.get_config", return_value=db_config):
            ing.upsert([record])
            record["sprint_name"] = "Sprint 100 Updated"
            count = ing.upsert([record])
        assert count == 1


class TestAbstractMethods:
    def test_cannot_instantiate_without_implementing_abstracts(self, app_config):
        with pytest.raises(TypeError):
            BaseIngestor(app_config)  # type: ignore[abstract]
