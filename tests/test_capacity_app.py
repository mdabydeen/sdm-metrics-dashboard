"""Tests for src/capacity/app.py and src/capacity/routes.py."""

import sqlite3
from pathlib import Path
from unittest.mock import patch
import pytest

import src.config as cfg_module

SCHEMA_SQL = Path(__file__).parent.parent / "db" / "schema.sql"


@pytest.fixture(autouse=True)
def reset_config():
    cfg_module._config = None
    yield
    cfg_module._config = None


@pytest.fixture
def db_config(tmp_path):
    db_file = tmp_path / "capacity_test.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(SCHEMA_SQL.read_text())
    conn.execute("INSERT INTO teams (team_id, team_name) VALUES ('t1', 'Team 1')")
    conn.execute("INSERT INTO engineers (engineer_id, display_name, team_id) VALUES ('e1', 'Eng 1', 't1')")
    conn.execute("INSERT INTO engineers (engineer_id, display_name, team_id) VALUES ('e2', 'Eng 2', 't1')")
    conn.execute(
        "INSERT INTO sprints (sprint_id, board_id, team_id, sprint_name, state) "
        "VALUES (1, 10, 't1', 'Sprint 1', 'active')"
    )
    conn.execute(
        "INSERT INTO sprints (sprint_id, board_id, team_id, sprint_name, state) "
        "VALUES (2, 10, 't1', 'Sprint 2', 'future')"
    )
    conn.commit()
    conn.close()
    return {
        "db": {"url": str(db_file)},
        "jira": {"api_token": "t", "base_url": "https://test.atlassian.net", "email": "t@t.com",
                 "story_points_field": "customfield_10016", "sprint_field": "customfield_10020",
                 "board_ids": [], "board_to_team": {}},
        "github": {"token": "t", "org": "org", "repos": {}},
        "servicenow": {"instance": "", "user": "", "password": ""},
        "teams": {"teams": []},
    }


@pytest.fixture
def client(db_config):
    """Create a TestClient with patched config."""
    from fastapi.testclient import TestClient
    with patch("src.db.connection.get_config", return_value=db_config):
        with patch("src.config.load_config", return_value=db_config):
            cfg_module._config = db_config
            from src.capacity.app import app
            with TestClient(app) as c:
                yield c


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestRootEndpoint:
    def test_root_returns_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Sprint Capacity" in resp.text


class TestGetSprints:
    def test_returns_sprints(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.get("/api/sprints")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        names = {s["sprint_name"] for s in data}
        assert "Sprint 1" in names
        assert "Sprint 2" in names


class TestGetEngineers:
    def test_returns_all_engineers(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.get("/api/engineers")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_filters_by_team(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.get("/api/engineers?team_id=t1")
        assert resp.status_code == 200
        assert len(resp.json()) == 2


class TestGetTeams:
    def test_returns_teams(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.get("/api/teams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["team_id"] == "t1"


class TestPostCapacity:
    def test_create_capacity_entry(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.post("/api/capacity", json={
                "sprint_id": 1,
                "engineer_id": "e1",
                "available_days": 8.0,
                "total_days": 10.0,
                "capacity_points": 32.0,
                "notes": "On call Monday",
            })
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_upsert_updates_existing(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            # First insert
            client.post("/api/capacity", json={
                "sprint_id": 1, "engineer_id": "e1", "available_days": 8.0,
            })
            # Upsert
            client.post("/api/capacity", json={
                "sprint_id": 1, "engineer_id": "e1", "available_days": 9.0,
            })
            # Verify latest value
            resp = client.get("/api/capacity/1")
        data = resp.json()
        assert len(data) == 1
        assert data[0]["available_days"] == 9.0


class TestGetCapacity:
    def test_returns_capacity_for_sprint(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            client.post("/api/capacity", json={
                "sprint_id": 1, "engineer_id": "e1", "available_days": 8.0,
            })
            resp = client.get("/api/capacity/1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["engineer_id"] == "e1"
        assert data[0]["display_name"] == "Eng 1"

    def test_returns_empty_for_no_entries(self, client, db_config):
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.get("/api/capacity/999")
        assert resp.status_code == 200
        assert resp.json() == []


class TestCSVImport:
    def test_import_csv(self, client, db_config):
        csv_content = "sprint_id,engineer_id,available_days,total_days,capacity_points,notes\n1,e1,8,10,32,test\n"
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.post(
                "/api/capacity/import",
                files={"file": ("capacity.csv", csv_content, "text/csv")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["imported"] == 1
        assert data["errors"] == []

    def test_import_csv_with_bad_row(self, client, db_config):
        csv_content = (
            "sprint_id,engineer_id,available_days,total_days,capacity_points,notes\n"
            "1,e1,8,10,32,ok\n"
            "bad,e1,notanumber,10,,\n"
        )
        with patch("src.db.connection.get_config", return_value=db_config):
            resp = client.post(
                "/api/capacity/import",
                files={"file": ("capacity.csv", csv_content, "text/csv")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["imported"] == 1
        assert len(data["errors"]) == 1
