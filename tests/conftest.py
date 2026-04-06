"""Shared test fixtures and configuration."""

import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Minimal teams config used across all tests
# ---------------------------------------------------------------------------
TEAMS_CONFIG = {
    "teams": [
        {
            "id": "payments-backend",
            "name": "Payments Backend",
            "sdm": "Jane Smith",
            "director": "Bob Director",
            "department": "Engineering",
            "jira_board_ids": [42],
            "github_repos": ["org/payments-api", "org/payments-worker"],
            "engineers": [
                {
                    "jira_id": "user:1a2b3c4d",
                    "name": "Alice Engineer",
                    "github": "alicedev",
                },
                {
                    "jira_id": "user:2b3c4d5e",
                    "name": "Bob Developer",
                    "github": "bobdev",
                },
            ],
        },
        {
            "id": "platform-infra",
            "name": "Platform Infrastructure",
            "sdm": "John SDM",
            "director": "Bob Director",
            "department": "Engineering",
            "jira_board_ids": [15],
            "github_repos": ["org/infra-core"],
            "engineers": [
                {
                    "jira_id": "user:3c4d5e6f",
                    "name": "Carlos Platform",
                    "github": "carlosp",
                },
            ],
        },
    ]
}


# ---------------------------------------------------------------------------
# Assembled app config used across all tests
# ---------------------------------------------------------------------------
@pytest.fixture
def app_config():
    """Return a fully-assembled application config dict."""
    return {
        "jira": {
            "base_url": "https://test.atlassian.net",
            "email": "test@example.com",
            "api_token": "test-token",
            "story_points_field": "customfield_10016",
            "sprint_field": "customfield_10020",
            "board_ids": [42, 15],
            "board_to_team": {"42": "payments-backend", "15": "platform-infra"},
        },
        "github": {
            "token": "ghp_test_token",
            "org": "org",
            "repos": {
                "org/payments-api": "payments-backend",
                "org/payments-worker": "payments-backend",
                "org/infra-core": "platform-infra",
            },
        },
        "servicenow": {
            "instance": "",
            "user": "",
            "password": "",
        },
        "db": {
            "url": ":memory:",
        },
        "teams": TEAMS_CONFIG,
    }


# ---------------------------------------------------------------------------
# Temporary SQLite database fixture (file-based so it works with connections)
# ---------------------------------------------------------------------------
SCHEMA_SQL = Path(__file__).parent.parent / "db" / "schema.sql"


@pytest.fixture
def tmp_db_path(tmp_path):
    """Return path to an initialised temporary SQLite database file."""
    db_file = tmp_path / "test_metrics.db"
    schema_sql = SCHEMA_SQL.read_text()

    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.executescript(schema_sql)
    conn.commit()
    conn.close()
    return str(db_file)


@pytest.fixture
def db_conn(tmp_db_path):
    """Return an open SQLite connection to the test DB (auto-closed)."""
    conn = sqlite3.connect(tmp_db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Patch get_config and get_db to use the in-memory/temp database
# ---------------------------------------------------------------------------
@pytest.fixture
def patched_config(app_config, tmp_db_path, monkeypatch):
    """Patch get_config so modules under test use our test config/db."""
    # Override the DATABASE_URL env var to point to the temp db
    monkeypatch.setenv("DATABASE_URL", tmp_db_path)
    # Patch the global cached config
    import src.config as cfg_module

    cfg_module._config = None  # reset cached singleton

    with (
        patch("src.config.load_teams_config", return_value=TEAMS_CONFIG),
        patch.dict(
            os.environ,
            {
                "JIRA_BASE_URL": "https://test.atlassian.net",
                "JIRA_EMAIL": "test@example.com",
                "JIRA_API_TOKEN": "test-token",
                "GITHUB_TOKEN": "ghp_test_token",
                "GITHUB_ORG": "org",
                "DATABASE_URL": tmp_db_path,
            },
        ),
    ):
        cfg_module._config = None
        yield

    # Cleanup: reset the singleton after the test
    cfg_module._config = None
