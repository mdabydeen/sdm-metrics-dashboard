"""Tests for src/ingestion/jira.py – JiraSprintIngestor, JiraIssueIngestor, JiraEpicIngestor."""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
import pytest

import src.config as cfg_module
from src.ingestion.jira import (
    JiraSprintIngestor,
    JiraIssueIngestor,
    JiraEpicIngestor,
    _safe_get,
)

SCHEMA_SQL = Path(__file__).parent.parent / "db" / "schema.sql"


@pytest.fixture(autouse=True)
def reset_config():
    cfg_module._config = None
    yield
    cfg_module._config = None


# ---------------------------------------------------------------------------
# DB fixture with required FK rows
# ---------------------------------------------------------------------------

@pytest.fixture
def db_config(tmp_path):
    db_file = tmp_path / "jira_test.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(SCHEMA_SQL.read_text())
    for team_id, name in [("payments-backend", "Payments Backend"), ("platform-infra", "Platform Infra")]:
        conn.execute(
            "INSERT OR IGNORE INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name)
        )
    # Insert a sprint for team lookup tests
    conn.execute(
        "INSERT INTO sprints (sprint_id, board_id, team_id, sprint_name, state) VALUES (?, ?, ?, ?, ?)",
        (100, 42, "payments-backend", "Sprint 100", "active"),
    )
    conn.commit()
    conn.close()
    return {
        "db": {"url": str(db_file)},
        "jira": {"api_token": "t"},
        "github": {"token": "t"},
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


def _make_sprint(sprint_id=1, board_id=42, name="Sprint 1", state="active"):
    return {
        "id": sprint_id,
        "originBoardId": board_id,
        "name": name,
        "state": state,
        "startDate": "2024-01-01T00:00:00.000Z",
        "endDate": "2024-01-14T00:00:00.000Z",
        "goal": "Test goal",
    }


def _make_issue(
    key="TEST-1",
    issue_type="Story",
    status="Done",
    story_points=5,
    assignee_id="user:abc",
    sprint_id=100,
    priority="High",
    labels=None,
    changelog_histories=None,
):
    """Build a JIRA issue dict matching the agile API response shape."""
    sprint_field_value = [{"id": sprint_id}] if sprint_id else None
    fields = {
        "issuetype": {"name": issue_type},
        "summary": f"Summary for {key}",
        "status": {"name": status},
        "priority": {"name": priority} if priority else None,
        "customfield_10016": story_points,
        "customfield_10020": sprint_field_value,
        "assignee": {"accountId": assignee_id} if assignee_id else None,
        "parent": {"key": "EPIC-1"} if issue_type != "Epic" else None,
        "labels": labels or [],
        "created": "2024-01-05T10:00:00.000+0000",
        "resolutiondate": "2024-01-10T15:00:00.000+0000" if status == "Done" else None,
    }
    return {
        "key": key,
        "fields": fields,
        "changelog": {"histories": changelog_histories or []},
    }


def _make_epic(key="EPIC-1", status="In Progress"):
    return {
        "key": key,
        "fields": {
            "summary": f"Epic {key}",
            "status": {"name": status},
        },
    }


# ---------------------------------------------------------------------------
# _safe_get helper
# ---------------------------------------------------------------------------

class TestSafeGet:
    def test_returns_value_from_dict(self):
        assert _safe_get({"a": 1}, "a") == 1

    def test_returns_default_for_missing_key(self):
        assert _safe_get({"a": 1}, "b", "default") == "default"

    def test_returns_default_for_none_obj(self):
        assert _safe_get(None, "a", "fallback") == "fallback"

    def test_returns_none_by_default(self):
        assert _safe_get(None, "a") is None


# ---------------------------------------------------------------------------
# JiraSprintIngestor
# ---------------------------------------------------------------------------

class TestJiraSprintIngestorFetchRaw:
    def test_fetches_sprints_from_all_boards(self, app_config):
        sprints_board42 = {"values": [_make_sprint(1, 42), _make_sprint(2, 42)]}
        sprints_board15 = {"values": [_make_sprint(3, 15)]}
        responses = [
            _mock_response(sprints_board42),
            _mock_response(sprints_board15),
        ]
        ing = JiraSprintIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()
        assert len(result) == 3

    def test_handles_api_error_gracefully(self, app_config):
        bad = _mock_response({}, 500)
        bad.raise_for_status.side_effect = Exception("500 Server Error")
        responses = [bad, _mock_response({"values": [_make_sprint(1, 15)]})]
        ing = JiraSprintIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()
        # Board 42 failed, board 15 succeeded
        assert len(result) == 1

    def test_uses_correct_api_endpoint(self, app_config):
        ing = JiraSprintIngestor(app_config)
        with patch("requests.get", return_value=_mock_response({"values": []})) as mock_get:
            ing.fetch_raw()
        # Should use agile sprint endpoint
        call_url = mock_get.call_args_list[0][0][0]
        assert "/rest/agile/1.0/board/" in call_url
        assert "/sprint" in call_url


class TestJiraSprintIngestorNormalize:
    def test_normalize_maps_fields(self, app_config):
        raw = [_make_sprint(1, 42, "Sprint 1", "active")]
        ing = JiraSprintIngestor(app_config)
        result = ing.normalize(raw)

        assert len(result) == 1
        r = result[0]
        assert r["sprint_id"] == 1
        assert r["board_id"] == 42
        assert r["team_id"] == "payments-backend"
        assert r["sprint_name"] == "Sprint 1"
        assert r["state"] == "active"

    def test_unknown_board_maps_to_unknown_team(self, app_config):
        raw = [_make_sprint(1, 999)]
        ing = JiraSprintIngestor(app_config)
        result = ing.normalize(raw)
        assert result[0]["team_id"] == "unknown"

    def test_synced_at_is_utc_iso_string(self, app_config):
        raw = [_make_sprint(1, 42)]
        ing = JiraSprintIngestor(app_config)
        result = ing.normalize(raw)
        dt = datetime.fromisoformat(result[0]["synced_at"])
        assert dt.tzinfo is not None  # timezone-aware


# ---------------------------------------------------------------------------
# JiraIssueIngestor
# ---------------------------------------------------------------------------

class TestJiraIssueIngestorFetchRaw:
    def test_uses_agile_board_api_not_jql_search(self, app_config):
        """Fix verification: should use /rest/agile/1.0/board/{id}/issue, not /rest/api/3/search."""
        ing = JiraIssueIngestor(app_config)
        with patch("requests.get", return_value=_mock_response({"issues": [], "total": 0})) as mock_get:
            ing.fetch_raw()
        for call in mock_get.call_args_list:
            url = call[0][0]
            assert "/rest/agile/1.0/board/" in url
            assert "/issue" in url
            # Must NOT use search API with invalid board= JQL
            assert "/rest/api/3/search" not in url

    def test_paginates_through_results(self, app_config):
        page1 = {"issues": [_make_issue(f"TEST-{i}") for i in range(100)], "total": 150}
        page2 = {"issues": [_make_issue(f"TEST-{i}") for i in range(100, 150)], "total": 150}
        empty = {"issues": [], "total": 0}

        responses = [
            _mock_response(page1),  # board 42 page 1
            _mock_response(page2),  # board 42 page 2
            _mock_response(empty),  # board 15
        ]
        ing = JiraIssueIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()
        assert len(result) == 150

    def test_handles_api_error_per_board(self, app_config):
        bad = _mock_response({}, 401)
        bad.raise_for_status.side_effect = Exception("401 Unauthorized")
        good = {"issues": [_make_issue("TEST-1")], "total": 1}
        responses = [bad, _mock_response(good)]
        ing = JiraIssueIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()
        assert len(result) == 1


class TestJiraIssueIngestorNormalize:
    def test_normalize_maps_all_fields(self, app_config, db_config):
        issue = _make_issue("TEST-1", "Story", "Done", 5, "user:abc", 100, "High", ["backend"])
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])

        assert len(result) == 1
        r = result[0]
        assert r["issue_id"] == "TEST-1"
        assert r["issue_type"] == "Story"
        assert r["status"] == "Done"
        assert r["story_points"] == 5
        assert r["assignee_id"] == "user:abc"
        assert r["sprint_id"] == 100
        assert r["epic_key"] == "EPIC-1"
        assert r["labels"] == "backend"
        assert r["priority"] == "High"

    def test_null_assignee_does_not_crash(self, app_config, db_config):
        """Fix verification: None assignee should not raise AttributeError."""
        issue = _make_issue("TEST-2", assignee_id=None)
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["assignee_id"] is None

    def test_null_priority_does_not_crash(self, app_config, db_config):
        """Fix verification: None priority should not raise AttributeError."""
        issue = _make_issue("TEST-3", priority=None)
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["priority"] is None

    def test_null_sprint_field_does_not_crash(self, app_config, db_config):
        """Fix verification: None sprint field should not raise TypeError."""
        issue = _make_issue("TEST-4", sprint_id=None)
        # Set the sprint field to explicitly None (not empty list)
        issue["fields"]["customfield_10020"] = None
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["sprint_id"] is None

    def test_null_issuetype_does_not_crash(self, app_config, db_config):
        """Fix verification: None issuetype should not raise AttributeError."""
        issue = _make_issue("TEST-5")
        issue["fields"]["issuetype"] = None
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["issue_type"] == ""

    def test_null_status_does_not_crash(self, app_config, db_config):
        """Fix verification: None status should not raise AttributeError."""
        issue = _make_issue("TEST-6")
        issue["fields"]["status"] = None
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["status"] == ""

    def test_null_fields_does_not_crash(self, app_config, db_config):
        """Entirely null fields dict should not crash."""
        issue = {"key": "TEST-7", "fields": None, "changelog": {"histories": []}}
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["issue_id"] == "TEST-7"

    def test_epic_does_not_set_epic_key(self, app_config, db_config):
        issue = _make_issue("EPIC-1", issue_type="Epic")
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        assert result[0]["epic_key"] is None

    def test_synced_at_is_timezone_aware(self, app_config, db_config):
        issue = _make_issue("TEST-8")
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            result = ing.normalize([issue])
        dt = datetime.fromisoformat(result[0]["synced_at"])
        assert dt.tzinfo is not None


class TestJiraIssueIngestorParseChangelog:
    def test_extracts_started_at_from_in_progress_transition(self, app_config):
        histories = [
            {
                "created": "2024-01-06T09:00:00.000+0000",
                "items": [
                    {"field": "status", "toString": "In Progress", "fromString": "To Do"}
                ],
            }
        ]
        issue = _make_issue("TEST-1", changelog_histories=histories)
        ing = JiraIssueIngestor(app_config)
        started_at, _ = ing._parse_changelog(issue)
        assert started_at == "2024-01-06T09:00:00.000+0000"

    def test_returns_none_when_no_in_progress_transition(self, app_config):
        issue = _make_issue("TEST-2", changelog_histories=[])
        ing = JiraIssueIngestor(app_config)
        started_at, _ = ing._parse_changelog(issue)
        assert started_at is None

    def test_marks_unplanned_when_sprint_changed(self, app_config):
        histories = [
            {
                "created": "2024-01-07T10:00:00.000+0000",
                "items": [
                    {
                        "field": "Sprint",
                        "fieldId": "customfield_10020",
                        "fromString": "Sprint 1",
                        "toString": "Sprint 2",
                    }
                ],
            }
        ]
        issue = _make_issue("TEST-3", changelog_histories=histories)
        ing = JiraIssueIngestor(app_config)
        _, is_unplanned = ing._parse_changelog(issue)
        assert is_unplanned == 1

    def test_not_unplanned_when_sprint_added_first_time(self, app_config):
        histories = [
            {
                "created": "2024-01-07T10:00:00.000+0000",
                "items": [
                    {
                        "field": "Sprint",
                        "fieldId": "customfield_10020",
                        "fromString": None,
                        "toString": "Sprint 1",
                    }
                ],
            }
        ]
        issue = _make_issue("TEST-4", changelog_histories=histories)
        ing = JiraIssueIngestor(app_config)
        _, is_unplanned = ing._parse_changelog(issue)
        assert is_unplanned == 0

    def test_handles_malformed_changelog_date(self, app_config):
        """Fix verification: bad dates should not crash, just skip."""
        histories = [
            {
                "created": "not-a-date",
                "items": [
                    {
                        "field": "Sprint",
                        "fieldId": "customfield_10020",
                        "fromString": "Sprint 1",
                        "toString": "Sprint 2",
                    }
                ],
            }
        ]
        issue = _make_issue("TEST-5", changelog_histories=histories)
        ing = JiraIssueIngestor(app_config)
        # Should not raise
        _, is_unplanned = ing._parse_changelog(issue)
        # Malformed date → skipped, so not marked unplanned
        assert is_unplanned == 0

    def test_handles_missing_created_in_history(self, app_config):
        histories = [
            {
                "items": [
                    {
                        "field": "Sprint",
                        "fieldId": "customfield_10020",
                        "fromString": "Sprint 1",
                        "toString": "Sprint 2",
                    }
                ],
            }
        ]
        issue = _make_issue("TEST-6", changelog_histories=histories)
        ing = JiraIssueIngestor(app_config)
        # Should not raise
        _, is_unplanned = ing._parse_changelog(issue)
        assert is_unplanned == 0


class TestJiraIssueIngestorGetTeamFromSprint:
    def test_resolves_team_from_sprint_in_db(self, app_config, db_config):
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            team = ing._get_team_from_sprint(100)
        assert team == "payments-backend"

    def test_returns_unknown_for_missing_sprint(self, app_config, db_config):
        ing = JiraIssueIngestor(app_config)
        with patch("src.db.connection.get_config", return_value=db_config):
            team = ing._get_team_from_sprint(99999)
        assert team == "unknown"

    def test_returns_unknown_for_none_sprint(self, app_config, db_config):
        ing = JiraIssueIngestor(app_config)
        team = ing._get_team_from_sprint(None)
        assert team == "unknown"

    def test_returns_unknown_for_zero_sprint(self, app_config, db_config):
        ing = JiraIssueIngestor(app_config)
        team = ing._get_team_from_sprint(0)
        assert team == "unknown"


# ---------------------------------------------------------------------------
# JiraEpicIngestor
# ---------------------------------------------------------------------------

class TestJiraEpicIngestorFetchRaw:
    def test_fetches_epics_with_pagination(self, app_config):
        page1 = {"issues": [_make_epic(f"EPIC-{i}") for i in range(100)], "total": 120}
        page2 = {"issues": [_make_epic(f"EPIC-{i}") for i in range(100, 120)], "total": 120}
        responses = [_mock_response(page1), _mock_response(page2)]
        ing = JiraEpicIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()
        assert len(result) == 120

    def test_handles_api_error(self, app_config):
        bad = _mock_response({}, 500)
        bad.raise_for_status.side_effect = Exception("500")
        ing = JiraEpicIngestor(app_config)
        with patch("requests.get", side_effect=[bad]):
            result = ing.fetch_raw()
        assert result == []


class TestJiraEpicIngestorNormalize:
    def test_normalize_maps_fields(self, app_config):
        raw = [_make_epic("EPIC-1", "In Progress")]
        ing = JiraEpicIngestor(app_config)
        result = ing.normalize(raw)

        assert len(result) == 1
        r = result[0]
        assert r["epic_key"] == "EPIC-1"
        assert r["epic_name"] == "Epic EPIC-1"
        assert r["status"] == "In Progress"
        assert r["confidence"] == 0.5

    def test_null_status_does_not_crash(self, app_config):
        """Fix verification: None status in epic should not crash."""
        epic = _make_epic("EPIC-2")
        epic["fields"]["status"] = None
        ing = JiraEpicIngestor(app_config)
        result = ing.normalize([epic])
        assert result[0]["status"] == ""

    def test_synced_at_is_timezone_aware(self, app_config):
        raw = [_make_epic("EPIC-3")]
        ing = JiraEpicIngestor(app_config)
        result = ing.normalize(raw)
        dt = datetime.fromisoformat(result[0]["synced_at"])
        assert dt.tzinfo is not None


# ---------------------------------------------------------------------------
# Full run() integration
# ---------------------------------------------------------------------------

class TestJiraSprintIngestorRun:
    def test_run_inserts_sprints_into_db(self, app_config, db_config):
        sprints = {"values": [_make_sprint(200, 42, "Sprint 200", "active")]}
        responses = [_mock_response(sprints), _mock_response({"values": []})]
        ing = JiraSprintIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            with patch("src.db.connection.get_config", return_value=db_config):
                count = ing.run()
        assert count == 1


class TestJiraIssueIngestorRun:
    def test_run_inserts_issues_into_db(self, app_config, db_config):
        # Insert the engineer FK that the issue's assignee references
        conn = sqlite3.connect(db_config["db"]["url"])
        conn.execute(
            "INSERT OR IGNORE INTO engineers (engineer_id, display_name, team_id) VALUES (?, ?, ?)",
            ("user:abc", "Test User", "payments-backend"),
        )
        conn.commit()
        conn.close()

        issues = {"issues": [_make_issue("TEST-100", sprint_id=100)], "total": 1}
        empty = {"issues": [], "total": 0}
        responses = [_mock_response(issues), _mock_response(empty)]
        ing = JiraIssueIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            with patch("src.db.connection.get_config", return_value=db_config):
                count = ing.run()
        assert count == 1
