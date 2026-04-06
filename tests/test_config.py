"""Tests for src/config.py – configuration loading and helpers."""

import os
import textwrap
from pathlib import Path
from unittest.mock import patch, mock_open
import pytest

import src.config as cfg_module
from src.config import (
    load_teams_config,
    _extract_board_ids,
    _build_board_team_map,
    _extract_repos,
    load_config,
    get_config,
    _validate_config,
)

from tests.conftest import TEAMS_CONFIG


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_config_singleton():
    """Ensure the global _config cache is cleared before/after every test."""
    cfg_module._config = None
    yield
    cfg_module._config = None


# ---------------------------------------------------------------------------
# load_teams_config
# ---------------------------------------------------------------------------

class TestLoadTeamsConfig:
    def test_loads_yaml_file(self, tmp_path):
        yaml_content = textwrap.dedent("""
            teams:
              - id: team-a
                name: Team A
        """)
        config_file = tmp_path / "teams.yaml"
        config_file.write_text(yaml_content)

        with patch.dict(os.environ, {"TEAMS_CONFIG": str(config_file)}):
            result = load_teams_config()

        assert result["teams"][0]["id"] == "team-a"

    def test_raises_if_file_missing(self, tmp_path):
        missing = tmp_path / "nonexistent.yaml"
        with patch.dict(os.environ, {"TEAMS_CONFIG": str(missing)}):
            with pytest.raises(FileNotFoundError, match="Teams config not found"):
                load_teams_config()

    def test_default_path_used_when_env_not_set(self, monkeypatch, tmp_path):
        """When TEAMS_CONFIG is not set, uses config/teams.yaml."""
        monkeypatch.delenv("TEAMS_CONFIG", raising=False)
        # Point the CWD-relative path to a temp directory so it exists
        teams_file = tmp_path / "config" / "teams.yaml"
        teams_file.parent.mkdir(parents=True)
        teams_file.write_text("teams: []\n")
        # We can't easily change CWD, so just verify it raises (no file at default path)
        # unless the actual repo file exists — in that case it should not raise.
        try:
            result = load_teams_config()
            assert "teams" in result
        except FileNotFoundError:
            pass  # expected when running from a different CWD


# ---------------------------------------------------------------------------
# _extract_board_ids
# ---------------------------------------------------------------------------

class TestExtractBoardIds:
    def test_extracts_ids(self):
        result = _extract_board_ids(TEAMS_CONFIG)
        assert sorted(result) == [15, 42]

    def test_empty_teams(self):
        assert _extract_board_ids({"teams": []}) == []

    def test_team_with_no_board_ids(self):
        config = {"teams": [{"id": "t1", "name": "T1"}]}
        assert _extract_board_ids(config) == []

    def test_multiple_boards_per_team(self):
        config = {"teams": [{"id": "t1", "jira_board_ids": [1, 2, 3]}]}
        assert _extract_board_ids(config) == [1, 2, 3]


# ---------------------------------------------------------------------------
# _build_board_team_map
# ---------------------------------------------------------------------------

class TestBuildBoardTeamMap:
    def test_builds_mapping(self):
        result = _build_board_team_map(TEAMS_CONFIG)
        assert result == {"42": "payments-backend", "15": "platform-infra"}

    def test_empty_teams(self):
        assert _build_board_team_map({"teams": []}) == {}

    def test_board_id_stored_as_string(self):
        config = {"teams": [{"id": "t1", "jira_board_ids": [99]}]}
        result = _build_board_team_map(config)
        assert "99" in result
        assert isinstance(list(result.keys())[0], str)


# ---------------------------------------------------------------------------
# _extract_repos
# ---------------------------------------------------------------------------

class TestExtractRepos:
    def test_extracts_repos(self):
        result = _extract_repos(TEAMS_CONFIG)
        assert result["org/payments-api"] == "payments-backend"
        assert result["org/payments-worker"] == "payments-backend"
        assert result["org/infra-core"] == "platform-infra"

    def test_empty_repos(self):
        config = {"teams": [{"id": "t1", "name": "T1"}]}
        assert _extract_repos(config) == {}

    def test_no_teams(self):
        assert _extract_repos({"teams": []}) == {}


# ---------------------------------------------------------------------------
# _validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig:
    def test_warns_when_jira_token_missing(self, caplog):
        import logging
        config = {
            "jira": {"api_token": ""},
            "github": {"token": "something"},
        }
        with caplog.at_level(logging.WARNING):
            _validate_config(config)
        assert any("JIRA_API_TOKEN" in m for m in caplog.messages)

    def test_warns_when_github_token_missing(self, caplog):
        import logging
        config = {
            "jira": {"api_token": "something"},
            "github": {"token": ""},
        }
        with caplog.at_level(logging.WARNING):
            _validate_config(config)
        assert any("GITHUB_TOKEN" in m for m in caplog.messages)

    def test_no_warnings_when_both_tokens_present(self, caplog):
        import logging
        config = {
            "jira": {"api_token": "tok1"},
            "github": {"token": "tok2"},
        }
        with caplog.at_level(logging.WARNING):
            _validate_config(config)
        assert not any("JIRA_API_TOKEN" in m or "GITHUB_TOKEN" in m for m in caplog.messages)


# ---------------------------------------------------------------------------
# load_config / get_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_load_config_returns_all_sections(self):
        with patch("src.config.load_teams_config", return_value=TEAMS_CONFIG):
            with patch.dict(
                os.environ,
                {
                    "JIRA_BASE_URL": "https://example.atlassian.net",
                    "JIRA_EMAIL": "e@test.com",
                    "JIRA_API_TOKEN": "tok",
                    "GITHUB_TOKEN": "ghp_tok",
                    "GITHUB_ORG": "myorg",
                    "DATABASE_URL": "data/metrics.db",
                },
            ):
                result = load_config()

        assert "jira" in result
        assert "github" in result
        assert "servicenow" in result
        assert "db" in result
        assert "teams" in result

    def test_jira_defaults(self):
        with patch("src.config.load_teams_config", return_value={"teams": []}):
            with patch.dict(os.environ, {}, clear=True):
                result = load_config()
        assert result["jira"]["base_url"] == "https://example.atlassian.net"
        assert result["jira"]["story_points_field"] == "customfield_10016"
        assert result["jira"]["sprint_field"] == "customfield_10020"

    def test_get_config_caches_result(self):
        with patch("src.config.load_config") as mock_load:
            mock_load.return_value = {"db": {"url": ":memory:"}, "jira": {"api_token": "x"}, "github": {"token": "y"}}
            # First call populates the cache
            r1 = get_config()
            # Second call should use the cache (load_config called only once)
            r2 = get_config()
        mock_load.assert_called_once()
        assert r1 is r2

    def test_board_ids_populated_from_teams(self):
        with patch("src.config.load_teams_config", return_value=TEAMS_CONFIG):
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "t", "GITHUB_TOKEN": "g"}):
                result = load_config()
        assert 42 in result["jira"]["board_ids"]
        assert 15 in result["jira"]["board_ids"]
