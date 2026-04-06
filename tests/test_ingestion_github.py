"""Tests for src/ingestion/github.py – GithubPRIngestor and GithubDeploymentIngestor."""

import sqlite3
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest
import requests

import src.config as cfg_module
from src.ingestion.github import GithubPRIngestor, GithubDeploymentIngestor

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
    db_file = tmp_path / "github_test.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(SCHEMA_SQL.read_text())
    for team_id, name in [("payments-backend", "Payments Backend"), ("platform-infra", "Platform Infra")]:
        conn.execute(
            "INSERT OR IGNORE INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name)
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

def _make_pr(number=1, repo_full="org/payments-api", state="open", title="Fix bug"):
    return {
        "_repo_full": repo_full,
        "number": number,
        "title": title,
        "state": state,
        "user": {"login": "alicedev"},
        "additions": 50,
        "deletions": 10,
        "review_comments": 3,
        "created_at": "2024-01-10T09:00:00Z",
        "merged_at": None,
        "closed_at": None,
    }


def _make_deployment(deploy_id=1001, repo_full="org/payments-api", environment="production"):
    return {
        "_repo_full": repo_full,
        "id": deploy_id,
        "environment": environment,
        "sha": "abc123def456",
        "created_at": "2024-01-15T14:00:00Z",
    }


def _mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


# ---------------------------------------------------------------------------
# GithubPRIngestor – _headers
# ---------------------------------------------------------------------------

class TestGithubPRIngestorHeaders:
    def test_headers_contain_auth_token(self, app_config):
        ing = GithubPRIngestor(app_config)
        headers = ing._headers()
        assert headers["Authorization"] == "token ghp_test_token"
        assert "application/vnd.github" in headers["Accept"]


# ---------------------------------------------------------------------------
# GithubPRIngestor – fetch_raw
# ---------------------------------------------------------------------------

class TestGithubPRIngestorFetchRaw:
    def test_fetches_prs_for_all_repos(self, app_config):
        """Should iterate each repo and collect PRs."""
        pr_page = [_make_pr(1, "org/payments-api"), _make_pr(2, "org/payments-api")]
        # Return one page of results (< 100 items stops pagination automatically)
        responses = [
            _mock_response(pr_page),                            # payments-api: 2 PRs (< 100, stops)
            _mock_response([_make_pr(3, "org/payments-worker")]),  # payments-worker: 1 PR
            _mock_response([_make_pr(4, "org/infra-core")]),      # infra-core: 1 PR
        ]

        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()

        assert len(result) == 4

    def test_attaches_repo_full_to_each_pr(self, app_config):
        pr = _make_pr(1, "org/payments-api")
        del pr["_repo_full"]  # remove so we test the attachment

        responses = [_mock_response([pr]), _mock_response([])]
        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()

        assert any(r.get("_repo_full") == "org/payments-api" for r in result)

    def test_paginates_when_full_page_returned(self, app_config):
        """When a page has exactly 100 items, should request the next page."""
        page1 = [_make_pr(i, "org/payments-api") for i in range(100)]
        page2 = [_make_pr(100, "org/payments-api")]
        # Only one repo in this test; remaining repos return empty
        responses = [
            _mock_response(page1),
            _mock_response(page2),
            _mock_response([]),  # stop
            _mock_response([]),  # payments-worker
            _mock_response([]),
            _mock_response([]),  # infra-core
        ]

        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()

        # Should have 101 from payments-api
        api_prs = [r for r in result if r.get("_repo_full") == "org/payments-api"]
        assert len(api_prs) == 101

    def test_handles_http_error_gracefully(self, app_config):
        """HTTP errors should be caught and logged; result is partial."""
        bad_resp = _mock_response([], 403)
        bad_resp.raise_for_status.side_effect = requests.HTTPError("403")
        good_prs = [_make_pr(1, "org/payments-worker")]

        responses = [
            bad_resp,                           # payments-api fails
            _mock_response(good_prs),           # payments-worker ok
            _mock_response([]),                 # stop
            _mock_response([]),                 # infra-core
        ]

        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()

        # payments-api failed; payments-worker succeeded
        assert any(r.get("_repo_full") == "org/payments-worker" for r in result)

    def test_handles_connection_error_gracefully(self, app_config):
        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=ConnectionError("Network unreachable")):
            result = ing.fetch_raw()
        assert result == []

    def test_repo_without_slash_uses_org_prefix(self, app_config):
        """A repo entry without '/' should use org as owner."""
        config = {**app_config}
        config["github"] = {
            **app_config["github"],
            "repos": {"payments-api": "payments-backend"},
        }
        responses = [_mock_response([_make_pr(1, "payments-api")]), _mock_response([])]
        ing = GithubPRIngestor(config)
        with patch("requests.get", side_effect=responses) as mock_get:
            ing.fetch_raw()
        call_url = mock_get.call_args_list[0][0][0]
        assert "org/payments-api" in call_url


# ---------------------------------------------------------------------------
# GithubPRIngestor – normalize
# ---------------------------------------------------------------------------

class TestGithubPRIngestorNormalize:
    def test_normalize_maps_fields(self, app_config):
        raw = [_make_pr(1, "org/payments-api")]
        ing = GithubPRIngestor(app_config)
        result = ing.normalize(raw)

        assert len(result) == 1
        r = result[0]
        assert r["pr_id"] == "org/payments-api#1"
        assert r["repo"] == "org/payments-api"
        assert r["team_id"] == "payments-backend"
        assert r["author"] == "alicedev"
        assert r["title"] == "Fix bug"
        assert r["state"] == "open"
        assert r["additions"] == 50
        assert r["deletions"] == 10
        assert r["review_count"] == 3
        assert r["opened_at"] == "2024-01-10T09:00:00Z"
        assert r["merged_at"] is None
        assert r["first_review_at"] is None

    def test_unknown_repo_gets_unknown_team(self, app_config):
        raw = [_make_pr(1, "unknown/repo")]
        ing = GithubPRIngestor(app_config)
        result = ing.normalize(raw)
        assert result[0]["team_id"] == "unknown"

    def test_missing_user_handled(self, app_config):
        pr = _make_pr(1, "org/payments-api")
        pr["user"] = None
        ing = GithubPRIngestor(app_config)
        result = ing.normalize([pr])
        assert result[0]["author"] is None

    def test_synced_at_is_recent_iso_string(self, app_config):
        raw = [_make_pr(1, "org/payments-api")]
        ing = GithubPRIngestor(app_config)
        result = ing.normalize(raw)
        # Should be parseable as a datetime
        datetime.fromisoformat(result[0]["synced_at"])

    def test_normalize_empty_list(self, app_config):
        ing = GithubPRIngestor(app_config)
        assert ing.normalize([]) == []


# ---------------------------------------------------------------------------
# GithubDeploymentIngestor – _headers
# ---------------------------------------------------------------------------

class TestGithubDeploymentIngestorHeaders:
    def test_headers_contain_auth_token(self, app_config):
        ing = GithubDeploymentIngestor(app_config)
        headers = ing._headers()
        assert "token ghp_test_token" == headers["Authorization"]


# ---------------------------------------------------------------------------
# GithubDeploymentIngestor – fetch_raw
# ---------------------------------------------------------------------------

class TestGithubDeploymentIngestorFetchRaw:
    def test_fetches_deployments_for_all_repos(self, app_config):
        deploy = _make_deployment(1001, "org/payments-api")
        del deploy["_repo_full"]

        responses = [
            _mock_response([deploy]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
        ]
        ing = GithubDeploymentIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()

        assert len(result) >= 1
        assert result[0]["_repo_full"] == "org/payments-api"

    def test_handles_http_error_gracefully(self, app_config):
        bad = _mock_response([], 500)
        bad.raise_for_status.side_effect = requests.HTTPError("500")
        responses = [bad, _mock_response([]), _mock_response([]), _mock_response([])]
        ing = GithubDeploymentIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()
        assert isinstance(result, list)

    def test_paginates_full_page(self, app_config):
        page1 = [_make_deployment(i, "org/payments-api") for i in range(100)]
        for d in page1:
            del d["_repo_full"]
        page2 = [_make_deployment(200, "org/payments-api")]
        del page2[0]["_repo_full"]

        responses = [
            _mock_response(page1),
            _mock_response(page2),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
        ]
        ing = GithubDeploymentIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            result = ing.fetch_raw()

        api_deploys = [r for r in result if r.get("_repo_full") == "org/payments-api"]
        assert len(api_deploys) == 101


# ---------------------------------------------------------------------------
# GithubDeploymentIngestor – normalize
# ---------------------------------------------------------------------------

class TestGithubDeploymentIngestorNormalize:
    def test_normalize_maps_fields(self, app_config):
        raw = [_make_deployment(1001, "org/payments-api")]
        ing = GithubDeploymentIngestor(app_config)
        result = ing.normalize(raw)

        assert len(result) == 1
        r = result[0]
        assert r["deployment_id"] == "org/payments-api:1001"
        assert r["repo"] == "org/payments-api"
        assert r["team_id"] == "payments-backend"
        assert r["environment"] == "production"
        assert r["sha"] == "abc123def456"
        assert r["deployed_at"] == "2024-01-15T14:00:00Z"
        assert r["caused_incident"] == 0

    def test_unknown_repo_gets_unknown_team(self, app_config):
        raw = [_make_deployment(1, "unknown/repo")]
        ing = GithubDeploymentIngestor(app_config)
        result = ing.normalize(raw)
        assert result[0]["team_id"] == "unknown"

    def test_normalize_empty_list(self, app_config):
        ing = GithubDeploymentIngestor(app_config)
        assert ing.normalize([]) == []

    def test_synced_at_is_recent_iso_string(self, app_config):
        raw = [_make_deployment(1, "org/payments-api")]
        ing = GithubDeploymentIngestor(app_config)
        result = ing.normalize(raw)
        datetime.fromisoformat(result[0]["synced_at"])


# ---------------------------------------------------------------------------
# Full run() integration
# ---------------------------------------------------------------------------

class TestGithubPRIngestorRun:
    def test_run_inserts_prs_into_db(self, app_config, db_config):
        pr = _make_pr(1, "org/payments-api")
        del pr["_repo_full"]

        responses = [
            _mock_response([pr]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
            _mock_response([]),
        ]

        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            with patch("src.db.connection.get_config", return_value=db_config):
                count = ing.run()

        assert count >= 1

    def test_run_empty_fetch_returns_zero(self, app_config, db_config):
        responses = [_mock_response([]) for _ in range(6)]
        ing = GithubPRIngestor(app_config)
        with patch("requests.get", side_effect=responses):
            with patch("src.db.connection.get_config", return_value=db_config):
                count = ing.run()
        assert count == 0
