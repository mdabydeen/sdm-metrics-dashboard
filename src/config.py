import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def load_teams_config() -> dict:
    """Load teams hierarchy from YAML."""
    teams_path = Path(os.environ.get("TEAMS_CONFIG", "config/teams.yaml"))
    if not teams_path.exists():
        raise FileNotFoundError(f"Teams config not found: {teams_path}")

    with open(teams_path) as f:
        return yaml.safe_load(f)


def _extract_board_ids(teams_config: dict) -> list:
    """Extract all JIRA board IDs from teams config."""
    board_ids = []
    for team in teams_config.get("teams", []):
        board_ids.extend(team.get("jira_board_ids", []))
    return board_ids


def _build_board_team_map(teams_config: dict) -> dict:
    """Create mapping of JIRA board ID -> team ID."""
    mapping = {}
    for team in teams_config.get("teams", []):
        for board_id in team.get("jira_board_ids", []):
            mapping[str(board_id)] = team["id"]
    return mapping


def _extract_repos(teams_config: dict) -> dict:
    """Create mapping of repo -> team ID."""
    mapping = {}
    for team in teams_config.get("teams", []):
        for repo in team.get("github_repos", []):
            mapping[repo] = team["id"]
    return mapping


def load_config() -> dict:
    """Load and assemble all configuration."""
    teams = load_teams_config()

    config = {
        "jira": {
            "base_url": os.environ.get("JIRA_BASE_URL", "https://example.atlassian.net"),
            "email": os.environ.get("JIRA_EMAIL", ""),
            "api_token": os.environ.get("JIRA_API_TOKEN", ""),
            "story_points_field": os.environ.get("JIRA_STORY_POINTS_FIELD", "customfield_10016"),
            "sprint_field": os.environ.get("JIRA_SPRINT_FIELD", "customfield_10020"),
            "board_ids": _extract_board_ids(teams),
            "board_to_team": _build_board_team_map(teams),
        },
        "github": {
            "token": os.environ.get("GITHUB_TOKEN", ""),
            "org": os.environ.get("GITHUB_ORG", ""),
            "repos": _extract_repos(teams),
        },
        "servicenow": {
            "instance": os.environ.get("SNOW_INSTANCE", ""),
            "user": os.environ.get("SNOW_USER", ""),
            "password": os.environ.get("SNOW_PASSWORD", ""),
        },
        "db": {
            "url": os.environ.get("DATABASE_URL", "data/metrics.db"),
        },
        "teams": teams,
    }

    # Validate required credentials
    _validate_config(config)

    return config


def _validate_config(config: dict):
    """Validate that required credentials are configured."""
    jira_token = config["jira"]["api_token"]
    github_token = config["github"]["token"]

    if not jira_token:
        import logging
        logging.warning("JIRA_API_TOKEN is not set. JIRA ingestion will fail with 401 errors.")
    if not github_token:
        import logging
        logging.warning("GITHUB_TOKEN is not set. GitHub ingestion will fail with 401 errors.")


# Global config instance
_config = None


def get_config() -> dict:
    """Get or load global config."""
    global _config
    if _config is None:
        _config = load_config()
    return _config
