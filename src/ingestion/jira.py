"""JIRA data ingestors."""

from datetime import UTC, datetime

import requests

from src.db.connection import get_db

from .base import BaseIngestor


def _safe_get(obj, key, default=None):
    """Safely get a key from an object that might be None."""
    if obj is None:
        return default
    return obj.get(key, default)


class JiraSprintIngestor(BaseIngestor):
    """Ingest JIRA sprints."""

    table_name = "sprints"

    def _auth(self):
        """HTTP basic auth tuple for JIRA."""
        return (self.config["jira"]["email"], self.config["jira"]["api_token"])

    def fetch_raw(self) -> list[dict]:
        """Fetch sprints from all configured JIRA boards."""
        base_url = self.config["jira"]["base_url"]
        board_ids = self.config["jira"]["board_ids"]

        sprints = []
        for board_id in board_ids:
            url = f"{base_url}/rest/agile/1.0/board/{board_id}/sprint"
            try:
                resp = requests.get(
                    url,
                    auth=self._auth(),
                    params={"state": "active,closed,future"},
                    timeout=30,
                )
                resp.raise_for_status()
                sprints.extend(resp.json().get("values", []))
            except Exception as e:
                self.logger.warning(f"Failed to fetch sprints from board {board_id}: {e}")

        return sprints

    def normalize(self, raw: list[dict]) -> list[dict]:
        """Normalize JIRA sprint data."""
        board_to_team = self.config["jira"]["board_to_team"]

        normalized = []
        for sprint in raw:
            normalized.append(
                {
                    "sprint_id": sprint["id"],
                    "board_id": sprint.get("originBoardId", 0),
                    "team_id": board_to_team.get(str(sprint.get("originBoardId", "")), "unknown"),
                    "sprint_name": sprint.get("name", ""),
                    "state": sprint.get("state", ""),
                    "start_date": sprint.get("startDate"),
                    "end_date": sprint.get("endDate"),
                    "goal": sprint.get("goal"),
                    "synced_at": datetime.now(UTC).isoformat(),
                }
            )

        return normalized


class JiraIssueIngestor(BaseIngestor):
    """Ingest JIRA issues with pagination."""

    table_name = "issues"

    def _auth(self):
        return (self.config["jira"]["email"], self.config["jira"]["api_token"])

    def _get_team_from_sprint(self, sprint_id: int) -> str:
        """Look up team_id from sprint_id via board_to_team map."""
        if not sprint_id:
            return "unknown"

        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT board_id FROM sprints WHERE sprint_id = ?", (sprint_id,))
                row = cursor.fetchone()
                if row:
                    board_id = row["board_id"]
                    return self.config["jira"]["board_to_team"].get(str(board_id), "unknown")
        except Exception as e:
            self.logger.warning(f"Failed to lookup team from sprint {sprint_id}: {e}")

        return "unknown"

    def fetch_raw(self) -> list[dict]:
        """Fetch all issues across all boards with pagination via the agile API."""
        base_url = self.config["jira"]["base_url"]
        board_ids = self.config["jira"]["board_ids"]

        all_issues = []
        for board_id in board_ids:
            url = f"{base_url}/rest/agile/1.0/board/{board_id}/issue"

            start_at = 0
            while True:
                try:
                    resp = requests.get(
                        url,
                        auth=self._auth(),
                        params={
                            "maxResults": 100,
                            "startAt": start_at,
                            "expand": "changelog",
                            "fields": "*all",
                        },
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    issues = data.get("issues", [])

                    if not issues:
                        break

                    all_issues.extend(issues)

                    # Check if there are more pages
                    if start_at + len(issues) >= data.get("total", 0):
                        break

                    start_at += 100

                except Exception as e:
                    self.logger.warning(f"Failed to fetch issues from board {board_id}: {e}")
                    break

        return all_issues

    def _parse_changelog(self, issue: dict) -> tuple:
        """Parse changelog to extract started_at and is_unplanned status."""
        changelog = issue.get("changelog", {}).get("histories", [])
        started_at = None
        is_unplanned = 0

        now = datetime.now(UTC)

        # Find when issue first entered "In Progress"
        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "status" and item.get("toString") == "In Progress":
                    started_at = history.get("created")
                    break
            if started_at:
                break

        # Check if sprint was added after sprint start (would indicate mid-sprint addition)
        sprint_field = self.config["jira"].get("sprint_field", "customfield_10020")
        for history in changelog:
            history_created = history.get("created")
            if not history_created:
                continue
            try:
                history_dt = datetime.fromisoformat(history_created.replace("Z", "+00:00"))
                if history_dt < now:
                    for item in history.get("items", []):
                        if (
                            item.get("fieldId") == sprint_field
                            and item.get("toString")
                            and item.get("fromString")
                        ):
                            # Sprint changed mid-sprint — mark as unplanned
                            is_unplanned = 1
            except (ValueError, TypeError):
                continue

        return started_at, is_unplanned

    def normalize(self, raw: list[dict]) -> list[dict]:
        """Normalize JIRA issue data with full field population."""
        story_points_field = self.config["jira"].get("story_points_field", "customfield_10016")
        sprint_field = self.config["jira"].get("sprint_field", "customfield_10020")

        normalized = []
        for issue in raw:
            fields = issue.get("fields") or {}

            # Extract story points directly from the configured custom field
            story_points = fields.get(story_points_field)

            # Extract sprint ID from sprint link field (guard against None)
            sprint_id = None
            sprint_link = fields.get(sprint_field) or []
            if sprint_link and len(sprint_link) > 0:
                sprint_id = _safe_get(sprint_link[0], "id")

            # Parse changelog for started_at and is_unplanned
            started_at, is_unplanned = self._parse_changelog(issue)

            # Get team_id from sprint
            team_id = self._get_team_from_sprint(sprint_id)

            # Null-safe field access for nullable JIRA objects
            assignee = fields.get("assignee")
            priority = fields.get("priority")
            issuetype = fields.get("issuetype") or {}
            status = fields.get("status") or {}
            parent = fields.get("parent")

            issue_type_name = _safe_get(issuetype, "name", "")

            normalized.append(
                {
                    "issue_id": issue["key"],
                    "issue_type": issue_type_name,
                    "summary": fields.get("summary", ""),
                    "status": _safe_get(status, "name", ""),
                    "priority": _safe_get(priority, "name"),
                    "story_points": story_points,
                    "assignee_id": _safe_get(assignee, "accountId"),
                    "team_id": team_id,
                    "sprint_id": sprint_id,
                    "epic_key": _safe_get(parent, "key") if issue_type_name != "Epic" else None,
                    "labels": ",".join(fields.get("labels") or []),
                    "created_at": fields.get("created"),
                    "resolved_at": fields.get("resolutiondate"),
                    "started_at": started_at,
                    "is_unplanned": is_unplanned,
                    "synced_at": datetime.now(UTC).isoformat(),
                }
            )

        return normalized


class JiraEpicIngestor(BaseIngestor):
    """Ingest JIRA epics with pagination."""

    table_name = "epics"

    def _auth(self):
        return (self.config["jira"]["email"], self.config["jira"]["api_token"])

    def fetch_raw(self) -> list[dict]:
        """Fetch all epics with pagination."""
        base_url = self.config["jira"]["base_url"]
        url = f"{base_url}/rest/api/3/search"

        epics = []
        start_at = 0

        while True:
            try:
                resp = requests.get(
                    url,
                    auth=self._auth(),
                    params={
                        "jql": "type = Epic",
                        "maxResults": 100,
                        "startAt": start_at,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                issues = data.get("issues", [])

                if not issues:
                    break

                epics.extend(issues)

                # Check if there are more pages
                if start_at + len(issues) >= data.get("total", 0):
                    break

                start_at += 100

            except Exception as e:
                self.logger.warning(f"Failed to fetch epics: {e}")
                break

        return epics

    def normalize(self, raw: list[dict]) -> list[dict]:
        """Normalize JIRA epic data."""
        normalized = []
        for epic in raw:
            fields = epic.get("fields") or {}
            status = fields.get("status") or {}

            normalized.append(
                {
                    "epic_key": epic["key"],
                    "epic_name": fields.get("summary", ""),
                    "team_id": "unknown",  # Will be inferred from child issues in Phase 2
                    "status": _safe_get(status, "name", ""),
                    "total_points": None,  # Will compute from child issues
                    "completed_points": 0,
                    "planned_start": None,
                    "planned_end": None,
                    "predicted_end": None,
                    "confidence": 0.5,
                    "synced_at": datetime.now(UTC).isoformat(),
                }
            )

        return normalized
