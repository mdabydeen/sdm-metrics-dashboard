"""GitHub data ingestors with pagination."""

from datetime import UTC, datetime

import requests

from .base import BaseIngestor


class GithubPRIngestor(BaseIngestor):
    """Ingest GitHub pull requests with pagination."""

    table_name = "pull_requests"

    def _headers(self):
        """GitHub API headers with auth token."""
        return {
            "Authorization": f"token {self.config['github']['token']}",
            "Accept": "application/vnd.github.v3+json",
        }

    def fetch_raw(self) -> list[dict]:
        """Fetch PRs from all configured repos with pagination."""
        org = self.config["github"]["org"]
        repos = self.config["github"]["repos"]

        all_prs = []
        for repo_full in repos:
            # Extract owner/repo from "org/repo" format
            if "/" in repo_full:
                owner, repo_name = repo_full.split("/", 1)
            else:
                owner = org
                repo_name = repo_full

            url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls"

            # Paginate through results
            page = 1
            while True:
                try:
                    resp = requests.get(
                        url,
                        headers=self._headers(),
                        params={"state": "all", "per_page": 100, "page": page},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    prs = resp.json()

                    if not prs:
                        break

                    # Attach repo_full to each PR for normalization
                    for pr in prs:
                        pr["_repo_full"] = repo_full

                    all_prs.extend(prs)

                    # Continue to next page if we got a full page
                    if len(prs) < 100:
                        break

                    page += 1

                except Exception as e:
                    self.logger.warning(f"Failed to fetch PRs from {owner}/{repo_name}: {e}")
                    break

        return all_prs

    def normalize(self, raw: list[dict]) -> list[dict]:
        """Normalize GitHub PR data."""
        repo_team_map = self.config["github"]["repos"]

        normalized = []
        for pr in raw:
            # Use the repo_full we attached during fetch
            repo_full = pr.get("_repo_full", "unknown/unknown")
            team_id = repo_team_map.get(repo_full, "unknown")

            # Null-safe user access
            user = pr.get("user") or {}

            normalized.append(
                {
                    "pr_id": f"{repo_full}#{pr['number']}",
                    "repo": repo_full,
                    "team_id": team_id,
                    "author": user.get("login"),
                    "title": pr.get("title", ""),
                    "state": pr.get("state", ""),
                    "additions": pr.get("additions", 0),
                    "deletions": pr.get("deletions", 0),
                    "review_count": pr.get("review_comments", 0),
                    "opened_at": pr.get("created_at"),
                    "first_review_at": None,  # Would need comment timeline
                    "merged_at": pr.get("merged_at"),
                    "closed_at": pr.get("closed_at"),
                    "synced_at": datetime.now(UTC).isoformat(),
                }
            )

        return normalized


class GithubDeploymentIngestor(BaseIngestor):
    """Ingest GitHub deployments with pagination."""

    table_name = "deployments"

    def _headers(self):
        return {
            "Authorization": f"token {self.config['github']['token']}",
            "Accept": "application/vnd.github.v3+json",
        }

    def fetch_raw(self) -> list[dict]:
        """Fetch recent deployments with pagination."""
        org = self.config["github"]["org"]
        repos = self.config["github"]["repos"]

        all_deployments = []
        for repo_full in repos:
            if "/" in repo_full:
                owner, repo_name = repo_full.split("/", 1)
            else:
                owner = org
                repo_name = repo_full

            url = f"https://api.github.com/repos/{owner}/{repo_name}/deployments"

            # Paginate through results
            page = 1
            while True:
                try:
                    resp = requests.get(
                        url,
                        headers=self._headers(),
                        params={"per_page": 100, "page": page, "environment": "production"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    deploys = resp.json()

                    if not deploys:
                        break

                    # Attach repo_full to each deployment
                    for deploy in deploys:
                        deploy["_repo_full"] = repo_full

                    all_deployments.extend(deploys)

                    # Continue to next page if we got a full page
                    if len(deploys) < 100:
                        break

                    page += 1

                except Exception as e:
                    self.logger.warning(
                        f"Failed to fetch deployments from {owner}/{repo_name}: {e}"
                    )
                    break

        return all_deployments

    def normalize(self, raw: list[dict]) -> list[dict]:
        """Normalize GitHub deployment data."""
        repo_team_map = self.config["github"]["repos"]

        normalized = []
        for deploy in raw:
            # Use the repo_full we attached during fetch
            repo_full = deploy.get("_repo_full", "unknown/unknown")
            team_id = repo_team_map.get(repo_full, "unknown")

            normalized.append(
                {
                    "deployment_id": f"{repo_full}:{deploy['id']}",
                    "repo": repo_full,
                    "team_id": team_id,
                    "environment": deploy.get("environment", "production"),
                    "sha": deploy.get("sha", ""),
                    "deployed_at": deploy.get("created_at"),
                    "caused_incident": 0,  # Would need incident tracking
                    "synced_at": datetime.now(UTC).isoformat(),
                }
            )

        return normalized
