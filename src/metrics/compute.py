"""Compute and populate sprint metrics."""

import contextlib
import logging
from datetime import datetime

from src.db import queries
from src.db.connection import get_db

logger = logging.getLogger(__name__)


class MetricsComputer:
    """Compute metrics for completed sprints and upsert into sprint_metrics table."""

    def __init__(self):
        pass

    def compute_sprint_metrics(self, sprint_id: int, team_id: str) -> dict:
        """Compute all metrics for a single sprint/team combination."""
        with get_db() as conn:
            cursor = conn.cursor()

            # Fetch all issues in this sprint
            cursor.execute("""
                SELECT
                    issue_id, issue_type, status, story_points,
                    started_at, resolved_at, is_unplanned
                FROM issues
                WHERE sprint_id = ? AND team_id = ?
            """, (sprint_id, team_id))
            issues = [dict(row) for row in cursor.fetchall()]

            if not issues:
                logger.debug(f"No issues found for sprint {sprint_id}, team {team_id}")
                return None

            # Compute velocity: sum of story points for Done issues
            velocity = sum(
                (i.get("story_points") or 0)
                for i in issues
                if i.get("status", "").lower() in ("done", "completed")
            )

            # Count story types
            bug_count = sum(1 for i in issues if i.get("issue_type", "").lower() == "bug")
            story_count = sum(1 for i in issues if i.get("issue_type", "").lower() == "story")

            # Committed points: sum of all issues that were not marked as unplanned
            committed_points = sum(
                (i.get("story_points") or 0)
                for i in issues
                if not i.get("is_unplanned", 0)
            )

            # Commitment accuracy
            commitment_accuracy = (velocity / committed_points) if committed_points > 0 else 0.0

            # Scope creep rate: unplanned issues / total issues
            unplanned_count = sum(1 for i in issues if i.get("is_unplanned", 0))
            scope_creep_rate = (unplanned_count / len(issues)) if len(issues) > 0 else 0.0

            # Average cycle time in hours
            cycle_times = []
            for issue in issues:
                if issue.get("started_at") and issue.get("resolved_at"):
                    with contextlib.suppress(Exception):
                        start = datetime.fromisoformat(issue["started_at"].replace("Z", "+00:00"))
                        end = datetime.fromisoformat(issue["resolved_at"].replace("Z", "+00:00"))
                        cycle_hours = (end - start).total_seconds() / 3600.0
                        cycle_times.append(cycle_hours)

            avg_cycle_time_hrs = sum(cycle_times) / len(cycle_times) if cycle_times else 0.0

            # Fetch capacity for this sprint
            cursor.execute("""
                SELECT SUM(available_days) as total_days
                FROM sprint_capacity
                WHERE sprint_id = ?
            """, (sprint_id,))
            capacity_row = cursor.fetchone()
            capacity_total_days = capacity_row["total_days"] if capacity_row and capacity_row["total_days"] else 0.0

            # Utilization: velocity / (capacity_total_days * story_points_per_day)
            # Default: 4 story points per day of capacity
            story_points_per_day = 4.0
            utilization = (
                (velocity / (capacity_total_days * story_points_per_day))
                if capacity_total_days > 0
                else 0.0
            )

            return {
                "sprint_id": sprint_id,
                "team_id": team_id,
                "velocity": round(velocity, 2),
                "committed_points": round(committed_points, 2),
                "commitment_accuracy": round(commitment_accuracy, 3),
                "scope_creep_rate": round(scope_creep_rate, 3),
                "bug_count": bug_count,
                "story_count": story_count,
                "avg_cycle_time_hrs": round(avg_cycle_time_hrs, 2),
                "capacity_total_days": round(capacity_total_days, 2),
                "utilization": round(utilization, 3),
            }

    def compute_all(self) -> int:
        """Compute metrics for all sprints and upsert into sprint_metrics table."""
        logger.info("Computing sprint metrics...")

        with get_db() as conn:
            cursor = conn.cursor()

            # Get all unique sprint/team combinations
            cursor.execute("""
                SELECT DISTINCT sprint_id, team_id FROM issues
                WHERE sprint_id IS NOT NULL AND team_id IS NOT NULL AND team_id != 'unknown'
            """)
            sprint_teams = [dict(row) for row in cursor.fetchall()]

        upserted = 0
        for st in sprint_teams:
            sprint_id = st["sprint_id"]
            team_id = st["team_id"]

            metrics = self.compute_sprint_metrics(sprint_id, team_id)
            if not metrics:
                continue

            # Upsert into sprint_metrics
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    queries.INSERT_OR_REPLACE_SPRINT_METRICS,
                    (
                        metrics["sprint_id"],
                        metrics["team_id"],
                        metrics["velocity"],
                        metrics["committed_points"],
                        metrics["commitment_accuracy"],
                        metrics["scope_creep_rate"],
                        metrics["bug_count"],
                        metrics["story_count"],
                        metrics["avg_cycle_time_hrs"],
                        metrics["capacity_total_days"],
                        metrics["utilization"],
                    ),
                )
                conn.commit()
                upserted += 1

        logger.info(f"Computed and upserted {upserted} sprint metrics")
        return upserted


def compute_metrics() -> int:
    """Entrypoint for the scheduler to compute all sprint metrics."""
    return MetricsComputer().compute_all()
