"""Database initialization and seeding."""

import logging
from pathlib import Path

from src.config import get_config
from src.db import queries
from src.db.connection import get_db

logger = logging.getLogger(__name__)


def apply_schema():
    """Create database schema from schema.sql."""
    schema_path = Path("db/schema.sql")
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return False

    with open(schema_path) as f:
        schema_sql = f.read()

    with get_db() as conn:
        cursor = conn.cursor()
        try:
            # Split and execute statements individually to preserve
            # the get_db() context manager's transaction semantics.
            # executescript() would implicitly commit and bypass our
            # rollback-on-error guarantee.
            for statement in schema_sql.split(";"):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            logger.info("Schema applied successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to apply schema: {e}")
            return False


def seed_teams():
    """Seed team and engineer data from config."""
    config = get_config()
    teams_config = config["teams"]

    with get_db() as conn:
        cursor = conn.cursor()

        for team in teams_config.get("teams", []):
            # Insert team
            cursor.execute(
                queries.INSERT_OR_REPLACE_TEAM,
                (
                    team["id"],
                    team["name"],
                    team.get("sdm"),
                    team.get("director"),
                    team.get("department"),
                ),
            )
            logger.info(f"Seeded team: {team['id']}")

            # Insert engineers
            for engineer in team.get("engineers", []):
                cursor.execute(
                    queries.INSERT_OR_REPLACE_ENGINEER,
                    (
                        engineer["jira_id"],
                        engineer["name"],
                        team["id"],
                        engineer.get("github"),
                        1,
                    ),
                )
                logger.info(f"Seeded engineer: {engineer['name']}")

        # No explicit commit needed — get_db() context manager commits on success
        logger.info("Teams and engineers seeded")


def init_db():
    """Initialize database: apply schema and seed initial data."""
    logger.info("Initializing database...")
    if not apply_schema():
        return False
    seed_teams()
    logger.info("Database initialization complete")
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
