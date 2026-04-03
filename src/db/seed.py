"""Database initialization and seeding."""

import logging
from pathlib import Path
from src.config import get_config
from src.db.connection import get_db
from src.db import queries

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
            cursor.executescript(schema_sql)
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

        conn.commit()
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
