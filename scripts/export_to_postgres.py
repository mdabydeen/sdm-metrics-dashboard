#!/usr/bin/env python3
"""
Phase 3 migration: SQLite → PostgreSQL

This script exports data from SQLite and imports it into PostgreSQL.
Assumes PostgreSQL database already exists and schema has been applied.

Usage:
    python scripts/export_to_postgres.py --sqlite data/metrics.db --postgres postgresql://user:pass@host:5432/sdm_metrics
"""

import argparse
import sqlite3
import logging
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate_table(sqlite_conn, pg_conn, table_name):
    """Migrate a single table from SQLite to PostgreSQL."""
    logger.info(f"Migrating table: {table_name}")

    # Read from SQLite
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in sqlite_cursor.description]
    rows = sqlite_cursor.fetchall()

    if not rows:
        logger.info(f"  No rows to migrate")
        return

    # Insert into PostgreSQL
    pg_cursor = pg_conn.cursor()
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

    try:
        execute_values(pg_cursor, sql, rows, page_size=100)
        pg_conn.commit()
        logger.info(f"  Migrated {len(rows)} rows")
    except Exception as e:
        logger.error(f"  Failed to migrate {table_name}: {e}")
        pg_conn.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description="Migrate SDM Metrics from SQLite to PostgreSQL")
    parser.add_argument("--sqlite", required=True, help="Path to SQLite database file")
    parser.add_argument("--postgres", required=True, help="PostgreSQL connection string")
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite)
    if not sqlite_path.exists():
        logger.error(f"SQLite file not found: {sqlite_path}")
        return 1

    logger.info(f"Starting migration from {sqlite_path} to {args.postgres}")

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    pg_conn = psycopg2.connect(args.postgres)

    try:
        # Tables to migrate (in dependency order)
        tables = [
            "teams",
            "engineers",
            "sprints",
            "issues",
            "issue_changelog",
            "epics",
            "sprint_capacity",
            "pull_requests",
            "deployments",
            "sprint_metrics",
            "sync_state",
        ]

        for table in tables:
            migrate_table(sqlite_conn, pg_conn, table)

        logger.info("Migration complete!")
        return 0

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1

    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    exit(main())
