import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from src.config import get_config

logger = logging.getLogger(__name__)


def get_connection():
    """Get database connection (SQLite or PostgreSQL)."""
    config = get_config()
    db_url = config["db"]["url"]

    if db_url.startswith("postgresql://"):
        try:
            import psycopg2
            return psycopg2.connect(db_url)
        except ImportError as e:
            raise ImportError("psycopg2 required for PostgreSQL. Install with: pip install psycopg2-binary") from e
    else:
        # SQLite
        # Auto-create data directory if it doesn't exist
        db_path = Path(db_url)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_url)
        # Enable WAL mode for better concurrency (multiple readers while writing)
        conn.execute("PRAGMA journal_mode=WAL")
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys=ON")
        # Return rows as dicts
        conn.row_factory = sqlite3.Row
        return conn


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
