"""Base ingestor class for all data sources."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from src.db.connection import get_db


logger = logging.getLogger(__name__)


class BaseIngestor(ABC):
    """
    Base class for all ingestors.

    Subclasses implement:
      - fetch_raw() → list of dicts from external API
      - normalize() → list of dicts matching DB schema
      - table_name → target table name
    """

    table_name: str = None

    def __init__(self, config: dict):
        """Initialize ingestor with config."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> int:
        """Execute the full ingest pipeline: fetch → normalize → upsert."""
        self.logger.info(f"Starting ingestion: {self.__class__.__name__}")

        try:
            raw = self.fetch_raw()
            self.logger.debug(f"Fetched {len(raw)} raw records")

            normalized = self.normalize(raw)
            self.logger.debug(f"Normalized {len(normalized)} records")

            count = self.upsert(normalized)
            self.logger.info(f"Upserted {count} records into {self.table_name}")

            return count
        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}", exc_info=True)
            raise

    @abstractmethod
    def fetch_raw(self) -> list[dict]:
        """Fetch raw data from external source."""
        pass

    @abstractmethod
    def normalize(self, raw: list[dict]) -> list[dict]:
        """Normalize raw data to match database schema."""
        pass

    def upsert(self, records: list[dict]) -> int:
        """Insert or replace records into the database."""
        if not records:
            return 0

        if not self.table_name:
            raise ValueError(f"{self.__class__.__name__} must define table_name")

        with get_db() as conn:
            cursor = conn.cursor()

            # Build INSERT OR REPLACE query
            cols = list(records[0].keys())
            placeholders = ", ".join(["?"] * len(cols))
            col_names = ", ".join(cols)
            sql = f"INSERT OR REPLACE INTO {self.table_name} ({col_names}) VALUES ({placeholders})"

            # Execute batch insert
            rows = [tuple(r.get(c) for c in cols) for r in records]
            cursor.executemany(sql, rows)
            conn.commit()

            return len(records)
