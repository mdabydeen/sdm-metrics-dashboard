"""FastAPI routes for capacity input."""

import logging
from fastapi import APIRouter, HTTPException, File, UploadFile
from pydantic import BaseModel
import csv
import io
from datetime import datetime

from src.db.connection import get_db
from src.db import queries
from src.config import get_config

logger = logging.getLogger(__name__)
router = APIRouter()


class CapacityEntry(BaseModel):
    """Capacity entry for a single engineer in a sprint."""
    sprint_id: int
    engineer_id: str
    available_days: float
    total_days: float = 10.0
    capacity_points: float = None
    notes: str = None


@router.get("/sprints")
def get_sprints():
    """Get list of recent sprints for dropdown (active, future, and recently closed)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sprint_id, sprint_name, team_id, start_date, end_date, state
            FROM sprints
            WHERE state IN ('active', 'future', 'closed')
            ORDER BY start_date DESC
            LIMIT 30
        """)
        sprints = [dict(row) for row in cursor.fetchall()]
    return sprints


@router.get("/engineers")
def get_engineers(team_id: str = None):
    """Get engineers, optionally filtered by team."""
    with get_db() as conn:
        cursor = conn.cursor()
        if team_id:
            cursor.execute("""
                SELECT engineer_id, display_name, team_id
                FROM engineers
                WHERE team_id = ? AND is_active = 1
                ORDER BY display_name
            """, (team_id,))
        else:
            cursor.execute("""
                SELECT engineer_id, display_name, team_id
                FROM engineers
                WHERE is_active = 1
                ORDER BY team_id, display_name
            """)
        engineers = [dict(row) for row in cursor.fetchall()]
    return engineers


@router.get("/teams")
def get_teams():
    """Get all teams."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT team_id, team_name, sdm_name, director_name
            FROM teams
            ORDER BY team_name
        """)
        teams = [dict(row) for row in cursor.fetchall()]
    return teams


@router.get("/capacity/{sprint_id}")
def get_capacity(sprint_id: int):
    """Get all capacity entries for a sprint."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                sc.id, sc.sprint_id, sc.engineer_id, sc.available_days,
                sc.total_days, sc.capacity_points, sc.notes,
                e.display_name, e.team_id
            FROM sprint_capacity sc
            JOIN engineers e ON e.engineer_id = sc.engineer_id
            WHERE sc.sprint_id = ?
            ORDER BY e.display_name
        """, (sprint_id,))
        entries = [dict(row) for row in cursor.fetchall()]
    return entries


@router.post("/capacity")
def create_capacity(entry: CapacityEntry):
    """Create or update a capacity entry."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                queries.INSERT_OR_REPLACE_CAPACITY,
                (
                    entry.sprint_id,
                    entry.engineer_id,
                    entry.available_days,
                    entry.total_days,
                    entry.capacity_points,
                    entry.notes,
                ),
            )
            conn.commit()

            logger.info(f"Recorded capacity for {entry.engineer_id} in sprint {entry.sprint_id}")

        return {
            "status": "ok",
            "message": f"Capacity recorded for sprint {entry.sprint_id}",
        }
    except Exception as e:
        logger.error(f"Failed to create capacity entry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/capacity/import")
async def import_capacity_csv(file: UploadFile = File(...)):
    """Import capacity entries from CSV.

    CSV format:
    sprint_id,engineer_id,available_days,total_days,capacity_points,notes
    1,user:abc123,8,10,32,"On-call rotation"
    """
    try:
        content = await file.read()
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))

        imported = 0
        errors = []

        with get_db() as conn:
            cursor = conn.cursor()

            for i, row in enumerate(reader, 1):
                try:
                    cursor.execute(
                        queries.INSERT_OR_REPLACE_CAPACITY,
                        (
                            int(row["sprint_id"]),
                            row["engineer_id"],
                            float(row["available_days"]),
                            float(row.get("total_days", 10)),
                            float(row.get("capacity_points")) if row.get("capacity_points") else None,
                            row.get("notes"),
                        ),
                    )
                    imported += 1
                except Exception as e:
                    errors.append(f"Row {i}: {str(e)}")

            conn.commit()

        logger.info(f"Imported {imported} capacity entries from CSV")

        return {
            "status": "ok",
            "imported": imported,
            "errors": errors,
        }
    except Exception as e:
        logger.error(f"CSV import failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
