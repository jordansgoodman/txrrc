from sqlite3 import Row, connect
from typing import List, Optional
from src.config import DB_PATH
from src.api.models.permit_models import PermitResponse

def _get_connection():
    """
    Internal helper to open a SQLite connection.
    Sets row_factory so rows behave like dictionaries.
    """
    conn = connect(DB_PATH)
    conn.row_factory = Row
    return conn

def get_all_permits(limit: int = 50) -> List[PermitResponse]:
    """
    Fetch a list of permits from the database.
    Default limit is 50.
    """
    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * 
            FROM permit_data
            ORDER BY permit_number ASC
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()

    return [PermitResponse(**dict(row)) for row in rows]

def get_permit_by_number(permit_number: str) -> Optional[PermitResponse]:
    """
    Fetch a single permit by its unique permit number.
    """
    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM permit_data
            WHERE permit_number = ?
        """, (permit_number,))
        row = cur.fetchone()

    return PermitResponse(**dict(row)) if row else None

def search_permits_by_well_name(well_name: str, limit: int = 50) -> List[PermitResponse]:
    """
    Search for permits by well_name (case-insensitive).
    """
    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM permit_data
            WHERE LOWER(well_name) LIKE LOWER(?)
            ORDER BY permit_number ASC
            LIMIT ?
        """, (f"%{well_name}%", limit))
        rows = cur.fetchall()

    return [PermitResponse(**dict(row)) for row in rows]
