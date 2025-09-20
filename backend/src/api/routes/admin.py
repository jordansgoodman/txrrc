from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from src.parsers.pulldata import pull_data, unpack_zip_files
from src.parsers.process import build_database

router = APIRouter()

def refresh_pipeline():
    """
    Pipeline to refresh the local SQLite database:
    1. Pull new .dat files from RRC.
    2. Process them into the database.
    """
    pull_data()
    unpack_zip_files()
    build_database()
    return {"status": "ok", "message": "Database refreshed"}

 

    

@router.post("/refresh-data")
def refresh_data(background_tasks: BackgroundTasks):
    """
    Trigger a background refresh of the database.
    This runs pull_data() + build_database() without blocking the request.
    """
    background_tasks.add_task(refresh_pipeline)
    return JSONResponse({"status": "refresh started"})
