from fastapi import APIRouter, HTTPException, Query
from typing import List
from src.api.models.permit_models import PermitResponse
from src.api.services.permits_service import (
    get_all_permits,
    get_permit_by_number,
    search_permits_by_well_name
)

router = APIRouter()

@router.get("/",response_model=List[PermitResponse])
def list_permits(limit: int = Query(50, le=100,description="Maximum number of permits to return")):
    return get_all_permits(limit=limit)

@router.get("/{permit_number}",response_model=PermitResponse)
def fetch_permit(permit_number: str):
    permit = get_permit_by_number(permit_number)
    if not permit:
        raise HTTPException(status_code=404, detail="Permit not found")
    return permit

@router.get("/search",response_model=List[PermitResponse])
def search_permits(well_name: str = Query(...,min_length=2,description="Well name to search for"), limit: int = 50):
    return search_permits_by_well_name(well_name,limit=limit)


