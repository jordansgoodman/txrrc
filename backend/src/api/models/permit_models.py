from pydantic import BaseModel
from datetime import date 
from typing import Optional 


class PermitResponse(BaseModel):
    """
    Represents a row from the permit_data table.
    All columns are included with correct types.
    """

    # Core identifiers
    record_type: Optional[str] = None
    permit_number: str
    well_name: Optional[str] = None
    well_number: Optional[str] = None
    sidetrack_flag: Optional[str] = None

    # Location and classification
    county_code: Optional[str] = None
    api_number: Optional[str] = None
    field_number: Optional[str] = None
    field_name: Optional[str] = None

    # Measurements
    total_depth: Optional[str] = None
    vertical_depth: Optional[str] = None
    horizontal_length: Optional[str] = None

    # Dates
    spud_date: Optional[str] = None
    completion_date: Optional[str] = None
    plug_back_date: Optional[str] = None
    test_date: Optional[str] = None
    shut_in_date: Optional[str] = None

    # Locations
    surface_location: Optional[str] = None
    bottom_hole_location: Optional[str] = None

    # Additional details
    acreage: Optional[str] = None
    spacing: Optional[str] = None
    remarks: Optional[str] = None

    # Coordinates
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    class Config:
        from_attributes = True 