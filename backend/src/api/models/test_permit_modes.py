from src.api.models.permit_models import PermitResponse

permit = PermitResponse(
    permit_number="083436699001",
    well_name="REED",
    spud_date="2025-01-15",
    latitude=30.2672,
    longitude=-97.7431
)

print(permit.model_dump_json(indent=2))

