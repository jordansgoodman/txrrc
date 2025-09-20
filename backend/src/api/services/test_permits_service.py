from src.api.services.permits_service import (
    get_all_permits,
    get_permit_by_number,
    search_permits_by_well_name
)

print("=== First 5 Permits ===")
permits = get_all_permits(limit=5)
for p in permits:
    print(p.model_dump())

print("\n=== Single Permit ===")
single_permit = get_permit_by_number("083436699001")
print(single_permit.model_dump() if single_permit else "Permit not found")

print("\n=== Search for 'REED' ===")
results = search_permits_by_well_name("REED")
for r in results:
    print(r.model_dump())
