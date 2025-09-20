from fastapi import FastAPI
from src.api.routes import permits, admin


app = FastAPI(
    title = "Texas RRC Oil & Gas Permits API",
    version = "0.1.0"
)

app.include_router(permits.router, prefix="/api/permits", tags=["Permits"])


app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])


@app.get("/")
async def root():
    return {"message": "Welcome to the Texas RRC Oil Permits API"}

