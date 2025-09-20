from fastapi import FastAPI
# from api.routes import permits  # configure later


app = FastAPI(
    title = "Texas RRC Oil & Gas Permits API",
    version = "0.1.0"
)

# app.include_router(permits.router, prefix="/api/permits", tags=["Permits"])

@app.get("/")
async def root():
    return {"message": "Welcome to the Texas RRC Oil Permits API"}

