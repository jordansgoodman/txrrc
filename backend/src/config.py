from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DOWNLOAD_DIR = DATA_DIR / "download"
DB_PATH = DATA_DIR / "database" / "rrc_permits.db"


class Settings:
    DATABASE_URL = f"sqlite:///{DB_PATH}"
    STRIPE_API_KEY = "api-key"

DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
(DB_PATH.parent).mkdir(parents=True, exist_ok=True)

settings = Settings()