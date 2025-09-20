from pathlib import Path

ROOT_DIR = Path.home() / "Documents" / "programming" / "txrrc" / "backend"

DOWNLOADS_DIR = ROOT_DIR / "data" / "download"
DB_DIR = ROOT_DIR / "data" / "database"

DB_PATH = DB_DIR / "rrc_permits.db"

ENCODING = "latin1"

# Ensure directories exist
DB_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

# Base URL for pulling permit data
BASE_URL = "https://mft.rrc.texas.gov/link/f5dfea9c-bb39-4a5e-a44e-fb522e088cba"

# Selenium settings
SELENIUM_HEADLESS = True
