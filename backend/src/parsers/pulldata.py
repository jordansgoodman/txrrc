# backend/src/parsers/pulldata.py

import time
import shutil
import zipfile
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from src.config import DOWNLOADS_DIR, BASE_URL, SELENIUM_HEADLESS


def clear_folder(folder_path: Path):
    if not folder_path.exists():
        print(f"Folder '{folder_path}' does not exist.")
        return
    for item in folder_path.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            print(f"Failed to delete {item}: {e}")


def downloads_in_progress(folder: Path) -> bool:
    return any(fname.name.endswith(".crdownload") for fname in folder.iterdir())


def unpack_zip_files():
    folder = DOWNLOADS_DIR
    extracted_files = []
    for zip_path in folder.glob("*.zip"):
        print(f"Unpacking {zip_path}...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(folder)
            extracted_files.extend([folder / name for name in zip_ref.namelist()])
        zip_path.unlink()
    return extracted_files


def pull_data():
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    clear_folder(DOWNLOADS_DIR)

    chrome_options = Options()
    if SELENIUM_HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(DOWNLOADS_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(BASE_URL)
        time.sleep(3)
        print("Opening page...")

        select_all_xpath = "/html/body/div[1]/div[2]/div[4]/div/form/div/div/table/thead/tr/th[1]/div/div[2]/span"
        driver.find_element(By.XPATH, select_all_xpath).click()
        time.sleep(1)
        print("Clicking checkbox...")

        download_all_xpath = "/html/body/div[3]/div/div/form/button/span"
        driver.find_element(By.XPATH, download_all_xpath).click()
        print("Clicking download all button...")

        print("Files are downloading...")

        time.sleep(10)
        while downloads_in_progress(DOWNLOADS_DIR):
            print("Waiting for downloads to finish...")
            time.sleep(60)

        print(f"All files downloaded to: {DOWNLOADS_DIR}")

    finally:
        driver.quit()


if __name__ == "__main__":
    pull_data()
    unpack_zip_files()
