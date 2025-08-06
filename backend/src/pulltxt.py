from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os
import shutil

download_dir = os.path.abspath("downloads")

if os.path.exists(download_dir):
    shutil.rmtree(download_dir)
    
os.makedirs(download_dir, exist_ok=True)

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=options)

try:
    url = "https://mft.rrc.texas.gov/link/beeeab0c-7d07-4111-af88-783c93677b2c"
    driver.get(url)
    driver.find_element(By.XPATH, '//*[@id="fileTable:0:j_id_2f"]').click()

    downloading = True

    print("Download started....")
    while downloading:
        time.sleep(1)
        downloading = any(f.endswith(".crdownload") for f in os.listdir(download_dir))

finally:
    driver.quit()

print(f"Download complete in: {download_dir}")
