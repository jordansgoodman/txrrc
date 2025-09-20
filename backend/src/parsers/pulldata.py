import os
import time
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# ----------------------------
# Config
# ----------------------------
base_url = "https://mft.rrc.texas.gov/link/f5dfea9c-bb39-4a5e-a44e-fb522e088cba"
download_dir = os.path.join(
    os.path.expanduser("~"),
    "Documents",
    "programming",
    "txrrc",
    "backend",
    "data",
    "download",
)
os.makedirs(download_dir, exist_ok=True)


# ----------------------------
# Helper: Clear download folder
# ----------------------------
def clear_folder(folder_path):
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")


clear_folder(download_dir)

# ----------------------------
# Selenium: Configure Chrome for download
# ----------------------------
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    },
)

driver = webdriver.Chrome(options=chrome_options)

# ----------------------------
# Open page and start download
# ----------------------------
driver.get(base_url)
time.sleep(3)
print("Opening page....")

select_all_xpath = "/html/body/div[1]/div[2]/div[4]/div/form/div/div/table/thead/tr/th[1]/div/div[2]/span"
driver.find_element(By.XPATH, select_all_xpath).click()
time.sleep(1)
print("Clicking checkbox...")

download_all_xpath = "/html/body/div[3]/div/div/form/button/span"
driver.find_element(By.XPATH, download_all_xpath).click()
print("Clicking download all button...")

print("Files are downloading...")


# ----------------------------
# Wait for download to finish
# ----------------------------
def downloads_in_progress(folder):
    return any(fname.endswith(".crdownload") for fname in os.listdir(folder))


time.sleep(10)

while downloads_in_progress(download_dir):
    time.sleep(60)

driver.quit()

print(f"All files downloaded to: {download_dir}")
