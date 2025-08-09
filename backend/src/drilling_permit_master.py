import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By



base_url = "https://mft.rrc.texas.gov/link/f5dfea9c-bb39-4a5e-a44e-fb522e088cba"
download_dir = "/Users/jordangoodman/programming/txappraisal/backend/data"

os.makedirs(download_dir, exist_ok=True)


chrome_options = Options()
chrome_options.add_argument("--headless=new")  # run headless
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,  # Save to data folder
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=chrome_options)

# ---- Open page ----
driver.get(base_url)
time.sleep(3)
print("Opening page....")

# ---- Click Select All checkbox ----
select_all_xpath = "/html/body/div[1]/div[2]/div[4]/div/form/div/div/table/thead/tr/th[1]/div/div[2]/span"
driver.find_element(By.XPATH, select_all_xpath).click()
time.sleep(1)
print("Clicking checkbox...")

# ---- Click Download All button ----
download_all_xpath = "/html/body/div[3]/div/div/form/button/span"
driver.find_element(By.XPATH, download_all_xpath).click()
print("Clicking download all button...")

# ---- Wait for downloads to finish ----
print("Files are downloading...")

def downloads_in_progress(folder):
    return any(fname.endswith(".crdownload") for fname in os.listdir(folder))

time.sleep(10)

while downloads_in_progress(download_dir):
    print("Still downloading...")
    time.sleep(60)

driver.quit()

print(f" All files downloaded to: {download_dir}")
