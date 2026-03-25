import os
import time
from playwright.sync_api import sync_playwright

# CONFIG
BASE_URL = "https://critmin.org/tariff-data/"
REPORTER = "USA"
TARIFF_TYPE = "mfn"
YEARS = range(1996, 2024)
OUTPUT_DIR = "data/mfn/USA"

# Create directory if not exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_data():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for year in YEARS:
            file_path = os.path.join(OUTPUT_DIR, f"{REPORTER}_{year}.csv")

            # Skip if already downloaded
            if os.path.exists(file_path):
                print(f"Skipping {year}, already exists.")
                continue

            url = f"{BASE_URL}?reporter={REPORTER}&type={TARIFF_TYPE}&year={year}"
            print(f"\nProcessing {year}...")
            print(f"URL: {url}")

            try:
                page.goto(url, timeout=60000)

                # Wait for page to load
                page.wait_for_timeout(3000)

                rows = page.locator("table tbody tr")
                if rows.count() == 0:
                    print(f"No rows for {year}, skipping.")
                    continue

                # Trigger download
                with page.expect_download(timeout=20000) as download_info:
                    page.click("button:has-text('Download')")

                download = download_info.value

                # Save file
                download.save_as(file_path)
                print(f"Saved: {file_path}")
                time.sleep(2)

            except Exception as e:
                print(f"Failed for {year}: {e}")

        browser.close()

if __name__ == "__main__":
    download_data()
