import os
import time
from playwright.sync_api import sync_playwright
from src.utils.gcs_setup import upload_to_gcs

# CONFIG
BASE_URL = "https://critmin.org"
POLICY_URL = "https://critmin.org/policies/"
ECONOMY = "USA"
YEARS = range(1970, 2024)

def safe_goto(context, url, retries=3):
    for attempt in range(retries):
        page = context.new_page()
        try:
            print(f"Attempt {attempt+1}: {url}")
            page.goto(url, timeout=60000, wait_until="networkidle")
            return page
        except Exception as e:
            print(f"Retry {attempt+1} failed: {e}")
            page.close()
            time.sleep(3 * (attempt + 1))
    raise Exception(f"Failed to load {url}")


def clean(text):
    return " ".join(text.split()) if text else ""

def extract_policy_data(page):
    # Title
    try:
        title = page.locator("main h1").inner_text(timeout=5000)
    except:
        title = ""

    # Description (all <p> under main before first <h3>)
    description = ""
    try:
        paragraphs = page.locator("main p")
        description = " ".join([paragraphs.nth(i).inner_text() for i in range(paragraphs.count())])
    except:
        pass

    # Products
    products = ""
    try:
        # Evaluate JS in the page to get all text nodes after Products <h3>
        products = page.locator("h3:has-text('Products')").evaluate("""
        (h3) => {
            let texts = [];
            let node = h3.nextSibling;
            while(node) {
                if(node.tagName === 'H3') break;
                if(node.nodeType === Node.TEXT_NODE) {
                    let t = node.textContent.trim();
                    if(t) texts.push(t);
                } else if(node.nodeType === Node.ELEMENT_NODE) {
                    let t = node.innerText.trim();
                    if(t) texts.push(t);
                }
                node = node.nextSibling;
            }
            return texts.join(" ");
        }
        """)
    except:
        pass

    return {
        "title": title.strip(),
        "description": description.strip(),
        "products": products.strip()
    }

def scrape_policies():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )

        for year in YEARS:
            file_path = os.path.join(OUTPUT_DIR, f"{ECONOMY}_{year}.json")

            url = f"{POLICY_URL}?country={ECONOMY}&year={year}"
            print(f"\nProcessing {year}...")
            print(f"URL: {url}")

            try:
                page = safe_goto(context, url)

                # Wait for table or confirm no data
                try:
                    page.wait_for_selector("table tbody tr", timeout=5000)
                except:
                    print(f"No rows for {year}, skipping.")
                    page.close()
                    continue

                rows = page.locator("table tbody tr")
                if rows.count() == 0:
                    print(f"No rows for {year}, skipping.")
                    page.close()
                    continue

                # Extract policy links
                links = page.locator("tbody tr td:first-child a")
                policy_links = []

                for i in range(links.count()):
                    href = links.nth(i).get_attribute("href")

                    if href and "/policies/" in href:
                        full_url = href if href.startswith("http") else BASE_URL + href
                        policy_links.append(full_url)

                print(f"Found {len(policy_links)} policies")
                print("Rows:", rows.count())
                print("Links:", links.count())

                page.close()

                data = []

                # Visit each policy page
                for link in policy_links:
                    print(f"Visiting {link}")
                    policy_page = safe_goto(context, link)

                    try:
                        policy_page.wait_for_selector("main h1", timeout=10000)
                    except:
                        print(f"Content not loaded for {link}")

                    # Use the extraction function
                    policy_data = extract_policy_data(policy_page)

                    data.append({
                        "year": year,
                        "country": ECONOMY,
                        "url": link,
                        "title": clean(policy_data["title"]),
                        "description": clean(policy_data["description"]),
                        "products": clean(policy_data["products"])
                    })

                    policy_page.close()
                    time.sleep(1)

                # Save to GCS
                upload_to_gcs(
                    bucket_name="usa-mineral-policies-data",
                    destination_blob=f"policies/USA_{year}.json",
                    data=data
                )

                print(f"Uploaded to GCS: policies/USA_{year}.json")

                time.sleep(3)

            except Exception as e:
                print(f"Failed for {year}: {e}")

        browser.close()


if __name__ == "__main__":
    scrape_policies()
