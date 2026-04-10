import asyncio
import json
import os
import time
from playwright.async_api import async_playwright
from src.utils.gcs_setup import upload_to_gcs

# CONFIG
BASE_URL = "https://critmin.org"
POLICY_URL = "https://critmin.org/policies/"
YEARS = range(1970, 2024)
MAX_WORKERS = 6        # Concurrent browser contexts
MAX_RETRIES = 3
OUTPUT_DIR = "output"  # Local output until GCS is ready
countries = [
    'AFG', 'ALB', 'DZA', 'AND', 'AGO', 'ATG', 'ARG', 
    'ARM', 'ABW', 'AUS', 'AUT', 'AZE', 'BHS', 'BHR', 
    'BGD', 'BRB', 'BLR', 'BEL', 'BLZ', 'BEN', 'BMU', 
    'BTN', 'BOL', 'BIH', 'BWA', 'BRA', 'BRN', 'BGR', 
    'BFA', 'BDI', 'CPV', 'KHM', 'CMR', 'CAN', 'CYM', 
    'CAF', 'TCD', 'CHL', 'COL', 'COM', 'COG', 'COK', 
    'CRI', 'CIV', 'HRV', 'CUB', 'CUW', 'CYP', 'CZE', 
    'COD', 'DNK', 'DJI', 'DMA', 'DOM', 'ECU', 'EGY', 
    'SLV', 'GNQ', 'EST', 'SWZ', 'ETH', 'FSM', 'FJI', 
    'FIN', 'FRA', 'PYF', 'GAB', 'GEO', 'DEU', 'GHA', 
    'GRC', 'GRL', 'GRD', 'GTM', 'GIN', 'GNB', 'GUY', 
    'HTI', 'VAT', 'HND', 'HKG', 'HUN', 'ISL', 'IND', 
    'IDN', 'IRN', 'IRQ', 'IRL', 'ISR', 'ITA', 'JAM', 
    'JPN', 'JOR', 'KAZ', 'KEN', 'KIR', 'KWT', 'KGZ', 
    'LAO', 'LVA', 'LBN', 'LSO', 'LBR', 'LBY', 'LIE', 
    'LTU', 'LUX', 'MAC', 'MDG', 'MWI', 'MYS', 'MDV', 
    'MLI', 'MLT', 'MRT', 'MUS', 'MEX', 'MDA', 'MNG', 
    'MNE', 'MSR', 'MAR', 'MOZ', 'MMR', 'NAM', 'NPL', 
    'NLD', 'NCL', 'NZL', 'NIC', 'NER', 'NGA', 'MKD', 
    'NOR', 'OMN', 'PAK', 'PLW', 'PSE', 'PAN', 'PNG', 
    'PRY', 'CHN', 'PER', 'PHL', 'POL', 'PRT', 'QAT', 
    'KOR', 'ROM', 'RUS', 'RWA', 'KNA', 'LCA', 'VCT', 
    'WSM', 'STP', 'SAU', 'SEN', 'SRB', 'SYC', 'SLE', 
    'SGP', 'SVK', 'SVN', 'SLB', 'SOM', 'ZAF', 'SSD', 
    'ESP', 'LKA', 'SDN', 'SUR', 'SWE', 'CHE', 'SYR', 
    'CHT', 'TJK', 'TZA', 'THA', 'GMB', 'TLS', 'TGO', 
    'TON', 'TTO', 'TUN', 'TUR', 'TKM', 'TCA', 'UGA', 
    'UKR', 'ARE', 'GBR', 'USA', 'XKX', 'URY', 'UZB', 
    'VUT', 'VEN', 'VNM', 'YEM', 'ZMB', 'ZWE'
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Clean text by collapsing whitespace
def clean(text):
    return " ".join(text.split()) if text else ""
 

# Check if country/year combo is already saved locally
def already_done(country, year):
    """Skip country/year combos already saved locally."""
    return os.path.exists(f"{OUTPUT_DIR}/{country}_{year}.json")
 

# Safe navigation with retries and exponential backoff
async def safe_goto(page, url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            # 'domcontentloaded' is much faster than 'networkidle'
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            return True
        except Exception as e:
            print(f"  [retry {attempt+1}] {url} — {e}")
            await asyncio.sleep(2 ** attempt)  # exponential backoff
    return False

# Extract title, description, and products from a policy page
async def extract_policy_data(page):
    title, description, products = "", "", ""
 
    try:
        title = await page.locator("main h1").inner_text(timeout=5000)
    except:
        pass
 
    try:
        paragraphs = page.locator("main p")
        count = await paragraphs.count()
        texts = await asyncio.gather(*[paragraphs.nth(i).inner_text() for i in range(count)])
        description = " ".join(texts)
    except:
        pass
 
    try:
        products = await page.locator("h3:has-text('Products')").evaluate("""
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
        "title": clean(title),
        "description": clean(description),
        "products": clean(products),
    }
 
 
async def scrape_country_year(context, country, year, semaphore):
    async with semaphore:
        if already_done(country, year):
            print(f"  [skip] {country} {year} already saved")
            return
 
        url = f"{POLICY_URL}?country={country}&year={year}"
        print(f"[start] {country} {year}")
 
        listing_page = await context.new_page()
        try:
            ok = await safe_goto(listing_page, url)
            if not ok:
                print(f"  [fail] could not load listing for {country} {year}")
                return

            try:
                await listing_page.wait_for_selector("table tbody tr", timeout=5000)
            except:
                print(f"  [empty] {country} {year}")
                return
 
            row_count = await listing_page.locator("table tbody tr").count()
            if row_count == 0:
                print(f"  [empty] {country} {year}")
                return
 
            # Grab all policy links at once
            links_els = listing_page.locator("tbody tr td:first-child a")
            link_count = await links_els.count()
            hrefs = await asyncio.gather(*[links_els.nth(i).get_attribute("href") for i in range(link_count)])
            policy_links = [
                (href if href.startswith("http") else BASE_URL + href)
                for href in hrefs if href and "/policies/" in href
            ]
            print(f"  [found] {country} {year}: {len(policy_links)} policies")
        finally:
            await listing_page.close()
 
        # Scrape each policy page — reuse pages within the same context
        data = []
        for link in policy_links:
            policy_page = await context.new_page()
            try:
                ok = await safe_goto(policy_page, link)
                if not ok:
                    print(f"    [fail] {link}")
                    continue
 
                try:
                    await policy_page.wait_for_selector("main h1", timeout=8000)
                except:
                    pass  # Try extraction anyway
 
                policy_data = await extract_policy_data(policy_page)
                data.append({
                    "year": year,
                    "country": country,
                    "url": link,
                    **policy_data,
                })
            except Exception as e:
                print(f"    [error] {link}: {e}")
            finally:
                await policy_page.close()
 
        if data:
            out_path = f"{OUTPUT_DIR}/{country}_{year}.json"
            with open(out_path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"  [saved] {out_path} ({len(data)} records)")
 
 
async def main():
    # Build the full work queue, skip already-done pairs
    tasks_all = [
        (country, year)
        for country in countries
        for year in YEARS
    ]
    print(f"Total tasks: {len(tasks_all)}")
 
    semaphore = asyncio.Semaphore(MAX_WORKERS)
 
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )
 
        # Fire off all tasks concurrently
        await asyncio.gather(*[
            scrape_country_year(context, country, year, semaphore)
            for country, year in tasks_all
        ])
 
        await browser.close()
 
    print("Done!")
 
 
if __name__ == "__main__":
    asyncio.run(main())