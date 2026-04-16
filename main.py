from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from config import countries, YEARS, MAX_WORKERS
from src.scraping.policy_scraper import scrape_country_year
from src.utils.bigquery_setup import upload_to_bigquery

def main():
    tasks = [(country, year) for country in countries for year in YEARS]
    print(f"Total tasks: {len(tasks)}")

    all_rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(scrape_country_year, country, year): (country, year)
            for country, year in tasks
        }

        for future in as_completed(future_map):
            country, year = future_map[future]
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                print(f"[error] {country} {year}: {e}")

    if not all_rows:
        print("No rows collected.")
        return

    df = pd.DataFrame(all_rows)
    print(f"Total rows collected: {len(df)}")

'''BATCH_SIZE = MAX_WORKERS * 2

def main():
    tasks = [(country, year) for country in countries for year in YEARS]
    print(f"Total tasks: {len(tasks)}")

    all_rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[i:i + BATCH_SIZE]
            future_map = {
                executor.submit(scrape_country_year, country, year): (country, year)
                for country, year in batch
            }

            found = False
            for future in as_completed(future_map):
                country, year = future_map[future]
                try:
                    rows = future.result()
                    if rows:
                        all_rows.extend(rows)
                        print(f"Found data for {country} {year}, stopping early.")
                        found = True
                        break
                    else:
                        print(f"[empty] {country} {year}")
                except Exception as e:
                    print(f"[error] {country} {year}: {e}")

            if found:
                break

    if not all_rows:
        print("No rows collected.")
        return

    df = pd.DataFrame(all_rows)
    print(f"Total rows collected: {len(df)}")'''

if __name__ == "__main__":
    main()
