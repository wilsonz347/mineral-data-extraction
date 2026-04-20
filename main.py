from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from config import countries, YEARS, MAX_WORKERS
from scraping.policy import scrape_country_year
from src.utils.bigquery_setup import upload_to_bigquery
from src.utils.helpers import load_checkpoint, save_checkpoint

RAW_TABLE_ID = "ita-development-project.mineral_data.critmin_policy_raw"
BATCH_SIZE = 20


def main():
    done = load_checkpoint()
    tasks = [
        (country, year)
        for country in countries
        for year in YEARS
        if (country, year) not in done
    ]
    print(f"Total tasks remaining: {len(tasks)}")

    if not tasks:
        print("Nothing left to do.")
        return

    batch_rows = []
    batch_done = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(scrape_country_year, country, year): (country, year)
            for country, year in tasks
        }

        for future in as_completed(future_map):
            country, year = future_map[future]
            try:
                rows = future.result()
                if rows:
                    batch_rows.extend(rows)
                batch_done.add((country, year))

                if len(batch_rows) >= BATCH_SIZE:
                    df = pd.DataFrame(batch_rows)
                    print(f"Uploading batch of {len(df)} rows")
                    upload_to_bigquery(df, RAW_TABLE_ID, write_disposition="WRITE_APPEND")
                    done.update(batch_done)
                    save_checkpoint(done)
                    batch_rows = []
                    batch_done = set()

            except Exception as e:
                print(f"[error] {country} {year}: {e}")

    if batch_rows:
        df = pd.DataFrame(batch_rows)
        print(f"Uploading final batch of {len(df)} rows")
        upload_to_bigquery(df, RAW_TABLE_ID, write_disposition="WRITE_APPEND")
        done.update(batch_done)
        save_checkpoint(done)

    print("Done.")


if __name__ == "__main__":
    main()
