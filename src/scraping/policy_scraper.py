import os
import json
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from config import BASE_URL, POLICY_URL, OUTPUT_DIR
from src.utils.helpers import clean, safe_get
import hashlib

def make_row_key(country, year, url):
    return hashlib.sha256(f"{country}|{year}|{url}".encode("utf-8")).hexdigest()

def parse_listing_page(html):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for row in soup.select("table tbody tr"):
        a = row.select_one("td:first-child a")
        if not a:
            continue
        href = a.get("href")
        if href and "/policies/" in href:
            links.append(href if href.startswith("http") else BASE_URL + href)
    return links

def parse_policy_page(html):
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    description = ""
    products = ""

    h1 = soup.select_one("main h1")
    if h1:
        title = clean(h1.get_text(" ", strip=True))

    paragraphs = soup.select("main p")
    if paragraphs:
        description = clean(" ".join(p.get_text(" ", strip=True) for p in paragraphs))

    products_h3 = None
    for h3 in soup.select("h3"):
        if "products" in h3.get_text(" ", strip=True).lower():
            products_h3 = h3
            break

    if products_h3:
        collected = []
        node = products_h3.next_sibling
        while node:
            if getattr(node, "name", None) == "h3":
                break
            if hasattr(node, "get_text"):
                txt = clean(node.get_text(" ", strip=True))
                if txt:
                    collected.append(txt)
            elif isinstance(node, str):
                txt = clean(node)
                if txt:
                    collected.append(txt)
            node = node.next_sibling
        products = clean(" ".join(collected))

    return {
        "title": title,
        "description": description,
        "products": products,
    }

def scrape_country_year(country, year):
    url = f"{POLICY_URL}?country={country}&year={year}"
    print(f"[start] {country} {year}")

    html = safe_get(url)
    if not html:
        print(f"  [fail] could not load listing for {country} {year}")
        return []

    policy_links = parse_listing_page(html)
    if not policy_links:
        print(f"  [empty] {country} {year}")
        return []

    print(f"  [found] {country} {year}: {len(policy_links)} policies")

    data = []
    for link in policy_links:
        page_html = safe_get(link)
        if not page_html:
            print(f"    [fail] {link}")
            continue

        policy_data = parse_policy_page(page_html)
        data.append({
            "row_key": make_row_key(country, year, link),
            "year": year,
            "country": country,
            "url": link,
            **policy_data,
            "scraped_at": datetime.now(timezone.utc).isoformat()
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = f"{OUTPUT_DIR}/{country}_{year}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"  [saved] {out_path}")
    return data
