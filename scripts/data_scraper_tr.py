import cloudscraper
import json
import pandas as pd
import time
import random
import os
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# config
BASE_URL      = "https://www.arabam.com"
TARGET_ROWS   = 15000
REQUEST_DELAY = (0.8, 2.0)
MAX_WORKERS   = 4

BRANDS = [
    "volkswagen", "renault", "ford", "fiat", "toyota", "opel", "hyundai",
    "bmw", "mercedes-benz", "audi", "peugeot", "honda", "nissan", "kia",
    "seat", "skoda", "citroen", "dacia", "volvo", "mazda", "mitsubishi",
    "suzuki", "chevrolet", "jeep", "porsche", "mini", "land-rover", "lexus",
]

YEARS = range(2015, 2026)

# fuel type inference from url slug
FUEL_PATTERNS = [
    (r'\btdi\b|\btdci\b|\bcdi\b|\bdci\b|\bhdi\b|\bjtd\b|\bcrdi\b', 'Diesel'),
    (r'\btsi\b|\btfsi\b|\bgdi\b|\bt-gdi\b|\bturbo\b|\betsi\b',     'Gasoline'),
    (r'\bhybrid\b|\bhev\b|\bphev\b|\bhibrit\b',                    'Hybrid'),
    (r'\belectric\b|\belektrik\b|\be-tron\b',                      'Electric'),
    (r'\blpg\b',                                                    'LPG'),
]

# state
data_lock = Lock()
all_data  = []
seen_ids  = set()


def get_scraper():
    return cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
    )


def fetch_with_retry(url, max_retries=4):
    # exponential backoff on 429 and 5xx
    scraper = get_scraper()
    for attempt in range(max_retries):
        try:
            res = scraper.get(url, timeout=20)
            if res.status_code == 200:
                return res
            if res.status_code == 429:
                wait = (2 ** attempt) * 10
                print(f"[rate-limit] waiting {wait}s")
                time.sleep(wait)
                continue
            if res.status_code >= 500:
                time.sleep(3 ** attempt)
                continue
            return None
        except Exception as e:
            print(f"[error] attempt {attempt + 1}/{max_retries}: {str(e)[:80]}")
            time.sleep(2 ** attempt)
    return None


def guess_fuel_type(url):
    # infer fuel type from url slug; returns None if no pattern matches
    text = (url or '').lower()
    for pattern, fuel in FUEL_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return fuel
    return None


def get_listing_urls(brand, year, skip):
    # returns list of (id, url) pairs; None on hard fail, [] when page is empty
    url = (
        f"{BASE_URL}/ikinci-el/otomobil/{brand}"
        f"?minYear={year}&maxYear={year}&take=20&skip={skip}"
    )
    res = fetch_with_retry(url)
    if not res:
        return None

    soup = BeautifulSoup(res.text, 'html.parser')
    rows = soup.select('tr.listing-list-item')
    if not rows:
        return []

    pairs = []
    for row in rows:
        listing_id = row.get('data-imp-id')
        if not listing_id or listing_id in seen_ids:
            continue
        link = row.select_one('a[href*="/ilan/"]')
        if link and link.get('href', '').startswith('/'):
            pairs.append((listing_id, BASE_URL + link['href']))
    return pairs


def scrape_detail(listing_id, url):
    # fetches detail page and extracts fields from JSON-LD Car object
    res = fetch_with_retry(url)
    if not res:
        return None

    soup     = BeautifulSoup(res.text, 'html.parser')
    car_data = None

    for tag in soup.find_all('script', type='application/ld+json'):
        try:
            obj = json.loads(tag.string or '')
            if obj.get('@type') == 'Car':
                car_data = obj
                break
        except (json.JSONDecodeError, AttributeError):
            continue

    if not car_data:
        return None

    offers = car_data.get('offers', {})
    try:
        price = int(offers.get('price'))
    except (TypeError, ValueError):
        price = None

    if not price or price < 1000:
        return None

    mileage_obj = car_data.get('mileageFromOdometer', {})
    try:
        mileage = int(mileage_obj.get('value')) if isinstance(mileage_obj, dict) else None
    except (TypeError, ValueError):
        mileage = None

    # city extracted from listing name: "... 2015 Model Mugla 121.000 km Beyaz"
    city       = None
    name_field = car_data.get('name', '')
    city_match = re.search(r'Model\s+([A-ZÇĞİÖŞÜa-zçğışöüİ]+)\s+[\d.,]+\s*km', name_field)
    if city_match:
        city = city_match.group(1)

    return {
        'id':           listing_id,
        'brand':        car_data.get('brand') or car_data.get('manufacturer'),
        'model':        car_data.get('model'),
        'year':         car_data.get('productionDate'),
        'price':        price,
        'currency':     offers.get('priceCurrency', 'TRY'),
        'hp':           None,   # not available in arabam.com listings
        'fuel_type':    guess_fuel_type(url),
        'mileage':      mileage,
        'body_type':    None,   # not available in arabam.com listings
        'transmission': car_data.get('vehicleTransmission'),
        'color':        car_data.get('color'),
        'city':         city,
        'country':      'TR',
        'url':          url,
    }


def save_partial(brand, year, rows):
    # checkpoint per brand/year combo
    os.makedirs('data/partial_tr', exist_ok=True)
    if rows:
        path = f"data/partial_tr/{brand}_{year}.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"[saved] {path} ({len(rows)} rows)")


def save_master():
    if not all_data:
        return None
    df = pd.DataFrame(all_data).drop_duplicates(subset='id')
    df.to_csv("../data/arabam_tr_raw.csv", index=False)
    print(f"[master] {len(df)} unique rows -> arabam_tr_raw.csv")
    return df


def main():
    global all_data, seen_ids
    os.makedirs('data', exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for brand in BRANDS:
            if len(all_data) >= TARGET_ROWS:
                break
            for year in YEARS:
                if len(all_data) >= TARGET_ROWS:
                    break

                combo_rows = []
                print(f"\n[start] {brand} / {year}  |  total: {len(all_data)}")

                for page_idx in range(50):
                    if len(all_data) >= TARGET_ROWS:
                        break

                    skip = page_idx * 20
                    time.sleep(random.uniform(*REQUEST_DELAY))

                    url_pairs = get_listing_urls(brand, year, skip)

                    if url_pairs is None:
                        print(f"[fail] skip={skip} — skipping {brand}/{year}")
                        break
                    if not url_pairs:
                        print(f"[done] skip={skip} empty — {brand}/{year} complete")
                        break

                    new_pairs = [(lid, u) for lid, u in url_pairs if lid not in seen_ids]
                    if not new_pairs:
                        continue

                    futures   = {executor.submit(scrape_detail, lid, u): lid for lid, u in new_pairs}
                    page_new  = 0

                    for future in as_completed(futures):
                        time.sleep(random.uniform(0.1, 0.3))
                        result = future.result()
                        if result:
                            lid = result['id']
                            if lid not in seen_ids:
                                seen_ids.add(lid)
                                with data_lock:
                                    all_data.append(result)
                                    combo_rows.append(result)
                                page_new += 1

                    print(f"  skip {skip:4d}: +{page_new:3d} | combo: {len(combo_rows):4d} | total: {len(all_data):6d}")

                save_partial(brand, year, combo_rows)
                save_master()

    df = save_master()
    if df is not None:
        print(f"\n[complete] {len(df)} unique rows")
        print(df[['brand', 'model', 'year', 'price', 'mileage', 'fuel_type', 'transmission', 'color', 'city']].head(10).to_string())
    else:
        print("[warning] no data collected")


if __name__ == "__main__":
    main()