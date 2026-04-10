import cloudscraper
import json
import pandas as pd
import time
import random
import os
import re
from bs4 import BeautifulSoup

# config
BODY_TYPES    = ["compact", "suv", "sedan", "station-wagon", "coupe", "convertible", "van"]
YEARS         = range(2015, 2026)
BASE_URL      = "https://www.autoscout24.com"
TARGET_ROWS   = 15000
REQUEST_DELAY = (1.0, 2.5)

# state
all_data = []
seen_ids = set()


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


def parse_next_data(html):
    # extract __NEXT_DATA__ json block from page html
    soup = BeautifulSoup(html, 'html.parser')
    tag  = soup.find('script', id='__NEXT_DATA__')
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError:
        return None


def parse_price(price_obj):
    # handles dict with numeric keys or formatted string e.g. "25.990"
    if not price_obj:
        return None
    if isinstance(price_obj, (int, float)):
        return int(price_obj)
    if isinstance(price_obj, dict):
        for key in ('amount', 'value', 'priceRaw', 'numericPrice'):
            if key in price_obj and price_obj[key] is not None:
                try:
                    return int(price_obj[key])
                except (ValueError, TypeError):
                    pass
        raw = re.sub(r'[^\d]', '', price_obj.get('priceFormatted', '') or '')
        return int(raw) if raw else None
    raw = re.sub(r'[^\d]', '', str(price_obj))
    return int(raw) if raw else None


def parse_hp(vehicle_details):
    # matches "110 kW (150 hp)" or "150 PS"; falls back to kW -> hp conversion
    if not vehicle_details:
        return None
    for item in vehicle_details:
        if not isinstance(item, dict) or item.get('ariaLabel') != 'Power':
            continue
        data = item.get('data', '')
        m = re.search(r'\((\d+)\s*(?:hp|ps|pk)\)', data, re.IGNORECASE)
        if m:
            return int(m.group(1))
        m = re.search(r'(\d+)\s*(?:hp|ps|pk)', data, re.IGNORECASE)
        if m:
            return int(m.group(1))
        m = re.search(r'(\d+)\s*kw', data, re.IGNORECASE)
        if m:
            return int(round(int(m.group(1)) * 1.341))
    return None


def parse_mileage(vehicle_details, vehicle_obj):
    # tries vehicleDetails first, then vehicle.mileageInKm as fallback
    if vehicle_details:
        for item in vehicle_details:
            if isinstance(item, dict) and item.get('ariaLabel') == 'Mileage':
                raw = re.sub(r'[^\d]', '', item.get('data', ''))
                if raw:
                    return int(raw)
    if isinstance(vehicle_obj, dict):
        raw = re.sub(r'[^\d]', '', str(vehicle_obj.get('mileageInKm', '')))
        if raw:
            return int(raw)
    return None


def parse_year(tracking, vehicle_details):
    # tracking.firstRegistration = "11-2020"; falls back to vehicleDetails
    if isinstance(tracking, dict):
        m = re.search(r'(\d{4})', str(tracking.get('firstRegistration', '')))
        if m:
            return int(m.group(1))
    if vehicle_details:
        for item in vehicle_details:
            if isinstance(item, dict) and item.get('ariaLabel') == 'First registration':
                m = re.search(r'(\d{4})', item.get('data', ''))
                if m:
                    return int(m.group(1))
    return None


def extract_listing_data(item, body_type_query):
    # returns None for duplicates or listings missing required fields
    try:
        listing_id = item.get('id')
        if not listing_id or listing_id in seen_ids:
            return None

        v        = item.get('vehicle', {}) or {}
        details  = item.get('vehicleDetails', []) or []
        tracking = item.get('tracking', {}) or {}

        price   = parse_price(item.get('price'))
        hp      = parse_hp(details)
        mileage = parse_mileage(details, v)
        year    = parse_year(tracking, details)

        if not price or price < 500:
            return None

        loc      = item.get('location', {}) or {}
        item_url = item.get('url', '')

        return {
            'id':           listing_id,
            'brand':        v.get('make'),
            'model':        v.get('model') or v.get('modelGroup'),
            'year':         year,
            'price':        price,
            'hp':           hp,
            'fuel_type':    v.get('fuel') or v.get('fuelType'),
            'mileage':      mileage,
            'body_type':    v.get('bodyType') or body_type_query,
            'transmission': v.get('transmission'),
            'country':      loc.get('countryCode') or 'EU',
            'url':          BASE_URL + item_url if item_url.startswith('/') else item_url,
        }
    except Exception as e:
        print(f"[parse-error] {e}")
        return None


def scrape_page(body_type, year, page):
    # returns None on hard fail, [] when page is empty
    url = (
        f"{BASE_URL}/lst/{body_type}"
        f"?fregfrom={year}&fregto={year}&page={page}&sort=standard&desc=0"
    )
    res = fetch_with_retry(url)
    if not res:
        return None
    raw = parse_next_data(res.text)
    if not raw:
        return []
    listings = raw.get('props', {}).get('pageProps', {}).get('listings', [])
    if not isinstance(listings, list):
        return []
    return [r for r in (extract_listing_data(i, body_type) for i in listings) if r]


def save_partial(body_type, year, rows):
    # checkpoint per body/year combo
    os.makedirs('data/partial', exist_ok=True)
    if rows:
        path = f"data/partial/{body_type}_{year}.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"[saved] {path} ({len(rows)} rows)")


def save_master():
    if not all_data:
        return None
    df = pd.DataFrame(all_data).drop_duplicates(subset='id')
    df.to_csv("../data/autoscout_eu_raw.csv", index=False)
    print(f"[master] {len(df)} unique rows -> autoscout_eu_raw.csv")
    return df


def main():
    global all_data, seen_ids
    os.makedirs('data', exist_ok=True)

    for body in BODY_TYPES:
        if len(all_data) >= TARGET_ROWS:
            break
        for year in YEARS:
            if len(all_data) >= TARGET_ROWS:
                break

            combo_rows = []
            print(f"\n[start] {body} / {year}  |  total: {len(all_data)}")

            for page in range(1, 21):
                if len(all_data) >= TARGET_ROWS:
                    break

                time.sleep(random.uniform(*REQUEST_DELAY))
                results = scrape_page(body, year, page)

                if results is None:
                    print(f"[fail] page {page} — skipping {body}/{year}")
                    break
                if not results:
                    print(f"[done] page {page} empty — {body}/{year} complete")
                    break

                new_rows = [r for r in results if r['id'] not in seen_ids]
                for r in new_rows:
                    seen_ids.add(r['id'])
                    combo_rows.append(r)
                    all_data.append(r)

                print(f"  page {page:02d}: +{len(new_rows):3d} | combo: {len(combo_rows):4d} | total: {len(all_data):6d}")

            save_partial(body, year, combo_rows)
            save_master()

    df = save_master()
    if df is not None:
        print(f"\n[complete] {len(df)} unique rows")
        print(df[['brand', 'model', 'year', 'price', 'hp', 'fuel_type', 'mileage']].head(10).to_string())
    else:
        print("[warning] no data collected")


if __name__ == "__main__":
    main()