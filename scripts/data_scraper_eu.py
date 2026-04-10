import cloudscraper
import json
import pandas as pd
import time
import random
import os
import re
from bs4 import BeautifulSoup

# --- CONFIG ---
BODY_TYPES = ["compact", "suv", "sedan", "station-wagon", "coupe", "convertible", "van"]
YEARS = range(2015, 2026)
BASE_URL = "https://www.autoscout24.com"
TARGET_ROWS = 15000
REQUEST_DELAY = (1.0, 2.5)

# --- STATE ---
all_data = []
seen_ids = set()


def get_scraper():
    return cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
    )


def fetch_with_retry(url, max_retries=4):
    scraper = get_scraper()
    for attempt in range(max_retries):
        try:
            res = scraper.get(url, timeout=20)
            if res.status_code == 200:
                return res
            if res.status_code == 429:
                wait = (2 ** attempt) * 10
                print(f"⏳ Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if res.status_code >= 500:
                time.sleep(3 ** attempt)
                continue
            return None
        except Exception as e:
            print(f"❌ Attempt {attempt+1}/{max_retries}: {str(e)[:80]}")
            time.sleep(2 ** attempt)
    return None


def parse_next_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    tag = soup.find('script', id='__NEXT_DATA__')
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError:
        return None


def parse_price(price_obj):
    """Extract integer price from price object.
    e.g. {"priceFormatted": "€ 25,990"} → 25990
    """
    if not price_obj:
        return None
    if isinstance(price_obj, (int, float)):
        return int(price_obj)
    if isinstance(price_obj, dict):
        # Try raw numeric fields first
        for key in ('amount', 'value', 'priceRaw', 'numericPrice'):
            if key in price_obj and price_obj[key] is not None:
                try:
                    return int(price_obj[key])
                except (ValueError, TypeError):
                    pass
        # Fall back to formatted string
        raw = price_obj.get('priceFormatted', '') or ''
        raw = re.sub(r'[^\d]', '', raw)
        return int(raw) if raw else None
    # plain string
    raw = re.sub(r'[^\d]', '', str(price_obj))
    return int(raw) if raw else None


def parse_hp(vehicle_details):
    """Extract HP from vehicleDetails array.
    e.g. {"data": "110 kW (150 hp)", "ariaLabel": "Power"} → 150
    """
    if not vehicle_details:
        return None
    for item in vehicle_details:
        if isinstance(item, dict) and item.get('ariaLabel') == 'Power':
            data = item.get('data', '')
            # Match "(150 hp)" or "150 hp" or "150 PS"
            m = re.search(r'\((\d+)\s*(?:hp|ps|pk)\)', data, re.IGNORECASE)
            if m:
                return int(m.group(1))
            m = re.search(r'(\d+)\s*(?:hp|ps|pk)', data, re.IGNORECASE)
            if m:
                return int(m.group(1))
            # kW only — convert to hp
            m = re.search(r'(\d+)\s*kw', data, re.IGNORECASE)
            if m:
                return int(round(int(m.group(1)) * 1.341))
    return None


def parse_mileage(vehicle_details, vehicle_obj):
    """Extract mileage as integer km.
    Tries vehicleDetails first, then vehicle.mileageInKm.
    """
    # From vehicleDetails
    if vehicle_details:
        for item in vehicle_details:
            if isinstance(item, dict) and item.get('ariaLabel') == 'Mileage':
                raw = re.sub(r'[^\d]', '', item.get('data', ''))
                if raw:
                    return int(raw)
    # From vehicle object
    if isinstance(vehicle_obj, dict):
        raw = vehicle_obj.get('mileageInKm', '')
        raw = re.sub(r'[^\d]', '', str(raw))
        if raw:
            return int(raw)
    return None


def parse_year(tracking, vehicle_details):
    """Extract registration year as integer.
    tracking.firstRegistration = "11-2020" or vehicleDetails ariaLabel=First registration "11/2020"
    """
    # From tracking
    if isinstance(tracking, dict):
        raw = tracking.get('firstRegistration', '')
        m = re.search(r'(\d{4})', str(raw))
        if m:
            return int(m.group(1))
    # From vehicleDetails
    if vehicle_details:
        for item in vehicle_details:
            if isinstance(item, dict) and item.get('ariaLabel') == 'First registration':
                m = re.search(r'(\d{4})', item.get('data', ''))
                if m:
                    return int(m.group(1))
    return None


def extract_listing_data(item, body_type_query):
    """Parse one listing dict into a flat record."""
    try:
        listing_id = item.get('id')
        if not listing_id or listing_id in seen_ids:
            return None

        v       = item.get('vehicle', {}) or {}
        details = item.get('vehicleDetails', []) or []
        tracking = item.get('tracking', {}) or {}

        price   = parse_price(item.get('price'))
        hp      = parse_hp(details)
        mileage = parse_mileage(details, v)
        year    = parse_year(tracking, details)

        if not price or price < 500:
            return None

        make  = v.get('make')
        model = v.get('model') or v.get('modelGroup')
        fuel  = v.get('fuel') or v.get('fuelType')
        trans = v.get('transmission')
        body  = v.get('bodyType') or body_type_query

        loc     = item.get('location', {}) or {}
        country = loc.get('countryCode') or 'EU'

        item_url = item.get('url', '')
        full_url = BASE_URL + item_url if item_url.startswith('/') else item_url

        return {
            'id':           listing_id,
            'brand':        make,
            'model':        model,
            'year':         year,
            'price':        price,
            'hp':           hp,
            'fuel_type':    fuel,
            'mileage':      mileage,
            'body_type':    body,
            'transmission': trans,
            'country':      country,
            'url':          full_url,
        }

    except Exception as e:
        print(f"  ⚠️ Item parse error: {e}")
        return None


def scrape_page(body_type, year, page):
    """Fetch one search result page, return list of records ([] = empty, None = hard fail)."""
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

    results = []
    for item in listings:
        record = extract_listing_data(item, body_type)
        if record:
            results.append(record)
    return results


def save_partial(body_type, year, rows):
    os.makedirs('data/partial', exist_ok=True)
    if rows:
        path = f"data/partial/{body_type}_{year}.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"  💾 Partial saved: {path} ({len(rows)} rows)")


def save_master():
    if not all_data:
        return None
    df = pd.DataFrame(all_data).drop_duplicates(subset='id')
    df.to_csv("../data/autoscout_eu_raw.csv", index=False)
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
            print(f"\n🔹 {body} | {year} | total: {len(all_data)}")

            for page in range(1, 21):
                if len(all_data) >= TARGET_ROWS:
                    break

                time.sleep(random.uniform(*REQUEST_DELAY))
                page_results = scrape_page(body, year, page)

                if page_results is None:
                    print(f"  ❌ Hard fail on page {page}, skipping combo")
                    break

                if not page_results:
                    print(f"  ↩ No results on page {page}, done with {body}/{year}")
                    break

                new_rows = [r for r in page_results if r['id'] not in seen_ids]
                for r in new_rows:
                    seen_ids.add(r['id'])
                    combo_rows.append(r)
                    all_data.append(r)

                print(f"  📄 p{page}: +{len(new_rows)} new | combo: {len(combo_rows)} | total: {len(all_data)}")

            # ✅ Save after every body/year combo
            save_partial(body, year, combo_rows)
            save_master()

    df = save_master()
    if df is not None:
        print(f"\n🎯 Complete! {len(df)} unique rows")
        print(df[['brand', 'model', 'year', 'price', 'hp', 'fuel_type', 'mileage']].head(10).to_string())
    else:
        print("\n❌ No data collected.")


if __name__ == "__main__":
    main()