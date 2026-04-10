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

# --- CONFIG ---
BASE_URL = "https://www.arabam.com"
TARGET_ROWS = 15000
REQUEST_DELAY = (0.8, 2.0)
MAX_WORKERS = 4

BRANDS = [
    "volkswagen", "renault", "ford", "fiat", "toyota", "opel", "hyundai",
    "bmw", "mercedes-benz", "audi", "peugeot", "honda", "nissan", "kia",
    "seat", "skoda", "citroen", "dacia", "volvo", "mazda", "mitsubishi",
    "suzuki", "chevrolet", "jeep", "porsche", "mini", "land-rover", "lexus",
]

YEARS = range(2015, 2026)

# Yakıt tipi tespiti için motor kodu eşlemeleri
FUEL_PATTERNS = [
    (r'\btdi\b|\btdci\b|\bcdi\b|\bdci\b|\bhdi\b|\bjtd\b|\bcrdi\b|\bd\b', 'Diesel'),
    (r'\btsi\b|\btfsi\b|\bgdi\b|\bt-gdi\b|\bturbo\b|\bt\b(?=\s)', 'Gasoline'),
    (r'\bhybrid\b|\bhev\b|\bphev\b', 'Hybrid'),
    (r'\belectric\b|\bev\b|\be-tron\b|\bi3\b|\bi-pace\b', 'Electric'),
    (r'\blpg\b', 'LPG'),
]

# --- STATE ---
data_lock = Lock()
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


def guess_fuel_type(text):
    """URL veya model adından yakıt tipini tahmin et."""
    text = (text or '').lower()
    for pattern, fuel in FUEL_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return fuel
    return None


def guess_hp_from_url(url):
    """
    URL'den motor hacmi ve tipi ile HP tahmini yap.
    Örn: 'golf-1-6-tdi' → yaklaşık 105hp
    Bu kaba bir tahmin — sadece HP yoksa kullanılır.
    """
    return None  # Tahmin yerine None bırakmak daha dürüst


def get_listing_urls(brand, year, skip):
    """Listing sayfasından ilan URL'lerini çek."""
    url = (
        f"{BASE_URL}/ikinci-el/otomobil/{brand}"
        f"?minYear={year}&maxYear={year}&take=20&skip={skip}"
    )
    res = fetch_with_retry(url)
    if not res:
        return None  # hard fail

    soup = BeautifulSoup(res.text, 'html.parser')
    rows = soup.select('tr.listing-list-item')

    if not rows:
        return []

    urls = []
    for row in rows:
        listing_id = row.get('data-imp-id')
        if not listing_id or listing_id in seen_ids:
            continue
        link = row.select_one('a[href*="/ilan/"]')
        if link and link.get('href', '').startswith('/'):
            urls.append((listing_id, BASE_URL + link['href']))

    return urls


def scrape_detail(listing_id, url):
    """Detail sayfasından JSON-LD ile tam veri çek."""
    res = fetch_with_retry(url)
    if not res:
        return None

    soup = BeautifulSoup(res.text, 'html.parser')

    # JSON-LD Car objesini bul
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

    # --- Alanları çıkar ---
    brand = car_data.get('brand') or car_data.get('manufacturer')
    model = car_data.get('model')
    year  = car_data.get('productionDate')
    color = car_data.get('color')
    trans = car_data.get('vehicleTransmission')

    # Fiyat
    offers = car_data.get('offers', {})
    price  = offers.get('price')
    currency = offers.get('priceCurrency', 'TRY')
    try:
        price = int(price)
    except (TypeError, ValueError):
        price = None

    if not price or price < 1000:
        return None

    # Kilometre
    mileage_obj = car_data.get('mileageFromOdometer', {})
    mileage = mileage_obj.get('value') if isinstance(mileage_obj, dict) else None
    try:
        mileage = int(mileage)
    except (TypeError, ValueError):
        mileage = None

    # Yakıt tipi — URL'den tahmin et
    fuel_type = guess_fuel_type(url)

    # HP — JSON-LD'de yok, None kalacak
    hp = None

    # Şehir — BreadcrumbList veya sayfadan
    city = None
    name_field = car_data.get('name', '')
    # "... 2015 Model Muğla 121.000 km Beyaz" formatından şehir çıkar
    city_match = re.search(r'Model\s+([A-ZÇĞİÖŞÜa-zçğışöüİ]+)\s+[\d.,]+\s*km', name_field)
    if city_match:
        city = city_match.group(1)

    # Body type — JSON-LD'de yok
    body_type = None

    return {
        'id':           listing_id,
        'brand':        brand,
        'model':        model,
        'year':         year,
        'price':        price,
        'currency':     currency,
        'hp':           hp,
        'fuel_type':    fuel_type,
        'mileage':      mileage,
        'body_type':    body_type,
        'transmission': trans,
        'color':        color,
        'city':         city,
        'country':      'TR',
        'url':          url,
    }


def save_partial(brand, year, rows):
    os.makedirs('data/partial_tr', exist_ok=True)
    if rows:
        path = f"data/partial_tr/{brand}_{year}.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"  💾 Partial: {path} ({len(rows)} rows)")


def save_master():
    if not all_data:
        return None
    df = pd.DataFrame(all_data).drop_duplicates(subset='id')
    df.to_csv("data/arabam_tr_raw.csv", index=False)
    print(f"💾 Master: {len(df)} rows → data/arabam_tr_raw.csv")
    return df


def main():
    global all_data, seen_ids
    print(f"🚀 arabam.com scrape başlıyor (hedef: {TARGET_ROWS} satır)...")
    os.makedirs('data', exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for brand in BRANDS:
            if len(all_data) >= TARGET_ROWS:
                break

            for year in YEARS:
                if len(all_data) >= TARGET_ROWS:
                    break

                combo_rows = []
                print(f"\n🔹 {brand} | {year} | toplam: {len(all_data)}")

                for page_idx in range(50):
                    if len(all_data) >= TARGET_ROWS:
                        break

                    skip = page_idx * 20
                    time.sleep(random.uniform(*REQUEST_DELAY))

                    # 1. Listing sayfasından URL'leri topla
                    url_pairs = get_listing_urls(brand, year, skip)

                    if url_pairs is None:
                        print(f"  ❌ Hard fail (skip={skip})")
                        break
                    if not url_pairs:
                        print(f"  ↩ Boş sayfa (skip={skip}), {brand}/{year} bitti")
                        break

                    # Daha önce görülmüş ID'leri filtrele
                    new_pairs = [(lid, u) for lid, u in url_pairs if lid not in seen_ids]
                    if not new_pairs:
                        continue

                    # 2. Detail sayfalarını paralel çek
                    futures = {
                        executor.submit(scrape_detail, lid, u): lid
                        for lid, u in new_pairs
                    }

                    page_new = 0
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

                    print(f"  📄 skip={skip}: +{page_new} | combo: {len(combo_rows)} | toplam: {len(all_data)}")

                # ✅ Her marka/yıl sonunda kaydet
                save_partial(brand, year, combo_rows)
                save_master()

    df = save_master()
    if df is not None:
        print(f"\n🎯 Tamamlandı! {len(df)} unique satır")
        print(df[['brand', 'model', 'year', 'price', 'mileage', 'fuel_type', 'transmission', 'color', 'city']].head(10).to_string())
    else:
        print("\n❌ Veri toplanamadı.")


if __name__ == "__main__":
    main()