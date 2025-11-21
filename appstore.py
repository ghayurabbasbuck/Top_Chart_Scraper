# topchart_category_scrape_itunes.py
"""
Category-wise top-chart scraper (Web-charts + iTunes Lookup API)
- Scrapes apps.apple.com/{country}/iphone/charts/{genre_id} for category-specific top charts.
- Uses iTunes Lookup API to fetch robust app details (release & update dates included).
- Keeps retry/backoff logic and CSV output per category.
- Optional: use app_store_scraper for extra fields (best-effort, can be flaky).
"""

import re
import time
import csv
import requests
import pandas as pd
from typing import List, Optional
from bs4 import BeautifulSoup

try:
    import pycountry
except Exception:
    pycountry = None

# Optional scraper lib (only used if USE_SCRAPER = True)
try:
    from app_store_scraper import AppStore
    HAS_SCRAPER = True
except Exception:
    HAS_SCRAPER = False

# -----------------------
# Config
# -----------------------
COUNTRY = "bt"           # two-letter country code (e.g. 'us', 'gb', 'pk')
LIMIT = 50               # how many apps per category
INPUT_CSV = "app_store_non_game_categories.csv"
OUTPUT_NAME_TPL = "topchart_cat_{country}_{cat}.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
MAX_RETRIES = 4
BACKOFF_FACTOR = 1.8
TIMEOUT = 12
SLEEP_BETWEEN_LOOKUPS = 0.5

# If True, attempt to use app_store_scraper for supplemental fields (best-effort)
USE_SCRAPER = False

# -----------------------
# Genre map (same as you had)
# -----------------------
GENRE_MAP = {
    "books": 6018, "business": 6000, "developer tools": 6026, "education": 6017,
    "entertainment": 6016, "finance": 6015, "food & drink": 6023, "graphics & design": 6027,
    "health & fitness": 6013, "lifestyle": 6012, "kids": 36, "magazines & newspapers": 6021,
    "medical": 6020, "music": 6011, "navigation": 6010, "news": 6009, "photo & video": 6008,
    "productivity": 6007, "reference": 6006, "safari extensions": 1460, "shopping": 6024,
    "social networking": 6005, "sports": 6004, "travel": 6003, "utilities": 6002, "weather": 6001,
}

# -----------------------
# Utilities
# -----------------------
def get_country_name(code: str) -> str:
    """Return friendly country name for a 2-letter code, fallback to code."""
    code = (code or "").upper()
    if pycountry:
        try:
            c = pycountry.countries.get(alpha_2=code)
            if c:
                return c.name
        except Exception:
            pass
    # common fallbacks
    FALLBACK = {"US": "United States", "GB": "United Kingdom", "PK": "Pakistan"}
    return FALLBACK.get(code, code)

def robust_get(url: str, headers: dict = None, max_retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    headers = headers or HEADERS
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r
            if r.status_code in (429, 503, 502, 504):
                print(f"[WARN] HTTP {r.status_code} for {url} (attempt {attempt}/{max_retries}) — backing off {delay}s")
            else:
                print(f"[WARN] HTTP {r.status_code} for {url} (attempt {attempt}/{max_retries}) — not retrying")
                return r
        except requests.RequestException as e:
            print(f"[WARN] Request exception for {url}: {e} (attempt {attempt}/{max_retries})")
        time.sleep(delay)
        delay *= BACKOFF_FACTOR
    return None

def load_categories(path: str) -> List[str]:
    try:
        df = pd.read_csv(path)
        cols_lower = [c.lower() for c in df.columns.astype(str)]
        if "category" in cols_lower:
            col = df.columns[cols_lower.index("category")]
            cats = df[col].dropna().astype(str).tolist()
            return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
        if df.shape[1] == 1:
            first_col = df.columns[0]
            cats = df[first_col].dropna().astype(str).tolist()
            return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
        for col in df.columns:
            if "cat" in str(col).lower() or "name" in str(col).lower():
                cats = df[col].dropna().astype(str).tolist()
                return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
    except Exception:
        pass
    df2 = pd.read_csv(path, header=None)
    first_col = df2.columns[0]
    cats = df2[first_col].dropna().astype(str).tolist()
    return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))

# -----------------------
# Scrape the App Store web charts page for a genre
# Example URL: https://apps.apple.com/us/iphone/charts/6018
# -----------------------
APPID_RE = re.compile(r"/id(\d+)(?:\?|$|/)")
def scrape_charts_page_for_genre(country: str, genre_id: int, limit: int = LIMIT) -> List[str]:
    """
    Scrape apps.apple.com web charts for a single genre id and return ordered list of app ids.
    """
    # we'll use the iphone charts page (works for many categories). If you need iPad, change path.
    url = f"https://apps.apple.com/{country}/iphone/charts/{genre_id}"
    r = robust_get(url)
    if not r or r.status_code != 200:
        print(f"[WARN] Charts page unavailable for genre {genre_id} (country {country})")
        return []
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        # On the charts page each listed app usually includes a link like /us/app/{slug}/id{APPID}
        ids = []
        for a in soup.find_all("a", href=True):
            m = APPID_RE.search(a["href"])
            if m:
                appid = m.group(1)
                if appid not in ids:
                    ids.append(appid)
                    if len(ids) >= limit:
                        break
        return ids
    except Exception as e:
        print(f"[ERROR] Parsing charts HTML for genre {genre_id}: {e}")
        return []

# -----------------------
# iTunes Lookup for details (stable)
# -----------------------
def lookup_app_details_itunes(app_id: str, country: str = COUNTRY) -> Optional[dict]:
    url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"
    r = robust_get(url)
    if not r or r.status_code != 200:
        return None
    try:
        results = r.json().get("results", [])
        return results[0] if results else None
    except Exception:
        return None

# -----------------------
# Optional: try using app_store_scraper for extra fields (best-effort)
# -----------------------
def lookup_with_scraper(app_id: str, app_name: str = None, country: str = COUNTRY) -> dict:
    if not HAS_SCRAPER:
        return {}
    try:
        # app_store_scraper expects app_name OR app_id depending on versions; we attempt both
        if app_name:
            a = AppStore(country=country, app_name=app_name)
        else:
            # some versions accept app_id param; try to pass app_id if supported
            a = AppStore(country=country, app_id=app_id, app_name=app_name)
        # attempt to call app_info() or review() / app
        details = {}
        if hasattr(a, "app_info"):
            details = a.app_info() or {}
        elif hasattr(a, "details"):
            details = a.details() or {}
        else:
            # no reliable method; return whatever attributes exist
            details = getattr(a, "__dict__", {})
        return details or {}
    except Exception as e:
        print(f"[WARN] app_store_scraper failed for {app_id} / {app_name}: {e}")
        return {}

# -----------------------
# Save CSV
# -----------------------
def save_rows_csv(rows: List[dict], category: str, country_code: str):
    safe_cat = "".join(c if c.isalnum() or c in " _-" else "_" for c in category).strip().replace(" ", "_")
    filename = OUTPUT_NAME_TPL.format(country=country_code, cat=safe_cat)
    keys = [
        "country", "country_name", "category", "genre_id", "rank",
        "app_id", "app_name", "developer", "url",
        "price", "averageUserRating", "userRatingCount",
        "primaryGenreName", "description",
        "launch_date", "update_date"
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"[SAVED] {filename} ({len(rows)} rows)")

# -----------------------
# Main runner
# -----------------------
def run(categories_csv: str = INPUT_CSV, country: str = COUNTRY, limit: int = LIMIT):
    print("[START] Loading categories from:", categories_csv)
    categories = load_categories(categories_csv)
    print(f"[INFO] {len(categories)} categories loaded.\n")

    country_name = get_country_name(country)
    for cat in categories:
        print("--------------------------------------------------")
        print(f"[CATEGORY] {cat}")
        print("--------------------------------------------------")
        gid = None
        # robust find
        k = cat.strip().lower()
        if k in GENRE_MAP:
            gid = GENRE_MAP[k]
        else:
            k2 = k.replace("&", "and")
            if k2 in GENRE_MAP:
                gid = GENRE_MAP[k2]
            else:
                for name, _gid in GENRE_MAP.items():
                    if name in k or k in name:
                        gid = _gid
                        break

        if gid is None:
            print(f"[WARN] No genre id match for '{cat}'. Skipping.")
            continue
        print(f"[INFO] Found genre id {gid} for '{cat}'")

        # scrape charts page to get ordered app ids for this category
        app_ids = scrape_charts_page_for_genre(country, gid, limit)
        if not app_ids:
            print(f"[WARN] No apps found on charts page for genre {gid}. Skipping.")
            continue

        rows = []
        for idx, app_id in enumerate(app_ids, start=1):
            print(f"[DETAIL] ({idx}/{len(app_ids)}) id {app_id}")
            # stable details via iTunes Lookup
            details = lookup_app_details_itunes(app_id, country)
            # fallback: minimal name from details or None
            name = details.get("trackName") if details else None
            # optional: try app_store_scraper for extra fields (best-effort)
            scraper_extra = {}
            if USE_SCRAPER:
                scraper_extra = lookup_with_scraper(app_id, app_name=name, country=country)

            row = {
                "country": country,
                "country_name": country_name,
                "category": cat,
                "genre_id": gid,
                "rank": idx,
                "app_id": app_id,
                "app_name": (details.get("trackName") if details else scraper_extra.get("name") or name),
                "developer": (details.get("sellerName") if details else scraper_extra.get("developerName") or scraper_extra.get("sellerName")),
                "url": (details.get("trackViewUrl") if details else None),
                "price": (details.get("price") if details else None),
                "averageUserRating": (details.get("averageUserRating") if details else None),
                "userRatingCount": (details.get("userRatingCount") if details else None),
                "primaryGenreName": (details.get("primaryGenreName") if details else None),
                "description": (details.get("description") if details else (scraper_extra.get("description") if isinstance(scraper_extra, dict) else None)),
                "launch_date": (details.get("releaseDate") if details else None),
                "update_date": (details.get("currentVersionReleaseDate") if details else None),
            }
            rows.append(row)
            time.sleep(SLEEP_BETWEEN_LOOKUPS)

        save_rows_csv(rows, cat, country)

    print("\n[COMPLETE] All done.")

# -----------------------
# Run if script executed directly
# -----------------------
if __name__ == "__main__":
    run()
