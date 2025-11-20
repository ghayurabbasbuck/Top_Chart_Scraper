# topchart_option_a_itunes.py
"""
Option A - iTunes Lookup API (recommended)
Adds launch_date, update_date, and country column.
"""

import time
import csv
import requests
import pandas as pd
from typing import List, Optional

# -----------------------
# Config
# -----------------------
COUNTRY = "ar"  # <-- Your selected country will now appear in output CSV
LIMIT = 50
INPUT_CSV = "app_store_non_game_categories.csv"
OUTPUT_NAME_TPL = "topchart_a_{cat}.csv"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
MAX_RETRIES = 4
BACKOFF_FACTOR = 1.8
TIMEOUT = 12

GENRE_MAP = {
    "books": 6018, "business": 6000, "developer tools": 6026, "education": 6017,
    "entertainment": 6016, "finance": 6015, "food & drink": 6023, "graphics & design": 6027,
    "health & fitness": 6013, "lifestyle": 6012, "kids": 36, "magazines & newspapers": 6021,
    "medical": 6020, "music": 6011, "navigation": 6010, "news": 6009, "photo & video": 6008,
    "productivity": 6007, "reference": 6006, "safari extensions": 1460, "shopping": 6024,
    "social networking": 6005, "sports": 6004, "travel": 6003, "utilities": 6002, "weather": 6001,
}

def robust_get(url: str, headers: dict = None, max_retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    headers = headers or HEADERS
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r
            if r.status_code in (429, 503):
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
            return list(dict.fromkeys([c.strip() for c in cats]))

        if df.shape[1] == 1:
            col = df.columns[0]
            cats = df[col].dropna().astype(str).tolist()
            return list(dict.fromkeys([c.strip() for c in cats]))

        for col in df.columns:
            if "cat" in col.lower() or "name" in col.lower():
                cats = df[col].dropna().astype(str).tolist()
                return list(dict.fromkeys([c.strip() for c in cats]))

    except:
        pass

    df2 = pd.read_csv(path, header=None)
    cats = df2[df2.columns[0]].dropna().astype(str).tolist()
    return list(dict.fromkeys([c.strip() for c in cats]))

def find_genre_id(category: str) -> Optional[int]:
    k = category.strip().lower()
    if k in GENRE_MAP:
        return GENRE_MAP[k]
    k2 = k.replace("&", "and")
    if k2 in GENRE_MAP:
        return GENRE_MAP[k2]
    for name, gid in GENRE_MAP.items():
        if name in k or k in name:
            return gid
    return None

def fetch_top_apps_by_genre(country: str, genre_id: int, limit: int = LIMIT):
    url = f"https://rss.applemarketingtools.com/api/v2/{country}/apps/top-free/{limit}/genre/{genre_id}.json"
    r = robust_get(url)
    if not r or r.status_code != 200:
        return []
    try:
        return r.json().get("feed", {}).get("results", [])
    except:
        return []

def fetch_country_topfree(country: str, limit: int = LIMIT):
    url = f"https://itunes.apple.com/{country}/rss/topfreeapplications/limit={limit}/json"
    r = robust_get(url)
    if not r or r.status_code != 200:
        return []
    try:
        feed = r.json().get("feed", {}).get("entry", [])
        out = []
        for e in feed:
            app_id = e.get("id", {}).get("attributes", {}).get("im:id")
            app_name = e.get("im:name", {}).get("label")
            if app_id:
                out.append({"id": str(app_id), "name": app_name})
        return out
    except:
        return []

def lookup_app_details(app_id: str, country: str = COUNTRY) -> Optional[dict]:
    url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"
    r = robust_get(url)
    if not r or r.status_code != 200:
        return None
    try:
        results = r.json().get("results", [])
        return results[0] if results else None
    except:
        return None

def save_rows_csv(rows: List[dict], category: str):
    safe = "".join(c if c.isalnum() or c in " _-" else "_" for c in category).replace(" ", "_")
    filename = OUTPUT_NAME_TPL.format(cat=safe)

    keys = [
        "country",   # <-- added new column
        "category", 
        "genre_id", 
        "rank",
        "app_id", "app_name", "developer", "url",
        "price", "averageUserRating", "userRatingCount",
        "primaryGenreName", "description",
        "launch_date", "update_date"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[SAVED] {filename} ({len(rows)} rows)")

def run(categories_csv: str = INPUT_CSV):
    print("[START] Loading categories:", categories_csv)
    categories = load_categories(categories_csv)
    print(f"[INFO] Loaded {len(categories)} categories\n")

    for cat in categories:
        print(f"\n===== CATEGORY: {cat} =====")
        gid = find_genre_id(cat)

        entries = fetch_top_apps_by_genre(COUNTRY, gid, LIMIT) if gid else []
        if not entries:
            entries = fetch_country_topfree(COUNTRY, LIMIT)

        rows = []

        for idx, e in enumerate(entries, start=1):
            app_id = str(e.get("id"))
            app_name = e.get("name")

            print(f"[DETAIL] {idx}. {app_name} ({app_id})")

            details = lookup_app_details(app_id, COUNTRY)

            row = {
                "country": COUNTRY,   # <-- NEW COLUMN ADDED
                "category": cat,
                "genre_id": gid,
                "rank": idx,
                "app_id": app_id,
                "app_name": details.get("trackName") if details else app_name,
                "developer": details.get("sellerName") if details else None,
                "url": details.get("trackViewUrl") if details else None,
                "price": details.get("price") if details else None,
                "averageUserRating": details.get("averageUserRating") if details else None,
                "userRatingCount": details.get("userRatingCount") if details else None,
                "primaryGenreName": details.get("primaryGenreName") if details else None,
                "description": details.get("description") if details else None,
                "launch_date": details.get("releaseDate") if details else None,
                "update_date": details.get("currentVersionReleaseDate") if details else None
            }

            rows.append(row)
            time.sleep(0.5)

        save_rows_csv(rows, cat)

    print("\n[COMPLETE] All done.")

if __name__ == "__main__":
    run()
