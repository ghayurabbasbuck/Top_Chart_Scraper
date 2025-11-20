"""
OPTION 3 — FIXED HYBRID

✔ Categories loaded from CSV (NOT app-store-scraper)
✔ Top charts fetched by RSS API (Apple official)
✔ App details via iTunes Lookup API
✔ app-store-scraper used ONLY as fallback to find app ID by name
"""

import time
import csv
import requests
import pandas as pd
from app_store_scraper import AppStore  # optional fallback only
from typing import List, Optional

COUNTRY = "us"
LIMIT = 20
INPUT_CSV = "app_store_non_game_categories.csv"
OUTPUT_NAME_TPL = "topchart_option3_{cat}.csv"

HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_RETRIES = 4
BACKOFF = 2
TIMEOUT = 10

GENRE_MAP = {
    "travel": 6003,
    "food & drink": 6023,
    "shopping": 6024,
    "business": 6000,
    "education": 6017,
    "finance": 6015,
    "weather": 6001,
    "news": 6009,
    "navigation": 6010,
    "productivity": 6007,
    "utilities": 6002,
    "lifestyle": 6012,
    "sports": 6004,
    "entertainment": 6016,
    "music": 6011,
    "health & fitness": 6013,
    "photo & video": 6008,
    "social networking": 6005,
    "books": 6018
}

def robust_get(url):
    delay = 1
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            print(f"[WARN] {r.status_code} → retry {attempt}")
        except:
            print(f"[WARN] Request error on try {attempt}")
        time.sleep(delay)
        delay *= BACKOFF
    return None


# --------------------------
# Step 1 — Load Categories
# --------------------------
def load_categories(path):
    df = pd.read_csv(path)
    col = df.columns[0]
    return df[col].dropna().astype(str).tolist()


# -------------------------------
# Step 2 — Fetch top charts by genre
# -------------------------------
def fetch_rss_top_chart(country, genre_id, limit=50):
    url = f"https://rss.applemarketingtools.com/api/v2/{country}/apps/top-free/{limit}/genre/{genre_id}.json"
    r = robust_get(url)
    if not r:
        return []
    try:
        return r.json().get("feed", {}).get("results", [])
    except:
        return []


# --------------------------------
# Step 3 — Lookup official details
# --------------------------------
def lookup_details(app_id):
    url = f"https://itunes.apple.com/lookup?id={app_id}&country={COUNTRY}"
    r = robust_get(url)
    if not r:
        return None
    js = r.json()
    return js.get("results", [{}])[0]


# ---------------------------------------------------
# OPTIONAL — Fallback: find ID using app-store-scraper
# ---------------------------------------------------
def fallback_find_app_id(app_name):
    try:
        scraper = AppStore(country=COUNTRY, app_name=app_name)
        scraper.search()
        if scraper.result:
            return scraper.result[0]["trackId"]
    except:
        return None
    return None


# --------------------------
# Step 4 — Save CSV
# --------------------------
def save(category, rows):
    fname = OUTPUT_NAME_TPL.format(cat=category.replace(" ", "_"))
    columns = [
        "country", "category", "genre_id", "rank",
        "app_id", "app_name", "developer", "url",
        "price", "rating", "rating_count",
        "launch_date", "update_date"
    ]
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)

    print(f"[SAVED] {fname}")


# --------------------------
# MAIN
# --------------------------
def run():
    print("[STEP] Loading categories...")
    categories = load_categories(INPUT_CSV)

    for cat in categories:
        print(f"\n===== CATEGORY: {cat} =====")

        gid = GENRE_MAP.get(cat.lower().strip())
        if not gid:
            print(f"[SKIP] No genre ID for {cat}")
            continue

        entries = fetch_rss_top_chart(COUNTRY, gid, LIMIT)
        print(f"[INFO] Received {len(entries)} apps from RSS")

        rows = []
        for idx, e in enumerate(entries, start=1):

            app_id = e.get("id")
            name = e.get("name")

            # fallback if RSS missing ID (rare)
            if not app_id:
                app_id = fallback_find_app_id(name)

            details = lookup_details(app_id) if app_id else {}

            row = {
                "country": COUNTRY,
                "category": cat,
                "genre_id": gid,
                "rank": idx,
                "app_id": app_id,
                "app_name": details.get("trackName", name),
                "developer": details.get("sellerName"),
                "url": details.get("trackViewUrl"),
                "price": details.get("price"),
                "rating": details.get("averageUserRating"),
                "rating_count": details.get("userRatingCount"),
                "launch_date": details.get("releaseDate"),
                "update_date": details.get("currentVersionReleaseDate")
            }
            rows.append(row)
            time.sleep(0.3)

        save(cat, rows)

    print("\n[COMPLETE] Option 3 Hybrid finished.")


if __name__ == "__main__":
    run()
