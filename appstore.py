# topchart_option_a_itunes.py
"""
Option A - iTunes Lookup API (recommended)
Adds launch_date and update_date fields; otherwise logic unchanged.
"""

import time
import csv
import requests
import pandas as pd
from typing import List, Optional

# -----------------------
# Config
# -----------------------
COUNTRY = "us"  # change to 'pk' or 'us' etc.
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
                print(f"[WARN] HTTP {r.status_code} for {url} (attempt {attempt}/{max_retries}) — not retrying further")
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
            original_col = df.columns[cols_lower.index("category")]
            cats = df[original_col].dropna().astype(str).tolist()
            return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
        if df.shape[1] == 1:
            first_col = df.columns[0]
            cats = df[first_col].dropna().astype(str).tolist()
            return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
        for col in df.columns:
            cn = str(col).lower()
            if "cat" in cn or "name" in cn:
                cats = df[col].dropna().astype(str).tolist()
                return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
    except Exception as e:
        print(f"[INFO] Could not read CSV with header mode: {e}")

    try:
        df2 = pd.read_csv(path, header=None)
        first_col = df2.columns[0]
        cats = df2[first_col].dropna().astype(str).tolist()
        return list(dict.fromkeys([c.strip() for c in cats if c.strip() != ""]))
    except Exception as e:
        print(f"[ERROR] Failed to read categories CSV: {e}")
        raise

def find_genre_id(category: str) -> Optional[int]:
    if not isinstance(category, str):
        return None
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
    if not r:
        print(f"[WARN] RSS unavailable for genre {genre_id}")
        return []
    if r.status_code != 200:
        print(f"[WARN] RSS returned {r.status_code} for genre {genre_id}")
        return []
    try:
        payload = r.json()
        results = payload.get("feed", {}).get("results", [])
        return results
    except Exception as e:
        print(f"[ERROR] RSS JSON parse error for genre {genre_id}: {e}")
        return []

def fetch_country_topfree(country: str, limit: int = LIMIT):
    url_alt = f"https://itunes.apple.com/{country}/rss/topfreeapplications/limit={limit}/json"
    r = robust_get(url_alt)
    if not r or r.status_code != 200:
        return []
    try:
        payload = r.json()
        entries = payload.get("feed", {}).get("entry", [])
        out = []
        for e in entries:
            app_id = e.get("id", {}).get("attributes", {}).get("im:id")
            name = e.get("im:name", {}).get("label")
            url = None
            try:
                url = e.get("link", {}).get("attributes", {}).get("href")
            except:
                url = None
            if app_id:
                out.append({"id": str(app_id), "name": name, "url": url})
        return out
    except Exception:
        return []

def lookup_app_details(app_id: str, country: str = COUNTRY) -> Optional[dict]:
    url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"
    r = robust_get(url)
    if not r:
        print(f"[ERROR] Lookup request failed for {app_id}")
        return None
    if r.status_code != 200:
        print(f"[WARN] Lookup HTTP {r.status_code} for {app_id}")
        return None
    try:
        j = r.json().get("results", [])
        if not j:
            return None
        return j[0]
    except Exception as e:
        print(f"[ERROR] Lookup JSON error for {app_id}: {e}")
        return None

def save_rows_csv(rows: List[dict], category: str):
    safe_cat = "".join(c if c.isalnum() or c in " _-" else "_" for c in category).strip().replace(" ", "_")
    filename = OUTPUT_NAME_TPL.format(cat=safe_cat)
    keys = [
        "category", "genre_id", "rank",
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

def run(categories_csv: str = INPUT_CSV):
    print("[START] Loading categories from:", categories_csv)
    categories = load_categories(categories_csv)
    print(f"[INFO] {len(categories)} categories loaded.")

    for cat in categories:
        print("\n--------------------------------------------------")
        print(f"[CATEGORY] {cat}")
        print("--------------------------------------------------")

        gid = find_genre_id(cat)
        if gid is None:
            print(f"[WARN] No genre id match for '{cat}'. Will fallback to country top if needed.")
        else:
            print(f"[INFO] Found genre id {gid} for '{cat}'")

        entries = []
        if gid:
            entries = fetch_top_apps_by_genre(COUNTRY, gid, LIMIT)

        if not entries:
            print(f"[INFO] Genre feed empty or not available for '{cat}' — falling back to country topfree list.")
            entries = fetch_country_topfree(COUNTRY, LIMIT)
            if not entries:
                print(f"[WARN] No entries available for '{cat}' even after fallback — skipping category.")
                continue

        rows = []
        for idx, e in enumerate(entries, start=1):
            app_id = str(e.get("id") or e.get("id", {}).get("label") or e.get("id", {}).get("attributes", {}).get("im:id", ""))
            if not app_id:
                print(f"[WARN] Missing app id in entry, skipping (entry index {idx}).")
                continue
            name = e.get("name") or (e.get("im:name", {}) or {}).get("label")
            print(f"[DETAIL] ({idx}/{len(entries)}) {name or ''} — {app_id}")

            details = lookup_app_details(app_id, COUNTRY)
            # map release dates into launch_date/update_date
            launch_date = details.get("releaseDate") if details else None
            update_date = details.get("currentVersionReleaseDate") if details else None

            row = {
                "category": cat,
                "genre_id": gid,
                "rank": idx,
                "app_id": app_id,
                "app_name": (details.get("trackName") if details else name),
                "developer": (details.get("sellerName") if details else None),
                "url": (details.get("trackViewUrl") if details else e.get("url")),
                "price": (details.get("price") if details else None),
                "averageUserRating": (details.get("averageUserRating") if details else None),
                "userRatingCount": (details.get("userRatingCount") if details else None),
                "primaryGenreName": (details.get("primaryGenreName") if details else None),
                "description": (details.get("description") if details else None),
                "launch_date": launch_date,
                "update_date": update_date,
            }
            rows.append(row)
            time.sleep(0.6)

        save_rows_csv(rows, cat)

    print("\n[COMPLETE] All done.")

if __name__ == "__main__":
    run()
