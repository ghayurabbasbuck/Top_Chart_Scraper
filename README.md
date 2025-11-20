# App Store Top Chart Scraper

A Python tool to scrape App Store top-chart apps by category and fetch detailed app information using the iTunes API.

## Overview

This scraper:
- Reads app categories from a CSV file
- Scrapes the App Store web charts page for each category
- Fetches detailed app information (ratings, price, release dates, etc.) using iTunes Lookup API
- Saves results to individual CSV files per category

## Features

- **Web Scraping**: Extracts app IDs from apps.apple.com charts pages
- **iTunes Lookup API**: Retrieves stable, reliable app details (no rate limits)
- **Retry Logic**: Automatic backoff for failed requests (429, 502, 503, 504 errors)
- **CSV Output**: One file per category with all app details
- **Multi-Country Support**: Works with any country code (us, gb, pk, ao, etc.)
- **Optional app_store_scraper**: Best-effort supplemental data (can be flaky)

## Configuration

Edit the top of `appstore.py` to customize:

```python
COUNTRY = "ao"                    # Two-letter country code
LIMIT = 50                        # Apps per category
INPUT_CSV = "app_store_non_game_categories.csv"
OUTPUT_NAME_TPL = "topchart_cat_{country}_{cat}.csv"
MAX_RETRIES = 4                   # Retry attempts for failed requests
BACKOFF_FACTOR = 1.8              # Retry delay multiplier
SLEEP_BETWEEN_LOOKUPS = 0.5       # Delay between iTunes API calls
USE_SCRAPER = False               # Use app_store_scraper for extra fields
```

## Input File Format

Create `app_store_non_game_categories.csv` with one category per row:

```
category
Books
Business
Education
Entertainment
Finance
```

Or as a simple list (auto-detected):

```
Books
Business
Education
```

## Output Files

For each category, a CSV file is created: `topchart_cat_{country}_{category}.csv`

**Columns:**
- `country`, `country_name`, `category`, `genre_id`, `rank`
- `app_id`, `app_name`, `developer`, `url`
- `price`, `averageUserRating`, `userRatingCount`
- `primaryGenreName`, `description`
- `launch_date`, `update_date`

## Usage

```bash
python appstore.py
```

The script will:
1. Load categories from `app_store_non_game_categories.csv`
2. Find matching genre IDs
3. Scrape charts pages for app IDs
4. Fetch app details from iTunes API
5. Save results to CSV files

## Requirements

```
requests
pandas
beautifulsoup4
pycountry (optional, for country names)
app_store_scraper (optional, set USE_SCRAPER=False to skip)
```

Install:
```bash
pip install requests pandas beautifulsoup4 pycountry
```

## Example Output

```
country,country_name,category,genre_id,rank,app_id,app_name,developer,url,price,averageUserRating,userRatingCount,...
ao,Angola,Business,6000,1,123456789,Example App,Example Inc,https://apps.apple.com/...,0.0,4.5,1000,...
```

## Notes

- **Rate Limiting**: iTunes API is stable and doesn't rate limit. Web scraping uses polite delays (0.5s between lookups).
- **Error Handling**: Failed requests retry up to 4 times with exponential backoff.
- **Best Effort**: The optional `app_store_scraper` library may fail; script continues without it.
- **Friendly**: Includes proper User-Agent and error reporting.

## Troubleshooting

- **"No apps found"**: Genre ID may not exist for that country or category name doesn't match.
- **HTTP 429**: Rate limited; increase `SLEEP_BETWEEN_LOOKUPS` or reduce `LIMIT`.
- **Empty CSV**: Check that your input CSV has valid category names.
