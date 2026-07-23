"""Stage 1: collect HVAC business listings from the Google Places API (New).

This is the most reliable and fully-authorized source in the pipeline: it is a
keyed API with published quotas, so it never trips bot/CAPTCHA defenses. Getting
as much as possible here is the single biggest thing you can do to avoid ever
needing to crawl a hostile site later.

Improvements over the smoke test:
  * Pagination (nextPageToken) to pull up to ~60 results per city.
  * Batch multiple cities in one run (comma-separated, or a cities.txt file).
  * Richer fields (formatted address) for better lead records.
  * Per-request quota accounting that also counts paginated calls.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parent
ENV_FILE = PROJECT_DIR / ".env"
OUTPUT_FILE = PROJECT_DIR / "places.tsv"
CITIES_FILE = PROJECT_DIR / "cities.txt"
LOG_FILE = PROJECT_DIR / "scraper.log"
USAGE_FILE = PROJECT_DIR / "google_api_usage.json"

PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# These are intentionally lower than the Google Console quota of 25 requests
# per day and the 1,000-request monthly Enterprise free-usage allowance.
LOCAL_DAILY_LIMIT = 20
LOCAL_MONTHLY_LIMIT = 750

# Each page is a separate billable request. Two pages (=40 businesses) per city
# is a good balance between coverage and staying inside the daily limit.
MAX_PAGES_PER_CITY = 2
# The Places API needs a moment before a freshly issued nextPageToken is valid.
PAGE_TOKEN_DELAY_SECONDS = 2.5

FIELD_NAMES = ["placeId", "companyName", "website", "phone", "address", "city"]
LOS_ANGELES_TIME = ZoneInfo("America/Los_Angeles")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class LosAngelesFormatter(logging.Formatter):
    """Format log timestamps in Los Angeles time regardless of PC settings."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        timestamp = datetime.fromtimestamp(record.created, LOS_ANGELES_TIME)
        return timestamp.strftime(datefmt or TIMESTAMP_FORMAT)


def configure_logging() -> None:
    """Log to both the console and the project-local log file."""
    formatter = LosAngelesFormatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler],
    )


def load_usage() -> dict[str, Any]:
    """Load the persistent local API counter, tolerating a missing file."""
    if not USAGE_FILE.exists():
        return {}

    try:
        return json.loads(USAGE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.warning("Could not read %s; starting a fresh counter.", USAGE_FILE.name)
        return {}


def reserve_one_api_request() -> None:
    """Reserve one request before sending it so failed calls also count."""
    now = datetime.now(LOS_ANGELES_TIME)
    today = now.strftime("%Y-%m-%d")
    month = now.strftime("%Y-%m")
    usage = load_usage()

    # Read the original date/month schema once for a seamless upgrade.
    last_request_at = str(usage.get("last_request_at", ""))
    stored_date = last_request_at[:10] or str(usage.get("date", ""))
    stored_month = last_request_at[:7] or str(usage.get("month", ""))
    daily_count = usage.get("daily_count", 0) if stored_date == today else 0
    monthly_count = usage.get("monthly_count", 0) if stored_month == month else 0

    if daily_count >= LOCAL_DAILY_LIMIT:
        raise RuntimeError(f"Local daily Google API limit reached ({LOCAL_DAILY_LIMIT}).")
    if monthly_count >= LOCAL_MONTHLY_LIMIT:
        raise RuntimeError(f"Local monthly Google API limit reached ({LOCAL_MONTHLY_LIMIT}).")

    usage = {
        "last_request_at": now.strftime(TIMESTAMP_FORMAT),
        "daily_count": daily_count + 1,
        "monthly_count": monthly_count + 1,
    }
    USAGE_FILE.write_text(json.dumps(usage, indent=2), encoding="utf-8")
    logging.info(
        "Reserved Google API request: daily=%s/%s, monthly=%s/%s",
        usage["daily_count"],
        LOCAL_DAILY_LIMIT,
        usage["monthly_count"],
        LOCAL_MONTHLY_LIMIT,
    )


def request_page(api_key: str, city: str, page_token: str | None) -> dict[str, Any]:
    """Run one Places API (New) Text Search request (one page)."""
    reserve_one_api_request()

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.websiteUri,"
            "places.nationalPhoneNumber,places.formattedAddress,nextPageToken"
        ),
    }
    body: dict[str, Any] = {
        "textQuery": f"HVAC {city}",
        "pageSize": 20,
        "languageCode": "en",
        "regionCode": "US",
    }
    if page_token:
        # When paginating, the query must stay identical; only the token changes.
        body["pageToken"] = page_token

    response = requests.post(
        PLACES_TEXT_SEARCH_URL,
        headers=headers,
        json=body,
        timeout=(10, 30),
    )
    response.raise_for_status()
    return response.json()


def fetch_places(api_key: str, city: str) -> list[dict[str, Any]]:
    """Fetch up to MAX_PAGES_PER_CITY pages of results for one city."""
    places: list[dict[str, Any]] = []
    page_token: str | None = None

    for page_number in range(1, MAX_PAGES_PER_CITY + 1):
        payload = request_page(api_key, city, page_token)
        page_places = payload.get("places", [])
        places.extend(page_places)
        logging.info("City %s page %s: %s places.", city, page_number, len(page_places))

        page_token = payload.get("nextPageToken")
        if not page_token:
            break
        # Give Google a moment to activate the token before requesting the next page.
        time.sleep(PAGE_TOKEN_DELAY_SECONDS)

    return places


def existing_place_ids() -> set[str]:
    """Read IDs already written so reruns do not duplicate companies."""
    if not OUTPUT_FILE.exists():
        return set()

    with OUTPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        return {
            row["placeId"]
            for row in csv.DictReader(handle, delimiter="\t")
            if row.get("placeId")
        }


def append_places(places: list[dict[str, Any]], city: str, known_ids: set[str]) -> int:
    """Append new companies to the UTF-8, tab-delimited output file.

    ``known_ids`` is updated in place so a multi-city run does not re-read the
    whole output file for every city.
    """
    rows: list[dict[str, str]] = []

    for place in places:
        place_id = place.get("id", "").strip()
        if not place_id or place_id in known_ids:
            continue

        display_name = place.get("displayName") or {}
        rows.append(
            {
                "placeId": place_id,
                "companyName": str(display_name.get("text", "")).strip(),
                "website": str(place.get("websiteUri", "")).strip(),
                "phone": str(place.get("nationalPhoneNumber", "")).strip(),
                "address": str(place.get("formattedAddress", "")).strip(),
                "city": city,
            }
        )
        known_ids.add(place_id)

    write_header = not OUTPUT_FILE.exists() or OUTPUT_FILE.stat().st_size == 0
    with OUTPUT_FILE.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELD_NAMES, delimiter="\t")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def collect_cities() -> list[str]:
    """Get one or more cities from stdin, or from cities.txt when blank.

    cities.txt holds one "City, ST" per line; blank lines and ``#`` comments are
    ignored. This makes large batch runs repeatable without retyping.
    """
    entered = input(
        "Enter cities separated by commas (example: Fresno, CA; Reno, NV),\n"
        "or press Enter to read cities.txt: "
    ).strip()

    if entered:
        raw = entered.replace(";", ",").split(",")
        # Re-pair "City" and "ST" tokens split by the comma above.
        cities: list[str] = []
        buffer: list[str] = []
        for token in (part.strip() for part in raw):
            if not token:
                continue
            buffer.append(token)
            if len(token) == 2 and token.isalpha():
                cities.append(", ".join(buffer))
                buffer = []
        if buffer:
            cities.append(", ".join(buffer))
        return cities

    if CITIES_FILE.exists():
        return [
            line.strip()
            for line in CITIES_FILE.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    return []


def main() -> int:
    configure_logging()
    load_dotenv(ENV_FILE)

    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
    if not api_key:
        logging.error("GOOGLE_PLACES_API_KEY is missing from %s", ENV_FILE)
        return 1

    cities = collect_cities()
    if not cities:
        logging.error("At least one city is required (stdin or cities.txt).")
        return 1

    known_ids = existing_place_ids()
    total_places = 0
    total_added = 0

    for city in cities:
        try:
            places = fetch_places(api_key, city)
            added = append_places(places, city, known_ids)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            detail = exc.response.text[:500] if exc.response is not None else str(exc)
            logging.error("Google Places returned HTTP %s: %s", status, detail)
            return 1
        except RuntimeError as exc:
            # A local quota limit stops the batch cleanly, keeping what we have.
            logging.warning("Stopping batch: %s", exc)
            break
        except (requests.RequestException, OSError, ValueError) as exc:
            logging.error("City %s failed: %s", city, exc)
            continue

        total_places += len(places)
        total_added += added
        logging.info("City %s: %s places, %s new rows.", city, len(places), added)

    logging.info(
        "Done. %s places across %s city(ies); added %s new rows.",
        total_places,
        len(cities),
        total_added,
    )
    logging.info("Output: %s", OUTPUT_FILE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
