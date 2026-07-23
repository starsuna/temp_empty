"""Stage 1: autonomous HVAC business harvester (Google Places API, New).

Runs by itself across the largest US cities (no manual city input), pulling HVAC
businesses into places.tsv. It keeps going until the Google API key/quota is
exhausted, at which point it logs the reason and stops cleanly.

RESUME GUARANTEE: progress is saved after every single city. If you cancel the
script (or it stops on quota), the next run continues from EXACTLY the city it
left off on. Already-seen businesses are de-duplicated by placeId, so a city
that was interrupted mid-way is safely re-run with no duplicates.

City list: the top ~1,000 US cities by population are downloaded once and cached
locally (us_cities.csv). Tiny rural towns are intentionally excluded -- they
cost API calls and return almost nothing.
"""

from __future__ import annotations

import csv
import hashlib
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
CITY_CACHE_FILE = PROJECT_DIR / "us_cities.csv"
PROGRESS_FILE = PROJECT_DIR / "harvest_progress.json"
LOG_FILE = PROJECT_DIR / "scraper.log"

PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Population-ranked top-1k US cities (City, State, Population, lat, lon). Public
# dataset; downloaded once, then read from the local cache on every later run.
CITY_DATASET_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv"
)

# Up to 3 pages (~60 businesses) per city -- the Places API maximum per query.
MAX_PAGES_PER_CITY = 3
# The API needs a moment before a freshly issued nextPageToken is valid.
PAGE_TOKEN_DELAY_SECONDS = 2.5
# Small courtesy pause between cities.
BETWEEN_CITY_DELAY_SECONDS = 0.5

# OPTIONAL SAFETY CAP on API requests per run. None = run until the key/quota
# actually runs out (what you asked for). On a PAID billing account that means
# it keeps spending until Google's own quota/billing limit is hit, so if you
# want a hard ceiling, set this to an integer (e.g. 5000) to stop early.
MAX_REQUESTS_PER_RUN: int | None = None

# Abort if this many cities fail in a row for a non-quota reason (network down,
# etc.) so the script does not spin forever on a systemic problem.
MAX_CONSECUTIVE_FAILURES = 6

# Fallback if the dataset download fails and there is no cache yet.
FALLBACK_CITIES: tuple[tuple[str, str], ...] = (
    ("New York", "New York"), ("Los Angeles", "California"), ("Chicago", "Illinois"),
    ("Houston", "Texas"), ("Phoenix", "Arizona"), ("Philadelphia", "Pennsylvania"),
    ("San Antonio", "Texas"), ("San Diego", "California"), ("Dallas", "Texas"),
    ("San Jose", "California"), ("Austin", "Texas"), ("Jacksonville", "Florida"),
    ("Fort Worth", "Texas"), ("Columbus", "Ohio"), ("Charlotte", "North Carolina"),
    ("Indianapolis", "Indiana"), ("San Francisco", "California"), ("Seattle", "Washington"),
    ("Denver", "Colorado"), ("Nashville", "Tennessee"), ("Oklahoma City", "Oklahoma"),
    ("Las Vegas", "Nevada"), ("Memphis", "Tennessee"), ("Louisville", "Kentucky"),
    ("Atlanta", "Georgia"), ("Fresno", "California"), ("Sacramento", "California"),
    ("Mesa", "Arizona"), ("Tampa", "Florida"), ("Tucson", "Arizona"),
)

FIELD_NAMES = ["placeId", "companyName", "website", "phone", "address", "city"]
LOS_ANGELES_TIME = ZoneInfo("America/Los_Angeles")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class QuotaExhausted(RuntimeError):
    """Raised when Google signals the API key/quota is spent -- our stop signal."""


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
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])


def load_cities() -> list[dict[str, str]]:
    """Return top US cities, biggest first. Download once, then use the cache."""
    if not CITY_CACHE_FILE.exists():
        logging.info("Downloading US city list (one time) from %s", CITY_DATASET_URL)
        try:
            response = requests.get(CITY_DATASET_URL, timeout=(10, 60))
            response.raise_for_status()
            CITY_CACHE_FILE.write_bytes(response.content)
            logging.info("Cached city list to %s", CITY_CACHE_FILE.name)
        except (requests.RequestException, OSError) as exc:
            logging.warning("City download failed (%s); using built-in fallback list.", exc)
            return [{"city": c, "state": s} for c, s in FALLBACK_CITIES]

    cities: list[dict[str, Any]] = []
    with CITY_CACHE_FILE.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            city = (row.get("City") or "").strip()
            state = (row.get("State") or "").strip()
            try:
                population = int(float(row.get("Population") or 0))
            except ValueError:
                population = 0
            if city and state:
                cities.append({"city": city, "state": state, "population": population})

    # Largest first so limited quota is always spent on the densest markets.
    cities.sort(key=lambda c: c["population"], reverse=True)
    return [{"city": c["city"], "state": c["state"]} for c in cities]


def queue_signature(cities: list[dict[str, str]]) -> str:
    """Stable hash of the ordered queue, to detect if the city list changed."""
    joined = "\n".join(f"{c['city']}|{c['state']}" for c in cities)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def load_progress() -> dict[str, Any]:
    if not PROGRESS_FILE.exists():
        return {}
    try:
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.warning("Could not read %s; starting progress from scratch.", PROGRESS_FILE.name)
        return {}


def save_progress(progress: dict[str, Any]) -> None:
    progress["last_update"] = datetime.now(LOS_ANGELES_TIME).strftime(TIMESTAMP_FORMAT)
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding="utf-8")


def request_page(api_key: str, query_text: str, page_token: str | None) -> dict[str, Any]:
    """Run one Places Text Search request; raise QuotaExhausted when spent."""
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.websiteUri,"
            "places.nationalPhoneNumber,places.formattedAddress,nextPageToken"
        ),
    }
    body: dict[str, Any] = {
        "textQuery": query_text,
        "pageSize": 20,
        "languageCode": "en",
        "regionCode": "US",
    }
    if page_token:
        body["pageToken"] = page_token

    response = requests.post(PLACES_TEXT_SEARCH_URL, headers=headers, json=body, timeout=(10, 30))

    # Quota / key exhaustion -> clean, intentional stop.
    if response.status_code == 429:
        raise QuotaExhausted("HTTP 429 RESOURCE_EXHAUSTED (quota or rate limit reached)")
    if response.status_code in (403, 400):
        body_text = response.text.lower()
        markers = ("resource_exhausted", "quota", "billing", "permission_denied",
                   "api key", "api_key not valid", "expired", "disabled")
        if any(marker in body_text for marker in markers):
            raise QuotaExhausted(f"HTTP {response.status_code}: {response.text[:300]}")

    response.raise_for_status()
    return response.json()


def fetch_places_for_city(api_key: str, city: dict[str, str], counters: dict[str, int]) -> list[dict[str, Any]]:
    """Fetch up to MAX_PAGES_PER_CITY pages for one city."""
    query_text = f"HVAC in {city['city']}, {city['state']}"
    places: list[dict[str, Any]] = []
    page_token: str | None = None

    for _ in range(MAX_PAGES_PER_CITY):
        if MAX_REQUESTS_PER_RUN is not None and counters["requests"] >= MAX_REQUESTS_PER_RUN:
            raise QuotaExhausted(f"local MAX_REQUESTS_PER_RUN cap ({MAX_REQUESTS_PER_RUN}) reached")

        payload = request_page(api_key, query_text, page_token)
        counters["requests"] += 1
        places.extend(payload.get("places", []))

        page_token = payload.get("nextPageToken")
        if not page_token:
            break
        time.sleep(PAGE_TOKEN_DELAY_SECONDS)

    return places


def existing_place_ids() -> set[str]:
    """Read IDs already written so reruns never duplicate companies."""
    if not OUTPUT_FILE.exists():
        return set()
    with OUTPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        return {
            row["placeId"]
            for row in csv.DictReader(handle, delimiter="\t")
            if row.get("placeId")
        }


def append_places(places: list[dict[str, Any]], city_label: str, known_ids: set[str]) -> int:
    """Append new companies to the UTF-8, tab-delimited output file."""
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
                "city": city_label,
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


def main() -> int:
    configure_logging()
    load_dotenv(ENV_FILE)

    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
    if not api_key:
        logging.error("GOOGLE_PLACES_API_KEY is missing from %s", ENV_FILE)
        return 1

    cities = load_cities()
    signature = queue_signature(cities)
    progress = load_progress()

    # Resume exactly where we left off, unless the city list itself changed.
    start_index = 0
    if progress.get("queue_signature") == signature:
        start_index = int(progress.get("next_index", 0))
    elif progress:
        logging.warning("City list changed since last run; restarting the sweep "
                        "(existing companies are still de-duplicated).")

    counters = {"requests": 0}
    total_added = int(progress.get("total_added", 0))
    known_ids = existing_place_ids()

    logging.info("Harvest starting at city %s/%s (%s already collected).",
                 start_index + 1, len(cities), len(known_ids))

    consecutive_failures = 0
    index = start_index
    stop_reason = "completed"

    try:
        for index in range(start_index, len(cities)):
            city = cities[index]
            label = f"{city['city']}, {city['state']}"
            logging.info("[%s/%s] Searching HVAC in %s", index + 1, len(cities), label)

            try:
                places = fetch_places_for_city(api_key, city, counters)
            except QuotaExhausted:
                raise
            except (requests.RequestException, ValueError) as exc:
                consecutive_failures += 1
                logging.warning("City %s failed (%s/%s consecutive): %s",
                                label, consecutive_failures, MAX_CONSECUTIVE_FAILURES, exc)
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    stop_reason = "too_many_failures"
                    break
                # Skip this city; mark it done so we do not loop on it forever.
                save_progress({"queue_signature": signature, "next_index": index + 1,
                               "total_added": total_added})
                continue

            consecutive_failures = 0
            added = append_places(places, label, known_ids)
            total_added += added

            # EXACT-RESUME POINT: only advance past this city once it is fully done.
            save_progress({
                "queue_signature": signature,
                "next_index": index + 1,
                "total_added": total_added,
                "cities_done": index + 1,
                "requests_this_run": counters["requests"],
            })
            logging.info("  %s: %s found, %s new (running total %s). Requests this run: %s",
                         label, len(places), added, total_added, counters["requests"])

            time.sleep(BETWEEN_CITY_DELAY_SECONDS)
        else:
            logging.info("Finished the entire city queue. Total companies collected: %s", total_added)
            save_progress({"queue_signature": signature, "next_index": len(cities),
                           "total_added": total_added, "cities_done": len(cities)})
            return 0

    except QuotaExhausted as exc:
        # Do NOT advance past the current city: it will be re-run next time.
        save_progress({"queue_signature": signature, "next_index": index,
                       "total_added": total_added, "cities_done": index})
        next_city = cities[index] if index < len(cities) else None
        logging.error("API KEY / QUOTA EXHAUSTED: %s", exc)
        logging.error("Stopping. Collected %s companies total; %s requests this run.",
                       total_added, counters["requests"])
        if next_city:
            logging.error("Re-run to resume EXACTLY at city %s/%s: %s, %s",
                           index + 1, len(cities), next_city["city"], next_city["state"])
        return 2
    except KeyboardInterrupt:
        save_progress({"queue_signature": signature, "next_index": index,
                       "total_added": total_added, "cities_done": index})
        logging.warning("Interrupted by user. Progress saved; re-run to resume at city %s/%s.",
                        index + 1, len(cities))
        return 130

    logging.error("Stopped early (%s). Progress saved; re-run to continue.", stop_reason)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
