"""Stage 2: politely crawl company websites for public email addresses.

Design stance (unchanged and deliberate): identify honestly, obey robots.txt,
pace requests, and *stop cleanly* the moment a site signals it does not want
automated traffic. That is not a limitation to work around -- it is precisely
what keeps this pipeline off rate-limit and block lists. The way to avoid
CAPTCHA/bot walls is to never fight them: take what open sites offer and skip
the rest.

Improvements over the smoke test:
  * JSON-LD / schema.org email extraction (many sites publish contact email in
    structured data that never appears in visible text).
  * Wider, smarter contact-page discovery.
  * More conservative default pacing to reduce the chance of tripping limits.
  * Optional MX validation (dnspython) so dead domains are dropped early.
"""

from __future__ import annotations

import csv
import html
import json
import logging
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:  # Optional: only used if installed. Never a hard dependency.
    import dns.resolver as _dns_resolver
except ImportError:  # pragma: no cover - optional dependency
    _dns_resolver = None


PROJECT_DIR = Path(__file__).resolve().parent
INPUT_FILE = PROJECT_DIR / "places.tsv"
OUTPUT_FILE = PROJECT_DIR / "emails.tsv"
STATE_FILE = PROJECT_DIR / "crawl_state.json"
LOG_FILE = PROJECT_DIR / "scraper.log"

# None = keep crawling until every collected website has been processed (it is
# resumable via crawl_state.json, so cancelling and re-running continues exactly
# where it left off). Set an integer if you want to cap a single run instead.
MAX_SITES_PER_RUN: int | None = None
MAX_HTML_BYTES = 2_000_000
# Slightly more conservative than before: gentler pacing is the cheapest, most
# reliable way to stay under a site's rate thresholds and never look abusive.
MIN_DELAY_SECONDS = 2.0
MAX_DELAY_SECONDS = 4.5
CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 20
# Validate that a domain can actually receive mail before trusting an address.
# Requires dnspython; when it is absent, validation is skipped (fail open).
VALIDATE_MX = True

# This identifies the client as automated research software. Pretending to be a
# real browser is unreliable and can violate site policies.
USER_AGENT = "HVACLeadResearch/0.3 (respectful local crawler)"

CONTACT_HINTS = (
    "contact",
    "about",
    "team",
    "our-story",
    "our_story",
    "who-we-are",
    "who_we_are",
    "get-in-touch",
    "reach-us",
    "reach-out",
    "connect",
    "support",
    "staff",
    "meet",
)

# Tried directly on each site even when the homepage does not link them with
# obvious text. Many sites have a /contact page that the nav labels as an icon
# or an image, so link-text discovery alone misses it.
# Trailing slashes match what these CMS/WordPress sites actually serve; the
# no-slash forms tend to 404. Only tried when the homepage links no contact
# page of its own (see build_subpages), to avoid pointless 404 requests.
COMMON_CONTACT_PATHS = (
    "/contact/",
    "/contact-us/",
    "/about-us/",
)

# Upper bound on pages fetched per site (homepage + this many sub-pages). Keeps
# request volume low and predictable so we never look like a heavy crawler.
MAX_SUBPAGES_PER_SITE = 4

EMAIL_PATTERN = re.compile(
    r"(?<![A-Z0-9._%+-])"
    r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,24}"
    r"(?![A-Z0-9._%+-])",
    re.IGNORECASE,
)

OUTPUT_FIELDS = [
    "placeId",
    "email",
    "companyName",
    "website",
    "phone",
    "city",
    "sourcePage",
]
LOS_ANGELES_TIME = ZoneInfo("America/Los_Angeles")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Cache MX lookups so a batch that shares a domain only resolves it once.
_mx_cache: dict[str, bool] = {}


class CrawlBlocked(RuntimeError):
    """Raised when a site's policy or response tells the crawler to stop."""


class LosAngelesFormatter(logging.Formatter):
    """Format log timestamps in Los Angeles time regardless of PC settings."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        timestamp = datetime.fromtimestamp(record.created, LOS_ANGELES_TIME)
        return timestamp.strftime(datefmt or TIMESTAMP_FORMAT)


def response_block_reason(response: requests.Response) -> str:
    """Return a concise reason when a response looks like an access challenge."""
    retry_after = response.headers.get("Retry-After", "").strip()
    if response.status_code == 429:
        suffix = f"; Retry-After={retry_after}" if retry_after else ""
        return f"HTTP 429 rate limit{suffix}"
    if response.status_code in {401, 403}:
        return f"HTTP {response.status_code} access denial"

    if response.headers.get("cf-mitigated", "").lower() == "challenge":
        return "Cloudflare challenge header"

    final_url = response.url.lower()
    if any(marker in final_url for marker in ("/captcha", "__cf_chl")):
        return "challenge URL"

    # Only use strong challenge-page phrases. Normal contact forms often load a
    # CAPTCHA widget, which by itself does not mean our page request was blocked.
    body = response.text[:500_000].lower()
    challenge_markers = (
        "<title>just a moment...</title>",
        "verify you are human",
        "checking your browser before accessing",
        "attention required! | cloudflare",
        "<title>access denied</title>",
    )
    for marker in challenge_markers:
        if marker in body:
            return f"challenge page marker: {marker}"
    return ""


def configure_logging() -> None:
    formatter = LosAngelesFormatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler],
    )


def build_session() -> requests.Session:
    """Create a session with conservative retry behavior."""
    retry = Retry(
        total=2,
        connect=2,
        read=1,
        status=2,
        backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5",
            "Accept-Language": "en-US,en;q=0.8",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def polite_delay() -> None:
    delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
    logging.info("Polite same-site delay: %.1f seconds.", delay)
    time.sleep(delay)


def normalize_page_url(url: str) -> str:
    """Remove fragments while preserving an ordinary HTTP(S) URL."""
    parsed = urlparse(url.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        return ""
    return urlunparse(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "/", "", parsed.query, "")
    )


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.warning("Could not read %s; starting with empty state.", STATE_FILE.name)
        return {}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def load_next_company(state: dict[str, Any]) -> dict[str, str] | None:
    """Return the first website that has not already been processed."""
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing Stage 1 output: {INPUT_FILE}")

    with INPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            place_id = (row.get("placeId") or "").strip()
            website = (row.get("website") or "").strip()
            if place_id and website and place_id not in state:
                return {key: (value or "").strip() for key, value in row.items()}
    return None


def load_robots(session: requests.Session, website: str) -> RobotFileParser:
    """Fetch and parse robots.txt; fail closed on blocking/server errors."""
    parsed = urlparse(website)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    robot_parser = RobotFileParser()
    robot_parser.set_url(robots_url)

    logging.info("Checking robots.txt: %s", robots_url)
    try:
        response = session.get(
            robots_url,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.RequestException as exc:
        raise CrawlBlocked(f"robots.txt could not be checked: {exc}") from exc

    block_reason = response_block_reason(response)
    if block_reason:
        raise CrawlBlocked(
            f"AUTOMATION_ALERT: robots.txt access restriction: {block_reason}"
        )
    if response.status_code >= 500:
        raise CrawlBlocked(
            f"AUTOMATION_ALERT: robots.txt unavailable: HTTP {response.status_code}"
        )

    if response.status_code == 200:
        robot_parser.parse(response.text.splitlines())
    else:
        # A missing robots.txt means there are no published crawl rules.
        robot_parser.parse([])
    return robot_parser


def fetch_html(
    session: requests.Session,
    robots: RobotFileParser,
    url: str,
) -> tuple[str, str]:
    """Fetch one allowed HTML page and return its final URL and text."""
    if not robots.can_fetch(USER_AGENT, url):
        raise CrawlBlocked(f"AUTOMATION_ALERT: robots.txt denies crawling {url}")

    polite_delay()
    response = session.get(
        url,
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        allow_redirects=True,
    )

    block_reason = response_block_reason(response)
    if block_reason:
        # We do NOT try to bypass this. A challenge means "no automated access";
        # we record it and move on to the next company.
        raise CrawlBlocked(f"AUTOMATION_ALERT: {block_reason} at {response.url}")
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        raise ValueError(f"not an HTML page ({content_type or 'unknown content type'})")
    if len(response.content) > MAX_HTML_BYTES:
        raise ValueError(f"HTML page exceeded {MAX_HTML_BYTES} bytes")

    return normalize_page_url(response.url), response.text


def deobfuscate_email_text(value: str) -> str:
    """Handle a few common, human-readable email obfuscations."""
    value = html.unescape(unquote(value))
    value = re.sub(r"\s*(?:\[at\]|\(at\))\s*", "@", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*(?:\[dot\]|\(dot\))\s*", ".", value, flags=re.IGNORECASE)
    return value


def _walk_json_for_emails(node: Any, found: set[str]) -> None:
    """Recursively pull email values out of parsed JSON-LD structured data."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key.lower() == "email" and isinstance(value, str):
                cleaned = value.split(":", 1)[-1] if value.lower().startswith("mailto:") else value
                found.update(EMAIL_PATTERN.findall(deobfuscate_email_text(cleaned)))
            else:
                _walk_json_for_emails(value, found)
    elif isinstance(node, list):
        for item in node:
            _walk_json_for_emails(item, found)
    elif isinstance(node, str) and "@" in node:
        found.update(EMAIL_PATTERN.findall(deobfuscate_email_text(node)))


def extract_jsonld_emails(soup: BeautifulSoup) -> set[str]:
    """Extract emails from schema.org JSON-LD blocks (Organization, ContactPoint)."""
    found: set[str] = set()
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            # Some sites emit slightly malformed JSON-LD; still try the regex.
            found.update(EMAIL_PATTERN.findall(deobfuscate_email_text(raw)))
            continue
        _walk_json_for_emails(data, found)
    return found


def extract_emails(page_html: str) -> set[str]:
    soup = BeautifulSoup(page_html, "html.parser")
    candidates: set[str] = set()

    # 1) mailto: links -- the most reliable, intentional signal.
    for link in soup.select('a[href^="mailto:"]'):
        address = unquote(link.get("href", "")[7:]).split("?", 1)[0]
        candidates.update(EMAIL_PATTERN.findall(deobfuscate_email_text(address)))

    # 2) JSON-LD structured data -- often present when visible text has nothing.
    candidates.update(extract_jsonld_emails(soup))

    # 3) Visible text only. Raw HTML commonly contains telemetry DSNs and
    #    developer identifiers that merely resemble email addresses.
    for hidden_element in soup(["script", "style", "noscript", "template"]):
        hidden_element.decompose()
    searchable_text = soup.get_text(" ")
    candidates.update(EMAIL_PATTERN.findall(deobfuscate_email_text(searchable_text)))

    # Placeholder domains left in website templates (never real inboxes).
    rejected_domains = {
        "example.com",
        "example.org",
        "sentry.io",
        "businessname.com",
        "yourbusiness.com",
        "yourcompany.com",
        "companyname.com",
        "yourdomain.com",
        "domain.com",
        "yourwebsite.com",
        "website.com",
        "yoursite.com",
        "mysite.com",
        "email.com",
        "youremail.com",
    }
    rejected_domain_fragments = (
        "sentry",
        "wixpress.com",
        "businessname",
        "yourbusiness",
        "yourcompany",
        "companyname",
        "yourdomain",
        "yourwebsite",
    )
    rejected_suffixes = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}
    rejected_local_parts = {"noreply", "no-reply", "donotreply", "do-not-reply"}
    accepted: set[str] = set()

    for candidate in candidates:
        email_address = candidate.lower().strip(".,;:()[]{}<>")
        local_part, domain = email_address.rsplit("@", 1)
        if domain in rejected_domains:
            continue
        if any(fragment in domain for fragment in rejected_domain_fragments):
            continue
        if local_part in rejected_local_parts:
            continue
        if re.fullmatch(r"[0-9a-f]{24,}", local_part):
            continue
        if email_address.endswith(tuple(rejected_suffixes)):
            continue
        accepted.add(email_address)

    return accepted


def domain_accepts_mail(domain: str) -> bool:
    """Return True if the domain has an MX (or A) record, else False.

    Fails open (returns True) when dnspython is unavailable or lookups error,
    so validation never silently discards real leads.
    """
    if not VALIDATE_MX or _dns_resolver is None:
        return True
    if domain in _mx_cache:
        return _mx_cache[domain]

    accepts = True
    try:
        _dns_resolver.resolve(domain, "MX")
    except (_dns_resolver.NoAnswer, _dns_resolver.NXDOMAIN):
        # No MX record: some small domains accept mail on the A record instead.
        try:
            _dns_resolver.resolve(domain, "A")
        except Exception:  # noqa: BLE001 - any failure means "cannot confirm"
            accepts = False
    except Exception:  # noqa: BLE001 - resolver/timeout errors: do not discard.
        accepts = True

    _mx_cache[domain] = accepts
    return accepts


def discover_contact_pages(home_url: str, page_html: str) -> list[str]:
    """Choose up to three same-domain contact/about links from the homepage."""
    soup = BeautifulSoup(page_html, "html.parser")
    home_host = urlparse(home_url).netloc.lower().removeprefix("www.")
    found: list[str] = []

    for link in soup.find_all("a", href=True):
        label = f"{link.get_text(' ', strip=True)} {link.get('href', '')}".lower()
        if not any(hint in label for hint in CONTACT_HINTS):
            continue

        candidate = normalize_page_url(urljoin(home_url, link["href"]))
        candidate_host = urlparse(candidate).netloc.lower().removeprefix("www.")
        if candidate and candidate_host == home_host and candidate not in found and candidate != home_url:
            found.append(candidate)
        if len(found) == MAX_SUBPAGES_PER_SITE:
            break

    return found


def build_subpages(home_url: str, page_html: str) -> list[str]:
    """Combine linked contact pages with common guessed paths, deduplicated.

    Linked pages come first (they are known to exist); guessed paths fill any
    remaining slots. A guessed path that does not exist simply 404s and is
    skipped, so this only ever helps.
    """
    pages = discover_contact_pages(home_url, page_html)

    # Only brute-force common paths when the homepage did NOT link a contact
    # page itself. When it did, that linked page is the real one; guessing on
    # top just generates 404s (each of which still costs a polite delay).
    if not pages:
        parsed = urlparse(home_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in COMMON_CONTACT_PATHS:
            candidate = normalize_page_url(base + path)
            if candidate and candidate != home_url and candidate not in pages:
                pages.append(candidate)
            if len(pages) >= MAX_SUBPAGES_PER_SITE:
                break

    return pages[:MAX_SUBPAGES_PER_SITE]


def crawl_company(company: dict[str, str]) -> dict[str, set[str]]:
    """Crawl homepage plus a few linked/guessed contact/about pages."""
    website = normalize_page_url(company["website"])
    if not website:
        raise ValueError("website is not an HTTP(S) URL")

    results: dict[str, set[str]] = {}
    with build_session() as session:
        robots = load_robots(session, website)
        home_url, home_html = fetch_html(session, robots, website)
        results[home_url] = extract_emails(home_html)

        for page_url in build_subpages(home_url, home_html):
            try:
                final_url, page_html = fetch_html(session, robots, page_url)
                results[final_url] = extract_emails(page_html)
            except CrawlBlocked as exc:
                logging.warning("%s", exc)
            except (requests.RequestException, ValueError) as exc:
                logging.info("Skipping linked page %s: %s", page_url, exc)

    # Drop addresses whose domain cannot receive mail (optional MX check).
    for source_page, addresses in results.items():
        deliverable = {a for a in addresses if domain_accepts_mail(a.rsplit("@", 1)[1])}
        dropped = addresses - deliverable
        if dropped:
            logging.info("Dropped %s undeliverable address(es) from %s.", len(dropped), source_page)
        results[source_page] = deliverable

    return results


def existing_email_keys() -> set[tuple[str, str]]:
    if not OUTPUT_FILE.exists():
        return set()
    with OUTPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        return {
            ((row.get("placeId") or "").strip(), (row.get("email") or "").lower().strip())
            for row in csv.DictReader(handle, delimiter="\t")
        }


def append_emails(company: dict[str, str], page_results: dict[str, set[str]]) -> int:
    known = existing_email_keys()
    rows: list[dict[str, str]] = []

    for source_page, addresses in page_results.items():
        for email_address in sorted(addresses):
            key = (company["placeId"], email_address)
            if key in known:
                continue
            rows.append(
                {
                    "placeId": company["placeId"],
                    "email": email_address,
                    "companyName": company["companyName"],
                    "website": company["website"],
                    "phone": company["phone"],
                    "city": company["city"],
                    "sourcePage": source_page,
                }
            )
            known.add(key)

    write_header = not OUTPUT_FILE.exists() or OUTPUT_FILE.stat().st_size == 0
    with OUTPUT_FILE.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, delimiter="\t")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def log_detection_summary(state: dict[str, Any]) -> None:
    """Summarize policy denials and access/challenge signals for this dataset."""
    errors = [str(item.get("error", "")) for item in state.values()]
    robots_denials = sum("robots.txt denies crawling" in error for error in errors)
    access_blocks = sum(
        "AUTOMATION_ALERT" in error and "robots.txt" not in error for error in errors
    )
    robots_check_blocks = sum(
        "AUTOMATION_ALERT" in error
        and "robots.txt" in error
        and "denies crawling" not in error
        for error in errors
    )
    failures = sum(item.get("status") == "failed" for item in state.values())
    logging.info(
        "Detection summary: robots_denials=%s, robots_check_blocks=%s, "
        "access_or_challenge_blocks=%s, other_failures=%s",
        robots_denials,
        robots_check_blocks,
        access_blocks,
        failures,
    )


def main() -> int:
    configure_logging()
    if VALIDATE_MX and _dns_resolver is None:
        logging.info("dnspython not installed; MX validation is off (addresses kept as-is).")
    state = load_state()

    processed = 0
    while MAX_SITES_PER_RUN is None or processed < MAX_SITES_PER_RUN:
        try:
            company = load_next_company(state)
        except (OSError, csv.Error) as exc:
            logging.error("Could not read Stage 1 output: %s", exc)
            return 1

        if company is None:
            logging.info("No uncrawled company websites remain.")
            log_detection_summary(state)
            logging.info("Email output: %s", OUTPUT_FILE)
            return 0

        place_id = company["placeId"]
        logging.info("Crawling %s (%s)", company["companyName"], company["website"])
        status = "completed"
        error = ""
        email_count = 0

        try:
            page_results = crawl_company(company)
            email_count = append_emails(company, page_results)
        except CrawlBlocked as exc:
            status = "blocked_or_disallowed"
            error = str(exc)
            logging.warning("Skipping %s: %s", company["website"], exc)
        except (requests.RequestException, OSError, ValueError) as exc:
            status = "failed"
            error = str(exc)
            logging.warning("Could not crawl %s: %s", company["website"], exc)

        state[place_id] = {
            "company": company["companyName"],
            "website": company["website"],
            "status": status,
            "email_count": email_count,
            "error": error,
            "processed_at": datetime.now(LOS_ANGELES_TIME).strftime(TIMESTAMP_FORMAT),
        }
        save_state(state)
        logging.info("Crawl result: status=%s, new_emails=%s", status, email_count)
        processed += 1

    logging.info("Stage 2 processed %s website(s).", processed)
    log_detection_summary(state)
    logging.info("Email output: %s", OUTPUT_FILE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
