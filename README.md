# HVAC Lead Email Pipeline

A two-stage pipeline for collecting **public business contact emails** for HVAC
outreach. The design goal is to *never trip* spam/bot/CAPTCHA defenses in the
first place — by pulling from authorized sources and behaving well — rather than
trying to defeat those defenses after the fact.

## Why there is no stealth/fingerprint/CAPTCHA-bypass layer

Those techniques don't *prevent* detection; they *escalate* it. A spoofed
browser fingerprint or a CAPTCHA solver is what turns a one-time block into a
burned domain/IP and a poisoned sender reputation — which is the thing that
actually kills a cold-outreach pipeline. The reliably block-resistant strategy
is the opposite, and it's what this project does:

1. **Get most data from an authorized API.** The Google Places API (Stage 1) is
   keyed, quota'd, and never serves a CAPTCHA. It's the highest-yield, lowest-
   risk source — so we pull as much as possible here first.
2. **Crawl only what's freely offered.** Stage 2 identifies itself honestly,
   obeys `robots.txt`, paces requests, and *stops cleanly* the instant a site
   signals it doesn't want automated traffic. Skipping the ~20-30% of sites that
   challenge you is far more productive than fighting them.
3. **Keep the data clean.** Optional MX validation drops undeliverable domains
   so your send list stays healthy and your sender reputation stays intact.

## Stage 1 — `email_scraper.py`

Collects business listings (name, website, phone, address) from Google Places.

- **Pagination:** up to `MAX_PAGES_PER_CITY` pages (~40 businesses) per city.
- **Batch cities:** enter several at the prompt (`Fresno, CA; Reno, NV`) or
  leave it blank to read `cities.txt` (one `City, ST` per line).
- **Quota accounting:** every page counts against local daily/monthly limits.

Requires `GOOGLE_PLACES_API_KEY` in a `.env` file next to the script.
Output: `places.tsv`.

## Stage 2 — `website_crawler.py`

Politely crawls each company's homepage plus up to three contact/about pages.

- **Extraction:** `mailto:` links, **JSON-LD / schema.org** structured data
  (often the only place an email is published), and visible text — with the
  same false-positive filtering as before (telemetry DSNs, image files, etc.).
- **Pacing:** randomized 2.0–4.5s same-site delay; conservative retries.
- **Back-off, not bypass:** challenge pages / 401 / 403 / 429 are recorded and
  skipped. Resumable via `crawl_state.json`.
- **MX validation (optional):** install `dnspython` to drop dead domains; the
  crawler runs fine without it.

Input: `places.tsv`. Output: `emails.tsv`.

## Setup

```bash
pip install -r requirements.txt
# create .env with:  GOOGLE_PLACES_API_KEY=your_key_here
python email_scraper.py      # Stage 1
python website_crawler.py    # Stage 2
```

## Getting the most leads (without fighting defenses)

- **Widen Stage 1**, not Stage 2. More cities and pages via the API yields far
  more contactable businesses than pushing on any single blocked site.
- Consider other **authorized** sources for the addresses the API/site don't
  give you: the business's Google Business Profile, licensed business-directory
  APIs, or state HVAC-contractor license registries (often public record).
- Respect each site's terms and applicable anti-spam law (e.g. CAN-SPAM) when
  you actually send. A smaller, clean, permission-respecting list outperforms a
  large harvested one.
