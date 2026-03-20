from __future__ import annotations

import asyncio
import csv
import json
import os
import re
from notion-client import Client
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import feedparser
import pandas as pd
from playwright.async_api import Page, async_playwright

# =========================================================
# CONFIG
# =========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_CSV = REPO_ROOT / "TLR_Catalog.csv"

# Set these in GitHub Actions or locally in your shell
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = "30c96882d77280278554cacf32360f88"
notion = Client(auth=NOTION_TOKEN)

CHANNEL_VIDEOS_URL = os.getenv("CHANNEL_VIDEOS_URL", "").strip()
CHANNEL_SHORTS_URL = os.getenv("CHANNEL_SHORTS_URL", "").strip()
PODBEAN_FEED_URL = os.getenv("PODBEAN_FEED_URL", "").strip()

WATCH_RE = re.compile(r"^https?://(www\.)?youtube\.com/watch\?v=([A-Za-z0-9_-]{11})")
SHORTS_RE = re.compile(r"^https?://(www\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})")


# =========================
# NOTION FUNCTIONS
# =========================

def get_existing_items():
    # read database

def add_new_item():
    # insert row

def update_existing_item():
    # optional later


# =========================================================
# YOUTUBE HELPERS
# =========================================================

def extract_video_id(href: str) -> Optional[str]:
    if not href:
        return None
    m = WATCH_RE.match(href)
    if m:
        return m.group(2)
    m = SHORTS_RE.match(href)
    if m:
        return m.group(2)
    return None


def build_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def iso_to_yyyymmdd(iso_str: str) -> str:
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", iso_str):
            return iso_str
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return iso_str


def yyyymmdd_to_pretty(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%B %-d, %Y")
    except Exception:
        try:
            # Windows-safe fallback if %-d fails elsewhere
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.strftime("%B %d, %Y").replace(" 0", " ")
        except Exception:
            return date_str


def yyyymmdd_to_month(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%B %Y")
    except Exception:
        return ""


def seconds_to_hhmmss(seconds: Optional[int]) -> str:
    if seconds is None:
        return ""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def iso8601_duration_to_seconds(d: str) -> Optional[int]:
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", d or "")
    if not m:
        return None
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s


async def auto_scroll(page: Page, max_idle_rounds: int = 6):
    idle = 0
    last_height = 0
    while idle < max_idle_rounds:
        height = await page.evaluate("document.documentElement.scrollHeight")
        if height == last_height:
            idle += 1
        else:
            idle = 0
            last_height = height

        await page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
        await page.wait_for_timeout(1200)


async def collect_video_ids(page: Page, channel_tab_url: str) -> List[str]:
    await page.goto(channel_tab_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)
    await auto_scroll(page)

    hrefs = await page.eval_on_selector_all("a[href]", "(els) => els.map(a => a.href)")

    ids: Set[str] = set()
    for href in hrefs:
        vid = extract_video_id(href)
        if vid:
            ids.add(vid)

    return sorted(ids)


async def parse_jsonld(page: Page) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    scripts = await page.eval_on_selector_all(
        'script[type="application/ld+json"]',
        "(els) => els.map(e => e.textContent)"
    )

    for txt in scripts:
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue

            if obj.get("@type") == "VideoObject":
                title = obj.get("name")
                upload = obj.get("uploadDate") or obj.get("datePublished")
                dur = obj.get("duration")
                dur_sec = iso8601_duration_to_seconds(dur) if isinstance(dur, str) else None

                thumb = obj.get("thumbnailUrl")
                if isinstance(thumb, list) and thumb:
                    thumb = thumb[0]
                elif not isinstance(thumb, str):
                    thumb = None

                return title, upload, dur_sec, thumb

    return None, None, None, None


async def parse_player_response(page: Page) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    try:
        pr = await page.evaluate("window.ytInitialPlayerResponse")
        if not pr:
            return None, None, None, None

        video_details = pr.get("videoDetails") or {}
        micro = (pr.get("microformat") or {}).get("playerMicroformatRenderer") or {}

        title = video_details.get("title")
        upload = micro.get("uploadDate") or micro.get("publishDate")

        dur_sec = None
        ls = video_details.get("lengthSeconds")
        if ls:
            try:
                dur_sec = int(ls)
            except Exception:
                dur_sec = None

        thumb = None
        thumbs = (video_details.get("thumbnail") or {}).get("thumbnails") or []
        if thumbs:
            thumb = thumbs[-1].get("url")

        return title, upload, dur_sec, thumb
    except Exception:
        return None, None, None, None


async def fetch_video_metadata(page: Page, video_id: str) -> Dict[str, str]:
    url = build_watch_url(video_id)
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(1200)

    title, upload, dur_sec, thumb = await parse_jsonld(page)

    if not (title and upload and dur_sec is not None and thumb):
        t2, u2, d2, th2 = await parse_player_response(page)
        title = title or t2
        upload = upload or u2
        dur_sec = dur_sec if dur_sec is not None else d2
        thumb = thumb or th2

    publish_date = iso_to_yyyymmdd(upload) if upload else ""
    duration = seconds_to_hhmmss(dur_sec)

    return {
        "video_id": video_id,
        "title": title or "",
        "publish_date": publish_date,
        "duration": duration,
        "youtube_url": url,
        "thumbnail_url": thumb or "",
    }


async def scrape_youtube(channel_tab_url: str, format_name: str, media_type: str) -> List[Dict[str, str]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        video_ids = await collect_video_ids(page, channel_tab_url)
        results: List[Dict[str, str]] = []

        for vid in video_ids:
            meta = await fetch_video_metadata(page, vid)

            row = {
                "Title": meta["title"],
                "Publish Date": yyyymmdd_to_pretty(meta["publish_date"]),
                "Format": format_name,
                "Media Type": media_type,
                "Series": "",
                "Pillar": "",
                "Month": yyyymmdd_to_month(meta["publish_date"]),
                "Episode Number": "",
                "Episode Part": "",
                "Primary Link": meta["youtube_url"],
                "YouTube URL": meta["youtube_url"],
                "Duration": meta["duration"],
                "Podbean Link": "",
                "Podbean Embeddable Link": "",
                "Video ID": meta["video_id"],
                "Summary": "",
                "Featured": "No",
                "_sort_date": meta["publish_date"],
            }
            results.append(row)

        await browser.close()

    return results


# =========================================================
# PODBEAN HELPERS
# =========================================================

def get_enclosure_url(entry) -> str:
    if getattr(entry, "enclosures", None):
        for enc in entry.enclosures:
            href = enc.get("href")
            if href:
                return href
    return ""


def podbean_duration(entry) -> str:
    # iTunes duration may appear as seconds or HH:MM:SS
    dur = entry.get("itunes_duration", "") or entry.get("duration", "")
    if not dur:
        return ""
    dur = str(dur).strip()
    if dur.isdigit():
        sec = int(dur)
        return seconds_to_hhmmss(sec)
    return dur


def fetch_podbean_audio() -> List[Dict[str, str]]:
    if not PODBEAN_FEED_URL:
        return []

    feed = feedparser.parse(PODBEAN_FEED_URL)
    results: List[Dict[str, str]] = []

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        audio_file = get_enclosure_url(entry).strip()

        published = ""
        if entry.get("published_parsed"):
            published = datetime(*entry.published_parsed[:6]).strftime("%Y-%m-%d")
        elif entry.get("updated_parsed"):
            published = datetime(*entry.updated_parsed[:6]).strftime("%Y-%m-%d")

        row = {
            "Title": title,
            "Publish Date": yyyymmdd_to_pretty(published),
            "Format": "Podcast Audio (Podbean)",
            "Media Type": "Longform",
            "Series": "",
            "Pillar": "",
            "Month": yyyymmdd_to_month(published),
            "Episode Number": "",
            "Episode Part": "",
            "Primary Link": link,
            "YouTube URL": "",
            "Duration": podbean_duration(entry),
            "Podbean Link": link,
            "Podbean Embeddable Link": audio_file,   # your site uses this as the HTML5 audio source
            "Video ID": "",
            "Summary": "",
            "Featured": "No",
            "_sort_date": published,
        }
        results.append(row)

    return results


# =========================================================
# PRESERVE MANUAL FIELDS
# =========================================================

MANUAL_FIELDS = [
    "Series",
    "Pillar",
    "Episode Number",
    "Episode Part",
    "Summary",
    "Featured",
]

OUTPUT_COLUMNS = [
    "Title",
    "Publish Date",
    "Format",
    "Media Type",
    "Series",
    "Pillar",
    "Month",
    "Episode Number",
    "Episode Part",
    "Primary Link",
    "YouTube URL",
    "Duration",
    "Podbean Link",
    "Podbean Embeddable Link",
    "Video ID",
    "Summary",
    "Featured",
]


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def existing_lookup_key(row: Dict[str, str]) -> str:
    fmt = row.get("Format", "")
    if fmt.startswith("Podcast Audio"):
        # Podbean row: prefer podbean link, then publish date
        return f"{fmt}|{row.get('Podbean Link', '').strip()}|{row.get('Publish Date', '').strip()}"
    else:
        # YouTube row: prefer stable video id, then youtube url
        return f"{fmt}|{row.get('Video ID', '').strip()}|{row.get('YouTube URL', '').strip()}"


def new_lookup_key(row: Dict[str, str]) -> str:
    fmt = row.get("Format", "")
    if fmt.startswith("Podcast Audio"):
        return f"{fmt}|{row.get('Podbean Link', '').strip()}|{row.get('Publish Date', '').strip()}"
    else:
        return f"{fmt}|{row.get('Video ID', '').strip()}|{row.get('YouTube URL', '').strip()}"


def preserve_manual_fields(new_rows: List[Dict[str, str]], existing_csv: Path) -> List[Dict[str, str]]:
    if not existing_csv.exists():
        return new_rows

    existing_df = pd.read_csv(existing_csv, dtype=str).fillna("")
    existing_records = existing_df.to_dict(orient="records")

    lookup: Dict[str, Dict[str, str]] = {}
    for row in existing_records:
        lookup[existing_lookup_key(row)] = row

    merged: List[Dict[str, str]] = []
    for row in new_rows:
        old = lookup.get(new_lookup_key(row))
        if old:
            for field in MANUAL_FIELDS:
                row[field] = old.get(field, row.get(field, ""))
        merged.append(row)

    return merged


# =========================================================
# DEFAULTS / LIGHT RULES
# =========================================================

def apply_default_series(row: Dict[str, str]) -> None:
    if row["Series"]:
        return

    fmt = row.get("Format", "")
    title = row.get("Title", "")

    if fmt == "YouTube Short":
        # leave blank if you prefer to classify manually
        row["Series"] = row["Series"] or ""
    elif fmt in {"Podcast Video (Youtube)", "Podcast Video (YouTube)", "Podcast Audio (Podbean)", "Podcast Clip"}:
        row["Series"] = "The Liberating River"
    elif fmt == "Special Video":
        row["Series"] = "Reflections"


def finalize_formats(row: Dict[str, str]) -> None:
    # normalize naming
    if row.get("Format") == "Podcast Video (Youtube)":
        row["Format"] = "Podcast Video (YouTube)"

    fmt = row.get("Format", "")
    if fmt == "YouTube Short":
        row["Media Type"] = "Shortform"
    elif fmt == "Podcast Clip":
        row["Media Type"] = "Clip"
    else:
        row["Media Type"] = "Longform"


# =========================================================
# MAIN
# =========================================================

async def build_catalog() -> List[Dict[str, str]]:
    if not CHANNEL_VIDEOS_URL:
        raise ValueError("CHANNEL_VIDEOS_URL is not set.")
    if not CHANNEL_SHORTS_URL:
        raise ValueError("CHANNEL_SHORTS_URL is not set.")
    if not PODBEAN_FEED_URL:
        raise ValueError("PODBEAN_FEED_URL is not set.")

    youtube_videos = await scrape_youtube(
        channel_tab_url=CHANNEL_VIDEOS_URL,
        format_name="Podcast Video (YouTube)",
        media_type="Longform",
    )

    youtube_shorts = await scrape_youtube(
        channel_tab_url=CHANNEL_SHORTS_URL,
        format_name="YouTube Short",
        media_type="Shortform",
    )

    podbean_audio = fetch_podbean_audio()

    all_rows = youtube_videos + youtube_shorts + podbean_audio

    # Normalize / defaults
    for row in all_rows:
        finalize_formats(row)
        apply_default_series(row)

    # Preserve your Notion/manual enrichment fields from existing CSV
    all_rows = preserve_manual_fields(all_rows, OUTPUT_CSV)

    # Sort newest first, then title
    all_rows.sort(
        key=lambda r: (
            r.get("_sort_date", ""),
            r.get("Title", "").lower(),
        ),
        reverse=True,
    )

    return all_rows


def write_catalog(rows: List[Dict[str, str]], output_csv: Path) -> None:
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            clean_row = {col: row.get(col, "") for col in OUTPUT_COLUMNS}
            writer.writerow(clean_row)


def main() -> None:
    rows = asyncio.run(build_catalog())
    write_catalog(rows, OUTPUT_CSV)
    print(f"Updated catalog: {OUTPUT_CSV}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
