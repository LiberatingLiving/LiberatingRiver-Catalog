from __future__ import annotations

import asyncio
import csv
import json
import os

import re, time

from notion_client import Client

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, cast

import feedparser
import pandas as pd
from playwright.async_api import Page, async_playwright

# =========================================================
# CONFIG
# =========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_CSV = REPO_ROOT / "TLR_Catalog.csv"

# Set these in GitHub Actions or locally in your shell
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
DATABASE_ID = "30c96882d77280278554cacf32360f88"
notion = Client(auth=NOTION_TOKEN) if NOTION_TOKEN else None

CHANNEL_VIDEOS_URL = os.getenv("CHANNEL_VIDEOS_URL", "").strip()
CHANNEL_SHORTS_URL = os.getenv("CHANNEL_SHORTS_URL", "").strip()
PODBEAN_FEED_URL = os.getenv("PODBEAN_FEED_URL", "").strip()

WATCH_RE = re.compile(r"^https?://(www\.)?youtube\.com/watch\?v=([A-Za-z0-9_-]{11})")
SHORTS_RE = re.compile(r"^https?://(www\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})")


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
        title = str(entry.get("title") or "").strip()
        link = str(entry.get("link") or "").strip()
        audio_file = str(get_enclosure_url(entry) or "").strip()

        # published = ""
        # if entry.get("published_parsed"):
        #     published = datetime(*entry.published_parsed[:6]).strftime("%Y-%m-%d")
        # elif entry.get("updated_parsed"):
        #     published = datetime(*entry.updated_parsed[:6]).strftime("%Y-%m-%d")


        published = ""

        if entry.get("published_parsed"):
            pp = cast(time.struct_time, entry.published_parsed)
            published = datetime(
                pp.tm_year,
                pp.tm_mon,
                pp.tm_mday,
                pp.tm_hour,
                pp.tm_min,
                pp.tm_sec
            ).strftime("%Y-%m-%d")

        elif entry.get("updated_parsed"):
            up = cast(time.struct_time, entry.updated_parsed)
            published = datetime(
                up.tm_year,
                up.tm_mon,
                up.tm_mday,
                up.tm_hour,
                up.tm_min,
                up.tm_sec
            ).strftime("%Y-%m-%d")

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
# NOTION FUNCTIONS
# =========================================================

def notion_ready() -> bool:
    return bool(NOTION_TOKEN and DATABASE_ID and notion)


def get_notion_client() -> Client:
    if notion is None:
        raise ValueError("Notion client is not configured.")
    return cast(Client, notion)


def pretty_to_iso(date_str: str) -> str:
    """
    Convert 'February 18, 2026' -> '2026-02-18'
    """
    if not date_str:
        return ""
    try:
        return datetime.strptime(date_str.strip(), "%B %d, %Y").strftime("%Y-%m-%d")
    except Exception:
        try:
            return datetime.strptime(date_str.strip(), "%B %-d, %Y").strftime("%Y-%m-%d")
        except Exception:
            return date_str

def safe_select(value: str) -> Optional[Dict]:
    value = (value or "").strip()
    if not value:
        return None
    return {"name": value}

def safe_multi_select(value: str) -> List[Dict]:
    """
    Convert a single value or comma-separated string into Notion multi_select format.
    """
    value = (value or "").strip()
    if not value:
        return []

    parts = [v.strip() for v in value.split(",") if v.strip()]
    return [{"name": part} for part in parts]

def safe_rich_text(value: str) -> List[Dict]:
    """
    Build Notion rich_text safely.
    """
    value = (value or "").strip()
    if not value:
        return []
    return [
        {
            "type": "text",
            "text": {
                "content": value[:2000]  # Notion text block safety
            }
        }
    ]

def safe_title(value: str) -> List[Dict]:
    """
    Build Notion title property safely.
    """
    value = (value or "").strip()
    if not value:
        value = "Untitled"
    return [
        {
            "type": "text",
            "text": {
                "content": value[:2000]
            }
        }
    ]

def query_all_notion_rows(database_id: str) -> List[Dict]:

    if not notion_ready():
        raise ValueError("Notion is not configured. Check NOTION_TOKEN and DATABASE_ID.")

    client = get_notion_client()
    
    results = []
    has_more = True
    next_cursor = None

    while has_more:

        if next_cursor:
            response = client.databases.query(
                database_id=database_id,
                start_cursor=next_cursor
            )
        else:
            response = client.databases.query(
                database_id=database_id
            )

        response = cast(dict, response)
        
        results.extend(response.get("results", []))
        has_more = response.get("has_more", False)
        next_cursor = response.get("next_cursor")

    return results

def get_existing_titles() -> Set[str]:
    """
    Return a set of existing Notion titles.
    """
    rows = query_all_notion_rows(DATABASE_ID)
    titles = set()

    for row in rows:
        title_property = row["properties"].get("Title", {}).get("title", [])
        if title_property:
            # Concatenate title fragments safely
            title_text = "".join(
                part.get("plain_text", "")
                for part in title_property
            ).strip()
            if title_text:
                titles.add(title_text)

    return titles

def get_existing_video_ids() -> Set[str]:
    """
    Return a set of existing Video IDs already stored in Notion.
    """
    rows = query_all_notion_rows(DATABASE_ID)
    video_ids = set()

    for row in rows:
        prop = row["properties"].get("Video ID", {})
        rich_text = prop.get("rich_text", [])
        if rich_text:
            video_id = "".join(
                part.get("plain_text", "")
                for part in rich_text
            ).strip()
            if video_id:
                video_ids.add(video_id)

    return video_ids

def build_notion_properties(row: Dict[str, str]) -> Dict:
    """
    Convert one catalog row into a Notion properties payload.
    This version mataches the actual Notion property types.
    Assumes your database property names match your catalog columns.
    """
    props = {
        "Title": {
            "title": safe_title(row.get("Title", ""))
        },

        "Publish Date": {
            "date": {"start": pretty_to_iso(row.get("Publish Date", ""))} if row.get("Publish Date") else None
        },

        "Format": {
            "select": safe_select(row.get("Format", ""))
        },

        # Media Type is a FORMULA in your database
        # Do not write to it

        "Series": {
            "select": safe_select(row.get("Series", ""))
        },

        "Pillar": {
            "select": safe_select(row.get("Pillar", ""))
        },

        # Month is likely a formula in your database
        # Do not write to it

        "Episode Number": {
            "number": int(float(row["Episode Number"])) if str(row.get("Episode Number", "")).strip() else None
        },

        # Episode Part is MULTI-SELECT
        "Episode Part": {
            "multi_select": safe_multi_select(row.get("Episode Part", ""))
        },

        # Primary Link is a FORMULA in your database
        # Do not write to it

        "YouTube URL": {
            "url": row.get("YouTube URL", "") or None
        },

        "Duration": {
            "rich_text": safe_rich_text(row.get("Duration", ""))
        },

        "Podbean Link": {
            "url": row.get("Podbean Link", "") or None
        },

        "Podbean Embeddable Link": {
            "url": row.get("Podbean Embeddable Link", "") or None
        },

        "Video ID": {
            "rich_text": safe_rich_text(row.get("Video ID", ""))
        },

        "Summary": {
            "rich_text": safe_rich_text(row.get("Summary", ""))
        },

        # Featured is CHECKBOX
        "Featured": {
            "checkbox": str(row.get("Featured", "")).strip().lower() in {"yes", "true", "1"}
        }
    }

    # Remove properties that ended up as None in invalid spots
    clean_props = {}
    for key, value in props.items():
        if isinstance(value, dict):
            # Keep date/url/number/select/title/rich_text structures intact
            clean_props[key] = value

    return clean_props

def add_row_to_notion(row: Dict[str, str]) -> None:
    """
    Insert a new row into the Notion database.
    """
    if not notion_ready():
        raise ValueError("Notion is not configured. Check NOTION_TOKEN and DATABASE_ID.")
    
    client = get_notion_client()
    properties = build_notion_properties(row)

    client.pages.create(

        parent={"database_id": DATABASE_ID},
        properties=properties
    )


def sync_new_rows_to_notion(rows: List[Dict[str, str]]) -> int:
    """
    Add only new rows to Notion.
    Uses Video ID first when available, then falls back to Title.
    Returns count of rows inserted.
    """
    if not notion_ready():
        raise ValueError("Notion is not configured. Check NOTION_TOKEN and DATABASE_ID.")

    existing_titles = get_existing_titles()
    existing_video_ids = get_existing_video_ids()

    inserted = 0

    for row in rows:
        title = (row.get("Title", "") or "").strip()
        video_id = (row.get("Video ID", "") or "").strip()

        # Prefer video ID when available
        if video_id and video_id in existing_video_ids:
            continue

        # Fallback to title if no video ID
        if not video_id and title in existing_titles:
            continue

        add_row_to_notion(row)
        inserted += 1

        if video_id:
            existing_video_ids.add(video_id)
        if title:
            existing_titles.add(title)

    return inserted

def test_single_notion_insert():
    test_row = {
        "Title": "TEST - Notion Sync Check",
        "Publish Date": "March 20, 2026",
        "Format": "Special Video",
        "Series": "Reflections",
        "Pillar": "General",
        "Episode Number": "",
        "Episode Part": "",
        "YouTube URL": "",
        "Duration": "00:30",
        "Podbean Link": "",
        "Podbean Embeddable Link": "",
        "Video ID": "TEST-123",
        "Summary": "Test row from automation.",
        "Featured": "No"
    }

    add_row_to_notion(test_row)
    print("Single test row inserted.")


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
        lookup[existing_lookup_key(row)] = row # type: ignore

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


# def main() -> None:
#     rows = asyncio.run(build_catalog())

#     print("NOTION_TOKEN loaded:", bool(NOTION_TOKEN))
#     print("DATABASE_ID:", DATABASE_ID)
#     print("notion_ready():", notion_ready())

#     try:
#         if notion_ready():
#             inserted_count = sync_new_rows_to_notion(rows)
#             print(f"Inserted {inserted_count} new row(s) into Notion.")
#         else:
#             print("Notion not configured; skipping Notion sync.")

#     except Exception as e:
#         print("NOTION SYNC ERROR:", repr(e))
    
#     write_catalog(rows, OUTPUT_CSV)
#     print(f"Updated catalog: {OUTPUT_CSV}")
#     print(f"Rows written: {len(rows)}")

def main() -> None:
    print("NOTION_TOKEN loaded:", bool(NOTION_TOKEN))
    print("DATABASE_ID:", DATABASE_ID)
    print("notion_ready():", notion_ready())

    test_single_notion_insert()

if __name__ == "__main__":
    main()
    
