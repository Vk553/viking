import os
import math
import time
import threading
import re
import json
import logging
import urllib.parse
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, status, Query, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, field_validator, model_validator, Field
from curl_cffi import requests

from indexnow import submit_urls_to_indexnow

# Load environment variables
load_dotenv()
# ==========================================
# 1. الإعدادات العامة والثوابت
# ==========================================
SECRET_TOKEN = os.getenv("VK_API_SECRET_TOKEN", "VK_SUPER_SECRET_2026")
INDEXNOW_KEY = os.getenv("INDEXNOW_KEY", "default_indexnow_key_replace_in_production")
security_scheme = HTTPBearer()
SITE_NAME = "Viking"

SUPPORTED_CONSOLES = ['ps1', 'ps2', 'ps3', 'ps4', 'ps5', 'pc', 'xbox', 'psp']

RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60
request_tracker = defaultdict(list)


def cleanup_old_requests(ip: str):
    current_time = time.time()
    request_tracker[ip] = [
        ts for ts in request_tracker[ip]
        if current_time - ts < RATE_LIMIT_WINDOW
    ]


def check_rate_limit(ip: str) -> bool:
    cleanup_old_requests(ip)
    if len(request_tracker[ip]) >= RATE_LIMIT_REQUESTS:
        return False
    request_tracker[ip].append(time.time())
    return True


# ==========================================
# Simple in-memory TTL cache (single-instance, thread-safe)
# Reduces redundant Neon compute by serving repeated reads from memory.
# ==========================================
_cache_store = {}
_cache_lock = threading.Lock()

def cache_get(key: str):
    """Return cached value if present and not expired, else None."""
    with _cache_lock:
        entry = _cache_store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if time.time() > expires_at:
            del _cache_store[key]
            return None
        return value

def cache_set(key: str, value, ttl_seconds: int):
    """Store a value in the cache with a TTL in seconds."""
    with _cache_lock:
        _cache_store[key] = (value, time.time() + ttl_seconds)


# ==========================================
# 2. إدارة الاتصال وقاعدة البيانات
# ==========================================
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


# ==========================================
# PostgreSQL Advisory Lock Helpers (Optimized)
# ==========================================
def acquire_advisory_lock(conn, sub_id: int) -> bool:
    """
    Acquire a PostgreSQL session-level advisory lock for the given anker_sub_id.
    Blocks until lock is acquired. Returns True if successful, False on error.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (sub_id,))
        return True
    except Exception as e:
        logging.error(f"Failed to acquire advisory lock for sub_id {sub_id}: {e}")
        return False


def release_advisory_lock(conn, sub_id: int) -> bool:
    """
    Release a PostgreSQL session-level advisory lock for the given anker_sub_id.
    Returns True if lock was actually released, False otherwise.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (sub_id,))
            result = cursor.fetchone()
            # PostgreSQL returns a tuple e.g. (True,) or (False,)
            released = result[0] if result else False

            if not released:
                logging.warning(f"Advisory lock for sub_id {sub_id} was not held by this session.")
            return released
    except Exception as e:
        logging.error(f"Failed to release advisory lock for sub_id {sub_id}: {e}")
        return False


# ==========================================
# AnkerGames Resolver Service
# ==========================================
async def resolve_pc_link(sub_id: int) -> Optional[str]:
    """
    Asynchronously resolve the AnkerGames download URL for a given sub_id.
    
    Steps:
    1. GET recent-updates page to extract CSRF token
    2. POST to generate-download-url endpoint with CSRF token
    3. Parse response to get download_url
    4. GET download_url and extract encrypted link using regex
    5. Decrypt URL using urllib.parse.unquote
    
    Returns: The resolved tunnel5 URL or None if resolution fails.
    """
    max_retries = 3
    base_delay = 1  # seconds
    
    print(f"[SERVER RESOLVER] Starting resolution for sub_id {sub_id}")
    
    for attempt in range(max_retries):
        try:
            print(f"[SERVER RESOLVER] Attempt {attempt + 1}/{max_retries} for sub_id {sub_id}")
            async with requests.AsyncSession(impersonate="chrome120") as session:
                # Step 1: Extract CSRF token
                timestamp = int(time.time())
                csrf_url = f"https://ankergames.net/recent-updates?_t={timestamp}"
                
                print(f"[SERVER RESOLVER] Fetching CSRF token from {csrf_url}")
                csrf_response = await session.get(csrf_url, timeout=12)
                csrf_response.raise_for_status()
                
                # Extract CSRF token from meta tag
                csrf_match = re.search(r'<meta name="csrf-token" content="([^"]+)"', csrf_response.text)
                if not csrf_match:
                    logging.error(f"Failed to extract CSRF token for sub_id {sub_id}")
                    print(f"[SERVER RESOLVER] Failed to extract CSRF token for sub_id {sub_id}")
                    return None
                
                csrf_token = csrf_match.group(1)
                print(f"[SERVER RESOLVER] CSRF token extracted successfully")
                
                # Step 2: Generate download URL
                generate_url = f"https://ankergames.net/generate-download-url/{sub_id}"
                headers = {
                    "X-CSRF-TOKEN": csrf_token,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/json",
                    "Referer": "https://ankergames.net/",
                    "Origin": "https://ankergames.net"
                }
                payload = {"g-recaptcha-response": "development-mode"}
                
                print(f"[SERVER RESOLVER] Generating download URL at {generate_url}")
                generate_response = await session.post(
                    generate_url,
                    headers=headers,
                    json=payload,
                    timeout=12
                )
                generate_response.raise_for_status()
                
                generate_data = generate_response.json()
                download_url = generate_data.get("download_url")
                
                if not download_url:
                    logging.error(f"No download_url in response for sub_id {sub_id}")
                    print(f"[SERVER RESOLVER] No download_url in response for sub_id {sub_id}")
                    return None
                
                print(f"[SERVER RESOLVER] Generated download URL: {download_url}")
                
                # Step 3: Extract encrypted link from download page
                print(f"[SERVER RESOLVER] Fetching download page to extract encrypted link")
                download_response = await session.get(download_url, timeout=12)
                download_response.raise_for_status()
                
                # Extract encrypted link using regex
                encrypted_match = re.search(r"downloadPage\('([^']+)'", download_response.text)
                if not encrypted_match:
                    logging.error(f"Failed to extract encrypted link for sub_id {sub_id}")
                    print(f"[SERVER RESOLVER] Failed to extract encrypted link for sub_id {sub_id}")
                    return None
                
                encrypted_url = encrypted_match.group(1)
                print(f"[SERVER RESOLVER] Encrypted URL extracted: {encrypted_url[:50]}...")
                
                # Step 4: Decrypt URL
                final_url = urllib.parse.unquote(encrypted_url)
                
                logging.info(f"Successfully resolved link for sub_id {sub_id}")
                print(f"[SERVER RESOLVER] Successfully resolved sub_id {sub_id} -> {final_url}")
                return final_url
                
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for sub_id {sub_id}: {e}")
            print(f"[SERVER RESOLVER] Attempt {attempt + 1}/{max_retries} failed for sub_id {sub_id}: {e}")
            if attempt < max_retries - 1:
                # Exponential backoff
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
            else:
                logging.error(f"All retries exhausted for sub_id {sub_id}")
                print(f"[SERVER RESOLVER] All retries exhausted for sub_id {sub_id}")
                return None
        except Exception as e:
            logging.error(f"Unexpected error resolving link for sub_id {sub_id}: {e}")
            print(f"[SERVER RESOLVER] Unexpected error resolving link for sub_id {sub_id}: {e}")
            return None
    
    print(f"[SERVER RESOLVER] Resolution failed for sub_id {sub_id} after all attempts")
    return None


# ==========================================
# Game Link Refresh Logic with Double-Checked Locking
# ==========================================
async def get_or_refresh_game_link(conn, link_id: int) -> Optional[str]:
    """
    Core lazy-refresh logic for game links with double-checked locking pattern.
    
    Args:
        conn: Database connection
        link_id: ID of the game_link record
        
    Returns:
        The cached URL (either existing or freshly resolved), or None if resolution fails
    """
    cursor = conn.cursor()
    try:
        # Step 1: Query game_links table by link_id
        cursor.execute("""
            SELECT id, game_id, label, anker_sub_id, cached_url, 
                   resolver_type, expires_at
            FROM game_links 
            WHERE id = %s
        """, (link_id,))
        
        link = cursor.fetchone()
        
        if not link:
            logging.error(f"Game link {link_id} not found")
            return None
        
        # Step 2: If static link or no anker_sub_id, return cached_url instantly
        if link['resolver_type'] == 'static' or link['anker_sub_id'] is None:
            return link['cached_url']
        
        # Step 3: Dynamic link - check expiration
        anker_sub_id = link['anker_sub_id']
        
        # Check if cached_url is valid and not expired (14-hour TTL)
        if link['cached_url'] and link['expires_at']:
            cursor.execute("SELECT NOW()")
            current_time = cursor.fetchone()['now']
            if link['expires_at'] > current_time:
                return link['cached_url']
        
        # Step 4: Link is expired or missing - acquire advisory lock
        acquire_advisory_lock(conn, anker_sub_id)
        
        try:
            # Step 5: Double-checked locking - re-query after acquiring lock
            cursor.execute("""
                SELECT cached_url, expires_at
                FROM game_links 
                WHERE id = %s
            """, (link_id,))
            
            refreshed_link = cursor.fetchone()
            
            # If another request refreshed it while we were waiting, return the fresh URL
            if refreshed_link and refreshed_link['cached_url'] and refreshed_link['expires_at']:
                cursor.execute("SELECT NOW()")
                current_time = cursor.fetchone()['now']
                if refreshed_link['expires_at'] > current_time:
                    logging.info(f"Link {link_id} was refreshed by another request")
                    return refreshed_link['cached_url']
            
            # Step 6: Resolve the link using AnkerGames resolver
            print(f"[SERVER RESOLVER] Starting resolution for link_id {link_id}, sub_id {anker_sub_id}")
            resolved_url = await resolve_pc_link(anker_sub_id)
            print(f"[SERVER RESOLVER] Sub ID: {anker_sub_id} -> Generated URL: {resolved_url}")
            
            if resolved_url:
                # Step 7: Update game_links with new URL and expiration
                cursor.execute("""
                    UPDATE game_links 
                    SET cached_url = %s,
                        expires_at = NOW() + INTERVAL '14 hours',
                        updated_at = NOW()
                    WHERE id = %s
                """, (resolved_url, link_id))
                
                conn.commit()
                logging.info(f"Successfully refreshed link {link_id} for sub_id {anker_sub_id}")
                print(f"[SERVER RESOLVER] Successfully updated link {link_id} in database")
                return resolved_url
            else:
                logging.error(f"Failed to resolve link {link_id} for sub_id {anker_sub_id}")
                print(f"[SERVER RESOLVER] Failed to resolve link {link_id} for sub_id {anker_sub_id}")
                # Return old cached URL even if expired as fallback
                return link['cached_url'] if link['cached_url'] else None
                
        finally:
            # Step 8: Always release the advisory lock
            release_advisory_lock(conn, anker_sub_id)
            
    except Exception as e:
        logging.error(f"Error in get_or_refresh_game_link for link_id {link_id}: {e}")
        print(f"[SERVER RESOLVER] Exception in get_or_refresh_game_link for link_id {link_id}: {e}")
        return None
    finally:
        cursor.close()


def slugify(title: str, console: str) -> str:
    """
    Generate a URL-friendly slug from title and console.
    Falls back to '{console}-game' if title has no transliterable characters.
    """
    # Transliterate Arabic and other non-ASCII characters to ASCII approximations
    # Basic Arabic to Latin transliteration map
    arabic_map = {
        'ا': 'a', 'أ': 'a', 'إ': 'i', 'آ': 'aa', 'ب': 'b', 'ت': 't', 'ث': 'th',
        'ج': 'j', 'ح': 'h', 'خ': 'kh', 'د': 'd', 'ذ': 'dh', 'ر': 'r', 'ز': 'z',
        'س': 's', 'ش': 'sh', 'ص': 's', 'ض': 'd', 'ط': 't', 'ظ': 'z', 'ع': 'a',
        'غ': 'gh', 'ف': 'f', 'ق': 'q', 'ك': 'k', 'ل': 'l', 'م': 'm', 'ن': 'n',
        'ه': 'h', 'و': 'w', 'ي': 'y', 'ى': 'a', 'ة': 'a', 'ء': ''
    }

    # Apply Arabic transliteration
    transliterated = ''
    for char in title:
        transliterated += arabic_map.get(char, char)

    # Convert to lowercase
    slug = transliterated.lower()

    # Replace non-alphanumeric characters with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', slug)

    # Strip leading/trailing hyphens
    slug = slug.strip('-')

    # Collapse multiple hyphens
    slug = re.sub(r'-+', '-', slug)

    # If slug is empty (pure non-transliterable), fall back to console-based slug
    if not slug:
        slug = f"{console.lower()}-game"

    return slug


def get_base_url(request: Request) -> str:
    """
    Extracts the host from headers safely and forces HTTPS for production.
    Prevents HTTP canonical mismatches behind Render's reverse proxy.
    """
    host = request.headers.get("x-forwarded-host") or request.url.hostname
    port = request.url.port

    # Check for local development to avoid breaking local testing
    if host in ["localhost", "127.0.0.1"] or str(host).startswith("localhost:"):
        port_str = f":{port}" if port and port not in (80, 443) else ""
        return f"http://{host}{port_str}"

    return f"https://{host}"


def build_seo_meta(game: dict, base_url: str, path_prefix: str = "game") -> dict:
    """
    Generate SEO metadata for a game page.
    Returns dict with title, description, h1, and canonical_url.
    """
    # Title template
    title = f"{game['title']} {game['console'].upper()} PKG Download + Update + DLC | {SITE_NAME}"

    # Description: keyword-rich but natural single sentence
    if game.get('description') and game['description'].strip():
        description = game['description'].strip()
    else:
        description = f"Download {game['title']} for {game['console'].upper()}. Includes update, DLC, fast download links, screenshots, and installation guide."

    # H1 template
    h1 = f"{game['title']} {game['console'].upper()}  Download"

    # Canonical URL (full URL, not just path)
    canonical_url = f"{base_url}/{path_prefix}/{game['id']}-{game['slug']}"

    return {
        "title": title,
        "description": description,
        "h1": h1,
        "canonical_url": canonical_url
    }


def check_urls_not_recently_submitted(urls: list[str], cooldown_hours: int = 24) -> list[str]:
    """
    Returns the subset of `urls` that have NOT been successfully submitted to IndexNow 
    within the cooldown window. Read-only — does not write to indexnow_log.
    Never raises: on any DB error, returns the full input list unchanged so that a 
    dedup-check failure never suppresses a legitimate submission.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        urls_to_submit = []
        for url in urls:
            cursor.execute("""
                SELECT 1 FROM indexnow_log
                WHERE url = %s
                AND submitted_at > NOW() - (%s || ' hours')::interval
            """, (url, cooldown_hours))
            if cursor.fetchone() is None:
                urls_to_submit.append(url)
        return urls_to_submit
    except Exception as e:
        logging.getLogger("indexnow").warning(
            f"Cooldown check failed, proceeding without dedup filtering: {e}"
        )
        return urls  # fail-open: never block a legitimate submission due to a DB hiccup
    finally:
        if conn is not None:
            conn.close()


def record_indexnow_submission(urls: list[str]) -> None:
    """
    Records URLs as submitted in indexnow_log. Call this ONLY after 
    submit_urls_to_indexnow() has confirmed success. Never raises — any DB error here 
    is logged and swallowed, since this is a best-effort dedup optimization, not a 
    correctness requirement.
    """
    if not urls:
        return
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for url in urls:
            cursor.execute("""
                INSERT INTO indexnow_log (url, submitted_at)
                VALUES (%s, NOW())
                ON CONFLICT (url) DO UPDATE SET submitted_at = NOW()
            """, (url,))
        conn.commit()
    except Exception as e:
        logging.getLogger("indexnow").warning(f"Failed to record IndexNow log entry: {e}")
    finally:
        if conn is not None:
            conn.close()


async def submit_to_indexnow_safely(urls: list[str]) -> None:
    """
    Full IndexNow submission pipeline, designed to run inside a BackgroundTask.
    Performs the cooldown check, the actual HTTP submission, and success-logging 
    all in one place — none of this runs on the request thread. 
    Guaranteed never to raise.
    """
    try:
        filtered = check_urls_not_recently_submitted(urls)
        if not filtered:
            return
        result = await submit_urls_to_indexnow(filtered)
        if result.get("success"):
            record_indexnow_submission(filtered)
        # On failure: do nothing. The URLs remain NOT in indexnow_log (or their old 
        # timestamp stands), so they stay eligible for retry on the next create/update 
        # event or the next natural trigger. This satisfies "failed submissions remain 
        # eligible for future retries."
    except Exception as e:
        logging.getLogger("indexnow").error(f"Unexpected error in IndexNow pipeline: {e}")


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
            title TEXT,
            console TEXT,
            cover_image TEXT,
            description TEXT,
            size TEXT,
            version TEXT,
            youtube_link TEXT,
            game_link TEXT,
            game_link_original TEXT,
            update_link TEXT,
            update_link_original TEXT,
            dlc_link TEXT,
            dlc_link_original TEXT,
            is_arabic INTEGER DEFAULT 0,
            extra_1_label TEXT,
            extra_1_url TEXT,
            extra_1_url_original TEXT,
            extra_2_label TEXT,
            extra_2_url TEXT,
            extra_2_url_original TEXT,
            extra_3_label TEXT,
            extra_3_url TEXT,
            extra_3_url_original TEXT,
            extra_4_label TEXT,
            extra_4_url TEXT,
            extra_4_url_original TEXT,
            extra_5_label TEXT,
            extra_5_url TEXT,
            extra_5_url_original TEXT,
            region TEXT,
            game_code TEXT,
            password TEXT,
            slug TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    columns_to_add = {
        "is_arabic": "INTEGER DEFAULT 0",
        "extra_1_label": "TEXT",
        "extra_1_url": "TEXT",
        "extra_2_label": "TEXT",
        "extra_2_url": "TEXT",
        "password": "TEXT",
        "slug": "TEXT",
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        # ── NEW COLUMNS (upgrade) ──────────────────────────────
        "extra_3_label": "TEXT",
        "extra_3_url": "TEXT",
        "extra_4_label": "TEXT",
        "extra_4_url": "TEXT",
        "extra_5_label": "TEXT",
        "extra_5_url": "TEXT",
        "region": "TEXT",
        "game_code": "TEXT",
        "requirements": "TEXT",
        "installation_guide": "TEXT",
        # ── ORIGINAL URL COLUMNS (upgrade) ────────────────────
        "game_link_original": "TEXT",
        "update_link_original": "TEXT",
        "dlc_link_original": "TEXT",
        "extra_1_url_original": "TEXT",
        "extra_2_url_original": "TEXT",
        "extra_3_url_original": "TEXT",
        "extra_4_url_original": "TEXT",
        "extra_5_url_original": "TEXT",
    }

    for col_name, col_def in columns_to_add.items():
        cursor.execute("""
            SELECT 1
            FROM pg_attribute
            WHERE attrelid = 'games'::regclass
              AND attname   = %s
              AND NOT attisdropped
        """, (col_name,))

        if cursor.fetchone() is None:
            cursor.execute(f"ALTER TABLE games ADD COLUMN {col_name} {col_def}")
            conn.commit()  # <--- CRITICAL FOR NEON POSTGRESQL
            print(f"[MIGRATION]: Added column {col_name} to games table")

    conn.commit()

    # Force verify Neon columns on startup
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'games';")
    existing_cols = [row[0] if isinstance(row, tuple) else row['column_name'] for row in cursor.fetchall()]
    print(f"[NEON DB COLUMNS]: {existing_cols}")

    # Create IndexNow log table for deduplication
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS indexnow_log (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create index for efficient cooldown checks
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_indexnow_log_url_submitted 
        ON indexnow_log (url, submitted_at)
    """)

    # Create game_links table for Viking Link Architecture
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS game_links (
            id SERIAL PRIMARY KEY,
            game_id INTEGER NOT NULL REFERENCES games(id) ON DELETE CASCADE,
            label TEXT NOT NULL,
            anker_sub_id INTEGER,
            cached_url TEXT NOT NULL,
            resolver_type TEXT NOT NULL DEFAULT 'static',
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for game_links
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_game_links_game_id 
        ON game_links (game_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_game_links_anker_sub_id 
        ON game_links (anker_sub_id)
    """)

    conn.commit()

    # Backfill routine: populate slug and updated_at for existing rows where slug IS NULL
    cursor.execute("SELECT id, title, console FROM games WHERE slug IS NULL")
    rows_to_backfill = cursor.fetchall()

    for row in rows_to_backfill:
        game_id = row['id']
        title = row['title']
        console = row['console']

        # Generate slug
        base_slug = slugify(title, console)

        # Check for duplicates and ensure uniqueness
        cursor.execute("SELECT id FROM games WHERE slug = %s AND id != %s", (base_slug, game_id))
        if cursor.fetchone():
            unique_slug = f"{base_slug}-{game_id}"
        else:
            unique_slug = base_slug

        # Update the row
        cursor.execute(
            "UPDATE games SET slug = %s, updated_at = %s WHERE id = %s",
            (unique_slug, datetime.now(), game_id)
        )

    conn.commit()

    # ── Performance: trigram index for fast ILIKE search on title ──
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_games_title_trgm
        ON games USING GIN (title gin_trgm_ops)
    """)
    conn.commit()

    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Vk Store API", lifespan=lifespan)

# Set up Jinja2 templates
templates = Jinja2Templates(directory="templates")

ALLOWED_ORIGINS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=500)


# ==========================================
# 3. نماذج البيانات (Pydantic Models)
# ==========================================
class GameBase(BaseModel):
    title: str
    console: str
    cover_image: Optional[str] = ""
    description: Optional[str] = ""
    requirements: Optional[str] = ""
    installation_guide: Optional[str] = ""
    size: Optional[str] = ""
    version: Optional[str] = ""
    youtube_link: Optional[str] = ""
    game_link: Optional[str] = ""
    game_link_original: Optional[str] = ""
    update_link: Optional[str] = ""
    update_link_original: Optional[str] = ""
    dlc_link: Optional[str] = ""
    dlc_link_original: Optional[str] = ""
    is_arabic: Optional[int] = 0
    extra_1_label: Optional[str] = ""
    extra_1_url: Optional[str] = ""
    extra_1_url_original: Optional[str] = ""
    extra_2_label: Optional[str] = ""
    extra_2_url: Optional[str] = ""
    extra_2_url_original: Optional[str] = ""
    # ── NEW FIELDS ──────────────────────────────
    extra_3_label: Optional[str] = ""
    extra_3_url: Optional[str] = ""
    extra_3_url_original: Optional[str] = ""
    extra_4_label: Optional[str] = ""
    extra_4_url: Optional[str] = ""
    extra_4_url_original: Optional[str] = ""
    extra_5_label: Optional[str] = ""
    extra_5_url: Optional[str] = ""
    extra_5_url_original: Optional[str] = ""
    region: Optional[str] = ""
    game_code: Optional[str] = ""
    # ────────────────────────────────────────────
    password: Optional[str] = ""
    slug: Optional[str] = ""
    updated_at: Optional[datetime] = None

    @field_validator('console')
    @classmethod
    def validate_console(cls, v):
        if v.lower() not in SUPPORTED_CONSOLES:
            raise ValueError(
                f"المنصة '{v}' غير مدعومة. المنصات المتاحة هي: {', '.join(SUPPORTED_CONSOLES)}"
            )
        return v.lower()

    @field_validator('is_arabic')
    @classmethod
    def validate_is_arabic(cls, v):
        if v not in (0, 1):
            raise ValueError("قيمة is_arabic يجب أن تكون 0 أو 1 فقط")
        return v

    @field_validator('cover_image', 'youtube_link', 'game_link', 'update_link', 'dlc_link',
                     'extra_1_url', 'extra_2_url', 'extra_3_url', 'extra_4_url', 'extra_5_url')
    @classmethod
    def validate_url_fields(cls, v):
        if v and v.strip():
            v = v.strip()
            if not (v.startswith('http://') or v.startswith('https://')):
                raise ValueError(
                    f"رابط غير آمن: '{v}'. يجب أن يبدأ بـ http:// أو https://"
                )
        return v


class GameCreate(GameBase):
    links: Optional[List[Dict[str, Any]]] = Field(default_factory=list)


class GameUpdate(GameBase):
    links: Optional[List[Dict[str, Any]]] = Field(default_factory=list)


# ==========================================
# GameLink Models for Viking Link Architecture
# ==========================================
class GameLinkBase(BaseModel):
    label: str
    anker_sub_id: Optional[int] = None
    cached_url: str
    resolver_type: str = "static"
    expires_at: Optional[datetime] = None

    @field_validator('resolver_type')
    @classmethod
    def validate_resolver_type(cls, v):
        if v not in ("ankergames", "static"):
            raise ValueError("resolver_type must be 'ankergames' or 'static'")
        return v

    @field_validator('cached_url')
    @classmethod
    def validate_cached_url(cls, v):
        if v and v.strip():
            v = v.strip()
            if not (v.startswith('http://') or v.startswith('https://')):
                raise ValueError(
                    f"رابط غير آمن: '{v}'. يجب أن يبدأ بـ http:// أو https://"
                )
        return v

    @model_validator(mode='before')
    @classmethod
    def auto_set_resolver_type(cls, data):
        """
        Dynamic resolver type helper.
        Automatically set resolver_type based on anker_sub_id presence.
        """
        if isinstance(data, dict):
            anker_sub_id = data.get('anker_sub_id')
            if anker_sub_id is not None and anker_sub_id > 0:
                data['resolver_type'] = 'ankergames'
            else:
                data['resolver_type'] = 'static'
        return data


def set_resolver_type_from_anker(anker_sub_id: Optional[int]) -> str:
    """
    Dynamic resolver type helper function.
    Automatically set resolver_type based on anker_sub_id presence.
    Use this when constructing GameLink instances programmatically.
    """
    if anker_sub_id is not None and anker_sub_id > 0:
        return "ankergames"
    return "static"


class GameLinkCreate(GameLinkBase):
    game_id: int


class GameLinkUpdate(GameLinkBase):
    pass


class GameLinkResponse(GameLinkBase):
    id: int
    game_id: int
    created_at: datetime
    updated_at: datetime


class GameResponse(GameBase):
    id: int
    created_at: datetime
    slug: str
    updated_at: datetime
    links: List[GameLinkResponse] = []


# ==========================================
# 4. دوال التحقق والحماية (Auth Dependency)
# ==========================================
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security_scheme)):
    if credentials.credentials != SECRET_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired Bearer Token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials


# ==========================================
# 5. الـ Endpoints الخاصة بالألعاب
# ==========================================

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"status": "OK", "message": "Vk-Store Server is awake!"}


@app.get("/api/games", status_code=status.HTTP_200_OK)
def get_games(
        console: Optional[str] = Query(None),
        is_arabic: Optional[int] = Query(None),
        search: Optional[str] = Query(None),
        page: int = Query(1, ge=1),
        limit: int = Query(12, ge=1, le=100),
        x_forwarded_for: Optional[str] = Query(None, alias="X-Forwarded-For")
):
    client_ip = x_forwarded_for if x_forwarded_for else "unknown"
    if not check_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Please try again later."
        )

    if is_arabic is not None and is_arabic not in (0, 1):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="قيمة is_arabic يجب أن تكون 0 أو 1 فقط"
        )

    cache_key = f"api_games:{console}:{is_arabic}:{search}:{page}:{limit}"
    cached_response = cache_get(cache_key)
    if cached_response is not None:
        return cached_response

    conn = get_db_connection()
    cursor = conn.cursor()

    base_query = "FROM games WHERE 1=1"
    params: List = []

    if console and console.lower() != 'all':
        base_query += " AND LOWER(console) = LOWER(%s)"
        params.append(console)

    if is_arabic is not None:
        base_query += " AND is_arabic = %s"
        params.append(is_arabic)

    if search:
        base_query += " AND title ILIKE %s"
        params.append(f"%{search}%")

    count_query = f"SELECT COUNT(*) as total {base_query}"
    cursor.execute(count_query, params)
    total_items = cursor.fetchone()["total"]
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1

    offset = (page - 1) * limit
    data_query = f"SELECT id, title, console, cover_image, size, region, game_code, is_arabic, slug {base_query} ORDER BY id DESC LIMIT %s OFFSET %s"
    data_params = params + [limit, offset]

    cursor.execute(data_query, data_params)
    rows = cursor.fetchall()
    games = [dict(row) for row in rows]

    conn.close()

    result = {
        "data": games,
        "pagination": {
            "current_page": page,
            "limit": limit,
            "total_items": total_items,
            "total_pages": total_pages
        }
    }
    cache_set(cache_key, result, ttl_seconds=60)
    return result


@app.get("/api/admin/games", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def get_games_admin(
        console: Optional[str] = Query(None),
        is_arabic: Optional[int] = Query(None),
        search: Optional[str] = Query(None),
        page: int = Query(1, ge=1),
        limit: int = Query(12, ge=1, le=100),
):
    if is_arabic is not None and is_arabic not in (0, 1):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="قيمة is_arabic يجب أن تكون 0 أو 1 فقط"
        )

    conn = get_db_connection()
    cursor = conn.cursor()

    base_query = "FROM games WHERE 1=1"
    params: List = []

    if console and console.lower() != 'all':
        base_query += " AND LOWER(console) = LOWER(%s)"
        params.append(console)

    if is_arabic is not None:
        base_query += " AND is_arabic = %s"
        params.append(is_arabic)

    if search:
        base_query += " AND title ILIKE %s"
        params.append(f"%{search}%")

    count_query = f"SELECT COUNT(*) as total {base_query}"
    cursor.execute(count_query, params)
    total_items = cursor.fetchone()["total"]
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1

    offset = (page - 1) * limit
    data_query = f"SELECT * {base_query} ORDER BY id DESC LIMIT %s OFFSET %s"
    data_params = params + [limit, offset]

    cursor.execute(data_query, data_params)
    rows = cursor.fetchall()
    games = [dict(row) for row in rows]

    conn.close()

    return {
        "data": games,
        "pagination": {
            "current_page": page,
            "limit": limit,
            "total_items": total_items,
            "total_pages": total_pages
        }
    }


@app.get("/api/games/{id}", response_model=GameResponse, status_code=status.HTTP_200_OK)
def get_game_by_id(id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM games WHERE id = %s", (id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة غير موجودة في قاعدة البيانات")
    
    # Fetch associated game_links
    cursor.execute("""
        SELECT id, label, resolver_type
        FROM game_links 
        WHERE game_id = %s
        ORDER BY id ASC
    """, (id,))
    
    links = cursor.fetchall()
    
    # Format links for frontend consumption
    formatted_links = []
    for link in links:
        formatted_links.append({
            "id": link['id'],
            "label": link['label'],
            "download_url": f"/download/{link['id']}",
            "resolver_type": link['resolver_type']
        })
    
    # Add links to the game response
    row['links'] = formatted_links
    
    conn.close()
    return row


@app.get("/api/reveal-link/{id_slug}", status_code=status.HTTP_200_OK)
def reveal_download_link(id_slug: str, type: str = Query("game_link")):
    """Returns only the requested link column's URL for a given game, used by download_link.html after the 3-step ad gate completes."""
    valid_link_columns = {
        "game_link", "update_link", "dlc_link",
        "extra_1_url", "extra_2_url", "extra_3_url", "extra_4_url", "extra_5_url"
    }
    if type not in valid_link_columns:
        raise HTTPException(status_code=400, detail="Invalid link type requested")

    parts = id_slug.split('-')
    if not parts or not parts[0].isdigit():
        raise HTTPException(status_code=404, detail="Invalid game URL")

    game_id = int(parts[0])

    conn = get_db_connection()
    cursor = conn.cursor()
    # Safe: `type` is validated against a strict whitelist above before being used in an f-string
    cursor.execute(f"SELECT {type} FROM games WHERE id = %s", (game_id,))
    row = cursor.fetchone()
    conn.close()

    if not row or not (row[type] and row[type].strip()):
        raise HTTPException(status_code=404, detail="الرابط غير متوفر")

    return {"url": row[type]}


@app.post("/api/games", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_token)])
def create_game(game: GameCreate, background_tasks: BackgroundTasks, request: Request):
    print("[DEBUG Backend Received]:", game.requirements, game.installation_guide)
    conn = get_db_connection()
    cursor = conn.cursor()

    # Compute slug from title and console
    base_slug = slugify(game.title, game.console)
    slug = base_slug  # Temporary, will update after getting ID if duplicate exists

    query = """
        INSERT INTO games (
            title, console, cover_image, description, requirements, installation_guide, size,
            version, youtube_link, game_link, game_link_original, update_link, update_link_original, dlc_link, dlc_link_original, is_arabic,
            extra_1_label, extra_1_url, extra_1_url_original, extra_2_label, extra_2_url, extra_2_url_original,
            extra_3_label, extra_3_url, extra_3_url_original, extra_4_label, extra_4_url, extra_4_url_original,
            extra_5_label, extra_5_url, extra_5_url_original, region, game_code,
            password, slug, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.requirements, game.installation_guide, game.size,
        game.version, game.youtube_link, game.game_link, game.game_link_original, game.update_link, game.update_link_original, game.dlc_link, game.dlc_link_original,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_1_url_original, game.extra_2_label, game.extra_2_url, game.extra_2_url_original,
        game.extra_3_label, game.extra_3_url, game.extra_3_url_original, game.extra_4_label, game.extra_4_url, game.extra_4_url_original,
        game.extra_5_label, game.extra_5_url, game.extra_5_url_original, game.region, game.game_code,
        game.password, slug, datetime.now()
    )

    try:
        cursor.execute(query, values)
        conn.commit()  # <--- CRITICAL FOR NEON POSTGRESQL
        print("[SUCCESS]: Game saved successfully to Neon DB!")
    except Exception as e:
        conn.rollback()  # Rollback on error to avoid stuck transactions
        print(f"[NEON DB ERROR]: Failed to execute INSERT query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    new_game_id = cursor.fetchone()["id"]

    # Insert game_links if provided
    if game.links and len(game.links) > 0:
        print(f"[DEBUG LINKS] Inserting {len(game.links)} links for game_id {new_game_id}")
        for link in game.links:
            try:
                label = link.get("label", "")
                cached_url = link.get("cached_url", "")
                anker_sub_id = link.get("anker_sub_id")
                resolver_type = link.get("resolver_type", "static")
                
                # Set expires_at for ankergames links
                if resolver_type == "ankergames":
                    expires_at = "NOW() + INTERVAL '14 hours'"
                else:
                    expires_at = "NULL"
                
                insert_link_query = """
                    INSERT INTO game_links (game_id, label, cached_url, anker_sub_id, resolver_type, expires_at)
                    VALUES (%s, %s, %s, %s, %s, {expires_at})
                """.format(expires_at=expires_at)
                
                cursor.execute(insert_link_query, (new_game_id, label, cached_url, anker_sub_id, resolver_type))
                print(f"[DEBUG LINKS] Inserted link: label={label}, resolver_type={resolver_type}")
            except Exception as e:
                print(f"[ERROR LINKS] Failed to insert link: {e}")
                # Continue with other links even if one fails
        
        conn.commit()  # Commit all link insertions
        print(f"[SUCCESS LINKS] All links inserted successfully for game_id {new_game_id}")

    # If slug was duplicate, update it with ID suffix
    cursor.execute("SELECT slug FROM games WHERE id = %s", (new_game_id,))
    current_slug = cursor.fetchone()["slug"]
    if current_slug == base_slug:
        cursor.execute("SELECT COUNT(*) as count FROM games WHERE slug = %s", (base_slug,))
        count = cursor.fetchone()["count"]
        if count > 1:
            # Update slug with ID suffix to ensure uniqueness
            unique_slug = f"{base_slug}-{new_game_id}"
            cursor.execute("UPDATE games SET slug = %s WHERE id = %s", (unique_slug, new_game_id))
            conn.commit()  # Commit slug update if it occurred

    cursor.execute("SELECT * FROM games WHERE id = %s", (new_game_id,))
    created_game = cursor.fetchone()
    conn.close()

    # Submit to IndexNow in background
    base_url = get_base_url(request)
    game_url = f"{base_url}/game/{created_game['id']}-{created_game['slug']}"
    info_url = f"{base_url}/information/{created_game['id']}-{created_game['slug']}"
    background_tasks.add_task(submit_to_indexnow_safely, [game_url, info_url])

    with _cache_lock:
        _cache_store.clear()

    return {
        "message": "تم إضافة اللعبة بنجاح",
        "data": created_game
    }


@app.put("/api/games/{id}", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def update_game(id: int, game: GameUpdate, background_tasks: BackgroundTasks, request: Request):
    print("[DEBUG Backend Received (UPDATE)]:", game.requirements, game.installation_guide)
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, slug, title, description, cover_image FROM games WHERE id = %s", (id,))
    check_row = cursor.fetchone()
    if not check_row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة المراد تعديلها غير موجودة")

    # Capture old values for comparison
    old_slug = check_row['slug']
    old_title = check_row['title']
    old_description = check_row['description']
    old_cover_image = check_row['cover_image']

    # Compute new slug from updated title and console
    new_slug = slugify(game.title, game.console)

    # Check for duplicate slugs (excluding current game)
    cursor.execute("SELECT id FROM games WHERE slug = %s AND id != %s", (new_slug, id))
    if cursor.fetchone():
        # Slug exists for another game, append ID suffix
        new_slug = f"{new_slug}-{id}"

    query = """
        UPDATE games SET
            title = %s, console = %s, cover_image = %s, description = %s, requirements = %s, installation_guide = %s, size = %s,
            version = %s, youtube_link = %s, game_link = %s, game_link_original = %s, update_link = %s, update_link_original = %s, dlc_link = %s, dlc_link_original = %s,
            is_arabic = %s, extra_1_label = %s, extra_1_url = %s, extra_1_url_original = %s, extra_2_label = %s, extra_2_url = %s, extra_2_url_original = %s,
            extra_3_label = %s, extra_3_url = %s, extra_3_url_original = %s, extra_4_label = %s, extra_4_url = %s, extra_4_url_original = %s,
            extra_5_label = %s, extra_5_url = %s, extra_5_url_original = %s, region = %s, game_code = %s,
            password = %s, slug = %s, updated_at = %s
        WHERE id = %s
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.requirements, game.installation_guide, game.size,
        game.version, game.youtube_link, game.game_link, game.game_link_original, game.update_link, game.update_link_original, game.dlc_link, game.dlc_link_original,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_1_url_original, game.extra_2_label, game.extra_2_url, game.extra_2_url_original,
        game.extra_3_label, game.extra_3_url, game.extra_3_url_original, game.extra_4_label, game.extra_4_url, game.extra_4_url_original,
        game.extra_5_label, game.extra_5_url, game.extra_5_url_original, game.region, game.game_code,
        game.password, new_slug, datetime.now(), id
    )

    try:
        cursor.execute(query, values)
        conn.commit()  # <--- CRITICAL FOR NEON POSTGRESQL
        print("[SUCCESS]: Game updated successfully to Neon DB!")
    except Exception as e:
        conn.rollback()  # Rollback on error to avoid stuck transactions
        print(f"[NEON DB ERROR]: Failed to execute UPDATE query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    # Handle game_links update if provided
    if game.links is not None:
        if len(game.links) > 0:
            print(f"[DEBUG LINKS] Updating {len(game.links)} links for game_id {id}")
            
            # Delete existing links for this game
            cursor.execute("DELETE FROM game_links WHERE game_id = %s", (id,))
            print(f"[DEBUG LINKS] Deleted existing links for game_id {id}")
            
            # Insert new links
            for link in game.links:
                try:
                    label = link.get("label", "")
                    cached_url = link.get("cached_url", "")
                    anker_sub_id = link.get("anker_sub_id")
                    resolver_type = link.get("resolver_type", "static")
                    
                    # Set expires_at for ankergames links
                    if resolver_type == "ankergames":
                        expires_at = "NOW() + INTERVAL '14 hours'"
                    else:
                        expires_at = "NULL"
                    
                    insert_link_query = """
                        INSERT INTO game_links (game_id, label, cached_url, anker_sub_id, resolver_type, expires_at)
                        VALUES (%s, %s, %s, %s, %s, {expires_at})
                    """.format(expires_at=expires_at)
                    
                    cursor.execute(insert_link_query, (id, label, cached_url, anker_sub_id, resolver_type))
                    print(f"[DEBUG LINKS] Inserted link: label={label}, resolver_type={resolver_type}")
                except Exception as e:
                    print(f"[ERROR LINKS] Failed to insert link: {e}")
                    # Continue with other links even if one fails
            
            conn.commit()  # Commit all link operations
            print(f"[SUCCESS LINKS] All links updated successfully for game_id {id}")
        else:
            # Empty links array provided - delete all existing links
            print(f"[DEBUG LINKS] Empty links array provided, deleting all links for game_id {id}")
            cursor.execute("DELETE FROM game_links WHERE game_id = %s", (id,))
            conn.commit()
            print(f"[SUCCESS LINKS] All links deleted for game_id {id}")

    cursor.execute("SELECT * FROM games WHERE id = %s", (id,))
    updated_game = cursor.fetchone()
    conn.close()

    # Submit to IndexNow in background if SEO-relevant fields changed
    urls_to_submit = []
    base_url = get_base_url(request)

    if old_slug != new_slug:
        # Slug changed: submit both old and new URLs for both /game/ and /information/
        urls_to_submit.extend([
            f"{base_url}/game/{id}-{old_slug}",
            f"{base_url}/information/{id}-{old_slug}",
            f"{base_url}/game/{id}-{new_slug}",
            f"{base_url}/information/{id}-{new_slug}"
        ])
    else:
        # Slug unchanged: check if title, description, or cover_image changed
        seo_fields_changed = (
            old_title != game.title or
            old_description != game.description or
            old_cover_image != game.cover_image
        )
        if seo_fields_changed:
            urls_to_submit.extend([
                f"{base_url}/game/{id}-{new_slug}",
                f"{base_url}/information/{id}-{new_slug}"
            ])

    if urls_to_submit:
        background_tasks.add_task(submit_to_indexnow_safely, urls_to_submit)

    with _cache_lock:
        _cache_store.clear()

    return {
        "message": "تم تعديل بيانات اللعبة بنجاح",
        "data": updated_game
    }


@app.delete("/api/games/{id}", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def delete_game(id: int, background_tasks: BackgroundTasks, request: Request):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, slug FROM games WHERE id = %s", (id,))
    check_row = cursor.fetchone()
    if not check_row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة المراد حذفها غير موجودة")

    # Capture slug before deletion
    game_slug = check_row['slug']

    cursor.execute("DELETE FROM games WHERE id = %s", (id,))
    conn.commit()
    conn.close()

    # Submit to IndexNow in background
    base_url = get_base_url(request)
    game_url = f"{base_url}/game/{id}-{game_slug}"
    info_url = f"{base_url}/information/{id}-{game_slug}"
    background_tasks.add_task(submit_to_indexnow_safely, [game_url, info_url])

    with _cache_lock:
        _cache_store.clear()

    return {"message": f"تم حذف اللعبة ذات الرقم التعريفي {id} بنجاح"}


# ==========================================
# 7. SSR Routes & SEO
# ==========================================

@app.get("/download.html")
def redirect_download_page(id: Optional[int] = Query(None)):
    """Redirect old ?id= query param URLs to new SEO-friendly /game/{id}-{slug} URLs"""
    if id is None or not str(id).isdigit():
        # No valid ID, preserve current error behavior by redirecting to home
        return RedirectResponse(url="/")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, slug FROM games WHERE id = %s", (id,))
    game = cursor.fetchone()
    conn.close()

    if not game:
        # Game not found, redirect to home
        return RedirectResponse(url="/")

    # 301 redirect to new SEO-friendly URL
    return RedirectResponse(url=f"/game/{game['id']}-{game['slug']}", status_code=301)


@app.get("/information/{id_slug}")
def information_page(id_slug: str, request: Request):
    """Render the game information/detail page (step before download)"""
    cache_key = f"information_page_html:{id_slug}"
    cached_html = cache_get(cache_key)
    if cached_html is not None:
        return Response(content=cached_html, media_type="text/html", headers={"Cache-Control": "public, max-age=1800"})

    # Parse the leading integer ID from the path (before the first hyphen)
    parts = id_slug.split('-')
    if not parts or not parts[0].isdigit():
        raise HTTPException(status_code=404, detail="Invalid game URL")

    game_id = int(parts[0])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM games WHERE id = %s", (game_id,))
    game = cursor.fetchone()
    
    if not game:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة غير موجودة في قاعدة البيانات")
    
    # Fetch associated game_links
    cursor.execute("""
        SELECT id, label, resolver_type
        FROM game_links 
        WHERE game_id = %s
        ORDER BY id ASC
    """, (game_id,))
    
    links = cursor.fetchall()
    
    # Format links for frontend consumption
    formatted_links = []
    for link in links:
        formatted_links.append({
            "id": link['id'],
            "label": link['label'],
            "download_url": f"/download/{link['id']}",
            "resolver_type": link['resolver_type']
        })
    
    # Add links to the game dict
    game['links'] = formatted_links
    
    conn.close()

    # Build SEO metadata
    base_url = get_base_url(request)
    seo_meta = build_seo_meta(game, base_url, path_prefix="information")

    # Build JSON-LD structured data
    json_ld_data = {
        "@context": "https://schema.org",
        "@type": "VideoGame",
        "name": game['title'],
        "operatingSystem": game['console'].upper(),
        "gamePlatform": game['console'].upper(),
        "description": seo_meta['description'],
        "image": game['cover_image'] or ""
    }
    json_ld_json = json.dumps(json_ld_data, default=str).replace('<', '\\u003c')

    # Convert game dict to JSON for inline embedding (excluding description for cleaner JSON)
    game_dict_for_embed = dict(game)
    game_dict_for_embed.pop('description', None)
    game_json = json.dumps(game_dict_for_embed, default=str).replace('<', '\\u003c')

    rendered = templates.TemplateResponse(
        request=request,
        name="information_game.html",
        context={
            "seo_meta": seo_meta,
            "game": game,
            "game_json": game_json,
            "json_ld_json": json_ld_json
        }
    )
    rendered_body = rendered.body.decode("utf-8")
    cache_set(cache_key, rendered_body, ttl_seconds=600)

    return Response(content=rendered_body, media_type="text/html", headers={"Cache-Control": "public, max-age=1800"})


@app.get("/game/{id_slug}")
def game_page(id_slug: str, request: Request):
    """Render individual game download page with SEO metadata"""
    cache_key = f"game_page_html:{id_slug}"
    cached_html = cache_get(cache_key)
    if cached_html is not None:
        return Response(content=cached_html, media_type="text/html", headers={"Cache-Control": "public, max-age=600"})

    # Parse the leading integer ID from the path (before the first hyphen)
    parts = id_slug.split('-')
    if not parts or not parts[0].isdigit():
        raise HTTPException(status_code=404, detail="Invalid game URL")

    game_id = int(parts[0])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM games WHERE id = %s", (game_id,))
    game = cursor.fetchone()
    
    if not game:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة غير موجودة في قاعدة البيانات")
    
    # Fetch associated game_links
    cursor.execute("""
        SELECT id, label, resolver_type
        FROM game_links 
        WHERE game_id = %s
        ORDER BY id ASC
    """, (game_id,))
    
    links = cursor.fetchall()
    
    # Format links for frontend consumption
    formatted_links = []
    for link in links:
        formatted_links.append({
            "id": link['id'],
            "label": link['label'],
            "download_url": f"/download/{link['id']}",
            "resolver_type": link['resolver_type']
        })
    
    # Add links to the game dict
    game['links'] = formatted_links
    
    conn.close()

    # Build SEO metadata
    base_url = get_base_url(request)
    seo_meta = build_seo_meta(game, base_url)

    # Build JSON-LD structured data
    json_ld_data = {
        "@context": "https://schema.org",
        "@type": "VideoGame",
        "name": game['title'],
        "operatingSystem": game['console'].upper(),
        "gamePlatform": game['console'].upper(),
        "description": seo_meta['description'],
        "image": game['cover_image'] or ""
    }
    json_ld_json = json.dumps(json_ld_data, default=str).replace('<', '\\u003c')

    # Convert game dict to JSON for inline embedding
    game_dict_for_embed = dict(game)
    game_dict_for_embed.pop('description', None)
    game_json = json.dumps(game_dict_for_embed, default=str).replace('<', '\\u003c')

    rendered = templates.TemplateResponse(
        request=request,
        name="download.html",
        context={
            "seo_meta": seo_meta,
            "game": game,
            "game_json": game_json,
            "json_ld_json": json_ld_json
        }
    )
    rendered_body = rendered.body.decode("utf-8")
    cache_set(cache_key, rendered_body, ttl_seconds=180)

    return Response(content=rendered_body, media_type="text/html", headers={"Cache-Control": "public, max-age=600"})


# ==========================================
# Viking Link Architecture - Download Endpoint
# ==========================================
@app.get("/download/{link_id}")
async def download_game_link(link_id: int, force_refresh: bool = False):
    """
    Redirect endpoint for game download links with lazy-refresh logic.
    
    Args:
        link_id: ID of the game_link record
        force_refresh: If True, bypass cache and force refresh for dynamic links
        
    Returns:
        HTTP 302 redirect to the actual download URL
    """
    conn = get_db_connection()
    
    try:
        if force_refresh:
            # Force refresh logic: bypass cache for dynamic links
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, anker_sub_id, resolver_type, cached_url
                FROM game_links 
                WHERE id = %s
            """, (link_id,))
            
            link = cursor.fetchone()
            cursor.close()
            
            if not link:
                raise HTTPException(
                    status_code=404, 
                    detail="Download link not found"
                )
            
            # For static links, just return cached URL
            if link['resolver_type'] == 'static' or link['anker_sub_id'] is None:
                download_url = link['cached_url']
            else:
                # Force refresh dynamic link with advisory lock
                print(f"[SERVER RESOLVER] Force refresh for link_id {link_id}, sub_id {link['anker_sub_id']}")
                acquire_advisory_lock(conn, link['anker_sub_id'])
                try:
                    resolved_url = await resolve_pc_link(link['anker_sub_id'])
                    print(f"[SERVER RESOLVER] Sub ID: {link['anker_sub_id']} -> Generated URL: {resolved_url}")
                    if resolved_url:
                        # Update the cached URL and expiration
                        cursor2 = conn.cursor()
                        cursor2.execute("""
                            UPDATE game_links 
                            SET cached_url = %s,
                                expires_at = NOW() + INTERVAL '14 hours',
                                updated_at = NOW()
                            WHERE id = %s
                        """, (resolved_url, link_id))
                        conn.commit()
                        cursor2.close()
                        print(f"[SERVER RESOLVER] Successfully updated link {link_id} in database (force refresh)")
                        download_url = resolved_url
                    else:
                        # Fallback to cached URL if resolution fails
                        print(f"[SERVER RESOLVER] Force refresh failed, using cached URL for link_id {link_id}")
                        download_url = link['cached_url']
                except Exception as e:
                    logging.error(f"Error during force refresh for link_id {link_id}: {e}")
                    print(f"[SERVER RESOLVER] Exception during force refresh: {e}")
                    download_url = link['cached_url']
                finally:
                    release_advisory_lock(conn, link['anker_sub_id'])
        else:
            # Normal lazy-refresh logic
            download_url = await get_or_refresh_game_link(conn, link_id)
        
        if download_url:
            # Return HTTP 302 redirect - zero bandwidth consumption on Render
            return RedirectResponse(url=download_url, status_code=302)
        else:
            raise HTTPException(
                status_code=503, 
                detail="Download link is currently unavailable. Please try again later."
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in download endpoint for link_id {link_id}: {e}")
        print(f"[SERVER RESOLVER] Exception in download endpoint for link_id {link_id}: {e}")
        raise HTTPException(
            status_code=503, 
            detail="Download link is currently unavailable. Please try again later."
        )
    finally:
        conn.close()


@app.get("/download-link/{id_slug}")
def download_link_page(id_slug: str, request: Request, type: str = Query("game_link")):
    """Render the ad-gated 3-step download link page for ANY link type (game_link, update_link, dlc_link, extra_1_url..extra_5_url). Does NOT expose the actual download URL in page source."""
    
    # Validate the requested link type against a strict whitelist
    valid_link_columns = {
        "game_link", "update_link", "dlc_link",
        "extra_1_url", "extra_2_url", "extra_3_url", "extra_4_url", "extra_5_url"
    }
    if type not in valid_link_columns:
        raise HTTPException(status_code=400, detail="Invalid link type requested")

    cache_key = f"download_link_page_html:{id_slug}:{type}"
    cached_html = cache_get(cache_key)
    if cached_html is not None:
        return Response(content=cached_html, media_type="text/html", headers={"Cache-Control": "public, max-age=180"})

    parts = id_slug.split('-')
    if not parts or not parts[0].isdigit():
        raise HTTPException(status_code=404, detail="Invalid game URL")

    game_id = int(parts[0])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, title, console, cover_image, slug, size, region, game_code, "
        "game_link, update_link, dlc_link, "
        "extra_1_label, extra_1_url, extra_2_label, extra_2_url, "
        "extra_3_label, extra_3_url, extra_4_label, extra_4_url, "
        "extra_5_label, extra_5_url "
        "FROM games WHERE id = %s", (game_id,)
    )
    game = cursor.fetchone()
    
    # Fetch associated game_links for Viking Link Architecture
    cursor.execute("""
        SELECT id, label, resolver_type
        FROM game_links 
        WHERE game_id = %s
        ORDER BY id ASC
    """, (game_id,))
    
    links = cursor.fetchall()
    
    # Format links for template consumption
    formatted_links = []
    for link in links:
        formatted_links.append({
            "id": link['id'],
            "label": link['label'],
            "download_url": f"/download/{link['id']}",
            "resolver_type": link['resolver_type']
        })
    
    # Add links to the game dict
    game['links'] = formatted_links
    
    conn.close()

    if not game or not (game.get(type) and game[type].strip()):
        raise HTTPException(status_code=404, detail="لا يوجد رابط تحميل متاح لهذا العنصر")

    # Determine a human-readable label for this link type
    static_labels = {
        "game_link": "تحميل اللعبة",
        "update_link": "تحميل التحديث",
        "dlc_link": "تحميل الإضافات",
    }
    if type in static_labels:
        link_label = static_labels[type]
    else:
        # extra_N_url -> use the game's own extra_N_label if set, else a generic fallback
        label_key = type.replace("_url", "_label")
        link_label = game.get(label_key) or "تحميل إضافي"

    base_url = get_base_url(request)
    seo_meta = {
        "title": f"تحميل {game['title']} - {SITE_NAME}",
        "canonical_url": f"{base_url}/download-link/{game['id']}-{game['slug']}?type={type}"
    }

    # Only expose display fields to the template — the actual link value is deliberately excluded
    display_game = {
        "id": game["id"],
        "title": game["title"],
        "console": game["console"],
        "cover_image": game["cover_image"],
        "slug": game["slug"],
        "size": game["size"],
        "region": game["region"],
        "game_code": game["game_code"],
    }

    rendered = templates.TemplateResponse(
        request=request,
        name="download_link.html",
        context={
            "seo_meta": seo_meta,
            "game": display_game,
            "link_type": type,
            "link_label": link_label
        }
    )
    rendered_body = rendered.body.decode("utf-8")
    cache_set(cache_key, rendered_body, ttl_seconds=180)

    return Response(content=rendered_body, media_type="text/html", headers={"Cache-Control": "public, max-age=180"})


@app.get("/")
def index_page(request: Request):
    """Render homepage with server-side rendered first page of games"""
    cache_key = "index_page_html"
    cached_html = cache_get(cache_key)
    if cached_html is not None:
        return Response(content=cached_html, media_type="text/html", headers={"Cache-Control": "public, max-age=300"})

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get first page of games (limit 24, matching GAMES_PER_PAGE in index.html)
    cursor.execute("SELECT * FROM games ORDER BY id DESC LIMIT 24")
    games = cursor.fetchall()
    conn.close()

    # Default homepage SEO meta
    base_url = get_base_url(request)
    seo_meta = {
        "title": f"{SITE_NAME} | Download PS5, PS4, PS3, PS2, PS1, PC, Xbox, PSP Games",
        "description": f"Download the latest games for all platforms including PS5, PS4, PS3, PS2, PS1, PC, Xbox 360, and PSP. Fast direct links, updates, DLCs, and installation guides.",
        "h1": f"{SITE_NAME} - Game Downloads",
        "canonical_url": f"{base_url}/"
    }

    rendered = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "seo_meta": seo_meta,
            "games": games
        }
    )
    # Render the template body to a string so it can be cached and reused
    rendered_body = rendered.body.decode("utf-8")
    cache_set(cache_key, rendered_body, ttl_seconds=120)

    return Response(content=rendered_body, media_type="text/html", headers={"Cache-Control": "public, max-age=300"})


@app.get("/sitemap.xml")
def sitemap(request: Request):
    """Generate XML sitemap for all games"""
    base_url = get_base_url(request)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, slug, updated_at FROM games ORDER BY id ASC")
    games = cursor.fetchall()
    conn.close()

    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'

    # Add homepage
    xml_content += f'  <url>\n'
    xml_content += f'    <loc>{base_url}/</loc>\n'
    xml_content += f'    <lastmod>{datetime.now().strftime("%Y-%m-%d")}</lastmod>\n'
    xml_content += f'    <changefreq>daily</changefreq>\n'
    xml_content += f'    <priority>1.0</priority>\n'
    xml_content += f'  </url>\n'

    # Add game pages and information pages
    for game in games:
        lastmod = game['updated_at'].strftime('%Y-%m-%d') if game['updated_at'] else datetime.now().strftime('%Y-%m-%d')
        
        # 1. Information Page (Higher priority as it is the landing page)
        info_loc = f"{base_url}/information/{game['id']}-{game['slug']}"
        xml_content += f'  <url>\n'
        xml_content += f'    <loc>{info_loc}</loc>\n'
        xml_content += f'    <lastmod>{lastmod}</lastmod>\n'
        xml_content += f'    <changefreq>weekly</changefreq>\n'
        xml_content += f'    <priority>0.9</priority>\n'
        xml_content += f'  </url>\n'

        # 2. Game Download Page
        game_loc = f"{base_url}/game/{game['id']}-{game['slug']}"
        xml_content += f'  <url>\n'
        xml_content += f'    <loc>{game_loc}</loc>\n'
        xml_content += f'    <lastmod>{lastmod}</lastmod>\n'
        xml_content += f'    <changefreq>weekly</changefreq>\n'
        xml_content += f'    <priority>0.8</priority>\n'
        xml_content += f'  </url>\n'

    xml_content += '</urlset>'

    return Response(
        content=xml_content,
        media_type="application/xml"
    )


@app.get("/robots.txt")
def robots(request: Request):
    """Generate robots.txt file"""
    base_url = get_base_url(request)
    content = f"""User-agent: *
Allow: /
Disallow: /api/

Sitemap: {base_url}/sitemap.xml
"""
    return Response(
        content=content,
        media_type="text/plain"
    )


@app.get("/{key}.txt")
def indexnow_key_verification(key: str):
    """Serve IndexNow key verification file"""
    if key == INDEXNOW_KEY:
        return Response(
            content=INDEXNOW_KEY,
            media_type="text/plain"
        )
    raise HTTPException(status_code=404, detail="Key not found")


# ==========================================
# 6. الـ Endpoint الخاص بالمسؤول (النسخة الاحتياطية)
# ==========================================

@app.get("/api/admin/backup-db", dependencies=[Depends(verify_token)])
def backup_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM games ORDER BY id ASC")
    rows = cursor.fetchall()
    conn.close()

    backup_data = {
        "backup_source": "PostgreSQL / Neon.tech",
        "table": "games",
        "total_records": len(rows),
        "data": [dict(row) for row in rows],
    }

    return JSONResponse(
        content=backup_data,
        headers={
            "Content-Disposition": "attachment; filename=vk_store_backup.json"
        }
    )
