import psycopg2
import psycopg2.extras
import os
import math
import time
import re
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, status, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, field_validator

# ==========================================
# 1. الإعدادات العامة والثوابت
# ==========================================
SECRET_TOKEN = os.getenv("VK_API_SECRET_TOKEN", "VK_SUPER_SECRET_2026")
security_scheme = HTTPBearer()
SITE_NAME = "VK Store"

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
# 2. إدارة الاتصال وقاعدة البيانات
# ==========================================
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


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


def build_seo_meta(game: dict, base_url: str) -> dict:
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
    h1 = f"{game['title']} {game['console'].upper()} PKG Download"

    # Canonical URL (full URL, not just path)
    canonical_url = f"{base_url}/game/{game['id']}-{game['slug']}"

    return {
        "title": title,
        "description": description,
        "h1": h1,
        "canonical_url": canonical_url
    }


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


# ==========================================
# 3. نماذج البيانات (Pydantic Models)
# ==========================================
class GameBase(BaseModel):
    title: str
    console: str
    cover_image: Optional[str] = ""
    description: Optional[str] = ""
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
    pass


class GameUpdate(GameBase):
    pass


class GameResponse(GameBase):
    id: int
    created_at: datetime
    slug: str
    updated_at: datetime


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
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="اللعبة غير موجودة في قاعدة البيانات")

    return row


@app.post("/api/games", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_token)])
def create_game(game: GameCreate):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Compute slug from title and console
    base_slug = slugify(game.title, game.console)
    slug = base_slug  # Temporary, will update after getting ID if duplicate exists

    query = """
        INSERT INTO games (
            title, console, cover_image, description, size,
            version, youtube_link, game_link, game_link_original, update_link, update_link_original, dlc_link, dlc_link_original, is_arabic,
            extra_1_label, extra_1_url, extra_1_url_original, extra_2_label, extra_2_url, extra_2_url_original,
            extra_3_label, extra_3_url, extra_3_url_original, extra_4_label, extra_4_url, extra_4_url_original,
            extra_5_label, extra_5_url, extra_5_url_original, region, game_code,
            password, slug, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.size,
        game.version, game.youtube_link, game.game_link, game.game_link_original, game.update_link, game.update_link_original, game.dlc_link, game.dlc_link_original,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_1_url_original, game.extra_2_label, game.extra_2_url, game.extra_2_url_original,
        game.extra_3_label, game.extra_3_url, game.extra_3_url_original, game.extra_4_label, game.extra_4_url, game.extra_4_url_original,
        game.extra_5_label, game.extra_5_url, game.extra_5_url_original, game.region, game.game_code,
        game.password, slug, datetime.now()
    )

    cursor.execute(query, values)
    new_game_id = cursor.fetchone()["id"]

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

    conn.commit()

    cursor.execute("SELECT * FROM games WHERE id = %s", (new_game_id,))
    created_game = cursor.fetchone()
    conn.close()

    return {
        "message": "تم إضافة اللعبة بنجاح",
        "data": created_game
    }


@app.put("/api/games/{id}", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def update_game(id: int, game: GameUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM games WHERE id = %s", (id,))
    check_row = cursor.fetchone()
    if not check_row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة المراد تعديلها غير موجودة")

    # Compute new slug from updated title and console
    new_slug = slugify(game.title, game.console)

    # Check for duplicate slugs (excluding current game)
    cursor.execute("SELECT id FROM games WHERE slug = %s AND id != %s", (new_slug, id))
    if cursor.fetchone():
        # Slug exists for another game, append ID suffix
        new_slug = f"{new_slug}-{id}"

    query = """
        UPDATE games SET
            title = %s, console = %s, cover_image = %s, description = %s, size = %s,
            version = %s, youtube_link = %s, game_link = %s, game_link_original = %s, update_link = %s, update_link_original = %s, dlc_link = %s, dlc_link_original = %s,
            is_arabic = %s, extra_1_label = %s, extra_1_url = %s, extra_1_url_original = %s, extra_2_label = %s, extra_2_url = %s, extra_2_url_original = %s,
            extra_3_label = %s, extra_3_url = %s, extra_3_url_original = %s, extra_4_label = %s, extra_4_url = %s, extra_4_url_original = %s,
            extra_5_label = %s, extra_5_url = %s, extra_5_url_original = %s, region = %s, game_code = %s,
            password = %s, slug = %s, updated_at = %s
        WHERE id = %s
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.size,
        game.version, game.youtube_link, game.game_link, game.game_link_original, game.update_link, game.update_link_original, game.dlc_link, game.dlc_link_original,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_1_url_original, game.extra_2_label, game.extra_2_url, game.extra_2_url_original,
        game.extra_3_label, game.extra_3_url, game.extra_3_url_original, game.extra_4_label, game.extra_4_url, game.extra_4_url_original,
        game.extra_5_label, game.extra_5_url, game.extra_5_url_original, game.region, game.game_code,
        game.password, new_slug, datetime.now(), id
    )

    cursor.execute(query, values)
    conn.commit()

    cursor.execute("SELECT * FROM games WHERE id = %s", (id,))
    updated_game = cursor.fetchone()
    conn.close()

    return {
        "message": "تم تعديل بيانات اللعبة بنجاح",
        "data": updated_game
    }


@app.delete("/api/games/{id}", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def delete_game(id: int):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM games WHERE id = %s", (id,))
    check_row = cursor.fetchone()
    if not check_row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة المراد حذفها غير موجودة")

    cursor.execute("DELETE FROM games WHERE id = %s", (id,))
    conn.commit()
    conn.close()

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


@app.get("/game/{id_slug}")
def game_page(id_slug: str, request: Request):
    """Render individual game download page with SEO metadata"""
    # Parse the leading integer ID from the path (before the first hyphen)
    parts = id_slug.split('-')
    if not parts or not parts[0].isdigit():
        raise HTTPException(status_code=404, detail="Invalid game URL")

    game_id = int(parts[0])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM games WHERE id = %s", (game_id,))
    game = cursor.fetchone()
    conn.close()

    if not game:
        raise HTTPException(status_code=404, detail="اللعبة غير موجودة في قاعدة البيانات")

    # Build SEO metadata
    base_url = str(request.base_url).rstrip("/")
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
    game_json = json.dumps(game, default=str).replace('<', '\\u003c')

    return templates.TemplateResponse(
        request=request,
        name="download.html",
        context={
            "seo_meta": seo_meta,
            "game": game,
            "game_json": game_json,
            "json_ld_json": json_ld_json
        }
    )


@app.get("/")
def index_page(request: Request):
    """Render homepage with server-side rendered first page of games"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get first page of games (limit 24, matching GAMES_PER_PAGE in index.html)
    cursor.execute("SELECT * FROM games ORDER BY id DESC LIMIT 24")
    games = cursor.fetchall()
    conn.close()

    # Default homepage SEO meta
    base_url = str(request.base_url).rstrip("/")
    seo_meta = {
        "title": f"{SITE_NAME} | Download PS5, PS4, PS3, PS2, PS1, PC, Xbox, PSP Games",
        "description": f"Download the latest games for all platforms including PS5, PS4, PS3, PS2, PS1, PC, Xbox 360, and PSP. Fast direct links, updates, DLCs, and installation guides.",
        "h1": f"{SITE_NAME} - Game Downloads",
        "canonical_url": f"{base_url}/"
    }

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "seo_meta": seo_meta,
            "games": games
        }
    )


@app.get("/sitemap.xml")
def sitemap(request: Request):
    """Generate XML sitemap for all games"""
    base_url = str(request.base_url).rstrip("/")
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

    # Add game pages
    for game in games:
        loc = f"{base_url}/game/{game['id']}-{game['slug']}"
        lastmod = game['updated_at'].strftime('%Y-%m-%d') if game['updated_at'] else datetime.now().strftime('%Y-%m-%d')

        xml_content += f'  <url>\n'
        xml_content += f'    <loc>{loc}</loc>\n'
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
    base_url = str(request.base_url).rstrip("/")
    content = f"""User-agent: *
Allow: /
Disallow: /api/

Sitemap: {base_url}/sitemap.xml
"""
    return Response(
        content=content,
        media_type="text/plain"
    )


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
