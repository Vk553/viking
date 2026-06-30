import psycopg2
import psycopg2.extras
import os
import math
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

# ==========================================
# 1. الإعدادات العامة والثوابت
# ==========================================
SECRET_TOKEN = os.getenv("VK_API_SECRET_TOKEN", "VK_SUPER_SECRET_2026")
security_scheme = HTTPBearer()

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
            update_link TEXT,
            dlc_link TEXT,
            is_arabic INTEGER DEFAULT 0,
            extra_1_label TEXT,
            extra_1_url TEXT,
            extra_2_label TEXT,
            extra_2_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    columns_to_add = {
        "is_arabic":     "INTEGER DEFAULT 0",
        "extra_1_label": "TEXT",
        "extra_1_url":   "TEXT",
        "extra_2_label": "TEXT",
        "extra_2_url":   "TEXT",
        "password":      "TEXT",
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
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Vk Store API", lifespan=lifespan)

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
    update_link: Optional[str] = ""
    dlc_link: Optional[str] = ""
    is_arabic: Optional[int] = 0
    extra_1_label: Optional[str] = ""
    extra_1_url: Optional[str] = ""
    extra_2_label: Optional[str] = ""
    extra_2_url: Optional[str] = ""
    password: Optional[str] = ""

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

    @field_validator('cover_image', 'youtube_link', 'game_link', 'update_link', 'dlc_link', 'extra_1_url', 'extra_2_url')
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
    created_at: datetime  # ✅ FIX: was `str` — PostgreSQL TIMESTAMP columns are returned as datetime objects


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

    query = """
        INSERT INTO games (
            title, console, cover_image, description, size,
            version, youtube_link, game_link, update_link, dlc_link, is_arabic,
            extra_1_label, extra_1_url, extra_2_label, extra_2_url, password
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.size,
        game.version, game.youtube_link, game.game_link, game.update_link, game.dlc_link,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_2_label,
        game.extra_2_url, game.password
    )

    cursor.execute(query, values)
    new_game_id = cursor.fetchone()["id"]
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

    query = """
        UPDATE games SET
            title = %s, console = %s, cover_image = %s, description = %s, size = %s,
            version = %s, youtube_link = %s, game_link = %s, update_link = %s, dlc_link = %s,
            is_arabic = %s, extra_1_label = %s, extra_1_url = %s, extra_2_label = %s,
            extra_2_url = %s, password = %s
        WHERE id = %s
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.size,
        game.version, game.youtube_link, game.game_link, game.update_link, game.dlc_link,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_2_label,
        game.extra_2_url, game.password, id
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
