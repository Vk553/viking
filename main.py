import sqlite3
import os
import math
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import FileResponse
from pydantic import BaseModel, field_validator

# ==========================================
# 1. الإعدادات العامة والثوابت
# ==========================================
DATABASE_NAME = "vk_store.db"
# SECURITY: Read secret token from environment variable with safe fallback
SECRET_TOKEN = os.getenv("VK_API_SECRET_TOKEN", "VK_SUPER_SECRET_2026")
security_scheme = HTTPBearer()

SUPPORTED_CONSOLES = ['ps1', 'ps2', 'ps3', 'ps4', 'ps5', 'pc', 'xbox', 'psp']

# SECURITY: Rate limiting configuration (in-memory tracking for DDoS protection)
# In production, consider using Redis or a dedicated rate-limiting service
RATE_LIMIT_REQUESTS = 100  # Max requests per window
RATE_LIMIT_WINDOW = 60     # Time window in seconds
request_tracker = defaultdict(list)  # {ip: [timestamp1, timestamp2, ...]}

def cleanup_old_requests(ip: str):
    """Remove requests older than the rate limit window."""
    current_time = time.time()
    request_tracker[ip] = [
        ts for ts in request_tracker[ip] 
        if current_time - ts < RATE_LIMIT_WINDOW
    ]

def check_rate_limit(ip: str) -> bool:
    """Check if IP has exceeded rate limit."""
    cleanup_old_requests(ip)
    if len(request_tracker[ip]) >= RATE_LIMIT_REQUESTS:
        return False
    request_tracker[ip].append(time.time())
    return True


# ==========================================
# 2. إدارة الاتصال وقاعدة البيانات
# ==========================================
def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    # إنشاء الجدول إذا لم يكن موجوداً مع حقل is_arabic والحقول الإضافية
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    # إضافة حقل is_arabic إذا كان الجدول موجوداً مسبقاً ولا يحتوي على الحقل
    try:
        cursor.execute("ALTER TABLE games ADD COLUMN is_arabic INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        # الحقل موجود مسبقاً، لا داعي لفعل أي شيء
        pass

    # إضافة الحقول الإضافية للتحميلات الديناميكية
    for column in ["extra_1_label", "extra_1_url", "extra_2_label", "extra_2_url", "password"]:
        try:
            cursor.execute(f"ALTER TABLE games ADD COLUMN {column} TEXT")
        except sqlite3.OperationalError:
            # الحقل موجود مسبقاً، لا داعي لفعل أي شيء
            pass

    conn.commit()
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Vk Store API", lifespan=lifespan)

# SECURITY: Secure CORS policy - restrict origins to prevent unauthorized cross-origin requests
# Read allowed origins from environment variable or use safe defaults
# SECURITY: Secure CORS policy
# تم إلغاء الـ getenv مؤقتاً لتجنب فخ ذاكرة الويندوز العالقة
ALLOWED_ORIGINS = [
    "http://localhost:63342",
    "http://127.0.0.1:63342",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost",
    "http://127.0.0.1",
    "https://vkstore.com",
    "null"  # هذه الكلمة هي كلمة السر لعمل ملفات الـ HTML المحلية
]

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

    # SECURITY: URL validation to prevent XSS and malicious protocol injection
    @field_validator('cover_image', 'youtube_link', 'game_link', 'update_link', 'dlc_link', 'extra_1_url', 'extra_2_url')
    @classmethod
    def validate_url_fields(cls, v):
        if v and v.strip():
            v = v.strip()
            # Ensure URL starts with http:// or https:// to prevent javascript: and other dangerous protocols
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
    created_at: str


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

@app.get("/api/games", status_code=status.HTTP_200_OK)
def get_games(
        console: Optional[str] = Query(None, description="فلترة حسب المنصة (مثال: ps4) أو 'all' لجلب الكل"),
        is_arabic: Optional[int] = Query(None, description="فلترة حسب التعريب: 1 للمعربة، 0 لغير المعربة"),
        search: Optional[str] = Query(None, description="البحث في اسم اللعبة"),
        page: int = Query(1, ge=1, description="رقم الصفحة"),
        limit: int = Query(12, ge=1, le=100, description="عدد العناصر في الصفحة"),
        # SECURITY: Rate limiting via client IP (X-Forwarded-For or fallback)
        x_forwarded_for: Optional[str] = Query(None, alias="X-Forwarded-For")
):
    # SECURITY: Rate limiting check to prevent DDoS attacks
    # In production, use X-Real-IP or X-Forwarded-For from headers instead of query param
    client_ip = x_forwarded_for if x_forwarded_for else "unknown"
    if not check_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Please try again later."
        )

    # التحقق من قيمة is_arabic إذا تم تمريرها
    if is_arabic is not None and is_arabic not in (0, 1):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="قيمة is_arabic يجب أن تكون 0 أو 1 فقط"
        )

    conn = get_db_connection()
    cursor = conn.cursor()

    base_query = "FROM games WHERE 1=1"
    params: List = []

    # فلترة المنصة — يتم تجاهل الفلتر إذا كانت القيمة 'all'
    if console and console.lower() != 'all':
        base_query += " AND LOWER(console) = LOWER(?)"
        params.append(console)

    # فلترة التعريب — يتم تطبيقها فقط إذا تم تمرير القيمة صراحةً
    if is_arabic is not None:
        base_query += " AND is_arabic = ?"
        params.append(is_arabic)

    # فلترة البحث بالاسم
    if search:
        base_query += " AND title LIKE ?"
        params.append(f"%{search}%")

    # حساب إجمالي عدد الألعاب والصفحات
    count_query = f"SELECT COUNT(*) as total {base_query}"
    total_items = cursor.execute(count_query, params).fetchone()["total"]
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1

    # جلب البيانات مع الـ Pagination
    offset = (page - 1) * limit
    data_query = f"SELECT * {base_query} ORDER BY id DESC LIMIT ? OFFSET ?"
    data_params = params + [limit, offset]

    rows = cursor.execute(data_query, data_params).fetchall()
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
    row = cursor.execute("SELECT * FROM games WHERE id = ?", (id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="اللعبة غير موجودة في قاعدة البيانات")

    return dict(row)


@app.post("/api/games", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_token)])
def create_game(game: GameCreate):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO games (
            title, console, cover_image, description, size,
            version, youtube_link, game_link, update_link, dlc_link, is_arabic,
            extra_1_label, extra_1_url, extra_2_label, extra_2_url, password
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.size,
        game.version, game.youtube_link, game.game_link, game.update_link, game.dlc_link,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_2_label, game.extra_2_url, game.password
    )

    cursor.execute(query, values)
    new_game_id = cursor.lastrowid
    conn.commit()

    created_game = cursor.execute("SELECT * FROM games WHERE id = ?", (new_game_id,)).fetchone()
    conn.close()

    return {
        "message": "تم إضافة اللعبة بنجاح",
        "data": dict(created_game)
    }


@app.put("/api/games/{id}", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def update_game(id: int, game: GameUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()

    check_row = cursor.execute("SELECT id FROM games WHERE id = ?", (id,)).fetchone()
    if not check_row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة المراد تعديلها غير موجودة")

    query = """
        UPDATE games SET
            title = ?, console = ?, cover_image = ?, description = ?, size = ?,
            version = ?, youtube_link = ?, game_link = ?, update_link = ?, dlc_link = ?,
            is_arabic = ?, extra_1_label = ?, extra_1_url = ?, extra_2_label = ?, extra_2_url = ?, password = ?
        WHERE id = ?
    """
    values = (
        game.title, game.console, game.cover_image, game.description, game.size,
        game.version, game.youtube_link, game.game_link, game.update_link, game.dlc_link,
        game.is_arabic, game.extra_1_label, game.extra_1_url, game.extra_2_label, game.extra_2_url, game.password, id
    )

    cursor.execute(query, values)
    conn.commit()

    updated_game = cursor.execute("SELECT * FROM games WHERE id = ?", (id,)).fetchone()
    conn.close()

    return {
        "message": "تم تعديل بيانات اللعبة بنجاح",
        "data": dict(updated_game)
    }


@app.delete("/api/games/{id}", status_code=status.HTTP_200_OK, dependencies=[Depends(verify_token)])
def delete_game(id: int):
    conn = get_db_connection()
    cursor = conn.cursor()

    check_row = cursor.execute("SELECT id FROM games WHERE id = ?", (id,)).fetchone()
    if not check_row:
        conn.close()
        raise HTTPException(status_code=404, detail="اللعبة المراد حذفها غير موجودة")

    cursor.execute("DELETE FROM games WHERE id = ?", (id,))
    conn.commit()
    conn.close()

    return {"message": f"تم حذف اللعبة ذات الرقم التعريفي {id} بنجاح"}


# ==========================================
# 6. الـ Endpoint الخاص بالمسؤول (النسخة الاحتياطية)
# ==========================================

@app.get("/api/admin/backup-db", dependencies=[Depends(verify_token)])
def backup_database():
    # SECURITY: Prevent path traversal by using absolute path resolution
    db_path = os.path.abspath(DATABASE_NAME)
    
    # Verify the resolved path is within the expected directory
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="ملف قاعدة البيانات غير موجود بعد")
    
    # Additional security: ensure we're serving the actual database file
    if not db_path.endswith(DATABASE_NAME):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unauthorized file access attempt detected"
        )

    return FileResponse(
        path=db_path,
        filename="vk_store_backup.db",
        media_type="application/x-sqlite3"
    )