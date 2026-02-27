"""
click_tracking_postgres.py - PostgreSQL VERSION with SHORT URLs
Tracks clicks using PostgreSQL on Render with 6-character tracking IDs.

Changes vs previous version:
- posts table now stores ayrshare_post_id + social_post_id + concept_key
  so click data can be joined with concept_analytics / concept_performance
- /api/confirm-post accepts and stores those extra fields
- /api/generate-tracking-url accepts and stores concept_key
- New endpoint: GET /api/concept-clicks  → clicks broken down by concept per platform
- New endpoint: GET /api/unified-report  → merges click data with Ayrshare engagement
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
from datetime import datetime
from urllib.parse import urlencode
import string
import random
import re
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

NONAI_API_KEY = os.getenv("NONAI_API_KEY", "VSr7lXcF.VEvhSiuHvPjiJ7j2pQdQ1eYa1lKNrJda")

app = FastAPI(
    title="NoNAI Click Tracking",
    description="Click tracking with PostgreSQL, SHORT URLs, and concept performance linkage",
    version="8.0_concept_analytics"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# CONFIG
# ======================================================
FINAL_DESTINATION = "https://nonai.life/"
PORT = 8000
DATABASE_URL = "postgresql://postgres:spcuRPdwqcUomUeBDlJaDpaeFzUdwotE@switchback.proxy.rlwy.net:44426/railway"
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set!")

#PUBLIC_URL = os.getenv("PUBLIC_URL", "http://44.193.35.107:8000")
PUBLIC_URL = os.getenv("PUBLIC_URL")

BOT_USER_AGENTS = [
    'facebookexternalhit', 'Twitterbot', 'LinkedInBot', 'WhatsApp',
    'TelegramBot', 'Slackbot', 'Discordbot', 'Googlebot', 'Bingbot',
    'YandexBot', 'DuckDuckBot', 'Applebot', 'Slurp', 'ia_archiver',
    'Mediapartners-Google', 'Bytespider', 'Pinterest', 'Iframely',
    'MetaInspector', 'bot', 'crawler', 'spider', 'scraper', 'checker',
    'monitor', 'headless', 'selenium', 'phantomjs', 'puppeteer',
]

ip_tracker = {}

# ======================================================
# PYDANTIC MODELS
# ======================================================
class TrackingURLRequest(BaseModel):
    platform: str = "facebook"
    badge_type: str = "gold"
    username: str = "unknown"
    concept_key: Optional[str] = None          # ← NEW: which creative concept

class UpdatePostRequest(BaseModel):
    tracking_id: str
    post_url: Optional[str] = None
    username: Optional[str] = None

class ConfirmPostRequest(BaseModel):
    tracking_id: str
    post_url: str
    platform: str
    username: Optional[str] = None
    ayrshare_post_id: Optional[str] = None     # ← NEW: top-level Ayrshare ID (for analytics API)
    social_post_id: Optional[str] = None        # ← NEW: platform-native post ID

# ======================================================
# DATABASE
# ======================================================
@contextmanager
def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ======================================================
# SHORT ID GENERATION
# ======================================================
def generate_short_id(length=6):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

def generate_unique_short_id(length=6, max_attempts=10):
    with get_db_connection() as conn:
        cur = conn.cursor()
        for attempt in range(max_attempts):
            short_id = generate_short_id(length)
            cur.execute("SELECT tracking_id FROM posts WHERE tracking_id = %s", (short_id,))
            if cur.fetchone() is None:
                return short_id
        print(f"⚠️ Could not find unique {length}-char ID in {max_attempts} attempts, trying {length+1} chars")
        return generate_unique_short_id(length + 1, max_attempts)

# ======================================================
# DATABASE SCHEMA INIT
# ======================================================
def init_database():
    """Create / migrate tables.  All ALTER TABLE calls use IF NOT EXISTS so
    they are safe to run on every startup against an existing database."""
    with get_db_connection() as conn:
        cur = conn.cursor()

        # ── posts table ────────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                tracking_id      VARCHAR(10)  PRIMARY KEY,
                username         VARCHAR(255),
                badge_type       VARCHAR(50),
                platform         VARCHAR(50),
                post_url         TEXT,
                clicks           INTEGER      DEFAULT 0,
                confirmed        BOOLEAN      DEFAULT FALSE,
                first_click      TIMESTAMP,
                last_click       TIMESTAMP,
                created_at       TIMESTAMP    DEFAULT NOW(),
                confirmed_at     TIMESTAMP,
                -- concept / Ayrshare linkage (may be NULL for old rows)
                concept_key      VARCHAR(100),
                ayrshare_post_id TEXT,
                social_post_id   TEXT
            )
        """)

        # Safe migrations for databases created before v8
        for col, dtype in [
            ("concept_key",      "VARCHAR(100)"),
            ("ayrshare_post_id", "TEXT"),
            ("social_post_id",   "TEXT"),
        ]:
            cur.execute(f"""
                ALTER TABLE posts ADD COLUMN IF NOT EXISTS {col} {dtype}
            """)

        # ── click_history table ────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS click_history (
                id           SERIAL       PRIMARY KEY,
                tracking_id  VARCHAR(10),
                timestamp    TIMESTAMP    DEFAULT NOW(),
                platform     VARCHAR(50),
                badge_type   VARCHAR(50),
                ip           VARCHAR(50),
                user_agent   TEXT,
                is_human     BOOLEAN      DEFAULT TRUE,
                concept_key  VARCHAR(100),
                FOREIGN KEY (tracking_id) REFERENCES posts(tracking_id) ON DELETE CASCADE
            )
        """)
        cur.execute("""
            ALTER TABLE click_history ADD COLUMN IF NOT EXISTS concept_key VARCHAR(100)
        """)

        # ── stats table ───────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                id                    INTEGER PRIMARY KEY DEFAULT 1,
                bot_requests_blocked  INTEGER DEFAULT 0,
                CHECK (id = 1)
            )
        """)
        cur.execute("""
            INSERT INTO stats (id, bot_requests_blocked)
            VALUES (1, 0) ON CONFLICT (id) DO NOTHING
        """)

        # ── indexes ───────────────────────────────────────────────────────
        cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_confirmed ON posts(confirmed)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_concept ON posts(concept_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_ayrshare ON posts(ayrshare_post_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_click_history_tracking ON click_history(tracking_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_click_history_ts ON click_history(timestamp DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_click_history_concept ON click_history(concept_key)")

        conn.commit()
        print("✅ Database tables initialised (v8 – concept analytics linkage)")

# ======================================================
# HELPERS
# ======================================================
def is_bot_request(user_agent: str, ip: str = None) -> bool:
    if not user_agent:
        return True
    ua = user_agent.lower()
    for bot_ua in BOT_USER_AGENTS:
        if bot_ua.lower() in ua:
            return True
    browser_indicators = ['mozilla', 'chrome', 'safari', 'firefox', 'edge',
                          'opera', 'webkit', 'gecko', 'msie', 'trident']
    mobile_indicators  = ['mobile', 'android', 'iphone', 'ipad', 'ipod']
    has_browser = any(i in ua for i in browser_indicators)
    has_mobile  = any(i in ua for i in mobile_indicators)
    if not has_browser and not has_mobile:
        for pattern in [r'python', r'requests', r'urllib', r'curl', r'wget',
                        r'http-client', r'go-http', r'java', r'okhttp']:
            if re.search(pattern, ua):
                return True
    return False

def is_rate_limited(ip: str, tracking_id: str) -> bool:
    key = f"{ip}_{tracking_id}"
    current_time = time.time()
    if key in ip_tracker:
        last_time, count = ip_tracker[key]
        if current_time - last_time > 3600:
            ip_tracker[key] = (current_time, 1)
            return False
        if (current_time - last_time) < 60 and count >= 5:
            return True
        ip_tracker[key] = (current_time, count + 1)
    else:
        ip_tracker[key] = (current_time, 1)
    return False

def clean_ip_tracker():
    current_time = time.time()
    old_keys = [k for k, (t, _) in ip_tracker.items() if current_time - t > 3600]
    for k in old_keys:
        del ip_tracker[k]

def increment_bot_counter():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE stats SET bot_requests_blocked = bot_requests_blocked + 1 WHERE id = 1")

def get_bot_counter():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT bot_requests_blocked FROM stats WHERE id = 1")
        result = cur.fetchone()
        return result[0] if result else 0

# ======================================================
# STARTUP
# ======================================================
@app.on_event("startup")
async def startup_event():
    print("="*70)
    print("🐘 NONAI CLICK TRACKING  v8.0 — CONCEPT ANALYTICS LINKAGE")
    print("="*70)
    print(f"📍 Port:        {PORT}")
    print(f"🌐 Public URL:  {PUBLIC_URL}")
    print(f"🎯 Redirects:   {FINAL_DESTINATION}")
    print(f"✂️  Short URLs:  6-char tracking IDs  e.g. /t/aB3xK9")
    try:
        init_database()
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM posts WHERE confirmed = TRUE")
            confirmed = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM posts WHERE confirmed = FALSE")
            pending = cur.fetchone()[0]
            cur.execute("SELECT SUM(clicks) FROM posts WHERE confirmed = TRUE")
            total_clicks = cur.fetchone()[0] or 0
        print(f"\n📊 Stats: {confirmed} confirmed posts | {pending} pending | {total_clicks} clicks")
        print(f"   Bot requests blocked: {get_bot_counter()}")
        print("="*70)
    except Exception as e:
        print(f"❌ Database initialisation error: {e}")
        raise

# ======================================================
# ROUTES
# ======================================================
@app.get("/")
async def index():
    return {
        "service":    "NoNAI Click Tracking",
        "status":     "running",
        "version":    "8.0_concept_analytics",
        "database":   "PostgreSQL",
        "public_url": PUBLIC_URL,
        "url_format": "Short 6-character IDs (e.g., /t/aB3xK9)",
        "endpoints": {
            "track":          "/t/{tracking_id}",
            "analytics":      "/api/analytics",
            "concept_clicks": "/api/concept-clicks",
            "unified_report": "/api/unified-report",
            "health":         "/health",
            "generate_url":   "/api/generate-tracking-url  (POST)",
            "confirm_post":   "/api/confirm-post           (POST)",
        }
    }

# ── Click tracking ─────────────────────────────────────────────────────────────
@app.get("/t/{tracking_id}")
async def track_click(tracking_id: str, request: Request,
                      p: str = "unknown", b: str = "unknown"):
    """
    Track clicks with bot detection + timing check.
    concept_key is read from the posts row so it is always consistent
    with what was stored at post-creation time.
    """
    try:
        user_agent = request.headers.get('user-agent', '')
        ip = request.headers.get('x-forwarded-for', request.client.host)

        clean_ip_tracker()

        if is_bot_request(user_agent, ip):
            increment_bot_counter()
            print(f"🤖 BLOCKED Bot: {tracking_id}")
            return RedirectResponse(url=FINAL_DESTINATION, status_code=302)

        if is_rate_limited(ip, tracking_id):
            print(f"🚫 Rate limited: {tracking_id} from {ip}")
            return RedirectResponse(url=FINAL_DESTINATION, status_code=302)

        with get_db_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute("""
                SELECT clicks, confirmed, confirmed_at, concept_key
                FROM posts WHERE tracking_id = %s
            """, (tracking_id,))
            post = cur.fetchone()

            if not post or not post['confirmed']:
                print(f"⚠️ Post not found or not confirmed: {tracking_id}")
                return RedirectResponse(url=FINAL_DESTINATION, status_code=302)

            # Grace-period check: ignore clicks within 30 s of posting (bot previews)
            if post['confirmed_at']:
                time_since = (datetime.now() - post['confirmed_at']).total_seconds()
                if time_since < 30:
                    increment_bot_counter()
                    print(f"🤖 BLOCKED: click too soon after posting ({time_since:.1f}s)")
                    return RedirectResponse(url=FINAL_DESTINATION, status_code=302)

            concept_key = post.get('concept_key')
            now = datetime.now()

            cur.execute("""
                UPDATE posts
                SET clicks      = clicks + 1,
                    last_click  = %s,
                    first_click = COALESCE(first_click, %s)
                WHERE tracking_id = %s
                RETURNING clicks
            """, (now, now, tracking_id))
            new_count = cur.fetchone()['clicks']

            # Store click with concept_key for cross-referencing
            cur.execute("""
                INSERT INTO click_history
                (tracking_id, platform, badge_type, ip, user_agent, is_human, concept_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (tracking_id, p, b,
                  ip[:15] if ip else "unknown",
                  user_agent[:100], True, concept_key))

            conn.commit()
            print(f"🖱️ REAL HUMAN CLICK  tracking_id={tracking_id}  "
                  f"total={new_count}  concept={concept_key}")

        return RedirectResponse(url=FINAL_DESTINATION, status_code=302)

    except Exception as e:
        print(f"❌ Error: {e}")
        return RedirectResponse(url=FINAL_DESTINATION, status_code=302)

# Legacy endpoint
@app.get("/track/{tracking_id}")
async def track_click_legacy(tracking_id: str, request: Request,
                              p: str = "unknown", b: str = "unknown"):
    return await track_click(tracking_id, request, p, b)

# ── Generate tracking URL ──────────────────────────────────────────────────────
@app.post("/api/generate-tracking-url")
async def generate_tracking_url(data: TrackingURLRequest):
    """
    Generate a SHORT tracking URL (pending until confirmed).
    Now also accepts concept_key so clicks can be tied to the creative concept.
    """
    try:
        tracking_id = generate_unique_short_id(length=6)

        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO posts
                (tracking_id, username, badge_type, platform, confirmed, concept_key)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (tracking_id, data.username, data.badge_type,
                  data.platform, False, data.concept_key))
            conn.commit()

        tracking_url = f"{PUBLIC_URL}/t/{tracking_id}"
        print(f"📝 Generated tracking URL (pending): {tracking_url}  concept={data.concept_key}")

        return {
            "tracking_id":   tracking_id,
            "tracking_url":  tracking_url,
            "public_url":    PUBLIC_URL,
            "post_info": {
                "platform":    data.platform,
                "badge_type":  data.badge_type,
                "username":    data.username,
                "concept_key": data.concept_key,
                "tracking_id": tracking_id,
                "initial_clicks": 0,
                "confirmed":   False
            }
        }

    except Exception as e:
        print(f"❌ Error generating URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── Confirm post ───────────────────────────────────────────────────────────────
@app.post("/api/confirm-post")
async def confirm_post(data: ConfirmPostRequest):
    """
    Confirm a post was successfully published.
    Now stores ayrshare_post_id + social_post_id so click data can be
    joined with concept_analytics from the scheduler.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()

            cur.execute("""
                UPDATE posts
                SET post_url         = %s,
                    confirmed        = TRUE,
                    confirmed_at     = %s,
                    platform         = %s,
                    ayrshare_post_id = COALESCE(%s, ayrshare_post_id),
                    social_post_id   = COALESCE(%s, social_post_id)
                WHERE tracking_id = %s
                RETURNING tracking_id
            """, (data.post_url, datetime.now(), data.platform,
                  data.ayrshare_post_id, data.social_post_id,
                  data.tracking_id))

            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Tracking ID not found")

            if data.username and data.username != 'unknown':
                cur.execute("UPDATE posts SET username = %s WHERE tracking_id = %s",
                            (data.username, data.tracking_id))

            conn.commit()

        print(f"✅ Post confirmed: {data.tracking_id}")
        print(f"   post_url:         {data.post_url}")
        print(f"   ayrshare_post_id: {data.ayrshare_post_id}")
        print(f"   social_post_id:   {data.social_post_id}")

        return {
            "status":           "success",
            "tracking_id":      data.tracking_id,
            "post_url":         data.post_url,
            "ayrshare_post_id": data.ayrshare_post_id,
            "social_post_id":   data.social_post_id,
            "confirmed":        True,
            "message":          "Post confirmed and ready for tracking"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Standard analytics ─────────────────────────────────────────────────────────
@app.get("/api/analytics")
async def get_analytics():
    """Comprehensive analytics — same as before but now includes concept_key."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute("""
                SELECT tracking_id, username, post_url, platform, badge_type,
                       concept_key, ayrshare_post_id, social_post_id,
                       clicks, first_click, last_click, created_at, confirmed_at
                FROM posts WHERE confirmed = TRUE
                ORDER BY clicks DESC
            """)
            posts = cur.fetchall()

            cur.execute("""
                SELECT COUNT(*) AS total_posts, SUM(clicks) AS total_clicks
                FROM posts WHERE confirmed = TRUE
            """)
            totals = cur.fetchone()

            cur.execute("""
                SELECT platform, SUM(clicks) AS clicks
                FROM posts WHERE confirmed = TRUE
                GROUP BY platform
            """)
            platform_stats = {r['platform']: r['clicks'] for r in cur.fetchall()}

            cur.execute("""
                SELECT badge_type, SUM(clicks) AS clicks
                FROM posts WHERE confirmed = TRUE
                GROUP BY badge_type
            """)
            badge_stats = {r['badge_type']: r['clicks'] for r in cur.fetchall()}

            # Clicks by concept
            cur.execute("""
                SELECT concept_key, SUM(clicks) AS clicks
                FROM posts WHERE confirmed = TRUE AND concept_key IS NOT NULL
                GROUP BY concept_key ORDER BY clicks DESC
            """)
            concept_stats = {r['concept_key']: r['clicks'] for r in cur.fetchall()}

            cur.execute("""
                SELECT ch.timestamp, ch.tracking_id, ch.platform, ch.badge_type,
                       ch.concept_key, p.post_url, p.username
                FROM click_history ch
                JOIN posts p ON ch.tracking_id = p.tracking_id
                WHERE ch.is_human = TRUE
                ORDER BY ch.timestamp DESC
                LIMIT 20
            """)
            recent_clicks = cur.fetchall()

            cur.execute("SELECT COUNT(*) AS count FROM posts WHERE confirmed = FALSE")
            pending_posts = cur.fetchone()['count']

        posts_with_clicks    = sum(1 for p in posts if p['clicks'] > 0)
        posts_without_clicks = len(posts) - posts_with_clicks

        all_posts = [{
            'tracking_id':       p['tracking_id'],
            'tracking_url':      f"{PUBLIC_URL}/t/{p['tracking_id']}",
            'username':          p['username'] or 'Unknown',
            'post_url':          p['post_url'] or 'N/A',
            'platform':          p['platform'] or 'unknown',
            'badge_type':        p['badge_type'] or 'unknown',
            'concept_key':       p['concept_key'],
            'ayrshare_post_id':  p['ayrshare_post_id'],
            'social_post_id':    p['social_post_id'],
            'clicks':            p['clicks'],
            'posted_at':         (p['confirmed_at'] or p['created_at']).isoformat(),
            'first_click':       p['first_click'].isoformat() if p['first_click'] else None,
            'last_click':        p['last_click'].isoformat() if p['last_click'] else None,
            'status':            'active' if p['clicks'] > 0 else 'no_clicks'
        } for p in posts]

        recent_formatted = [{
            'timestamp':    c['timestamp'].isoformat(),
            'tracking_id':  c['tracking_id'],
            'tracking_url': f"{PUBLIC_URL}/t/{c['tracking_id']}",
            'post_url':     c['post_url'] or 'N/A',
            'platform':     c['platform'] or 'unknown',
            'badge_type':   c['badge_type'] or 'unknown',
            'concept_key':  c['concept_key'],
            'username':     c['username'] or 'Unknown'
        } for c in recent_clicks]

        return {
            'total_clicks':            totals['total_clicks'] or 0,
            'total_posts':             totals['total_posts'] or 0,
            'pending_posts':           pending_posts,
            'posts_with_clicks':       posts_with_clicks,
            'posts_without_clicks':    posts_without_clicks,
            'avg_clicks_per_post':     (totals['total_clicks'] or 0) / max(totals['total_posts'] or 1, 1),
            'clicks_by_platform':      platform_stats,
            'clicks_by_badge_type':    badge_stats,
            'clicks_by_concept':       concept_stats,
            'top_posts':               all_posts[:50],
            'recent_clicks':           recent_formatted,
            'all_posts':               all_posts,
            'bot_requests_blocked':    get_bot_counter(),
            'stats': {
                'human_clicks':        totals['total_clicks'] or 0,
                'bot_requests_blocked': get_bot_counter(),
                'confirmed_posts':     totals['total_posts'] or 0,
                'pending_posts':       pending_posts
            },
            'url_format': 'short_6_char'
        }

    except Exception as e:
        print(f"❌ Analytics error: {e}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ── NEW: Clicks broken down by concept per platform ───────────────────────────
@app.get("/api/concept-clicks")
async def concept_clicks():
    """
    Returns click counts grouped by (platform, concept_key) so you can see
    which creative concept drives the most traffic on each platform.
    Complement to concept_performance in the scheduler DB.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute("""
                SELECT
                    p.platform,
                    p.concept_key,
                    COUNT(DISTINCT p.tracking_id)      AS total_posts,
                    SUM(p.clicks)                      AS total_clicks,
                    AVG(p.clicks)                      AS avg_clicks_per_post,
                    MAX(p.clicks)                      AS max_clicks,
                    SUM(CASE WHEN p.clicks > 0 THEN 1 ELSE 0 END) AS posts_with_clicks
                FROM posts p
                WHERE p.confirmed = TRUE
                  AND p.concept_key IS NOT NULL
                GROUP BY p.platform, p.concept_key
                ORDER BY p.platform, total_clicks DESC
            """)
            rows = cur.fetchall()

            # Group by platform
            by_platform: dict = {}
            for row in rows:
                plat = row['platform'] or 'unknown'
                if plat not in by_platform:
                    by_platform[plat] = []
                by_platform[plat].append({
                    'concept_key':        row['concept_key'],
                    'total_posts':        int(row['total_posts']),
                    'total_clicks':       int(row['total_clicks'] or 0),
                    'avg_clicks_per_post': round(float(row['avg_clicks_per_post'] or 0), 2),
                    'max_clicks':         int(row['max_clicks'] or 0),
                    'posts_with_clicks':  int(row['posts_with_clicks'] or 0),
                })

            # Best concept per platform
            best_per_platform = {
                plat: concepts[0]['concept_key'] if concepts else None
                for plat, concepts in by_platform.items()
            }

            return {
                "by_platform":        by_platform,
                "best_per_platform":  best_per_platform,
                "total_concepts_tracked": len(set(r['concept_key'] for r in rows))
            }

    except Exception as e:
        print(f"❌ concept-clicks error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── NEW: Unified report (clicks + Ayrshare engagement) ───────────────────────
@app.get("/api/unified-report")
async def unified_report():
    """
    Joins click data with Ayrshare engagement data (from concept_analytics
    table written by the scheduler) via ayrshare_post_id.

    If concept_analytics table doesn't exist yet (scheduler not run),
    falls back gracefully to click-only data.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Check if concept_analytics table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'concept_analytics'
                ) AS exists
            """)
            has_concept_analytics = cur.fetchone()['exists']

            if has_concept_analytics:
                cur.execute("""
                    SELECT
                        p.tracking_id,
                        p.platform,
                        p.concept_key,
                        p.ayrshare_post_id,
                        p.post_url,
                        p.clicks                        AS link_clicks,
                        p.confirmed_at                  AS posted_at,
                        ca.engagement_score,
                        ca.likes,
                        ca.comments,
                        ca.shares,
                        ca.impressions,
                        ca.reach,
                        ca.views,
                        ca.analytics_fetched_at
                    FROM posts p
                    LEFT JOIN concept_analytics ca
                        ON ca.ayrshare_post_id = p.ayrshare_post_id
                        AND ca.platform = p.platform
                    WHERE p.confirmed = TRUE
                    ORDER BY p.confirmed_at DESC
                    LIMIT 200
                """)
            else:
                # Fallback: clicks only
                cur.execute("""
                    SELECT
                        tracking_id, platform, concept_key,
                        ayrshare_post_id, post_url,
                        clicks AS link_clicks, confirmed_at AS posted_at,
                        NULL AS engagement_score,
                        NULL AS likes, NULL AS comments,
                        NULL AS shares, NULL AS impressions,
                        NULL AS reach, NULL AS views,
                        NULL AS analytics_fetched_at
                    FROM posts
                    WHERE confirmed = TRUE
                    ORDER BY confirmed_at DESC
                    LIMIT 200
                """)

            rows = cur.fetchall()

        report = []
        for row in rows:
            report.append({
                'tracking_id':        row['tracking_id'],
                'tracking_url':       f"{PUBLIC_URL}/t/{row['tracking_id']}",
                'platform':           row['platform'],
                'concept_key':        row['concept_key'],
                'ayrshare_post_id':   row['ayrshare_post_id'],
                'post_url':           row['post_url'],
                'posted_at':          row['posted_at'].isoformat() if row['posted_at'] else None,
                # Click-tracking data
                'link_clicks':        int(row['link_clicks'] or 0),
                # Ayrshare engagement data (may be None if not yet fetched)
                'engagement_score':   float(row['engagement_score']) if row['engagement_score'] else None,
                'likes':              row['likes'],
                'comments':           row['comments'],
                'shares':             row['shares'],
                'impressions':        row['impressions'],
                'reach':              row['reach'],
                'views':              row['views'],
                'analytics_fetched_at': (row['analytics_fetched_at'].isoformat()
                                         if row['analytics_fetched_at'] else None),
            })

        # Summary: by concept, combined score = engagement_score + (link_clicks * 5)
        from collections import defaultdict
        summary: dict = defaultdict(lambda: {
            'total_posts': 0, 'total_link_clicks': 0,
            'total_engagement_score': 0.0, 'combined_score': 0.0,
            'platforms': set()
        })
        for r in report:
            ck = r['concept_key'] or 'unknown'
            summary[ck]['total_posts']            += 1
            summary[ck]['total_link_clicks']      += r['link_clicks']
            summary[ck]['total_engagement_score'] += r['engagement_score'] or 0
            summary[ck]['combined_score']         += (r['engagement_score'] or 0) + r['link_clicks'] * 5
            summary[ck]['platforms'].add(r['platform'])

        summary_list = sorted([
            {
                'concept_key':            ck,
                'total_posts':            v['total_posts'],
                'total_link_clicks':      v['total_link_clicks'],
                'total_engagement_score': round(v['total_engagement_score'], 1),
                'combined_score':         round(v['combined_score'], 1),
                'platforms':              list(v['platforms']),
            }
            for ck, v in summary.items()
        ], key=lambda x: x['combined_score'], reverse=True)

        return {
            'has_ayrshare_analytics': has_concept_analytics,
            'total_records':          len(report),
            'concept_summary':        summary_list,
            'posts':                  report,
        }

    except Exception as e:
        print(f"❌ unified-report error: {e}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ── Misc endpoints ─────────────────────────────────────────────────────────────
@app.get("/api/public-url")
async def get_public_url_endpoint():
    return {
        "public_url":       PUBLIC_URL,
        "status":           "online",
        "final_destination": FINAL_DESTINATION,
        "url_format":       "SHORT - 6 characters (e.g., /t/aB3xK9)"
    }

@app.post("/api/reset-all")
async def reset_all():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM click_history")
            cur.execute("DELETE FROM posts")
            cur.execute("UPDATE stats SET bot_requests_blocked = 0 WHERE id = 1")
            conn.commit()
        global ip_tracker
        ip_tracker = {}
        return {"status": "success", "message": "All data reset", "total_clicks": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM posts WHERE confirmed = TRUE")
            confirmed = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM posts WHERE confirmed = FALSE")
            pending = cur.fetchone()[0]
            cur.execute("SELECT SUM(clicks) FROM posts WHERE confirmed = TRUE")
            total_clicks = cur.fetchone()[0] or 0
        return {
            "status":               "healthy",
            "timestamp":            datetime.now().isoformat(),
            "total_posts":          confirmed,
            "pending_posts":        pending,
            "total_clicks":         total_clicks,
            "bot_requests_blocked": get_bot_counter(),
            "public_url":           PUBLIC_URL,
            "database":             "PostgreSQL",
            "version":              "8.0_concept_analytics",
            "url_format":           "6-character IDs"
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e),
                "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)