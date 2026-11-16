"""
Automated News Aggregator & Reader (GitHub Native)
Feeds:
1. Football: BBC Sport Premier League (https://feeds.bbci.co.uk/sport/football/premier-league/rss.xml)
2. Technology: The Verge ONLY (https://www.theverge.com/rss/...)
3. Gadgets: Mashable SEA Gadgets (https://sea.mashable.com/gadgets.xml)
4. Gaming: IGN Japan Games (https://jp.ign.com/games.xml)
5. Review: IGN Japan Game Reviews (https://jp.ign.com/review.xml)

Production Features:
- 30 Fresh Articles PER Category (150 items total) with Database Backfill on feed outage.
- Parallel Multi-Feed Fetching with per-feed fault isolation and strict HTTP timeouts.
- GUID + Canonical URL Deduplication (prevents redundant Gemini re-translations).
- Category-Specific Translation Prompts (English & Japanese -> Thai across 5 categories).
- Thread-safe Rate Limiter + Exponential Backoff with Random Jitter for Gemini API Free Tier.
- Normalized UTC Timestamps (`published_ts`, `published_at`, `published_th`) and strict newest-first sorting.
- Minimal UI with custom logo, font size controls, 16:9 covers, inline screenshots, and YouTube embeds.
"""
import calendar
import concurrent.futures
import datetime
import html
import json
import logging
import os
import random
import re
import sys
import threading
import time
import urllib.parse

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NewsPipeline")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "news.json")
INDEX_HTML = os.path.join(BASE_DIR, "index.html")
README_FILE = os.path.join(BASE_DIR, "README.md")


def _load_env_value(key: str) -> str:
    val = (os.getenv(key) or "").strip()
    if not val:
        env_file = os.path.join(BASE_DIR, ".env")
        if os.path.exists(env_file):
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith(f"{key}="):
                        val = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    return val


GEMINI_API_KEY = _load_env_value("GEMINI_API_KEY")
GEMINI_MODEL = _load_env_value("GEMINI_MODEL") or "gemini-3.1-flash-lite"

try:
    TARGET_PER_CATEGORY = max(1, int(_load_env_value("MAX_ARTICLES") or "30"))
except ValueError:
    TARGET_PER_CATEGORY = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

_rate_limit_lock = threading.Lock()
_last_api_call_ts = 0.0
try:
    MIN_API_INTERVAL_SEC = float(_load_env_value("MIN_API_INTERVAL_SEC") or "1.5")
except ValueError:
    MIN_API_INTERVAL_SEC = 1.5

THAI_MONTHS = [
    "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.",
    "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."
]

TRACKING_QUERY_PARAMS = {
    "fbclid", "gclid", "ref", "source", "ncid", "_ga", "mc_cid", "mc_eid", "cmpid"
}

BAD_IMG_KEYWORDS = [
    "flexiimages", ".svg", "nosize", "flags/", "logo", "avatar", "icon",
    "/users/", "banner", "badge", "tracking", "sponsored", "pixel", "ad-",
    "scorecardresearch", "placeholder", "author", "profile", "button", "1x1",
    "ign30-logo", "ign_kw_logo", "/user/", "/avatars/", "data:image"
]

CURATED_GAMING_COVERS = [
    "https://images.unsplash.com/photo-1550745165-9bc0b252726f?w=1200&q=80",
    "https://images.unsplash.com/photo-1542751371-adc38448a05e?w=1200&q=80",
    "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80",
    "https://images.unsplash.com/photo-1538481199705-c710c4e965fc?w=1200&q=80",
    "https://images.unsplash.com/photo-1579373903781-fd5c0c30c4cd?w=1200&q=80",
    "https://images.unsplash.com/photo-1563089145-599997674d42?w=1200&q=80",
    "https://images.unsplash.com/photo-1518709268805-4e9042af9f23?w=1200&q=80",
    "https://images.unsplash.com/photo-1612287233207-6b4d32a90ca4?w=1200&q=80",
    "https://images.unsplash.com/photo-1580234811497-9df7fd2f357e?w=1200&q=80",
    "https://images.unsplash.com/photo-1509198397868-475647b2a1e5?w=1200&q=80",
]

CATEGORY_FALLBACK_COVERS = {
    "Football": "https://images.unsplash.com/photo-1508098682722-e99c43a406b2?w=1200&q=80",
    "Technology": "https://images.unsplash.com/photo-1518770660439-4636190af475?w=1200&q=80",
    "Gadgets": "https://images.unsplash.com/photo-1519389950473-47ba0277781c?w=1200&q=80",
    "Gaming": "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80",
    "Review": "https://images.unsplash.com/photo-1542751371-adc38448a05e?w=1200&q=80",
}

CATEGORIES_ORDER = ["Football", "Technology", "Gadgets", "Gaming", "Review"]

CATEGORIES_CONFIG = {
    "Football": [
        "https://feeds.bbci.co.uk/sport/football/premier-league/rss.xml",
        "https://www.theguardian.com/football/premierleague/rss",
        "https://www.skysports.com/rss/11095",
    ],
    "Technology": [
        "https://www.theverge.com/rss/index.xml",
        "https://www.theverge.com/rss/tech/index.xml",
        "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
        "https://www.theverge.com/rss/policy/index.xml",
        "https://www.theverge.com/rss/reviews/index.xml",
        "https://www.theverge.com/rss/gadgets/index.xml",
        "https://www.theverge.com/rss/creators/index.xml",
    ],
    "Gadgets": [
        "https://sea.mashable.com/gadgets.xml",
        "https://www.engadget.com/rss.xml",
        "https://www.gsmarena.com/rss-news-reviews.php3",
        "https://9to5mac.com/feed/",
        "https://9to5google.com/feed/",
        "https://gizmodo.com/tech/feed",
    ],
    "Gaming": [
        "https://jp.ign.com/games.xml",
        "https://www.gamespot.com/feeds/game-news/",
        "https://feeds.feedburner.com/ign/games-all",
        "https://www.pcgamer.com/rss/",
        "https://www.polygon.com/rss/index.xml",
        "https://www.rockpapershotgun.com/feed",
    ],
    "Review": [
        "https://jp.ign.com/review.xml",
        "https://www.gamespot.com/feeds/reviews/",
        "https://feeds.feedburner.com/ign/reviews",
        "https://www.eurogamer.net/feed/reviews",
    ],
}

VERGE_SUBTAG_MAP = {
    "ai-artificial-intelligence": "AI",
    "tech": "TECH",
    "entertainment": "ENTERTAINMENT",
    "science": "SCIENCE",
    "gadgets": "GADGETS",
    "policy": "POLICY",
    "transportation": "TRANSPORTATION",
    "reviews": "REVIEWS",
    "creators": "CREATORS",
    "column": "COLUMN",
    "podcast": "PODCAST",
    "games": "GAMING",
    "gaming": "GAMING",
}

CLUB_SUBTAG_PATTERNS = [
    (r"\bman(?:chester)?\s*utd\b|\bmanchester\s+united\b|\bold\s+trafford\b", "MAN UTD"),
    (r"\bliverpool\b|\banfield\b|\bvan\s+dijk\b|\bsalah\b", "LIVERPOOL"),
    (r"\barsenal\b|\barteta\b|\bgunners\b|\bsaka\b", "ARSENAL"),
    (r"\bman(?:chester)?\s*city\b|\bguardiola\b|\bhaaland\b", "MAN CITY"),
    (r"\bchelsea\b|\bstamford\s+bridge\b|\bpalmer\b", "CHELSEA"),
    (r"\btottenham\b|\bspurs\b", "SPURS"),
    (r"\bnewcastle\b|\bmagpies\b", "NEWCASTLE"),
    (r"\baston\s+villa\b|\bvilla\b", "ASTON VILLA"),
    (r"\beverton\b|\bgoodison\b", "EVERTON"),
    (r"\bwest\s+ham\b|\bhammers\b", "WEST HAM"),
    (r"\bcrystal\s+palace\b", "CRYSTAL PALACE"),
    (r"\bbrighton\b", "BRIGHTON"),
    (r"\bfulham\b", "FULHAM"),
    (r"\bbournemouth\b", "BOURNEMOUTH"),
    (r"\bbrentford\b", "BRENTFORD"),
    (r"\bnottingham\s+forest\b|\bforest\b", "FOREST"),
    (r"\bwolves\b|\bwolverhampton\b", "WOLVES"),
    (r"\bleicester\b", "LEICESTER"),
    (r"\bsouthampton\b", "SOUTHAMPTON"),
    (r"\bipswich\b", "IPSWICH"),
    (r"\bleeds\b", "LEEDS"),
    (r"\bsunderland\b", "SUNDERLAND"),
    (r"\bburnley\b", "BURNLEY"),
    (r"\bengland\b|\btuchel\b", "ENGLAND"),
]

GAME_SUBTAG_PATTERNS = [
    (r"zelda|ocarina", "ZELDA"),
    (r"metal\s*gear", "METAL GEAR"),
    (r"monster\s*hunter", "MONSTER HUNTER"),
    (r"fire\s*emblem", "FIRE EMBLEM"),
    (r"call\s*of\s*duty", "CALL OF DUTY"),
    (r"grand\s*theft\s*auto|gta", "GTA"),
    (r"final\s*fantasy", "FINAL FANTASY"),
    (r"elder\s*scrolls", "ELDER SCROLLS"),
    (r"dragon.?s\s*dogma", "DRAGON'S DOGMA"),
    (r"silent\s*hill", "SILENT HILL"),
    (r"resident\s*evil|biohazard", "RESIDENT EVIL"),
    (r"elden\s*ring", "ELDEN RING"),
    (r"onimusha", "ONIMUSHA"),
    (r"castlevania", "CASTLEVANIA"),
    (r"ace\s*combat", "ACE COMBAT"),
    (r"gears\s*of\s*war", "GEARS OF WAR"),
    (r"minecraft", "MINECRAFT"),
    (r"marvel|wolverine", "MARVEL"),
    (r"houkai|honkai|star\s*rail", "HONKAI: STAR RAIL"),
    (r"persona", "PERSONA"),
    (r"switch[\s-]*2", "SWITCH 2"),
    (r"\bps5\b|playstation[\s-]*5", "PS5"),
    (r"\bxbox\b", "XBOX"),
    (r"steam-frame|google-pixel|gaming-device", "HARDWARE"),
]


def extract_sub_tag(item: dict) -> str:
    """Derive a clean uppercase sub-category tag from URL path and article title/bullets."""
    cat = item.get("tag") or item.get("category_hint") or ""
    link = item.get("link", "")
    title_raw = (item.get("original_title") or item.get("title") or "") + " " + (item.get("thai_title") or "")
    title = title_raw.lower()
    bullets_txt = " ".join(item.get("bullet_points") or [])[:400].lower()

    seg = ""
    if link:
        try:
            parts = urllib.parse.urlsplit(link).path.strip("/").split("/")
            seg = parts[0].lower() if parts and parts[0] else ""
        except Exception:
            seg = ""

    if cat == "Technology":
        return VERGE_SUBTAG_MAP.get(seg, "TECH")

    if cat == "Gadgets":
        if seg == "smart-home":
            return "SMART HOME"
        if seg == "mobile":
            return "MOBILE"
        if seg == "tablets":
            return "TABLETS"
        if seg == "earbuds":
            return "EARBUDS"
        body = f"{seg} {title}"
        if any(k in body for k in ["airpod", "earbud", "headphone", "speaker", "audio"]):
            return "AUDIO"
        if any(k in body for k in ["glasses", "watch", "ring", "wearable", "garmin", "fitbit"]):
            return "WEARABLES"
        if any(k in body for k in ["iphone", "galaxy", "pixel", "smartphone", "android", "phone"]):
            return "MOBILE"
        if any(k in body for k in ["ipad", "tablet", "kindle"]):
            return "TABLETS"
        if any(k in body for k in ["macbook", "laptop", "pc", "surface"]):
            return "LAPTOPS"
        if any(k in body for k in ["tv", "home", "vacuum", "security", "wifi", "router"]):
            return "SMART HOME"
        if "9to5mac.com" in link.lower():
            return "APPLE"
        if "9to5google.com" in link.lower():
            return "GOOGLE"
        return "GADGETS"

    if cat == "Football":
        body = f"{title} {bullets_txt}"
        for pat, label in CLUB_SUBTAG_PATTERNS:
            if re.search(pat, body, re.IGNORECASE):
                return label
        return "PREMIER LEAGUE"

    if cat in ("Gaming", "Review"):
        is_ign_jp = "jp.ign.com" in link.lower()
        body = f"{seg.replace('-', ' ')} {title}" if is_ign_jp else title
        for pat, label in GAME_SUBTAG_PATTERNS:
            if re.search(pat, body, re.IGNORECASE):
                return label
        if is_ign_jp and seg and seg not in ("games", "article", "review"):
            clean_seg = re.sub(r"-\d+$", "", seg).replace("-", " ").upper().strip()
            if len(clean_seg) > 22:
                clean_seg = " ".join(clean_seg.split()[:3])[:22].strip()
            if clean_seg:
                return clean_seg
        if "switch 2" in body or "nintendo" in body:
            return "NINTENDO"
        if "ps5" in body or "playstation" in body:
            return "PLAYSTATION"
        if "xbox" in body:
            return "XBOX"
        if "steam" in body or "pc" in body:
            return "PC GAMING"
        return "GAMING" if cat == "Gaming" else "GAME REVIEW"

    return ""

SYSTEM_PROMPT = """คุณคือนักข่าวและบรรณาธิการข่าวมืออาชีพ แปลและถ่ายทอดเนื้อหาข่าวและบทความรีวิวจากภาษาต้นฉบับ (ภาษาอังกฤษ หรือ ภาษาญี่ปุ่น) เป็นภาษาไทยอย่างละเอียด ครบถ้วนสมบูรณ์ทุกมิติ และตรงตามข้อเท็จจริง (Fact-based) 100%
กฎสำคัญที่สุด:
1. การแปลเนื้อหาฉบับเต็ม ("full_paragraphs"):
   - ต้องแปลและถ่ายทอดเนื้อหาข่าวอย่างละเอียด ครบทุกประเด็น ทุกคำสัมภาษณ์ (Direct quotes), ตัวเลขสถิติ, ข้อมูลและบริบทเหตุการณ์, รวมถึงบทวิเคราะห์ทั้งหมดที่ปรากฏในต้นฉบับ
   - ห้ามตัดทอนคำสัมภาษณ์เด็ดขาด (แปลคำพูดในเครื่องหมายคำพูดอย่างครบถ้วนทุกประโยค)
   - ห้ามแต่งเติมข้อมูลที่ไม่มีในต้นฉบับ และแบ่งเป็นย่อหน้าตามสัดส่วนความยาวจริงของข่าวต้นฉบับ (4-15 ย่อหน้าขึ้นไปตามเนื้อหาจริง)
2. สรุปประเด็นสำคัญ ("bullet_points"):
   - สรุปสาระสำคัญหลัก 4-6 ข้ออย่างกระชับ ชัดเจน และครอบคลุมเป็นภาษาไทย
3. พาดหัวข่าว ("thai_title"):
   - แปลพาดหัวข่าวภาษาไทยให้กระชับ ดึงดูด ตรงประเด็น ทับศัพท์ถูกต้องตาม Fact ห้ามคืนค่าเป็นภาษาต้นฉบับล้วน
4. ทับศัพท์ชื่อเฉพาะ สโมสร นักกีฬา แบรนด์ ฮาร์ดแวร์ โมเดล AI และชื่อเกมสากล ให้ถูกต้องตามหลักสากล

กฎแท็ก (Tag): ให้ระบุเป็นหนึ่งในห้าหมวดนี้เท่านั้น: "Football", "Technology", "Gadgets", "Gaming", "Review"
"""

CATEGORY_PROMPTS = {
    "Football": """บทบาทเฉพาะหมวดหมู่ (Premier League - BBC Sport / The Guardian / Sky Sports):
- คุณคือบรรณาธิการข่าวฟุตบอลพรีเมียร์ลีกอังกฤษมืออาชีพ เชี่ยวชาญข่าวสโมสร นักเตะ ตลาดซื้อขาย และสถิติฟุตบอลพรีเมียร์ลีก
- ใช้สำนวนข่าวกีฬาฟุตบอลไทยที่เป็นธรรมชาติ แม่นยำ ไม่แปลตรงตัวแบบแข็งกระด้าง (เช่น clean sheet = คลีนชีต, transfer window = ตลาดซื้อขายนักเตะ, cap = การลงสนามรับใช้ทีมชาติ, draw = ผลเสมอ/เสมอกัน, manager/boss = กุนซือ/ผู้จัดการทีม)
- ทับศัพท์ชื่อนักเตะ สโมสร ผู้จัดการทีม และสนามแข่งขันตามมาตรฐานสื่อกีฬาชั้นนำของไทย เก็บทุกคำพูดสัมภาษณ์ (Direct quotes) และสถิติตัวเลขครบถ้วน""",
    "Technology": """บทบาทเฉพาะหมวดหมู่ (The Verge - Technology & AI):
- คุณคือบรรณาธิการข่าวเทคโนโลยีและนวัตกรรมระดับแนวหน้า สไตล์ The Verge ภาษาไทย
- ใช้ภาษาที่ทันสมัย คมชัด ลึกซึ้ง คงคำศัพท์เทคนิคที่วงการไอทีไทยนิยมใช้ทับศัพท์หรือคงรูปภาษาอังกฤษไว้ตามความเหมาะสม (เช่น AI, LLM, API, SDK, GPU, NPU, Chipset, Benchmark, Cloud, Open-source, Firmware, OS)
- ระบุชื่อบริษัท รุ่นผลิตภัณฑ์ สเปก ราคา และบริบททางธุรกิจหรือนโยบายเทคโนโลยีอย่างแม่นยำ""",
    "Gadgets": """บทบาทเฉพาะหมวดหมู่ (Gadgets - Mashable SEA / Engadget / GSMArena / 9to5Mac / 9to5Google / Gizmodo):
- คุณคือบรรณาธิการข่าวแกดเจ็ต สมาร์ตโฟน และอุปกรณ์อิเล็กทรอนิกส์มืออาชีพ
- ใช้ภาษาที่อ่านสนุก ทันสมัย เข้าใจง่าย คงชื่อแบรนด์และชื่อรุ่นสินค้าภาษาอังกฤษไว้เสมอ (เช่น Apple, iPhone, Samsung, Google Pixel, Sennheiser, Sony, Garmin)
- เก็บรายละเอียดฟีเจอร์เด่น สเปกการใช้งาน ผลทดสอบ ราคา วันวางจำหน่าย และความน่าสนใจของอุปกรณ์อย่างครบถ้วน""",
    "Gaming": """บทบาทเฉพาะหมวดหมู่ (Gaming News - IGN Japan / GameSpot / IGN US / PC Gamer / Polygon / Rock Paper Shotgun):
- คุณคือบรรณาธิการสื่อเกมมืออาชีพ แปลข่าวเกมจากต้นฉบับ (ภาษาญี่ปุ่น หรือ ภาษาอังกฤษ) เป็นภาษาไทย
- แปลงชื่อเกมเป็นชื่อภาษาอังกฤษสากลที่เกมเมอร์ไทยคุ้นเคย (เช่น 『ゼルダの伝説 時のオカリナ』 -> The Legend of Zelda: Ocarina of Time) ห้ามแปลชื่อเกมเป็นภาษาไทยแบบตรงตัว
- คงชื่อแพลตฟอร์ม (PS5, Xbox Series X|S, Switch, Switch 2, PC, Steam) ชื่อค่ายพัฒนา/ผู้จัดจำหน่าย (เช่น Nintendo, Sony, Capcom, Square Enix, Konami, Kojima Productions) และใช้คำศัพท์เกมเมอร์สากลอย่างเป็นธรรมชาติ""",
    "Review": """บทบาทเฉพาะหมวดหมู่ (Game Reviews - IGN Japan / GameSpot / IGN US / Eurogamer):
- คุณคือบรรณาธิการและนักวิจารณ์เกมมืออาชีพ แปลบทความรีวิวเกมจากต้นฉบับ (ภาษาญี่ปุ่น หรือ ภาษาอังกฤษ) เป็นภาษาไทยเชิงลึก
- แปลงชื่อเกมเป็นชื่อภาษาอังกฤษสากลเสมอ
- ถ่ายทอดรายละเอียดการรีวิวอย่างครบถ้วน ทั้งระบบเกมเพลย์ (Gameplay), การออกแบบด่าน, เนื้อเรื่อง, กราฟิก/เฟรมเรต, จุดเด่น, จุดสังเกต และบทสรุปคำตัดสิน (Verdict) ด้วยสำนวนรีวิวเกมที่อ่านสนุก น่าติดตาม และได้อารมณ์เหมือนอ่านนิตยสารเกมชั้นนำ"""
}


def has_thai_chars(text: str) -> bool:
    return bool(text and any("\u0e00" <= c <= "\u0e7f" for c in text))


def clean_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean = re.sub(r"<[^>]+>", "", raw_html).strip()
    return " ".join(html.unescape(clean).split())


def normalize_url(url: str) -> str:
    """Canonicalize URL by enforcing HTTPS, lowercasing host, removing tracking params and trailing slash."""
    if not url:
        return ""
    raw = url.strip()
    try:
        parts = urllib.parse.urlsplit(raw)
    except Exception:
        return raw
    if not parts.scheme and not parts.netloc:
        return raw.rstrip("/")

    scheme = "https" if parts.scheme.lower() in ("http", "https", "") else parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or ""
    if len(path) > 1:
        path = path.rstrip("/")

    filtered_q = []
    for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
        kl = k.lower()
        if kl.startswith("utm_") or kl.startswith("at_") or kl in TRACKING_QUERY_PARAMS:
            continue
        filtered_q.append((k, v))
    query = urllib.parse.urlencode(filtered_q, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def normalize_guid(guid: str, link: str = "") -> str:
    """Normalize entry GUID/ID or fall back to canonicalized link."""
    raw = (guid or "").strip()
    if raw:
        if raw.lower().startswith(("http://", "https://")):
            return normalize_url(raw)
        return raw
    return normalize_url(link)


def to_thailand_time(dt_utc: datetime.datetime) -> str:
    """Convert UTC datetime to Thailand Time (UTC+7) string."""
    th_tz = datetime.timezone(datetime.timedelta(hours=7))
    th_dt = dt_utc.astimezone(th_tz)
    day = th_dt.day
    month = THAI_MONTHS[th_dt.month - 1]
    year = th_dt.year
    hour = th_dt.strftime("%H:%M")
    return f"{day} {month} {year} • {hour} น."


def parse_existing_timestamp(item: dict, fallback_utc: datetime.datetime) -> datetime.datetime:
    """Parse stored timestamps (published_ts, published_at, or published_th) into UTC datetime."""
    ts = item.get("published_ts")
    if isinstance(ts, (int, float)) and ts > 0:
        return datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc)

    pub_at = (item.get("published_at") or "").strip()
    if pub_at:
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M UTC", "%Y-%m-%d %H:%M:%S UTC"):
            try:
                dt = datetime.datetime.strptime(pub_at, fmt)
                return dt.replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                pass
        try:
            iso_dt = datetime.datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
            if iso_dt.tzinfo is None:
                iso_dt = iso_dt.replace(tzinfo=datetime.timezone.utc)
            return iso_dt.astimezone(datetime.timezone.utc)
        except ValueError:
            pass

    th_tz = datetime.timezone(datetime.timedelta(hours=7))
    pub_th = (item.get("published_th") or "").strip()
    if pub_th:
        m1 = re.match(r"^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", pub_th)
        if m1:
            y, mo, d, h, mi = map(int, m1.groups())
            return datetime.datetime(y, mo, d, h, mi, tzinfo=th_tz).astimezone(datetime.timezone.utc)

        m2 = re.match(r"^(\d{1,2})\s+([^\s]+)\s+(\d{4})\s*[•·-]\s*(\d{2}):(\d{2})", pub_th)
        if m2:
            day_s, month_th, year_s, hour_s, min_s = m2.groups()
            if month_th in THAI_MONTHS:
                month_idx = THAI_MONTHS.index(month_th) + 1
                year_val = int(year_s)
                if year_val > 2400:
                    year_val -= 543
                return datetime.datetime(
                    year_val, month_idx, int(day_s), int(hour_s), int(min_s), tzinfo=th_tz
                ).astimezone(datetime.timezone.utc)

    return fallback_utc


def is_valid_translation(existing_item: dict) -> bool:
    """Return True if an article already has a valid Thai translation."""
    if existing_item.get("_translation_failed"):
        return False
    thai_title = (existing_item.get("thai_title") or "").strip()
    if not has_thai_chars(thai_title):
        return False
    full_paras = existing_item.get("full_paragraphs")
    if not isinstance(full_paras, list) or not full_paras:
        return False
    if not any(has_thai_chars(p) for p in full_paras if isinstance(p, str)):
        return False
    bullets = existing_item.get("bullet_points")
    if not isinstance(bullets, list) or not bullets:
        return False
    return True


def make_hd_image_url(url: str) -> str:
    """Ensure images are high-resolution HD (1080p/1200px) instead of low-res thumbnails."""
    if not url:
        return ""
    url = url.strip()
    # Upgrade BBC thumbnails (/240/, /320/, /480/ -> /1200/)
    url = re.sub(r"/(standard|branded_sport|branded_news)/\d+/", r"/\1/1200/", url)
    url = re.sub(r"/ic/\d+x\d+/", r"/ic/1024x576/", url)
    # Upgrade IGN Japan & Mashable SEA thumbnails (sm.*/t/...280.jpg -> .1200.jpg)
    if "sm.ign.com/t/" in url or "sm.mashable.com/t/" in url:
        url = re.sub(r"\.\d+\.(jpg|jpeg|png|webp)(\?.*)?$", r".1200.\1", url, flags=re.IGNORECASE)
    # Upgrade Eurogamer (assetsio.gnwcdn.com)
    if "assetsio.gnwcdn.com" in url:
        url = re.sub(r"width=\d+", "width=1200", url)
    # Upgrade PC Gamer (cdn.mos.cms.futurecdn.net)
    if "futurecdn.net" in url:
        url = re.sub(r"-\d+-\d+\.jpg$", "-1920-80.jpg", url)
    # Upgrade Gematsu resize suffixes e.g. -320x180.jpg -> .jpg
    url = re.sub(r"-\d+x\d+(\.\w+)$", r"\1", url)
    # Upgrade The Verge images
    if "theverge.com" in url or "platform.theverge" in url:
        if "?" in url:
            url = re.sub(r"w=\d+", "w=1200", url)
            if "w=" not in url:
                url += "&w=1200"
    return url


def _is_usable_image_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        return False
    ul = u.lower()
    return not any(bad in ul for bad in BAD_IMG_KEYWORDS)


def _image_stem(url: str) -> str:
    """Return filename stem without size suffixes so cover images aren't duplicated in gallery."""
    if not url:
        return ""
    base = os.path.basename(url.split("?")[0]).lower()
    base = re.sub(r"\.\d+(\.\w+)$", r"\1", base)
    return base


def extract_entry_thumbnail(entry) -> str:
    """Extract best thumbnail URL across RSS/Atom standards: media:content, media:thumbnail, enclosure, HTML body."""
    media_content = entry.get("media_content") or []
    for mc in media_content:
        u = (mc.get("url") or "").strip()
        mtype = (mc.get("type") or "").lower()
        medium = (mc.get("medium") or "").lower()
        if u and (medium == "image" or mtype.startswith("image/") or (not mtype.startswith(("video/", "audio/")) and _is_usable_image_url(u))):
            return make_hd_image_url(u)

    media_thumb = entry.get("media_thumbnail") or []
    for mt in media_thumb:
        u = (mt.get("url") or "").strip()
        if _is_usable_image_url(u):
            return make_hd_image_url(u)

    enclosures = entry.get("enclosures") or []
    for enc in enclosures:
        u = (enc.get("href") or enc.get("url") or "").strip()
        etype = (enc.get("type") or "").lower()
        u_clean = u.split("?")[0].lower()
        if u and (etype.startswith("image/") or any(u_clean.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp"))):
            if _is_usable_image_url(u):
                return make_hd_image_url(u)

    html_blobs = []
    for c in (entry.get("content") or []):
        if isinstance(c, dict) and c.get("value"):
            html_blobs.append(c["value"])
    for field in ("summary", "description"):
        val = entry.get(field)
        if val:
            html_blobs.append(val)

    for blob in html_blobs:
        for img_m in re.finditer(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', blob, re.IGNORECASE):
            cand = html.unescape(img_m.group(1)).strip()
            if _is_usable_image_url(cand):
                return make_hd_image_url(cand)

    return ""


def _extract_meta_image(page_html: str) -> str:
    patterns = [
        r'<meta[^>]+(?:property|name)=["\']og:image:secure_url["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']og:image:secure_url["\']',
        r'<meta[^>]+(?:property|name)=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']og:image["\']',
        r'<meta[^>]+(?:property|name)=["\']twitter:image(?::src)?["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']twitter:image(?::src)?["\']',
    ]
    for pat in patterns:
        m = re.search(pat, page_html, re.IGNORECASE)
        if m:
            cand = html.unescape(m.group(1)).strip()
            if _is_usable_image_url(cand):
                return make_hd_image_url(cand)
    return ""


def scrape_article_details(item: dict) -> dict:
    """Scrape article webpage for HD lead image, paragraphs, inline screenshots, YouTube videos, and review score."""
    link = item["link"]
    feed_img = item.get("feed_image", "")
    lead_img = make_hd_image_url(feed_img) if _is_usable_image_url(feed_img) else ""
    gallery_images = []
    videos = []
    clean_paras = []
    review_score = item.get("review_score", "")

    try:
        resp = requests.get(link, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        content = resp.text

        # 1. Lead / Cover Image (prefer og:image if missing or non-IGN)
        meta_img = _extract_meta_image(content)
        if not lead_img or ("jp.ign.com" in link.lower() and meta_img):
            if meta_img:
                lead_img = meta_img

        # 2. Extract review score & verdict box for IGN Japan reviews
        if "jp.ign.com" in link.lower():
            rev_block_m = re.search(
                r'<div class=["\']article-review["\']>(.*?)<div class=["\']page-content["\']\s+id=["\']bottom-list-reviews["\']>',
                content, re.S | re.I
            )
            rev_source = rev_block_m.group(1) if rev_block_m else content
            score_m = re.search(
                r'<span class=["\'][^"\']*hexagon-content["\']>\s*(?:<div>)?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:</div>)?',
                rev_source, re.I
            )
            if score_m:
                review_score = score_m.group(1).strip()

            score_text_m = re.search(r'<div class=["\']score-text["\']>([^<]+)</div>', rev_source, re.I)
            blurb_m = re.search(r'<div class=["\']blurb["\'][^>]*>(.*?)</div>', rev_source, re.S | re.I)
            verdict_text = clean_text(blurb_m.group(1)) if blurb_m else ""
            score_label = clean_text(score_text_m.group(1)) if score_text_m else ""
        else:
            verdict_text = ""
            score_label = ""
            if not review_score:
                rv_m = re.search(r'"ratingValue"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?', content, re.I)
                best_m = re.search(r'"bestRating"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?', content, re.I)
                if rv_m:
                    try:
                        val = float(rv_m.group(1))
                        best = float(best_m.group(1)) if best_m else 10.0
                        if best == 5.0:
                            val = val * 2.0
                        elif best == 100.0:
                            val = round(val / 10.0, 1)
                        review_score = str(int(val)) if val.is_integer() else f"{val:.1f}"
                    except Exception:
                        pass

        # 3. Extract article container (prioritize IGN Japan article-page, fallback to <article>)
        art_m = (
            re.search(r'<article[^>]+class=["\'][^"\']*article-page[^"\']*["\'][^>]*>(.*?)</article>', content, re.DOTALL | re.IGNORECASE)
            or re.search(r"<article[^>]*>(.*?)</article>", content, re.DOTALL | re.IGNORECASE)
        )
        article_html = art_m.group(1) if art_m else content

        # Remove related-article float boxes & script/style noise before parsing paragraphs/media
        article_html = re.sub(r'<aside[^>]*class=["\'][^"\']*floatrecirc[^"\']*["\'][^>]*>.*?</aside>', "", article_html, flags=re.S | re.I)
        article_html = re.sub(r'<div[^>]*class=["\'][^"\']*recirc-title[^"\']*["\'][^>]*>.*?</div>', "", article_html, flags=re.S | re.I)
        article_html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", article_html, flags=re.S | re.I)

        # 4. Extract ALL paragraphs (supports both closed </p> and IGN Japan's unclosed <p> tags)
        raw_p = re.findall(
            r"<p[^>]*>(.*?)(?=<p[>\s]|</p>|<div|<section|<article|<h[1-6]|<blockquote|$)",
            article_html, re.DOTALL | re.IGNORECASE
        )
        bad_phrases = [
            "terms of service", "privacy policy", "cookie", "sign up", "newsletter",
            "subscribe", "all rights reserved", "follow us", "comments", "please login",
            "image source", "image caption", "image copyright", "published", "hours ago",
            "days ago", "minutes ago", "read more:", "also read:", "related:", "loading...",
            "advertisement", "sponsored", "listen to the latest", "watch:", "quiz:",
            "bbc homepage", "skip to content", "accessibility help", "welcome to pc gamer club",
            "membership journey starts here", "keep exploring and earning", "free article view",
            "posts from this", "sign up for", "have you listened to", "cast your vote",
            "social links navigation", "please follow our community guidelines",
            "you must confirm your public display name",
        ]
        seen_p = set()
        for p in raw_p:
            clean = re.sub(r"<[^>]+>", "", p).strip()
            clean = " ".join(html.unescape(clean).split())
            if len(clean) > 20 and not any(bad in clean.lower() for bad in bad_phrases):
                if clean not in seen_p:
                    seen_p.add(clean)
                    clean_paras.append(clean)

        if verdict_text:
            verdict_prefix = f"บทสรุปคำตัดสิน (Verdict) [คะแนน {review_score}/10" + (f" - {score_label}" if score_label else "") + "]: "
            verdict_line = verdict_prefix + verdict_text
            if verdict_line not in seen_p:
                clean_paras.append(verdict_line)

        # 5. Extract genuine screenshots from article body ONLY (prioritize data-src over lazy placeholder src)
        img_tags = re.findall(r"<img[^>]+>", article_html, re.IGNORECASE)
        seen_stems = set()
        lead_stem = _image_stem(lead_img)
        if lead_stem:
            seen_stems.add(lead_stem)

        for tag_str in img_tags:
            m_src = (
                re.search(r'data-src=["\'](https?://[^"\']+)["\']', tag_str, re.IGNORECASE)
                or re.search(r'src=["\'](https?://[^"\']+)["\']', tag_str, re.IGNORECASE)
            )
            if not m_src:
                continue
            g = html.unescape(m_src.group(1)).strip()
            g_clean = g.split("?")[0].lower()
            if any(ext in g_clean for ext in [".jpg", ".jpeg", ".png", ".webp"]) and _is_usable_image_url(g):
                stem = _image_stem(g)
                if stem and stem not in seen_stems:
                    seen_stems.add(stem)
                    gallery_images.append(make_hd_image_url(g))

        # 6. Extract embedded YouTube videos from article body
        yt_ids = re.findall(
            r"(?:youtube\.com/embed/|youtube-nocookie\.com/embed/|youtu\.be/|youtube\.com/watch\?v=)([a-zA-Z0-9_-]{11})",
            article_html, re.IGNORECASE
        )
        seen_vid = set()
        for vid in yt_ids:
            if vid not in seen_vid:
                seen_vid.add(vid)
                videos.append(f"https://www.youtube.com/embed/{vid}")

    except Exception as e:
        logger.debug(f"Direct scrape fallback for {link}: {e}")

    # Fallback to Jina if direct failed
    if not clean_paras:
        try:
            jina_url = f"https://r.jina.ai/{link}"
            resp = requests.get(jina_url, headers=HEADERS, timeout=12)
            resp.raise_for_status()
            lines = resp.text.splitlines()
            start = False
            for line in lines:
                if line.startswith("Markdown Content:"):
                    start = True
                    continue
                if not start:
                    continue
                line = line.strip()
                if not line or line.startswith("!") or line.startswith("#"):
                    continue
                line = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", line)
                if len(line) > 25 and not any(bad in line.lower() for bad in ["source:", "sign up", "cookie"]):
                    clean_paras.append(line)
        except Exception:
            pass

    clean_paras = list(dict.fromkeys(clean_paras))
    item["cover_image_url"] = lead_img
    item["gallery_images"] = gallery_images[:8]
    item["videos"] = videos[:3]
    item["review_score"] = review_score
    item["raw_paragraphs"] = clean_paras[:45] if clean_paras else [item["description"]]
    return item


def _wait_for_rate_limit():
    """Enforce minimum interval between Gemini API calls across threads."""
    global _last_api_call_ts
    with _rate_limit_lock:
        now = time.monotonic()
        target_ts = max(now, _last_api_call_ts + MIN_API_INTERVAL_SEC)
        sleep_for = target_ts - now
        _last_api_call_ts = target_ts
    if sleep_for > 0:
        time.sleep(sleep_for)


def call_gemini(item_info: dict) -> dict:
    """Call Gemini with category-specific context, rate pacing, and exponential backoff + jitter."""
    title = item_info.get("title", item_info.get("original_title", ""))
    category_hint = item_info.get("category_hint", item_info.get("tag", "Technology"))
    cat_prompt = CATEGORY_PROMPTS.get(category_hint, CATEGORY_PROMPTS["Technology"])
    paras_text = "\n\n".join(item_info.get("raw_paragraphs", []))

    if not GEMINI_API_KEY:
        logger.warning(f"GEMINI_API_KEY missing; skipping translation for '{title[:40]}'")
        return {
            "thai_title": title,
            "tag": category_hint,
            "bullet_points": [item_info.get("description", title)],
            "full_paragraphs": item_info.get("raw_paragraphs", [])[:8],
            "_translation_failed": True,
        }

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    system_instruction_text = f"{SYSTEM_PROMPT}\n\n{cat_prompt}"

    user_prompt = f"""หัวข้อต้นฉบับ: {title}
หมวดหมู่: {category_hint}

{cat_prompt}

เนื้อหาต้นฉบับเต็ม:
{paras_text}

คำสั่งสำคัญอย่างยิ่ง:
กรุณาแปลเนื้อหานี้เป็นภาษาไทยอย่างละเอียด ลึกซึ้ง ครบถ้วนทุกประเด็น รวมถึงคำสัมภาษณ์ทั้งหมด (Direct quotes), ตัวเลข, สถิติ, และบริบทเหตุการณ์ทั้งหมด ห้ามตัดทอนหรือแต่งเติม โดยแบ่งเป็นย่อหน้าตามลำดับเนื้อหาจริง (4-15 ย่อหน้าขึ้นไปตามความยาวต้นฉบับ)

ตอบกลับเป็น JSON:
{{
  "thai_title": "พาดหัวข่าวภาษาไทยที่กระชับ ตรงตาม Fact (ห้ามเป็นภาษาต้นฉบับล้วน)",
  "tag": "{category_hint}",
  "bullet_points": [
    "ประเด็นสำคัญที่ 1 (ภาษาไทย)...",
    "ประเด็นสำคัญที่ 2 (ภาษาไทย)...",
    "ประเด็นสำคัญที่ 3 (ภาษาไทย)...",
    "ประเด็นสำคัญที่ 4 (ภาษาไทย)..."
  ],
  "full_paragraphs": [
    "ย่อหน้าที่ 1 แปลครบถ้วน...",
    "ย่อหน้าที่ 2 แปลครบถ้วน...",
    "ย่อหน้าที่ 3 แปลครบถ้วน...",
    "ย่อหน้าที่ 4 แปลครบถ้วน..."
  ]
}}"""

    payload = {
        "system_instruction": {"parts": [{"text": system_instruction_text}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2},
    }

    for attempt in range(4):
        try:
            _wait_for_rate_limit()
            resp = requests.post(url, json=payload, timeout=45)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait_s = (4 * (2 ** attempt)) + random.uniform(1.0, 3.5)
                logger.warning(
                    f"Gemini HTTP {resp.status_code} for '{title[:30]}...'. Backing off {wait_s:.1f}s..."
                )
                time.sleep(wait_s)
                continue
            resp.raise_for_status()

            data = resp.json()
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(raw_text)

            tag = parsed.get("tag", "").strip()
            if "rev" in tag.lower():
                parsed["tag"] = "Review"
            elif "gadget" in tag.lower():
                parsed["tag"] = "Gadgets"
            elif "game" in tag.lower():
                parsed["tag"] = "Gaming"
            elif "tech" in tag.lower():
                parsed["tag"] = "Technology"
            elif "foot" in tag.lower():
                parsed["tag"] = "Football"
            else:
                parsed["tag"] = category_hint

            thai_t = (parsed.get("thai_title") or "").strip()
            full_p = parsed.get("full_paragraphs") or []
            bullets = parsed.get("bullet_points") or []
            if (
                has_thai_chars(thai_t)
                and isinstance(full_p, list)
                and any(has_thai_chars(p) for p in full_p if isinstance(p, str))
                and isinstance(bullets, list)
                and len(bullets) > 0
            ):
                parsed["_translation_failed"] = False
                return parsed

            wait_s = (2 * (2 ** attempt)) + random.uniform(0.5, 2.0)
            logger.warning(f"Incomplete Thai output for '{title[:30]}...'; retrying in {wait_s:.1f}s")
            time.sleep(wait_s)
        except Exception as e:
            wait_s = (4 * (2 ** attempt)) + random.uniform(1.0, 3.5)
            logger.warning(f"Gemini attempt {attempt + 1} failed for '{title[:30]}...': {e} (wait {wait_s:.1f}s)")
            time.sleep(wait_s)

    return {
        "thai_title": title,
        "tag": category_hint,
        "bullet_points": [item_info.get("description", title)],
        "full_paragraphs": item_info.get("raw_paragraphs", [])[:8],
        "_translation_failed": True,
    }


def sanitize_article(item: dict) -> dict:
    """Return a clean dictionary without internal temporary fields."""
    return {k: v for k, v in item.items() if not k.startswith("_") and k != "raw_paragraphs"}


def update_ui_files(news_list: list[dict]):
    """Update README.md and index.html with minimal header, custom logo, 5 categories, font controls, and media layout."""
    clean_news = [sanitize_article(x) for x in news_list]
    now_th = to_thailand_time(datetime.datetime.now(datetime.timezone.utc))

    football_count = sum(1 for x in clean_news if x.get("tag") == "Football")
    tech_count = sum(1 for x in clean_news if x.get("tag") == "Technology")
    gadgets_count = sum(1 for x in clean_news if x.get("tag") == "Gadgets")
    gaming_count = sum(1 for x in clean_news if x.get("tag") == "Gaming")
    review_count = sum(1 for x in clean_news if x.get("tag") == "Review")

    # 1. README.md
    readme_lines = [
        "# ⚡ Pure News — Automated News & Game Reviews Aggregator",
        "",
        "> **Live Automated News Reader:** Auto-scraped from BBC Premier League, The Verge, Mashable SEA Gadgets, IGN Japan Games, and IGN Japan Reviews.",
        f"> *อัปเดตล่าสุด: `{now_th}`* • *รวมทั้งหมด: `{len(clean_news)} รายการ`* (`⚽ Premier League: {football_count}` | `⚡ Tech: {tech_count}` | `📱 Gadgets: {gadgets_count}` | `🎮 IGN Games: {gaming_count}` | `⭐ Reviews: {review_count}`)",
        "",
        "### 🌐 [กดที่นี่เพื่อเปิดหน้าเว็บอ่านข่าว (GitHub Pages)](https://puresinghara.github.io/pure-news/)",
        "",
        "---",
        "",
        f"### 📰 ตารางสรุปข่าวและรีวิวล่าสุด ({len(clean_news)} รายการ)",
        "",
        "| หมวดหมู่ | หัวข้อข่าว & ประเด็นสำคัญ | เวลาเผยแพร่ (เวลาไทย) | แหล่งข่าว |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for item in clean_news:
        score_str = f" ⭐{item['review_score']}/10" if item.get("review_score") else ""
        tag_badge = f"`[{item['tag']}{score_str}]`"
        bullets_md = "<br>".join([f"• {b}" for b in item.get("bullet_points", [])[:3]])
        headline = f"**{item['thai_title']}**<br>{bullets_md}"
        link = f"[อ่านต้นฉบับ]({item['link']})"
        pub = item.get("published_th", item.get("published_at", ""))
        readme_lines.append(f"| {tag_badge} | {headline} | {pub} | {link} |")

    readme_lines.append("")
    readme_lines.append("---")
    readme_lines.append("*Automated 100% by GitHub Actions and Google Gemini AI.*")

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(readme_lines))

    # 2. index.html
    json_data_str = json.dumps(clean_news, ensure_ascii=False).replace("</", "<\\/")
    html_content = f'''<!DOCTYPE html>
<html lang="th" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Pure News</title>
  <link rel="icon" type="image/jpeg" href="logo.jpg">
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;700;800&family=Prompt:wght@300;400;500;600;700&display=swap');
    body {{ font-family: 'Prompt', sans-serif; }}
    .brand-title {{ font-family: 'Chakra Petch', sans-serif; letter-spacing: -0.02em; }}
  </style>
</head>
<body class="bg-[#09090b] text-zinc-100 min-h-screen antialiased selection:bg-[#ff3366] selection:text-white">

  <!-- Clean Minimal Header (Logo + Pure News + Category Tabs) -->
  <header class="border-b border-zinc-800 bg-[#0c0c0e]/95 sticky top-0 z-40 backdrop-blur-md">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 py-3.5 flex flex-col md:flex-row md:items-center justify-between gap-4">
      
      <!-- Brand Logo -->
      <div class="flex items-center gap-3">
        <img src="logo.jpg" alt="Pure News" class="w-10 h-10 rounded-md object-cover border border-zinc-700/80 shadow-sm bg-[#faf7f2]">
        <h1 class="brand-title text-2xl md:text-3xl font-extrabold uppercase text-white tracking-wider leading-none">
          PURE NEWS
        </h1>
      </div>

      <!-- Clean Category Filter Tabs -->
      <div class="flex items-center gap-2 overflow-x-auto pb-1 md:pb-0">
        <button onclick="filterTag('ALL')" id="filterALL" class="px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded transition whitespace-nowrap">ALL</button>
        <button onclick="filterTag('Football')" id="filterFootball" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded transition whitespace-nowrap">PREMIER LEAGUE</button>
        <button onclick="filterTag('Technology')" id="filterTechnology" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-cyan-400 font-mono text-xs rounded transition whitespace-nowrap">THE VERGE</button>
        <button onclick="filterTag('Gadgets')" id="filterGadgets" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-emerald-400 font-mono text-xs rounded transition whitespace-nowrap">GADGETS</button>
        <button onclick="filterTag('Gaming')" id="filterGaming" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-amber-400 font-mono text-xs rounded transition whitespace-nowrap">GAMING</button>
        <button onclick="filterTag('Review')" id="filterReview" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-purple-400 font-mono text-xs rounded transition whitespace-nowrap">GAME REVIEWS</button>
        <button onclick="refreshLiveNews(this)" title="ดึงข้อมูลข่าวล่าสุด" class="p-1.5 bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 text-zinc-300 rounded transition ml-1 shrink-0">
          <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
        </button>
      </div>

    </div>
  </header>

  <!-- Main Container -->
  <main class="max-w-7xl mx-auto px-4 sm:px-6 py-8">
    <div id="newsGrid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      <!-- Cards rendered here -->
    </div>
  </main>

  <!-- Top-Pinned Modal Dialog -->
  <div id="modalOverlay" onclick="if(event.target===this) closeModal()" class="fixed inset-0 bg-black/85 backdrop-blur-md z-50 hidden flex items-start sm:items-center justify-center p-0 sm:p-4 overflow-y-auto">
    <div class="w-full max-w-3xl bg-[#0c0c0e] sm:border sm:border-zinc-800 sm:rounded-lg shadow-2xl overflow-hidden my-0 sm:my-auto max-h-screen sm:max-h-[92vh] flex flex-col">
      
      <!-- Modal Header (Sticky) -->
      <div class="flex items-center justify-between px-3.5 sm:px-6 py-3 border-b border-zinc-800 bg-[#0e0e11] sticky top-0 z-10 gap-2">
        <div class="flex items-center gap-2 min-w-0">
          <span id="modalTag" class="px-2 py-0.5 rounded text-[11px] sm:text-xs font-mono font-bold uppercase tracking-wider bg-[#ff3366]/15 text-[#ff3366] border border-[#ff3366]/30 truncate"></span>
        </div>
        <div class="flex items-center gap-1.5 sm:gap-2 shrink-0">
          <div id="ttsControls" class="flex items-center bg-zinc-900 border border-zinc-800 rounded-full p-0.5">
            <button id="ttsBtn" onclick="toggleSpeech()" title="ฟังเสียงอ่านข่าว" class="px-2.5 py-1 text-xs font-mono font-bold text-emerald-400 hover:text-white hover:bg-zinc-800 rounded-full transition flex items-center gap-1">
              <span id="ttsIcon">🔊</span>
              <span id="ttsLabel" class="hidden sm:inline">ฟังข่าว</span>
            </button>
            <button id="ttsRateBtn" onclick="cycleSpeechRate()" title="ความเร็วเสียงอ่าน" class="px-2 py-1 text-[11px] font-mono text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-full transition">1x</button>
          </div>
          <div class="flex items-center bg-zinc-900 border border-zinc-800 rounded-full p-0.5">
            <button onclick="changeFontSize(-1)" title="ลดขนาดตัวอักษร" class="px-2 sm:px-2.5 py-1 text-xs font-mono font-bold text-zinc-300 hover:text-white hover:bg-zinc-800 rounded-full transition">A-</button>
            <span id="fontSizeLabel" class="hidden sm:inline text-[11px] font-mono text-zinc-500 px-1.5 select-none">100%</span>
            <button onclick="changeFontSize(1)" title="เพิ่มขนาดตัวอักษร" class="px-2 sm:px-2.5 py-1 text-xs font-mono font-bold text-zinc-300 hover:text-white hover:bg-zinc-800 rounded-full transition">A+</button>
          </div>
          <button onclick="closeModal()" aria-label="Close" class="w-8 h-8 rounded-full bg-zinc-900 hover:bg-zinc-800 text-zinc-400 hover:text-white flex items-center justify-center text-xl transition leading-none">&times;</button>
        </div>
      </div>

      <!-- Modal Body (Always resets to scrollTop = 0) -->
      <div id="modalBody" class="p-5 sm:p-6 md:p-8 overflow-y-auto space-y-5 sm:space-y-6">
        <!-- HD Cover Image (16:9 aspect-video so cover never gets awkwardly cropped) -->
        <div class="w-full aspect-video overflow-hidden rounded-md border border-zinc-800 bg-zinc-950 relative">
          <img id="modalImg" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7" referrerpolicy="no-referrer" alt="" class="w-full h-full object-cover">
          <div id="modalScoreBadge" class="hidden absolute top-3 right-3 bg-[#ff3366] text-white font-mono font-extrabold text-sm px-3 py-1.5 rounded shadow-xl border border-white/20"></div>
        </div>

        <!-- Sub-Tag & Publish Date Row -->
        <div class="flex items-center flex-wrap gap-2 text-xs font-mono text-zinc-400">
          <span id="modalSubTag" class="hidden px-2 py-0.5 rounded text-[11px] font-mono font-semibold uppercase tracking-wider bg-zinc-800 text-zinc-200 border border-zinc-700"></span>
          <span id="modalDate" class="text-xs font-mono text-zinc-400"></span>
        </div>

        <!-- Title -->
        <h2 id="modalTitle" class="brand-title text-xl md:text-2xl font-bold text-white leading-snug"></h2>

        <!-- Key Facts Box -->
        <div class="bg-black/60 border-l-2 border-[#ff3366] p-4 rounded-sm">
          <div class="text-xs font-mono text-[#ff3366] uppercase font-bold tracking-wider mb-2">📌 สรุปประเด็นสำคัญ (KEY FACTS):</div>
          <ul id="modalBullets" class="space-y-2 text-sm text-zinc-200"></ul>
        </div>

        <!-- Video Section (Shown when article has YouTube trailers/videos) -->
        <div id="modalVideoSection" class="hidden space-y-3 pt-1">
          <div class="text-xs font-mono text-red-400 uppercase tracking-widest flex items-center gap-1.5 font-bold">
            <span>🎬 วิดีโอตัวอย่าง / TRAILER ในข่าว :</span>
          </div>
          <div id="modalVideoContainer" class="space-y-4"></div>
        </div>

        <!-- Full Multi-Paragraph Translation with Interleaved Magazine Screenshots -->
        <div class="space-y-4 pt-2">
          <div class="text-xs font-mono text-zinc-500 uppercase tracking-widest">// เนื้อหาฉบับเต็ม :</div>
          <div id="modalFullContent" class="space-y-4 text-zinc-300 text-sm md:text-base leading-relaxed font-light"></div>
        </div>

        <!-- Remaining Screenshots Gallery -->
        <div id="modalGallerySection" class="hidden pt-4 border-t border-zinc-800/80">
          <div class="text-xs font-mono text-amber-400 uppercase tracking-widest mb-3 flex items-center gap-1.5">
            <span>📸 ภาพประกอบเพิ่มเติม :</span>
          </div>
          <div id="modalGalleryGrid" class="grid grid-cols-2 sm:grid-cols-3 gap-2.5"></div>
        </div>
      </div>

      <!-- Modal Footer -->
      <div class="px-6 py-4 border-t border-zinc-800 bg-[#0e0e11] flex items-center justify-between">
        <a id="modalSource" href="#" target="_blank" rel="noopener noreferrer" class="px-4 py-2 bg-zinc-900 border border-zinc-700 text-xs font-mono text-white rounded-sm hover:bg-zinc-800 flex items-center gap-1.5 transition">
          <span>อ่านต้นฉบับเต็ม</span>
          <span>→</span>
        </a>
        <button onclick="closeModal()" class="px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-xs font-mono text-zinc-300 rounded-sm transition">ปิด</button>
      </div>

    </div>
  </div>

  <script>
    const TRANSPARENT_GIF = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';
    const SVG_FALLBACK_URI = "data:image/svg+xml;charset=UTF-8," + encodeURIComponent(
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 450">' +
      '<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">' +
      '<stop offset="0%" stop-color="#18181b"/><stop offset="100%" stop-color="#09090b"/>' +
      '</linearGradient></defs>' +
      '<rect width="800" height="450" fill="url(#g)"/>' +
      '<text x="50%" y="48%" dominant-baseline="middle" text-anchor="middle" fill="#ff3366" font-family="monospace" font-size="36" font-weight="bold">PURE NEWS</text>' +
      '<text x="50%" y="58%" dominant-baseline="middle" text-anchor="middle" fill="#71717a" font-family="monospace" font-size="16">IMAGE UNAVAILABLE</text>' +
      '</svg>'
    );
    const CATEGORY_FALLBACKS = {{
      Football: 'https://images.unsplash.com/photo-1508098682722-e99c43a406b2?w=1200&q=80',
      Technology: 'https://images.unsplash.com/photo-1518770660439-4636190af475?w=1200&q=80',
      Gadgets: 'https://images.unsplash.com/photo-1519389950473-47ba0277781c?w=1200&q=80',
      Gaming: 'https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80',
      Review: 'https://images.unsplash.com/photo-1542751371-adc38448a05e?w=1200&q=80'
    }};

    const FONT_STEPS = [
      {{ pct: '85%', body: '0.875rem', bullet: '0.8125rem', title: '1.25rem' }},
      {{ pct: '100%', body: '1rem', bullet: '0.875rem', title: '1.5rem' }},
      {{ pct: '115%', body: '1.125rem', bullet: '1rem', title: '1.65rem' }},
      {{ pct: '130%', body: '1.25rem', bullet: '1.0625rem', title: '1.8rem' }},
      {{ pct: '150%', body: '1.4rem', bullet: '1.15rem', title: '2rem' }}
    ];
    let currentFontIdx = 1;
    try {{
      const savedIdx = parseInt(localStorage.getItem('pureNewsFontIdx') || '1', 10);
      if (!isNaN(savedIdx) && savedIdx >= 0 && savedIdx < FONT_STEPS.length) {{
        currentFontIdx = savedIdx;
      }}
    }} catch (e) {{}}

    let allNews = {json_data_str};
    let currentTag = 'ALL';
    let isModalOpen = false;

    const SPEECH_RATES = [1.0, 1.25, 1.5];
    let speechRateIdx = 0;
    let isSpeaking = false;
    let speechQueue = [];
    let currentSpeechStep = 0;
    let currentModalItem = null;
    let speechSessionId = 0;

    if ('speechSynthesis' in window) {{
      window.speechSynthesis.onvoiceschanged = () => {{}};
    }}

    function getThaiVoice() {{
      if (!('speechSynthesis' in window)) return null;
      const voices = window.speechSynthesis.getVoices() || [];
      const thaiVoices = voices.filter(v => v.lang && v.lang.toLowerCase().startsWith('th'));
      if (thaiVoices.length === 0) return null;
      const preferred = thaiVoices.find(v => {{
        const n = (v.name || '').toLowerCase();
        return n.includes('natural') || n.includes('premwadee') || n.includes('niwat') || n.includes('google') || n.includes('kanya');
      }});
      return preferred || thaiVoices[0];
    }}

    function highlightSpeakingParagraph(paraIdx) {{
      const activeClasses = ['bg-zinc-800/60', 'border-l-2', 'border-[#ff3366]', 'pl-3', 'py-1', 'rounded-r', 'text-white'];
      document.querySelectorAll('#modalFullContent p[data-para-idx]').forEach(p => {{
        p.classList.remove(...activeClasses);
      }});
      if (paraIdx >= 0) {{
        const target = document.querySelector(`#modalFullContent p[data-para-idx="${{paraIdx}}"]`);
        if (target) {{
          target.classList.add(...activeClasses);
        }}
      }}
    }}

    function updateTtsButtonUI() {{
      const btn = document.getElementById('ttsBtn');
      const icon = document.getElementById('ttsIcon');
      const label = document.getElementById('ttsLabel');
      if (!btn || !icon || !label) return;
      if (isSpeaking) {{
        icon.textContent = '⏹';
        label.textContent = 'หยุด';
        btn.className = 'px-2.5 py-1 text-xs font-mono font-bold text-[#ff3366] hover:text-white hover:bg-zinc-800 rounded-full transition flex items-center gap-1';
      }} else {{
        icon.textContent = '🔊';
        label.textContent = 'ฟังข่าว';
        btn.className = 'px-2.5 py-1 text-xs font-mono font-bold text-emerald-400 hover:text-white hover:bg-zinc-800 rounded-full transition flex items-center gap-1';
      }}
    }}

    function stopSpeech() {{
      speechSessionId++;
      isSpeaking = false;
      speechQueue = [];
      currentSpeechStep = 0;
      if ('speechSynthesis' in window) {{
        window.speechSynthesis.cancel();
      }}
      highlightSpeakingParagraph(-1);
      updateTtsButtonUI();
    }}

    function speakCurrentStep(sessionId) {{
      if (!isSpeaking || sessionId !== speechSessionId || currentSpeechStep >= speechQueue.length) {{
        if (sessionId === speechSessionId) stopSpeech();
        return;
      }}
      const stepItem = speechQueue[currentSpeechStep];
      highlightSpeakingParagraph(stepItem.paraIdx);

      const u = new SpeechSynthesisUtterance(stepItem.text);
      u.lang = 'th-TH';
      u.rate = SPEECH_RATES[speechRateIdx] || 1.0;
      const thVoice = getThaiVoice();
      if (thVoice) u.voice = thVoice;

      u.onend = () => {{
        if (sessionId === speechSessionId && isSpeaking) {{
          currentSpeechStep++;
          speakCurrentStep(sessionId);
        }}
      }};
      u.onerror = (e) => {{
        if (e && e.error === 'interrupted') return;
        if (sessionId === speechSessionId && isSpeaking) {{
          currentSpeechStep++;
          speakCurrentStep(sessionId);
        }}
      }};
      window.speechSynthesis.speak(u);
    }}

    function startSpeech(resumeStep = 0) {{
      if (!('speechSynthesis' in window) || !currentModalItem) return;
      window.speechSynthesis.cancel();
      speechSessionId++;
      const sessionId = speechSessionId;

      speechQueue = [];
      const headline = (currentModalItem.thai_title || currentModalItem.original_title || '').trim();
      if (headline) {{
        speechQueue.push({{ text: headline, paraIdx: -1 }});
      }}
      const bodyParas = Array.isArray(currentModalItem.full_paragraphs) ? currentModalItem.full_paragraphs : [];
      bodyParas.forEach((p, idx) => {{
        if (p && String(p).trim()) {{
          speechQueue.push({{ text: String(p).trim(), paraIdx: idx }});
        }}
      }});

      if (speechQueue.length === 0) return;
      currentSpeechStep = Math.max(0, Math.min(resumeStep, speechQueue.length - 1));
      isSpeaking = true;
      updateTtsButtonUI();
      speakCurrentStep(sessionId);
    }}

    function toggleSpeech() {{
      if (isSpeaking) {{
        stopSpeech();
      }} else {{
        startSpeech(0);
      }}
    }}

    function cycleSpeechRate() {{
      speechRateIdx = (speechRateIdx + 1) % SPEECH_RATES.length;
      const rateBtn = document.getElementById('ttsRateBtn');
      if (rateBtn) rateBtn.textContent = SPEECH_RATES[speechRateIdx] + 'x';
      if (isSpeaking) {{
        startSpeech(currentSpeechStep);
      }}
    }}

    window.addEventListener('beforeunload', () => stopSpeech());

    function applyFontSize() {{
      const step = FONT_STEPS[currentFontIdx] || FONT_STEPS[1];
      const fullEl = document.getElementById('modalFullContent');
      const bulletEl = document.getElementById('modalBullets');
      const titleEl = document.getElementById('modalTitle');
      const labelEl = document.getElementById('fontSizeLabel');
      if (fullEl) fullEl.style.fontSize = step.body;
      if (bulletEl) bulletEl.style.fontSize = step.bullet;
      if (titleEl) titleEl.style.fontSize = step.title;
      if (labelEl) labelEl.textContent = step.pct;
    }}

    function changeFontSize(delta) {{
      const next = Math.max(0, Math.min(FONT_STEPS.length - 1, currentFontIdx + delta));
      if (next !== currentFontIdx) {{
        currentFontIdx = next;
        try {{ localStorage.setItem('pureNewsFontIdx', String(currentFontIdx)); }} catch (e) {{}}
        applyFontSize();
      }}
    }}

    function escapeHtml(str) {{
      if (str === null || str === undefined) return '';
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function handleImgError(img, tag) {{
      const step = parseInt(img.getAttribute('data-fb-step') || '0', 10);
      if (step === 0) {{
        img.setAttribute('data-fb-step', '1');
        img.src = CATEGORY_FALLBACKS[tag] || CATEGORY_FALLBACKS.Gaming;
      }} else {{
        img.onerror = null;
        img.src = SVG_FALLBACK_URI;
      }}
    }}

    function handleGalleryImgError(img) {{
      img.onerror = null;
      const wrapper = img.parentElement;
      if (wrapper) wrapper.remove();
      const grid = document.getElementById('modalGalleryGrid');
      const sec = document.getElementById('modalGallerySection');
      if (grid && sec && grid.children.length === 0) {{
        sec.classList.add('hidden');
      }}
    }}

    async function refreshLiveNews(btn) {{
      const svg = btn ? btn.querySelector('svg') : null;
      if (svg) svg.classList.add('animate-spin');
      try {{
        const r = await fetch('data/news.json?t=' + Date.now(), {{ cache: 'no-store' }});
        if (r.ok) {{
          const data = await r.json();
          if (Array.isArray(data) && data.length > 0) {{
            allNews = data;
            render();
          }}
        }}
      }} catch (err) {{
        // Ignore offline/file protocol fetch failures
      }} finally {{
        if (svg) svg.classList.remove('animate-spin');
      }}
    }}

    function filterTag(tag) {{
      currentTag = tag;
      ['ALL', 'Football', 'Technology', 'Gadgets', 'Gaming', 'Review'].forEach(t => {{
        const btn = document.getElementById('filter' + t);
        if (btn) {{
          if (t === tag) {{
            btn.className = "px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded transition whitespace-nowrap";
          }} else {{
            let textColor = 'text-zinc-300';
            if (t === 'Gaming') textColor = 'text-amber-400';
            if (t === 'Technology') textColor = 'text-cyan-400';
            if (t === 'Gadgets') textColor = 'text-emerald-400';
            if (t === 'Review') textColor = 'text-purple-400';
            btn.className = `px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 ${{textColor}} font-mono text-xs rounded transition whitespace-nowrap`;
          }}
        }}
      }});
      render();
      window.scrollTo({{ top: 0, left: 0, behavior: 'instant' }});
      document.documentElement.scrollTop = 0;
      document.body.scrollTop = 0;
      requestAnimationFrame(() => {{
        window.scrollTo(0, 0);
      }});
    }}

    function render() {{
      const grid = document.getElementById('newsGrid');
      grid.innerHTML = '';

      const filtered = allNews
        .filter(item => (currentTag === 'ALL') || (item.tag === currentTag))
        .slice()
        .sort((a, b) => (b.published_ts || 0) - (a.published_ts || 0));

      if (filtered.length === 0) {{
        grid.innerHTML = '<div class="col-span-3 text-center py-16 text-zinc-500 font-mono">ไม่มีข่าวในหมวดนี้</div>';
        return;
      }}

      filtered.forEach((item, cardIdx) => {{
        const originalIndex = allNews.indexOf(item);
        const div = document.createElement('div');
        div.className = 'bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition flex flex-col justify-between overflow-hidden rounded cursor-pointer group';
        div.onclick = () => openModal(originalIndex);
        
        const bulletsHtml = (item.bullet_points || []).slice(0, 3).map(b => 
          `<li class="flex items-start gap-1.5"><span class="text-[#ff3366] font-bold">›</span><span>${{escapeHtml(b)}}</span></li>`
        ).join('');

        let tagColor = 'text-[#ff3366]';
        if (item.tag === 'Technology') tagColor = 'text-cyan-400';
        else if (item.tag === 'Gadgets') tagColor = 'text-emerald-400';
        else if (item.tag === 'Gaming') tagColor = 'text-amber-400';
        else if (item.tag === 'Review') tagColor = 'text-purple-400';

        const reviewBadge = item.review_score ? `<div class="absolute top-2.5 right-2.5 bg-[#ff3366] text-white font-mono font-extrabold text-xs px-2.5 py-1 rounded shadow-lg border border-white/20">⭐ ${{escapeHtml(item.review_score)}} / 10</div>` : '';

        const imgSrc = item.cover_image_url || CATEGORY_FALLBACKS[item.tag] || CATEGORY_FALLBACKS.Gaming;
        const loadingAttr = cardIdx < 6 ? 'eager' : 'lazy';
        const safeTag = escapeHtml(item.tag || '');
        const safeSubTag = escapeHtml(item.sub_tag || '');
        const subTagBadge = safeSubTag ? `<span class="px-1.5 py-0.5 rounded bg-zinc-800/90 text-zinc-200 border border-zinc-700/80 text-[10px] font-mono font-semibold tracking-wide uppercase">${{safeSubTag}}</span>` : '';
        const safeDate = escapeHtml(item.published_th || item.published_at || '');
        const safeTitle = escapeHtml(item.thai_title || item.original_title || '');

        div.innerHTML = `
          <div>
            <div class="w-full aspect-video overflow-hidden bg-zinc-950 relative">
              <img src="${{escapeHtml(imgSrc)}}" data-tag="${{safeTag}}" referrerpolicy="no-referrer" loading="${{loadingAttr}}" decoding="async" class="w-full h-full object-cover group-hover:scale-105 transition duration-500" onerror="handleImgError(this, this.dataset.tag)">
              ${{reviewBadge}}
            </div>
            <div class="p-5">
              <div class="flex items-center flex-wrap gap-1.5 text-[11px] font-mono mb-2.5">
                <span class="${{tagColor}} font-bold uppercase">${{safeTag}}</span>
                ${{subTagBadge}}
                <span class="text-zinc-600">•</span>
                <span class="text-zinc-500">${{safeDate}}</span>
              </div>
              <h3 class="brand-title text-base font-bold text-white group-hover:text-[#ff3366] transition mb-3 leading-snug line-clamp-2">
                ${{safeTitle}}
              </h3>
              <ul class="text-xs text-zinc-300 space-y-1.5 font-light">
                ${{bulletsHtml}}
              </ul>
            </div>
          </div>
          <div class="p-5 pt-0 border-t border-zinc-800/80 text-[11px] font-mono text-zinc-500 flex items-center justify-between">
            <span class="text-zinc-300 group-hover:text-white transition">อ่านรายละเอียดเต็ม →</span>
            <span>${{safeSubTag || (safeTag + ' FEED')}}</span>
          </div>
        `;
        grid.appendChild(div);
      }});
    }}

    function openModal(idx) {{
      const item = allNews[idx];
      if (!item) return;

      stopSpeech();
      currentModalItem = item;
      const ttsWrap = document.getElementById('ttsControls');
      if (ttsWrap && !('speechSynthesis' in window)) {{
        ttsWrap.classList.add('hidden');
      }}

      isModalOpen = true;
      history.pushState({{ modalOpen: true, newsId: item.id }}, '', '#news-' + item.id);

      document.getElementById('modalTag').textContent = item.tag || '';
      const modalSubTag = document.getElementById('modalSubTag');
      if (modalSubTag) {{
        if (item.sub_tag) {{
          modalSubTag.textContent = item.sub_tag;
          modalSubTag.classList.remove('hidden');
        }} else {{
          modalSubTag.classList.add('hidden');
          modalSubTag.textContent = '';
        }}
      }}
      const modalImg = document.getElementById('modalImg');
      modalImg.onerror = null;
      modalImg.src = TRANSPARENT_GIF;
      modalImg.setAttribute('data-fb-step', '0');
      modalImg.onerror = () => handleImgError(modalImg, item.tag);
      modalImg.src = item.cover_image_url || CATEGORY_FALLBACKS[item.tag] || SVG_FALLBACK_URI;

      const scoreBadge = document.getElementById('modalScoreBadge');
      if (scoreBadge) {{
        if (item.review_score) {{
          scoreBadge.textContent = `⭐ ${{item.review_score}} / 10`;
          scoreBadge.classList.remove('hidden');
        }} else {{
          scoreBadge.classList.add('hidden');
          scoreBadge.textContent = '';
        }}
      }}

      document.getElementById('modalTitle').textContent = item.thai_title || item.original_title || '';
      document.getElementById('modalDate').textContent = item.published_th || item.published_at || '';

      const bulletsList = document.getElementById('modalBullets');
      bulletsList.innerHTML = '';
      (item.bullet_points || []).forEach(b => {{
        const li = document.createElement('li');
        li.className = 'flex items-start gap-2';
        li.innerHTML = `<span class="text-[#ff3366] font-bold">›</span><span>${{escapeHtml(b)}}</span>`;
        bulletsList.appendChild(li);
      }});

      // Embedded YouTube Video Section
      const videoSec = document.getElementById('modalVideoSection');
      const videoContainer = document.getElementById('modalVideoContainer');
      videoContainer.innerHTML = '';
      if (Array.isArray(item.videos) && item.videos.length > 0) {{
        videoSec.classList.remove('hidden');
        item.videos.forEach(vUrl => {{
          const wrap = document.createElement('div');
          wrap.className = 'w-full aspect-video rounded-md overflow-hidden border border-zinc-800 bg-black shadow-lg';
          wrap.innerHTML = `<iframe src="${{escapeHtml(vUrl)}}?rel=0" class="w-full h-full" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen loading="lazy"></iframe>`;
          videoContainer.appendChild(wrap);
        }});
      }} else {{
        videoSec.classList.add('hidden');
      }}

      // Interleave up to 4 inline screenshots cleanly between paragraphs (Magazine layout)
      const fullContentDiv = document.getElementById('modalFullContent');
      fullContentDiv.innerHTML = '';
      const paras = Array.isArray(item.full_paragraphs) && item.full_paragraphs.length > 0 ? item.full_paragraphs : (item.bullet_points || []);
      const images = Array.isArray(item.gallery_images) ? item.gallery_images.slice() : [];
      const maxInline = Math.min(images.length, 4);
      const step = maxInline > 0 ? Math.max(2, Math.floor(paras.length / (maxInline + 1))) : 0;
      let inlineUsedCount = 0;

      paras.forEach((p, pIdx) => {{
        const pElem = document.createElement('p');
        pElem.setAttribute('data-para-idx', String(pIdx));
        pElem.className = 'transition-all duration-200';
        pElem.textContent = p;
        fullContentDiv.appendChild(pElem);

        if (step > 0 && inlineUsedCount < maxInline && (pIdx + 1) % step === 0 && (pIdx + 1) < paras.length) {{
          const imgUrl = images[inlineUsedCount++];
          const fig = document.createElement('figure');
          fig.className = 'my-5 rounded-md overflow-hidden border border-zinc-800 bg-zinc-950';
          fig.innerHTML = `<a href="${{escapeHtml(imgUrl)}}" target="_blank" rel="noopener noreferrer" class="block aspect-video overflow-hidden group"><img src="${{escapeHtml(imgUrl)}}" referrerpolicy="no-referrer" loading="lazy" decoding="async" onerror="this.closest('figure').remove()" class="w-full h-full object-cover group-hover:scale-105 transition duration-500"></a>`;
          fullContentDiv.appendChild(fig);
        }}
      }});

      // Remaining Screenshots Gallery (only show images not already displayed inline)
      const gallerySec = document.getElementById('modalGallerySection');
      const galleryGrid = document.getElementById('modalGalleryGrid');
      galleryGrid.innerHTML = '';
      const remainingImages = images.slice(inlineUsedCount);
      if (remainingImages.length > 0) {{
        gallerySec.classList.remove('hidden');
        remainingImages.forEach(imgUrl => {{
          const a = document.createElement('a');
          a.href = imgUrl;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'block aspect-video overflow-hidden rounded bg-zinc-950 border border-zinc-800 hover:border-[#ff3366] transition group relative';
          a.innerHTML = `<img src="${{escapeHtml(imgUrl)}}" referrerpolicy="no-referrer" loading="lazy" decoding="async" onerror="handleGalleryImgError(this)" class="w-full h-full object-cover group-hover:scale-105 transition duration-300">`;
          galleryGrid.appendChild(a);
        }});
      }} else {{
        gallerySec.classList.add('hidden');
      }}

      document.getElementById('modalSource').href = item.link || '#';
      applyFontSize();
      
      // Open and pin scroll position at the top cover image
      const modalOverlay = document.getElementById('modalOverlay');
      const modalBody = document.getElementById('modalBody');
      modalOverlay.classList.remove('hidden');
      document.body.style.overflow = 'hidden';

      modalOverlay.scrollTop = 0;
      if (modalBody) modalBody.scrollTop = 0;
      requestAnimationFrame(() => {{
        modalOverlay.scrollTop = 0;
        if (modalBody) modalBody.scrollTop = 0;
      }});
    }}

    function closeModal(fromHistory = false) {{
      if (!isModalOpen) return;
      isModalOpen = false;
      stopSpeech();
      currentModalItem = null;
      document.getElementById('modalVideoContainer').innerHTML = '';
      document.getElementById('modalOverlay').classList.add('hidden');
      document.body.style.overflow = '';

      if (!fromHistory && history.state && history.state.modalOpen) {{
        history.back();
      }}
    }}

    window.addEventListener('popstate', () => {{
      if (isModalOpen) {{
        closeModal(true);
      }}
    }});

    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape' && isModalOpen) {{
        closeModal();
      }}
    }});

    window.addEventListener('DOMContentLoaded', () => {{
      const hash = window.location.hash;
      if (hash && hash.startsWith('#news-')) {{
        const newsId = hash.replace('#news-', '');
        const idx = allNews.findIndex(n => String(n.id) === newsId);
        if (idx !== -1) {{
          openModal(idx);
        }}
      }}
    }});

    render();
  </script>
</body>
</html>'''

    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)


def fetch_feed_entries(url: str, category: str, now_utc: datetime.datetime) -> list[dict]:
    """Fetch entries from a single RSS/Atom feed with timeout, GUID, UTC timestamp, and thumbnail extraction."""
    results = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        if getattr(feed, "bozo", False) and not getattr(feed, "entries", None):
            logger.warning(f"[{category}] Malformed feed {url}: {getattr(feed, 'bozo_exception', 'unknown')}")
            return results

        for entry in feed.entries:
            link = (getattr(entry, "link", "") or "").strip()
            title = (getattr(entry, "title", "") or "").strip()
            if not link or any(x in title.lower() for x in ["who am i", "quiz", "football daily"]):
                continue

            entry_tags = [
                t.get("term", "") for t in (getattr(entry, "tags", None) or [])
                if isinstance(t, dict)
            ]
            if category == "Review":
                if any(x in entry_tags for x in ["映画", "アニメ", "ドラマ", "エンタメ"]) or title.startswith("映画"):
                    continue
            elif category == "Gaming":
                tags_lower = [t.lower() for t in entry_tags]
                if "レビュー" in entry_tags or "レビュー" in title or "review" in tags_lower or " review" in title.lower():
                    continue

            raw_guid = getattr(entry, "id", "") or getattr(entry, "guid", "") or link
            canonical_link = normalize_url(link)
            guid = normalize_guid(raw_guid, link)

            pub_tuple = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
            if pub_tuple:
                pub_dt = datetime.datetime.fromtimestamp(calendar.timegm(pub_tuple), tz=datetime.timezone.utc)
                if pub_dt > now_utc:
                    pub_dt = now_utc
            else:
                pub_dt = now_utc

            feed_img = extract_entry_thumbnail(entry)
            raw_summary = getattr(entry, "summary", "") or getattr(entry, "description", "")

            results.append({
                "guid": guid,
                "title": title,
                "link": link,
                "canonical_link": canonical_link,
                "description": clean_text(raw_summary),
                "published_ts": int(pub_dt.timestamp()),
                "published_at": pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "published_th": to_thailand_time(pub_dt),
                "category_hint": category,
                "feed_image": feed_img,
                "_pub_dt": pub_dt,
            })
    except Exception as e:
        logger.warning(f"[{category}] Feed error for {url}: {e}")
    return results


def fetch_category_feeds(cat: str, feeds: list[str], now_utc: datetime.datetime) -> list[dict]:
    """Fetch all feeds for a category in parallel with isolated fault handling (Promise.allSettled pattern)."""
    candidates = []
    seen_guids = set()
    seen_links = set()
    seen_titles = set()

    max_workers = min(6, max(1, len(feeds)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_feed_entries, url, cat, now_utc): url for url in feeds}
        for future in concurrent.futures.as_completed(future_to_url):
            feed_url = future_to_url[future]
            try:
                entries = future.result()
            except Exception as e:
                logger.warning(f"[{cat}] Unhandled feed exception for {feed_url}: {e}")
                continue
            for e in entries:
                g = e["guid"]
                cl = e["canonical_link"]
                tl = (e.get("title") or "").strip().lower()
                if g in seen_guids or cl in seen_links or (tl and tl in seen_titles):
                    continue
                seen_guids.add(g)
                seen_links.add(cl)
                if tl:
                    seen_titles.add(tl)
                candidates.append(e)

    return candidates


def normalize_existing_item(it: dict, now_utc: datetime.datetime) -> dict | None:
    """Normalize schema, GUID, canonical link, timestamps, and images for an existing DB entry."""
    link = (it.get("link") or "").strip()
    if not link:
        return None

    tag = it.get("tag", "") or it.get("category_hint", "")
    if "chelsea" in tag.lower() or "foot" in tag.lower():
        tag = "Football"
    elif "tech" in tag.lower():
        tag = "Technology"
    elif "gadget" in tag.lower():
        tag = "Gadgets"
    elif "rev" in tag.lower():
        tag = "Review"
    elif "game" in tag.lower() or "gaming" in tag.lower():
        tag = "Gaming"
    it["tag"] = tag
    it["category_hint"] = tag

    canonical_link = normalize_url(link)
    guid = normalize_guid(it.get("guid", ""), link)
    it["canonical_link"] = canonical_link
    it["guid"] = guid

    pub_dt = parse_existing_timestamp(it, now_utc)
    it["published_ts"] = int(pub_dt.timestamp())
    it["published_at"] = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    it["published_th"] = to_thailand_time(pub_dt)

    if it.get("gallery_images"):
        it["gallery_images"] = [
            make_hd_image_url(g) for g in it["gallery_images"]
            if _is_usable_image_url(g)
        ]
    if "videos" not in it or not isinstance(it["videos"], list):
        it["videos"] = []
    if "review_score" not in it:
        it["review_score"] = ""
    it["sub_tag"] = extract_sub_tag(it)
    it["cover_image_url"] = make_hd_image_url(it.get("cover_image_url", ""))
    return it


def _deduplicate_cover_images(articles: list[dict], cat: str, global_used_images: set):
    for idx_art, art in enumerate(articles):
        img = (art.get("cover_image_url") or "").strip()
        if not img or img in global_used_images:
            found_unique = False
            for g in art.get("gallery_images", []):
                if g and g not in global_used_images:
                    art["cover_image_url"] = g
                    global_used_images.add(g)
                    found_unique = True
                    break
            if not found_unique:
                if cat in ("Gaming", "Review"):
                    fallback_img = CURATED_GAMING_COVERS[idx_art % len(CURATED_GAMING_COVERS)]
                else:
                    base_fb = CATEGORY_FALLBACK_COVERS.get(cat, CATEGORY_FALLBACK_COVERS["Gaming"])
                    fallback_img = f"{base_fb}&sig={cat.lower()}-{idx_art}"
                art["cover_image_url"] = fallback_img
                global_used_images.add(fallback_img)
        else:
            global_used_images.add(img)


def load_existing_database(now_utc: datetime.datetime) -> tuple[list[dict], dict[str, dict], dict[str, dict], dict[str, list[dict]]]:
    existing_news = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                existing_news = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load existing database {DATA_FILE}: {e}")

    normalized_items = []
    existing_by_guid = {}
    existing_by_link = {}
    existing_by_cat = {cat: [] for cat in CATEGORIES_ORDER}

    for raw in existing_news:
        it = normalize_existing_item(dict(raw), now_utc)
        if not it:
            continue
        cat = it.get("tag", "")
        if cat == "Technology" and "theverge.com" not in it["link"].lower():
            continue
        if cat in ("Gaming", "Review") and any(bad_dom in it["link"].lower() for bad_dom in ("gematsu.com", "videogameschronicle.com")):
            continue
        normalized_items.append(it)
        existing_by_guid[it["guid"]] = it
        existing_by_link[it["canonical_link"]] = it
        existing_by_link[it["link"].strip()] = it
        if cat in existing_by_cat:
            existing_by_cat[cat].append(it)

    for cat in existing_by_cat:
        existing_by_cat[cat].sort(key=lambda x: x.get("published_ts", 0), reverse=True)

    return normalized_items, existing_by_guid, existing_by_link, existing_by_cat


def normalize_existing_database():
    """Normalize existing data/news.json schema/timestamps/order and regenerate UI without API calls."""
    logger.info("Running --normalize-only on existing database...")
    os.makedirs(DATA_DIR, exist_ok=True)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    _, _, _, existing_by_cat = load_existing_database(now_utc)

    combined_db = []
    global_used_images = set()

    for cat in CATEGORIES_ORDER:
        seen_keys = set()
        cat_items = []
        for it in existing_by_cat.get(cat, []):
            key = it["guid"] or it["canonical_link"]
            if key in seen_keys or it["canonical_link"] in seen_keys:
                continue
            seen_keys.add(key)
            seen_keys.add(it["canonical_link"])
            cat_items.append(it)

        cat_items.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
        cat_items = cat_items[:TARGET_PER_CATEGORY]
        _deduplicate_cover_images(cat_items, cat, global_used_images)
        combined_db.extend([sanitize_article(x) for x in cat_items])
        logger.info(f"[{cat}] Normalized {len(cat_items)} articles.")

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(combined_db, f, ensure_ascii=False, indent=2)

    update_ui_files(combined_db)
    logger.info(f"[✓] Normalized {len(combined_db)} stories and regenerated index.html & README.md.")


def run():
    logger.info("==================================================")
    logger.info("  STARTING PURE NEWS PIPELINE (30 PER TOPIC)")
    logger.info("==================================================")

    os.makedirs(DATA_DIR, exist_ok=True)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    twelve_hours_ago = now_utc - datetime.timedelta(hours=12)

    normalized_items, existing_by_guid, existing_by_link, existing_by_cat = load_existing_database(now_utc)
    logger.info(f"Loaded {len(normalized_items)} existing stories from database.")

    category_results = {}
    global_used_images = set()

    for cat in CATEGORIES_ORDER:
        feeds = CATEGORIES_CONFIG[cat]
        logger.info(f"--- Processing Category: {cat} ---")
        candidates = fetch_category_feeds(cat, feeds, now_utc)
        candidates.sort(key=lambda x: x["published_ts"], reverse=True)

        within_12h = [c for c in candidates if c["_pub_dt"] >= twelve_hours_ago]
        logger.info(f"[{cat}] Candidates fetched: {len(candidates)} (within 12h: {len(within_12h)})")

        selected_candidates = within_12h[:]
        if len(selected_candidates) < TARGET_PER_CATEGORY:
            logger.info(f"[{cat}] Backfilling from latest feed candidates toward {TARGET_PER_CATEGORY} articles...")
            selected_set = {c["canonical_link"] for c in selected_candidates}
            for c in candidates:
                if c["canonical_link"] not in selected_set:
                    selected_candidates.append(c)
                    selected_set.add(c["canonical_link"])
                if len(selected_candidates) >= TARGET_PER_CATEGORY:
                    break

        selected_candidates = selected_candidates[:TARGET_PER_CATEGORY]

        final_category_articles = []
        new_items_to_translate = []
        matched_existing_map = {}

        for item in selected_candidates:
            link = item["link"]
            if cat == "Technology" and "theverge.com" not in link.lower():
                continue

            existing_item = (
                existing_by_guid.get(item["guid"])
                or existing_by_link.get(item["canonical_link"])
                or existing_by_link.get(link)
            )
            if existing_item:
                matched_existing_map[item["canonical_link"]] = existing_item
                if is_valid_translation(existing_item):
                    existing_item["tag"] = cat
                    existing_item["category_hint"] = cat
                    existing_item["guid"] = item["guid"]
                    existing_item["canonical_link"] = item["canonical_link"]
                    existing_item["published_ts"] = item["published_ts"]
                    existing_item["published_at"] = item["published_at"]
                    existing_item["published_th"] = item["published_th"]
                    existing_item["cover_image_url"] = make_hd_image_url(existing_item.get("cover_image_url", ""))
                    if "videos" not in existing_item:
                        existing_item["videos"] = []
                    if "review_score" not in existing_item:
                        existing_item["review_score"] = ""
                    existing_item["sub_tag"] = extract_sub_tag(existing_item)
                    final_category_articles.append(existing_item)
                else:
                    logger.info(f"[{cat}] Existing article needs valid Thai translation: {item['title'][:45]}...")
                    new_items_to_translate.append(item)
            else:
                new_items_to_translate.append(item)

        logger.info(
            f"[{cat}] Reusing {len(final_category_articles)} cached articles. "
            f"Translating {len(new_items_to_translate)} new/pending articles."
        )

        failed_fallback_entries = []
        if new_items_to_translate:
            logger.info(f"[{cat}] Scraping full content, screenshots & videos for {len(new_items_to_translate)} articles...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                scraped_items = list(executor.map(scrape_article_details, new_items_to_translate))

            logger.info(f"[{cat}] Translating {len(scraped_items)} articles with {GEMINI_MODEL} (rate-limited)...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                translations = list(executor.map(call_gemini, scraped_items))

            for idx_new, (item, trans) in enumerate(zip(scraped_items, translations)):
                prev_existing = matched_existing_map.get(item["canonical_link"])
                entry = {
                    "id": int(time.time() * 1000) + len(final_category_articles) + idx_new,
                    "guid": item["guid"],
                    "original_title": item["title"],
                    "thai_title": trans.get("thai_title", item["title"]),
                    "tag": cat,
                    "sub_tag": "",
                    "bullet_points": trans.get("bullet_points", []),
                    "full_paragraphs": trans.get("full_paragraphs", item["raw_paragraphs"][:8]),
                    "link": item["link"],
                    "canonical_link": item["canonical_link"],
                    "cover_image_url": item["cover_image_url"],
                    "gallery_images": item.get("gallery_images", []),
                    "videos": item.get("videos", []),
                    "review_score": item.get("review_score", ""),
                    "published_ts": item["published_ts"],
                    "published_at": item["published_at"],
                    "published_th": item["published_th"],
                    "category_hint": cat,
                }
                entry["sub_tag"] = extract_sub_tag(entry)
                if trans.get("_translation_failed"):
                    if prev_existing and is_valid_translation(prev_existing):
                        final_category_articles.append(prev_existing)
                    else:
                        failed_fallback_entries.append(entry)
                    continue

                final_category_articles.append(entry)
                existing_by_guid[item["guid"]] = entry
                existing_by_link[item["canonical_link"]] = entry

        # Database backfill if RSS feeds failed or returned fewer than TARGET_PER_CATEGORY
        if len(final_category_articles) < TARGET_PER_CATEGORY:
            used_guids = {a.get("guid") for a in final_category_articles if a.get("guid")}
            used_links = {a.get("canonical_link") for a in final_category_articles if a.get("canonical_link")}
            for cached in existing_by_cat.get(cat, []):
                if cached.get("guid") in used_guids or cached.get("canonical_link") in used_links:
                    continue
                if not is_valid_translation(cached):
                    continue
                final_category_articles.append(cached)
                used_guids.add(cached.get("guid"))
                used_links.add(cached.get("canonical_link"))
                if len(final_category_articles) >= TARGET_PER_CATEGORY:
                    break

        if len(final_category_articles) < TARGET_PER_CATEGORY and failed_fallback_entries:
            for fb_entry in failed_fallback_entries:
                final_category_articles.append(fb_entry)
                if len(final_category_articles) >= TARGET_PER_CATEGORY:
                    break

        # Strict newest-first sort across reused + newly translated + backfilled items
        final_category_articles.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
        final_category_articles = final_category_articles[:TARGET_PER_CATEGORY]

        _deduplicate_cover_images(final_category_articles, cat, global_used_images)
        category_results[cat] = [sanitize_article(x) for x in final_category_articles]
        logger.info(f"[{cat}] Completed! Total articles: {len(category_results[cat])}/{TARGET_PER_CATEGORY}")

    combined_db = []
    for cat in CATEGORIES_ORDER:
        combined_db.extend(category_results.get(cat, []))

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(combined_db, f, ensure_ascii=False, indent=2)

    update_ui_files(combined_db)

    logger.info("==================================================")
    logger.info(f"[✓] Pure News pipeline complete! Maintained {len(combined_db)} total stories.")
    logger.info("==================================================")


if __name__ == "__main__":
    if "--normalize-only" in sys.argv[1:]:
        normalize_existing_database()
    else:
        run()
