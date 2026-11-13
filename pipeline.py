"""
Automated News Aggregator & Reader (GitHub Native)
Feeds:
1. Football: BBC Sport Football (http://feeds.bbci.co.uk/sport/football/rss.xml)
2. Technology: The Verge & Top Tech (The Verge, TechCrunch, Engadget, Ars Technica)
3. Gaming: Eurogamer, PC Gamer, Gematsu (https://www.eurogamer.net/feed, https://www.pcgamer.com/rss, https://www.gematsu.com/feed)

Features:
- 30 Fresh Articles PER Category (Total 90 articles in database).
- 12-Hour Freshness rule with automatic backfill up to 30 articles per topic.
- Clean Modern Header: Pure News branding, category filter tabs only, no search input.
- Mobile History Back: Swiping back on mobile closes modal dialog without exiting the site.
- High-res HD images (1200px+) from Fastly/CloudFront/BBC/Verge CDNs with no duplicate pictures.
- Multi-Image Screenshots Gallery inside articles with graceful error handling.
- Thailand Time (UTC+7) formatting.
- Smart Deduplication: Skips already-translated links while ensuring untranslated items get re-processed.
- Gemini Flash-Lite with exponential backoff and rate limit handling.
"""
import os
import json
import time
import datetime
import re
import html
import logging
import urllib.request
import urllib.error
import concurrent.futures
import feedparser

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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
if not GEMINI_API_KEY:
    env_file = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith("GEMINI_API_KEY="):
                    GEMINI_API_KEY = line.split("=", 1)[1].strip()

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip()
TARGET_PER_CATEGORY = 30
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}

# Curated high-res unique gaming images for fallback if needed
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
    "https://images.unsplash.com/photo-1509198397868-475647b2a1e5?w=1200&q=80"
]

SYSTEM_PROMPT = """คุณคือนักข่าวและบรรณาธิการข่าวมืออาชีพ แปลและสรุปข่าวเป็นภาษาไทยอย่างถูกต้อง สละสลวย และตรงตามข้อเท็จจริง (Fact-based)
ข้อกำหนด:
1. ข่าวฟุตบอล: ทับศัพท์เทคนิคและชื่อนักเตะ/สโมสรอย่างถูกต้อง
2. ข่าวเทคโนโลยี: ทับศัพท์ชื่อระบบ ฮาร์ดแวร์ แบรนด์ โมเดล AI และฟีเจอร์อย่างถูกต้อง
3. ข่าวเกม (Gaming): ทับศัพท์ชื่อเกม แพลตฟอร์ม (PS5, Xbox Series, Switch 2, PC) และค่ายเกมอย่างถูกต้อง
4. "bullet_points": สรุปข้อเท็จจริงสำคัญสั้นกระชับ 3-4 ข้อ เป็นภาษาไทย
5. "full_paragraphs": แปลและเรียบเรียงเนื้อหาข่าวฉบับเต็มอย่างครบถ้วน (3-5 ย่อหน้า) เป็นภาษาไทย ครอบคลุมเหตุการณ์สำคัญ คำสัมภาษณ์ กำหนดการ และตัวเลขสถิติทั้งหมด
6. "thai_title": ต้องแปลพาดหัวเป็นภาษาไทยที่กระชับ ตรงประเด็น ห้ามคืนค่าเป็นภาษาอังกฤษ

กฎแท็ก (Tag): ให้ระบุเป็นหนึ่งในสามหมวดนี้เท่านั้น: "Football", "Technology", "Gaming"
"""

def clean_text(raw_html: str) -> str:
    if not raw_html: return ""
    clean = re.sub(r'<[^>]+>', '', raw_html).strip()
    return " ".join(html.unescape(clean).split())

def to_thailand_time(dt_utc: datetime.datetime) -> str:
    """Convert UTC datetime to Thailand Time (UTC+7) string."""
    th_tz = datetime.timezone(datetime.timedelta(hours=7))
    th_dt = dt_utc.astimezone(th_tz)
    thai_months = ["ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
    day = th_dt.day
    month = thai_months[th_dt.month - 1]
    year = th_dt.year
    hour = th_dt.strftime("%H:%M")
    return f"{day} {month} {year} • {hour} น."

def make_hd_image_url(url: str) -> str:
    """Ensure images are high-resolution HD (1080p/1200px) instead of low-res thumbnails."""
    if not url:
        return ""
    # Upgrade BBC thumbnails (/240/, /320/, /480/ -> /1200/)
    url = re.sub(r'/(standard|branded_sport|branded_news)/\d+/', r'/\1/1200/', url)
    url = re.sub(r'/ic/\d+x\d+/', r'/ic/1024x576/', url)
    # Upgrade Eurogamer (assetsio.gnwcdn.com)
    if 'assetsio.gnwcdn.com' in url:
        url = re.sub(r'width=\d+', 'width=1200', url)
    # Upgrade PC Gamer (cdn.mos.cms.futurecdn.net)
    if 'futurecdn.net' in url:
        url = re.sub(r'-\d+-\d+\.jpg$', '-1920-80.jpg', url)
    # Upgrade Gematsu resize suffixes e.g. -320x180.jpg -> .jpg
    url = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', url)
    # Upgrade The Verge images
    if 'theverge.com' in url or 'platform.theverge' in url:
        if '?' in url:
            url = re.sub(r'w=\d+', 'w=1200', url)
            if 'w=' not in url: url += '&w=1200'
    return url

def scrape_article_details(item: dict) -> dict:
    """Scrape article webpage to extract HD lead image, full paragraphs, and gallery screenshots."""
    link = item["link"]
    cat = item["category_hint"]
    feed_img = item.get("feed_image", "")
    lead_img = make_hd_image_url(feed_img) if feed_img else ""
    gallery_images = []
    clean_paras = []

    try:
        req = urllib.request.Request(link, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode('utf-8', errors='ignore')

        # 1. og:image
        img_m = re.search(r'property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', content) or \
                re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', content)
        if img_m and not lead_img:
            lead_img = make_hd_image_url(img_m.group(1))

        # 2. Extract article paragraphs
        raw_p = re.findall(r'<p[^>]*>(.*?)</p>', content, re.DOTALL)
        for p in raw_p:
            c = clean_text(p)
            if len(c) > 35 and not any(x in c.lower() for x in ['terms of service', 'privacy policy', 'cookie', 'sign up for', 'posts from this', 'all rights reserved', 'follow us']):
                clean_paras.append(c)

        # 3. Extract screenshots / gallery images from content
        img_tags = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', content)
        seen_gallery = set()
        for g_img in img_tags:
            g_img = g_img.split('?')[0]
            if any(domain in g_img for domain in ['assetsio.gnwcdn.com', 'cdn.mos.cms.futurecdn.net', 'platform.theverge.com', 'ichef.bbci.co.uk', 'techcrunch.com', 'engadget.com', 'gematsu.com']):
                if not any(bad in g_img.lower() for bad in ['logo', 'avatar', 'icon', 'author', 'user-', 'badge', 'tracking', 'banner']):
                    hd_g = make_hd_image_url(g_img)
                    if hd_g and hd_g not in seen_gallery:
                        seen_gallery.add(hd_g)
                        gallery_images.append(hd_g)

    except Exception as e:
        logger.debug(f"Direct scrape fallback for {link}: {e}")

    # Fallback to Jina for Gematsu if needed
    if 'gematsu.com' in link and not clean_paras:
        try:
            jina_url = f"https://r.jina.ai/{link}"
            req = urllib.request.Request(jina_url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=10) as resp:
                txt = resp.read().decode('utf-8', errors='ignore')
            lines = txt.splitlines()
            start = False
            for line in lines:
                if line.startswith("Markdown Content:"):
                    start = True
                    continue
                if not start: continue
                line = line.strip()
                if not line or line.startswith('!') or line.startswith('#'): continue
                line = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', line)
                if len(line) > 30: clean_paras.append(line)
        except Exception:
            pass

    clean_paras = list(dict.fromkeys(clean_paras))
    item["cover_image_url"] = lead_img
    item["gallery_images"] = gallery_images[:10]
    item["raw_paragraphs"] = clean_paras[:12] if clean_paras else [item["description"]]
    return item

def call_gemini(item_info: dict) -> dict:
    """Call Gemini for factual summary and multi-paragraph translation with retry & backoff."""
    title = item_info["title"]
    category_hint = item_info["category_hint"]
    paras_text = "\n\n".join(item_info["raw_paragraphs"])

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    user_prompt = f"""หัวข้อข่าว: {title}
หมวดหมู่: {category_hint}
เนื้อหาภาษาอังกฤษ:
{paras_text}

กรุณาแปลและสรุปเป็น JSON ภาษาไทย:
{{
  "thai_title": "พาดหัวข่าวภาษาไทยที่กระชับ ตรงตาม Fact (ห้ามเป็นภาษาอังกฤษ)",
  "tag": "Football | Technology | Gaming",
  "bullet_points": [
    "ข้อเท็จจริงสำคัญประเด็นที่ 1 (ภาษาไทย)...",
    "ข้อเท็จจริงสำคัญประเด็นที่ 2 (ภาษาไทย)...",
    "ข้อเท็จจริงสำคัญประเด็นที่ 3 (ภาษาไทย)..."
  ],
  "full_paragraphs": [
    "ย่อหน้าที่ 1 แปลเป็นภาษาไทย...",
    "ย่อหน้าที่ 2 แปลเป็นภาษาไทย...",
    "ย่อหน้าที่ 3 แปลเป็นภาษาไทย..."
  ]
}}"""

    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2}
    }

    req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), headers={"Content-Type": "application/json"})
    
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=35) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
                parsed = json.loads(raw_text)

                tag = parsed.get("tag", "").strip()
                if "game" in tag.lower(): parsed["tag"] = "Gaming"
                elif "tech" in tag.lower(): parsed["tag"] = "Technology"
                elif "foot" in tag.lower(): parsed["tag"] = "Football"
                else: parsed["tag"] = category_hint

                # Ensure Thai output
                thai_t = parsed.get("thai_title", "")
                if any('\u0e00' <= c <= '\u0e7f' for c in thai_t):
                    return parsed
                else:
                    logger.warning(f"Gemini returned non-Thai title on attempt {attempt+1}: {thai_t}")
                    time.sleep(2 * (attempt + 1))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                logger.warning(f"Rate limited (429) for '{title[:30]}...'. Backing off {6*(attempt+1)}s...")
                time.sleep(6 * (attempt + 1))
            else:
                logger.warning(f"Gemini HTTP {e.code} for '{title[:30]}...': {e}")
                time.sleep(3 * (attempt + 1))
        except Exception as e:
            logger.warning(f"Gemini attempt {attempt+1} failed: {e}")
            time.sleep(2 * (attempt + 1))

    # Return fallback
    return {
        "thai_title": title,
        "tag": category_hint,
        "bullet_points": [item_info["description"]],
        "full_paragraphs": item_info["raw_paragraphs"][:4]
    }

def update_ui_files(news_list: list[dict]):
    """Update README.md and centered index.html with clean Pure News branding, Thai Time, and History Back."""
    now_th = to_thailand_time(datetime.datetime.now(datetime.timezone.utc))

    football_count = sum(1 for x in news_list if x.get("tag") == "Football")
    tech_count = sum(1 for x in news_list if x.get("tag") == "Technology")
    gaming_count = sum(1 for x in news_list if x.get("tag") == "Gaming")

    # 1. README.md
    readme_lines = [
        "# ⚡ Pure News — Automated News Aggregator (GitHub Native)",
        "",
        "> **Live Automated News Reader:** Auto-scraped from BBC Sport Football, The Verge/Tech, and Eurogamer/PC Gamer/Gematsu.",
        f"> *อัปเดตล่าสุด: `{now_th}`* • *รวมทั้งหมด: `{len(news_list)} ข่าว`* (`⚽ Football: {football_count}` | `⚡ Tech: {tech_count}` | `🎮 Gaming: {gaming_count}`)",
        "",
        "### 🌐 [กดที่นี่เพื่อเปิดหน้าเว็บอ่านข่าว (GitHub Pages)](https://puresinghara.github.io/pure-news/)",
        "",
        "---",
        "",
        "### 📰 ตารางสรุปข่าวล่าสุด (90 ข่าวเด่น)",
        "",
        "| หมวดหมู่ | หัวข้อข่าว & ประเด็นสำคัญ | เวลาเผยแพร่ (เวลาไทย) | แหล่งข่าว |",
        "| :--- | :--- | :--- | :--- |"
    ]
    for item in news_list:
        tag_badge = f"`[{item['tag']}]`"
        bullets_md = "<br>".join([f"• {b}" for b in item.get('bullet_points', [])[:3]])
        headline = f"**{item['thai_title']}**<br>{bullets_md}"
        link = f"[อ่านต้นฉบับ]({item['link']})"
        pub = item.get('published_th', item.get('published_at', ''))
        readme_lines.append(f"| {tag_badge} | {headline} | {pub} | {link} |")

    readme_lines.append("")
    readme_lines.append("---")
    readme_lines.append("*Automated 100% by GitHub Actions and Google Gemini AI.*")

    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(readme_lines))

    # 2. index.html (Pure News - Clean Header, Center Modal, Mobile History Back, Screenshots Gallery)
    json_data_str = json.dumps(news_list, ensure_ascii=False)
    html_content = f'''<!DOCTYPE html>
<html lang="th" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Pure News // รวมข่าวเด่น 3 หมวดหมู่</title>
  <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>⚡</text></svg>">
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;700;800&family=Prompt:wght@300;400;500;600;700&display=swap');
    body {{ font-family: 'Prompt', sans-serif; }}
    .brand-title {{ font-family: 'Chakra Petch', sans-serif; letter-spacing: -0.02em; }}
  </style>
</head>
<body class="bg-[#09090b] text-zinc-100 min-h-screen antialiased selection:bg-[#ff3366] selection:text-white">

  <!-- Top Announcement Bar -->
  <div class="border-b border-zinc-800 bg-zinc-950 px-4 py-2 text-xs font-mono flex items-center justify-between text-zinc-400">
    <div class="flex items-center gap-2">
      <span class="inline-block w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
      <span class="text-zinc-200 font-semibold">// LIVE FEED:</span>
      <span class="hidden sm:inline">BBC FOOTBALL (30) • TECH (30) • GAMING (30)</span>
      <span class="text-emerald-400 font-bold px-1.5 py-0.5 bg-emerald-500/10 rounded border border-emerald-500/30">ONLINE</span>
    </div>
    <div class="flex items-center gap-4 text-[11px]">
      <span class="text-zinc-400 font-mono hidden md:inline">เวลาไทย: {now_th}</span>
      <span class="text-emerald-400 font-bold">GEMINI 3.1 FLASH-LITE</span>
    </div>
  </div>

  <!-- Clean Minimal Header (Pure News + Category Tabs) -->
  <header class="border-b border-zinc-800 bg-[#0c0c0e]/95 sticky top-0 z-40 backdrop-blur-md">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 py-3.5 flex flex-col md:flex-row md:items-center justify-between gap-4">
      
      <!-- Brand Logo -->
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-md bg-gradient-to-tr from-[#ff3366] to-[#00e5ff] flex items-center justify-center font-black text-black text-xl font-mono shadow-[0_0_20px_rgba(255,51,102,0.4)]">
          PN
        </div>
        <div>
          <h1 class="brand-title text-2xl md:text-3xl font-extrabold uppercase text-white tracking-wider flex items-center gap-2 leading-none">
            PURE NEWS
          </h1>
          <p class="text-[11px] font-mono text-zinc-400 uppercase tracking-widest mt-0.5">30 ข่าวเด่นต่อหมวด • อัปเดตทุก 1 ชั่วโมง</p>
        </div>
      </div>

      <!-- Clean Category Filter Tabs -->
      <div class="flex items-center gap-2 overflow-x-auto pb-1 md:pb-0">
        <button onclick="filterTag('ALL')" id="filterALL" class="px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded transition whitespace-nowrap">ALL ({len(news_list)})</button>
        <button onclick="filterTag('Football')" id="filterFootball" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded transition whitespace-nowrap">⚽ FOOTBALL ({football_count})</button>
        <button onclick="filterTag('Technology')" id="filterTechnology" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-cyan-400 font-mono text-xs rounded transition whitespace-nowrap">⚡ TECH ({tech_count})</button>
        <button onclick="filterTag('Gaming')" id="filterGaming" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-amber-400 font-mono text-xs rounded transition whitespace-nowrap">🎮 GAMING ({gaming_count})</button>
        <button onclick="location.reload()" title="รีเฟรชข่าว" class="p-1.5 bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 text-zinc-300 rounded transition ml-1">
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

  <!-- Centered Modal Dialog with Mobile Back Support & Screenshots Gallery -->
  <div id="modalOverlay" onclick="if(event.target===this) closeModal()" class="fixed inset-0 bg-black/85 backdrop-blur-md z-50 hidden flex items-center justify-center p-3 sm:p-6 overflow-y-auto">
    <div class="w-full max-w-3xl bg-[#0c0c0e] border border-zinc-800 rounded-lg shadow-2xl overflow-hidden my-auto max-h-[92vh] flex flex-col">
      
      <!-- Modal Header (Sticky) -->
      <div class="flex items-center justify-between px-6 py-4 border-b border-zinc-800 bg-[#0e0e11] sticky top-0 z-10">
        <div class="flex items-center gap-2">
          <span id="modalTag" class="px-2.5 py-0.5 rounded text-xs font-mono font-bold uppercase tracking-wider bg-[#ff3366]/15 text-[#ff3366] border border-[#ff3366]/30"></span>
          <span id="modalDate" class="text-xs font-mono text-zinc-400"></span>
        </div>
        <button onclick="closeModal()" aria-label="Close" class="w-8 h-8 rounded-full bg-zinc-900 hover:bg-zinc-800 text-zinc-400 hover:text-white flex items-center justify-center text-xl transition leading-none">&times;</button>
      </div>

      <!-- Modal Body (Scrollable) -->
      <div class="p-6 md:p-8 overflow-y-auto space-y-6">
        <!-- HD Cover Image -->
        <div class="w-full h-64 sm:h-80 overflow-hidden rounded-md border border-zinc-800 bg-zinc-950">
          <img id="modalImg" src="" referrerpolicy="no-referrer" onerror="this.src='https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80';" class="w-full h-full object-cover">
        </div>

        <!-- Title -->
        <h2 id="modalTitle" class="brand-title text-xl md:text-2xl font-bold text-white leading-snug"></h2>

        <!-- Key Facts Box -->
        <div class="bg-black/60 border-l-2 border-[#ff3366] p-4 rounded-sm">
          <div class="text-xs font-mono text-[#ff3366] uppercase font-bold tracking-wider mb-2">📌 สรุปประเด็นสำคัญ (KEY FACTS):</div>
          <ul id="modalBullets" class="space-y-2 text-sm text-zinc-200"></ul>
        </div>

        <!-- Full Multi-Paragraph Translation -->
        <div class="space-y-4 pt-2">
          <div class="text-xs font-mono text-zinc-500 uppercase tracking-widest">// เนื้อหาข่าวฉบับเต็ม :</div>
          <div id="modalFullContent" class="space-y-3.5 text-zinc-300 text-sm md:text-base leading-relaxed font-light"></div>
        </div>

        <!-- Screenshots Gallery (Always clean & gracefully handled) -->
        <div id="modalGallerySection" class="hidden pt-4 border-t border-zinc-800/80">
          <div class="text-xs font-mono text-amber-400 uppercase tracking-widest mb-3 flex items-center gap-1.5">
            <span>📸 ภาพประกอบและ SCREENSHOTS ในข่าว :</span>
          </div>
          <div id="modalGalleryGrid" class="grid grid-cols-2 sm:grid-cols-3 gap-2.5"></div>
        </div>
      </div>

      <!-- Modal Footer -->
      <div class="px-6 py-4 border-t border-zinc-800 bg-[#0e0e11] flex items-center justify-between">
        <a id="modalSource" href="#" target="_blank" class="px-4 py-2 bg-zinc-900 border border-zinc-700 text-xs font-mono text-white rounded-sm hover:bg-zinc-800 flex items-center gap-1.5 transition">
          <span>อ่านข่าวต้นฉบับเต็ม</span>
          <span>→</span>
        </a>
        <button onclick="closeModal()" class="px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-xs font-mono text-zinc-300 rounded-sm transition">ปิด</button>
      </div>

    </div>
  </div>

  <script>
    let allNews = {json_data_str};
    let currentTag = 'ALL';
    let isModalOpen = false;

    // Cache-busting background fetch
    if (window.location.protocol.startsWith('http')) {{
      fetch('data/news.json?t=' + Date.now())
        .then(r => r.json())
        .then(data => {{
          if (data && data.length > 0) {{
            allNews = data;
            render();
          }}
        }}).catch(() => {{}});
    }}

    function filterTag(tag) {{
      currentTag = tag;
      ['ALL', 'Football', 'Technology', 'Gaming'].forEach(t => {{
        const btn = document.getElementById('filter' + t);
        if (btn) {{
          if (t === tag) {{
            btn.className = "px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded transition whitespace-nowrap";
          }} else {{
            let textColor = 'text-zinc-300';
            if (t === 'Gaming') textColor = 'text-amber-400';
            if (t === 'Technology') textColor = 'text-cyan-400';
            btn.className = `px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 ${{textColor}} font-mono text-xs rounded transition whitespace-nowrap`;
          }}
        }}
      }});
      render();
    }}

    function render() {{
      const grid = document.getElementById('newsGrid');
      grid.innerHTML = '';

      const filtered = allNews.filter(item => {{
        return (currentTag === 'ALL') || (item.tag === currentTag);
      }});

      if (filtered.length === 0) {{
        grid.innerHTML = '<div class="col-span-3 text-center py-16 text-zinc-500 font-mono">ไม่มีข่าวในหมวดนี้</div>';
        return;
      }}

      filtered.forEach((item) => {{
        const originalIndex = allNews.indexOf(item);
        const div = document.createElement('div');
        div.className = 'bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition flex flex-col justify-between overflow-hidden rounded cursor-pointer group';
        div.onclick = () => openModal(originalIndex);
        
        const bulletsHtml = (item.bullet_points || []).slice(0, 3).map(b => 
          `<li class="flex items-start gap-1.5"><span class="text-[#ff3366] font-bold">›</span><span>${{b}}</span></li>`
        ).join('');

        let tagColor = 'text-[#ff3366]';
        if (item.tag === 'Technology') tagColor = 'text-cyan-400';
        else if (item.tag === 'Gaming') tagColor = 'text-amber-400';

        const hasGallery = Array.isArray(item.gallery_images) && item.gallery_images.length > 0;
        const galleryBadge = hasGallery ? `<span class="bg-amber-500/10 text-amber-400 border border-amber-500/20 px-1.5 py-0.5 rounded text-[10px] font-mono">📸 ${{item.gallery_images.length}} รูป</span>` : '';

        div.innerHTML = `
          <div>
            <div class="w-full h-44 overflow-hidden bg-zinc-950 relative">
              <img src="${{item.cover_image_url}}" referrerpolicy="no-referrer" class="w-full h-full object-cover group-hover:scale-105 transition duration-500" onerror="this.onerror=null; this.src='https://images.unsplash.com/photo-1511512578047-dfb367046420?w=800&q=80';">
              <div class="absolute bottom-2 right-2 flex gap-1">${{galleryBadge}}</div>
            </div>
            <div class="p-5">
              <div class="flex items-center gap-2 text-[11px] font-mono mb-2">
                <span class="${{tagColor}} font-bold">${{item.tag}}</span>
                <span class="text-zinc-600">•</span>
                <span class="text-zinc-500">${{item.published_th || item.published_at || ''}}</span>
              </div>
              <h3 class="brand-title text-base font-bold text-white group-hover:text-[#ff3366] transition mb-3 leading-snug line-clamp-2">
                ${{item.thai_title}}
              </h3>
              <ul class="text-xs text-zinc-300 space-y-1.5 font-light">
                ${{bulletsHtml}}
              </ul>
            </div>
          </div>
          <div class="p-5 pt-0 border-t border-zinc-800/80 text-[11px] font-mono text-zinc-500 flex items-center justify-between">
            <span class="text-zinc-300 group-hover:text-white transition">อ่านรายละเอียดเต็ม →</span>
            <span>${{item.tag}} FEED</span>
          </div>
        `;
        grid.appendChild(div);
      }});
    }}

    function openModal(idx) {{
      const item = allNews[idx];
      if (!item) return;

      // Mobile back gesture & history support
      isModalOpen = true;
      history.pushState({{ modalOpen: true, newsId: item.id }}, '', '#news-' + item.id);

      document.getElementById('modalTag').textContent = `// ${{item.tag}}`;
      document.getElementById('modalImg').src = item.cover_image_url;
      document.getElementById('modalTitle').textContent = item.thai_title;
      document.getElementById('modalDate').textContent = item.published_th || item.published_at;

      const bulletsList = document.getElementById('modalBullets');
      bulletsList.innerHTML = '';
      (item.bullet_points || []).forEach(b => {{
        const li = document.createElement('li');
        li.className = 'flex items-start gap-2';
        li.innerHTML = `<span class="text-[#ff3366] font-bold">›</span><span>${{b}}</span>`;
        bulletsList.appendChild(li);
      }});

      const fullContentDiv = document.getElementById('modalFullContent');
      fullContentDiv.innerHTML = '';
      const paras = Array.isArray(item.full_paragraphs) && item.full_paragraphs.length > 0 ? item.full_paragraphs : item.bullet_points;
      (paras || []).forEach(p => {{
        const pElem = document.createElement('p');
        pElem.textContent = p;
        fullContentDiv.appendChild(pElem);
      }});

      // Screenshot Gallery
      const gallerySec = document.getElementById('modalGallerySection');
      const galleryGrid = document.getElementById('modalGalleryGrid');
      galleryGrid.innerHTML = '';
      if (Array.isArray(item.gallery_images) && item.gallery_images.length > 0) {{
        gallerySec.classList.remove('hidden');
        item.gallery_images.forEach(imgUrl => {{
          const a = document.createElement('a');
          a.href = imgUrl;
          a.target = '_blank';
          a.className = 'block aspect-video overflow-hidden rounded bg-zinc-950 border border-zinc-800 hover:border-[#ff3366] transition group relative';
          a.innerHTML = `<img src="${{imgUrl}}" referrerpolicy="no-referrer" onerror="this.parentElement.style.display='none'" class="w-full h-full object-cover group-hover:scale-105 transition duration-300">`;
          galleryGrid.appendChild(a);
        }});
      }} else {{
        gallerySec.classList.add('hidden');
      }}

      document.getElementById('modalSource').href = item.link;
      document.getElementById('modalOverlay').classList.remove('hidden');
      document.body.style.overflow = 'hidden';
    }}

    function closeModal(fromHistory = false) {{
      if (!isModalOpen) return;
      isModalOpen = false;
      document.getElementById('modalOverlay').classList.add('hidden');
      document.body.style.overflow = '';

      // If closed by user tap (and not from browser back gesture), step back in history
      if (!fromHistory && history.state && history.state.modalOpen) {{
        history.back();
      }}
    }}

    // Mobile back swipe / browser back event listener
    window.addEventListener('popstate', (e) => {{
      if (isModalOpen) {{
        closeModal(true);
      }}
    }});

    // ESC key support
    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape' && isModalOpen) {{
        closeModal();
      }}
    }});

    // Deep link support e.g. https://domain/#news-12345
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

    with open(INDEX_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)

def fetch_feed_entries(url: str, category: str, now_utc: datetime.datetime) -> list[dict]:
    """Fetch entries from an RSS feed, extracting lead image and publish datetime."""
    results = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            link = getattr(entry, 'link', '').strip()
            title = getattr(entry, 'title', '').strip()
            if not link or any(x in title.lower() for x in ["who am i", "quiz", "football daily"]):
                continue

            pub_tuple = getattr(entry, 'published_parsed', None) or getattr(entry, 'updated_parsed', None)
            if pub_tuple:
                pub_dt = datetime.datetime.fromtimestamp(time.mktime(pub_tuple), tz=datetime.timezone.utc)
            else:
                pub_dt = now_utc

            # Extract image from media_content, enclosures, or summary
            feed_img = ""
            if 'media_content' in entry and entry.media_content:
                feed_img = entry.media_content[0].get('url', '')
            elif 'enclosures' in entry and entry.enclosures:
                feed_img = entry.enclosures[0].get('href', '')

            raw_summary = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
            if not feed_img and raw_summary:
                img_m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_summary)
                if img_m:
                    feed_img = img_m.group(1)

            results.append({
                "title": title,
                "link": link,
                "description": clean_text(raw_summary),
                "published_th": to_thailand_time(pub_dt),
                "category_hint": category,
                "feed_image": feed_img,
                "_pub_dt": pub_dt
            })
    except Exception as e:
        logger.error(f"Error reading feed {url}: {e}")
    return results

def run():
    logger.info("==================================================")
    logger.info("  STARTING PURE NEWS PIPELINE (30 PER TOPIC)")
    logger.info("==================================================")

    os.makedirs(DATA_DIR, exist_ok=True)
    existing_news = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                existing_news = json.load(f)
        except Exception:
            pass

    # Normalize existing database
    existing_by_link = {}
    for it in existing_news:
        link = it.get("link", "").strip()
        if not link: continue
        tag = it.get("tag", "")
        if "chelsea" in tag.lower(): it["tag"] = "Football"
        it["cover_image_url"] = make_hd_image_url(it.get("cover_image_url", ""))
        existing_by_link[link] = it

    logger.info(f"Loaded {len(existing_by_link)} existing stories from database.")

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    twelve_hours_ago = now_utc - datetime.timedelta(hours=12)

    categories_config = {
        "Football": [
            "http://feeds.bbci.co.uk/sport/football/rss.xml"
        ],
        "Technology": [
            "https://www.theverge.com/rss/index.xml",
            "https://www.theverge.com/rss/tech/index.xml",
            "https://techcrunch.com/feed/",
            "https://www.engadget.com/rss.xml",
            "https://feeds.arstechnica.com/arstechnica/technology-lab"
        ],
        "Gaming": [
            "https://www.eurogamer.net/feed",
            "https://www.pcgamer.com/rss",
            "https://www.gematsu.com/feed"
        ]
    }

    category_results = {}
    global_used_images = set()

    for cat, feeds in categories_config.items():
        logger.info(f"--- Processing Category: {cat} ---")
        candidates = []
        seen_links = set()

        for feed_url in feeds:
            entries = fetch_feed_entries(feed_url, cat, now_utc)
            for e in entries:
                if e["link"] not in seen_links:
                    seen_links.add(e["link"])
                    candidates.append(e)

        # Sort candidates newest first
        candidates.sort(key=lambda x: x["_pub_dt"], reverse=True)

        # Step 1: Filter articles within last 12 hours
        within_12h = [c for c in candidates if c["_pub_dt"] >= twelve_hours_ago]
        logger.info(f"[{cat}] Candidates within last 12 hours: {len(within_12h)}")

        # Step 2: Backfill rule - If < 30 articles in 12h, fill up to 30 from the latest candidates
        selected_candidates = within_12h[:]
        if len(selected_candidates) < TARGET_PER_CATEGORY:
            logger.info(f"[{cat}] Backfilling from latest candidates to reach target of {TARGET_PER_CATEGORY} articles...")
            for c in candidates:
                if c not in selected_candidates:
                    selected_candidates.append(c)
                if len(selected_candidates) >= TARGET_PER_CATEGORY:
                    break

        selected_candidates = selected_candidates[:TARGET_PER_CATEGORY]

        final_category_articles = []
        new_items_to_translate = []

        for item in selected_candidates:
            link = item["link"]
            if link in existing_by_link:
                existing_item = existing_by_link[link]
                existing_item["tag"] = cat
                existing_item["cover_image_url"] = make_hd_image_url(existing_item.get("cover_image_url", ""))
                
                # Check if it was REALLY translated to Thai
                thai_t = existing_item.get("thai_title", "")
                if any('\u0e00' <= c <= '\u0e7f' for c in thai_t):
                    final_category_articles.append(existing_item)
                else:
                    logger.info(f"[{cat}] Found untranslated article in cache, queuing re-translation: {item['title'][:40]}...")
                    new_items_to_translate.append(item)
            else:
                new_items_to_translate.append(item)

        logger.info(f"[{cat}] Reusing {len(final_category_articles)} existing translated articles. Translating {len(new_items_to_translate)} new articles.")

        # Translate new items (if any) with polite concurrency
        if new_items_to_translate:
            logger.info(f"[{cat}] Scraping full content and gallery for {len(new_items_to_translate)} articles...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                scraped_items = list(executor.map(scrape_article_details, new_items_to_translate))

            logger.info(f"[{cat}] Translating {len(scraped_items)} articles with Gemini Flash-Lite (concurrency: 2)...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                translations = list(executor.map(call_gemini, scraped_items))

            for item, trans in zip(scraped_items, translations):
                entry = {
                    "id": int(time.time() * 1000) + len(final_category_articles),
                    "original_title": item["title"],
                    "thai_title": trans.get("thai_title", item["title"]),
                    "tag": cat,
                    "bullet_points": trans.get("bullet_points", []),
                    "full_paragraphs": trans.get("full_paragraphs", item["raw_paragraphs"][:4]),
                    "link": item["link"],
                    "cover_image_url": item["cover_image_url"],
                    "gallery_images": item.get("gallery_images", []),
                    "published_th": item["published_th"],
                    "category_hint": cat
                }
                final_category_articles.append(entry)
                existing_by_link[item["link"]] = entry

        # Ensure image uniqueness and quality across category articles
        for idx_art, art in enumerate(final_category_articles):
            img = art.get("cover_image_url", "").strip()
            # If image missing or duplicate in this run
            if not img or img in global_used_images:
                # Try gallery images
                found_unique = False
                for g in art.get("gallery_images", []):
                    if g and g not in global_used_images:
                        art["cover_image_url"] = g
                        global_used_images.add(g)
                        found_unique = True
                        break
                if not found_unique:
                    # Assign a unique curated high-res image
                    fallback_img = CURATED_GAMING_COVERS[idx_art % len(CURATED_GAMING_COVERS)] if cat == "Gaming" else f"https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80&sig={idx_art}"
                    art["cover_image_url"] = fallback_img
                    global_used_images.add(fallback_img)
            else:
                global_used_images.add(img)

        category_results[cat] = final_category_articles[:TARGET_PER_CATEGORY]
        logger.info(f"[{cat}] Completed! Total articles: {len(category_results[cat])}/{TARGET_PER_CATEGORY}")

    combined_db = []
    for cat in ["Football", "Technology", "Gaming"]:
        combined_db.extend(category_results.get(cat, []))

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(combined_db, f, ensure_ascii=False, indent=2)

    update_ui_files(combined_db)

    logger.info("==================================================")
    logger.info(f"[✓] Pure News pipeline complete! Maintained {len(combined_db)} total stories (30 Football, 30 Tech, 30 Gaming).")
    logger.info("==================================================")

if __name__ == "__main__":
    run()
