"""
Automated News Aggregator & Reader (GitHub Native)
Feeds:
1. Football: BBC Sport Football (http://feeds.bbci.co.uk/sport/football/rss.xml)
2. Technology: The Verge & Top Tech (The Verge, TechCrunch, Engadget, Ars Technica)
3. Gaming: Gematsu (https://www.gematsu.com/feed + Gematsu Highlights)

Features:
- 30 Fresh Articles PER Category (Total 90 articles in database).
- 12-Hour Freshness rule with automatic backfill up to 30 articles per topic.
- Multi-image screenshot gallery for Gaming/Tech/Football (0 KB storage, pure URL hotlink).
- Crystal-clear 1200px HD images for all articles.
- Timezone: Thailand Time (UTC+7).
- Full multi-paragraph translation with Gemini Flash-Lite.
- Smart Deduplication: skips already-translated links to strictly protect API quota.
- Centered luxury Verge cyberpunk UI with modal dialog and interactive screenshot gallery.
"""
import os
import json
import time
import datetime
import re
import html
import logging
import urllib.request
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

SYSTEM_PROMPT = """คุณคือนักข่าวและบรรณาธิการข่าวมืออาชีพ แปลและสรุปข่าวเป็นภาษาไทยอย่างถูกต้อง สละสลวย และตรงตามข้อเท็จจริง (Fact-based)
ข้อกำหนด:
1. ข่าวฟุตบอล: ทับศัพท์เทคนิคและชื่อนักเตะ/สโมสรอย่างถูกต้อง
2. ข่าวเทคโนโลยี: ทับศัพท์ชื่อระบบ ฮาร์ดแวร์ แบรนด์ โมเดล AI และฟีเจอร์อย่างถูกต้อง
3. ข่าวเกม (Gaming): ทับศัพท์ชื่อเกม แพลตฟอร์ม (PS5, Xbox Series, Switch 2, PC) และค่ายเกมอย่างถูกต้อง
4. "bullet_points": สรุปข้อเท็จจริงสำคัญสั้นกระชับ 3-4 ข้อ
5. "full_paragraphs": แปลและเรียบเรียงเนื้อหาข่าวฉบับเต็มอย่างครบถ้วน (3-5 ย่อหน้า) ครอบคลุมเหตุการณ์สำคัญ คำสัมภาษณ์ กำหนดการ และตัวเลขสถิติทั้งหมด

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
        return "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80"
    # Upgrade BBC thumbnails (/240/, /320/, /480/ -> /1200/)
    url = re.sub(r'/(standard|branded_sport|branded_news)/\d+/', r'/\1/1200/', url)
    url = re.sub(r'/ic/\d+x\d+/', r'/ic/1024x576/', url)
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
    
    lead_img = make_hd_image_url(feed_img) if feed_img else None
    gallery_images = []
    clean_paras = []

    # Case A: Gaming from Gematsu
    if 'gematsu.com' in link:
        try:
            # Use Jina AI Reader to cleanly bypass Cloudflare and get full article & screenshots
            jina_url = f"https://r.jina.ai/{link}"
            req = urllib.request.Request(jina_url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=12) as resp:
                txt = resp.read().decode('utf-8', errors='ignore')
            
            # Extract screenshots
            raw_imgs = re.findall(r'!\[.*?\]\((https://www\.gematsu\.com/wp-content/uploads/[^\)]+)\)', txt)
            seen_imgs = set()
            for img in raw_imgs:
                clean_img = make_hd_image_url(img.split('?')[0])
                if clean_img not in seen_imgs and not any(x in clean_img.lower() for x in ['logo', 'avatar', 'icon', 'banner']):
                    seen_imgs.add(clean_img)
                    gallery_images.append(clean_img)
            
            if gallery_images and not lead_img:
                lead_img = gallery_images[0]
            
            # Extract paragraphs
            lines = txt.splitlines()
            start = False
            for line in lines:
                if line.startswith("Markdown Content:"):
                    start = True
                    continue
                if not start: continue
                line = line.strip()
                if not line or line.startswith('!') or line.startswith('###') or line.startswith('##') or line.startswith('#'):
                    continue
                line = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', line) # strip links
                if len(line) > 30 and not any(line.startswith(x) for x in ['Source:', 'Thanks,', 'Watch the']):
                    clean_paras.append(line)
        except Exception as e:
            logger.warning(f"Jina scrape warning for {link}: {e}")

    # Case B: The Verge / TechCrunch / Ars Technica
    elif any(d in link for d in ['theverge.com', 'techcrunch.com', 'arstechnica.com', 'engadget.com']):
        try:
            req = urllib.request.Request(link, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=8) as resp:
                content = resp.read().decode('utf-8', errors='ignore')

            img_m = re.search(r'property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', content) or \
                    re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', content)
            if img_m and not lead_img:
                lead_img = make_hd_image_url(img_m.group(1))

            raw_p = re.findall(r'<p[^>]*>(.*?)</p>', content, re.DOTALL)
            for p in raw_p:
                c = clean_text(p)
                if len(c) > 35 and not any(x in c for x in ['Posts from this', 'Sign up for', 'Terms of Service', 'Privacy Policy']):
                    clean_paras.append(c)
        except Exception:
            pass

    # Case C: BBC Sport Football
    else:
        try:
            req = urllib.request.Request(link, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=8) as resp:
                content = resp.read().decode('utf-8', errors='ignore')

            img_m = re.search(r'property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', content) or \
                    re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', content)
            if img_m and not lead_img:
                lead_img = make_hd_image_url(img_m.group(1))

            raw_p = re.findall(r'<p[^>]*>(.*?)</p>', content, re.DOTALL)
            for p in raw_p:
                c = clean_text(p)
                if len(c) > 35 and not any(c.startswith(x) for x in ['Photo by', 'Follow ', 'Sign up', 'Share this', 'Published', 'Image source', 'Cast your']):
                    clean_paras.append(c)
        except Exception:
            pass

    if not lead_img:
        lead_img = "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80"
    
    clean_paras = list(dict.fromkeys(clean_paras))
    item["cover_image_url"] = lead_img
    item["gallery_images"] = gallery_images[:12] # Limit gallery to top 12 screenshots
    item["raw_paragraphs"] = clean_paras[:12] if clean_paras else [item["description"]]
    return item

def call_gemini(item_info: dict) -> dict:
    """Call Gemini for factual summary and multi-paragraph translation."""
    title = item_info["title"]
    category_hint = item_info["category_hint"]
    paras_text = "\n\n".join(item_info["raw_paragraphs"])

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    user_prompt = f"""หัวข้อข่าว: {title}
หมวดหมู่: {category_hint}
เนื้อหาภาษาอังกฤษ:
{paras_text}

กรุณาแปลและสรุปเป็น JSON:
{{
  "thai_title": "พาดหัวข่าวภาษาไทยที่กระชับ ตรงตาม Fact",
  "tag": "Football | Technology | Gaming",
  "bullet_points": [
    "ข้อเท็จจริงสำคัญประเด็นที่ 1...",
    "ข้อเท็จจริงสำคัญประเด็นที่ 2...",
    "ข้อเท็จจริงสำคัญประเด็นที่ 3..."
  ],
  "full_paragraphs": [
    "ย่อหน้าที่ 1...",
    "ย่อหน้าที่ 2...",
    "ย่อหน้าที่ 3..."
  ]
}}"""

    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2}
    }

    req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), headers={"Content-Type": "application/json"})
    for attempt in range(3):
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
                return parsed
        except Exception:
            time.sleep(2)

    return {
        "thai_title": title,
        "tag": category_hint,
        "bullet_points": [item_info["description"]],
        "full_paragraphs": item_info["raw_paragraphs"][:4]
    }

def update_ui_files(news_list: list[dict]):
    """Update README.md and centered index.html with Thailand Time (UTC+7)."""
    now_th = to_thailand_time(datetime.datetime.now(datetime.timezone.utc))

    football_count = sum(1 for x in news_list if x.get("tag") == "Football")
    tech_count = sum(1 for x in news_list if x.get("tag") == "Technology")
    gaming_count = sum(1 for x in news_list if x.get("tag") == "Gaming")

    # 1. README.md
    readme_lines = [
        "# ⚡ Automated News Aggregator (GitHub Native)",
        "",
        "> **Live Automated News Reader:** Auto-scraped from BBC Sport Football, The Verge/Tech, and Gematsu (Gaming).",
        f"> *อัปเดตล่าสุด: `{now_th}`* • *รวมทั้งหมด: `{len(news_list)} ข่าว`* (`⚽ Football: {football_count}` | `⚡ Tech: {tech_count}` | `🎮 Gaming: {gaming_count}`)",
        "",
        "### 🌐 [กดที่นี่เพื่อเปิดหน้าเว็บอ่านข่าว (GitHub Pages)](https://puresinghara.github.io/pure-news/)",
        "",
        "---",
        "",
        "### 📰 ตารางสรุปข่าวล่าสุด",
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

    # 2. index.html (Centered luxury Verge reader with Screenshot Gallery & Thailand time)
    json_data_str = json.dumps(news_list, ensure_ascii=False)
    html_content = f'''<!DOCTYPE html>
<html lang="th" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>THE VERGE // LIVE NEWS READER</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@400;600;700&family=Prompt:wght@300;400;500;600;700&display=swap');
    body {{ font-family: 'Prompt', sans-serif; }}
    .verge-title {{ font-family: 'Chakra Petch', sans-serif; letter-spacing: -0.02em; }}
  </style>
</head>
<body class="bg-[#09090b] text-zinc-100 min-h-screen antialiased selection:bg-[#ff3366] selection:text-white">

  <!-- Top Announcement Bar -->
  <div class="border-b border-zinc-800 bg-zinc-950 px-4 py-2 text-xs font-mono flex items-center justify-between text-zinc-400">
    <div class="flex items-center gap-2">
      <span class="inline-block w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
      <span class="text-zinc-200 font-semibold">// LIVE FEED:</span>
      <span class="hidden sm:inline">BBC FOOTBALL (30) • TECH (30) • GEMATSU GAMING (30)</span>
      <span class="text-emerald-400 font-bold px-1.5 py-0.5 bg-emerald-500/10 rounded border border-emerald-500/30">ONLINE</span>
    </div>
    <div class="flex items-center gap-4 text-[11px]">
      <span class="text-zinc-400 font-mono hidden md:inline">เวลาไทย: {now_th}</span>
      <span class="text-emerald-400 font-bold">ENGINE: GEMINI 3.1 FLASH-LITE</span>
    </div>
  </div>

  <!-- Header -->
  <header class="border-b-2 border-zinc-800 bg-[#0c0c0e] sticky top-0 z-40 backdrop-blur-md bg-opacity-95">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 py-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 bg-gradient-to-tr from-[#ff3366] to-[#00e5ff] flex items-center justify-center font-black text-black text-2xl font-mono shadow-[0_0_20px_rgba(255,51,102,0.4)]">
          V
        </div>
        <div>
          <h1 class="verge-title text-2xl md:text-3xl font-black uppercase text-white flex items-center gap-2">
            THE VERGE <span class="text-[#ff3366] text-xs font-mono font-bold tracking-widest px-2 py-0.5 border border-[#ff3366]/40 rounded-sm">READER</span>
          </h1>
          <p class="text-[11px] font-mono text-zinc-400 uppercase tracking-widest">30 ข่าวเด่นต่อหมวด • อัปเดตทุก 1 ชั่วโมง</p>
        </div>
      </div>

      <!-- Tag Filters -->
      <div class="flex flex-wrap items-center gap-2">
        <button onclick="filterTag('ALL')" id="filterALL" class="px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded-sm transition">ALL ({len(news_list)})</button>
        <button onclick="filterTag('Football')" id="filterFootball" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded-sm transition">⚽ FOOTBALL ({football_count})</button>
        <button onclick="filterTag('Technology')" id="filterTechnology" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-cyan-400 font-mono text-xs rounded-sm transition">⚡ TECH ({tech_count})</button>
        <button onclick="filterTag('Gaming')" id="filterGaming" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-amber-400 font-mono text-xs rounded-sm transition">🎮 GAMING ({gaming_count})</button>
      </div>

      <!-- Search & Refresh -->
      <div class="flex items-center gap-2">
        <input id="searchInput" type="text" oninput="handleSearch()" placeholder="ค้นหาข่าว..." class="bg-zinc-900 border border-zinc-800 text-xs px-3 py-1.5 rounded-sm text-zinc-200 placeholder-zinc-500 focus:outline-none focus:border-[#ff3366] w-40 sm:w-48 font-sans">
        <button onclick="location.reload()" title="รีเฟรชหน้าเว็บ" class="p-1.5 bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 text-zinc-300 rounded-sm transition">
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

  <!-- Centered Luxury Verge Modal (Clean Dialog) -->
  <div id="modalOverlay" onclick="if(event.target===this) closeModal()" class="fixed inset-0 bg-black/85 backdrop-blur-md z-50 hidden flex items-center justify-center p-3 sm:p-6 overflow-y-auto">
    <div class="w-full max-w-3xl bg-[#0c0c0e] border border-zinc-800 rounded-lg shadow-2xl overflow-hidden my-auto max-h-[92vh] flex flex-col">
      
      <!-- Modal Header (Sticky) -->
      <div class="flex items-center justify-between px-6 py-4 border-b border-zinc-800 bg-[#0e0e11] sticky top-0 z-10">
        <div class="flex items-center gap-2">
          <span id="modalTag" class="px-2.5 py-0.5 rounded text-xs font-mono font-bold uppercase tracking-wider bg-[#ff3366]/15 text-[#ff3366] border border-[#ff3366]/30"></span>
          <span id="modalDate" class="text-xs font-mono text-zinc-400"></span>
        </div>
        <button onclick="closeModal()" class="w-8 h-8 rounded-full bg-zinc-900 hover:bg-zinc-800 text-zinc-400 hover:text-white flex items-center justify-center text-xl transition leading-none">&times;</button>
      </div>

      <!-- Modal Body (Scrollable) -->
      <div class="p-6 md:p-8 overflow-y-auto space-y-6">
        <!-- HD Cover Image -->
        <div class="w-full h-64 sm:h-80 overflow-hidden rounded-md border border-zinc-800 bg-zinc-950">
          <img id="modalImg" src="" referrerpolicy="no-referrer" class="w-full h-full object-cover">
        </div>

        <!-- Title -->
        <h2 id="modalTitle" class="verge-title text-xl md:text-2xl font-bold text-white leading-snug"></h2>

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

        <!-- Multi-Image Screenshots Gallery (if available) -->
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
    let searchQuery = '';

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
            btn.className = "px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded-sm transition";
          }} else {{
            let textColor = 'text-zinc-300';
            if (t === 'Gaming') textColor = 'text-amber-400';
            if (t === 'Technology') textColor = 'text-cyan-400';
            btn.className = `px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 ${{textColor}} font-mono text-xs rounded-sm transition`;
          }}
        }}
      }});
      render();
    }}

    function handleSearch() {{
      searchQuery = document.getElementById('searchInput').value.toLowerCase();
      render();
    }}

    function render() {{
      const grid = document.getElementById('newsGrid');
      grid.innerHTML = '';

      const filtered = allNews.filter(item => {{
        const matchTag = (currentTag === 'ALL') || (item.tag === currentTag);
        const matchQuery = !searchQuery || 
          (item.thai_title || '').toLowerCase().includes(searchQuery) ||
          (item.tag || '').toLowerCase().includes(searchQuery);
        return matchTag && matchQuery;
      }});

      if (filtered.length === 0) {{
        grid.innerHTML = '<div class="col-span-3 text-center py-16 text-zinc-500 font-mono">ไม่มีข่าวในหมวดนี้</div>';
        return;
      }}

      filtered.forEach((item) => {{
        const originalIndex = allNews.indexOf(item);
        const div = document.createElement('div');
        div.className = 'bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition flex flex-col justify-between overflow-hidden rounded-sm cursor-pointer group';
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
              <h3 class="verge-title text-base font-bold text-white group-hover:text-[#ff3366] transition mb-3 leading-snug line-clamp-2">
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
          a.className = 'block aspect-video overflow-hidden rounded bg-zinc-950 border border-zinc-800 hover:border-amber-400 transition group';
          a.innerHTML = `<img src="${{imgUrl}}" referrerpolicy="no-referrer" class="w-full h-full object-cover group-hover:scale-105 transition duration-300">`;
          galleryGrid.appendChild(a);
        }});
      }} else {{
        gallerySec.classList.add('hidden');
      }}

      document.getElementById('modalSource').href = item.link;
      document.getElementById('modalOverlay').classList.remove('hidden');
    }}

    function closeModal() {{
      document.getElementById('modalOverlay').classList.add('hidden');
    }}

    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape') closeModal();
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

            # Extract image from summary if present (Gematsu lead image)
            raw_summary = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
            img_m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_summary)
            feed_img = img_m.group(1) if img_m else ""

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
    logger.info("  STARTING 3-CATEGORY NEWS PIPELINE (30 PER TOPIC)")
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
        # Normalize category tag
        tag = it.get("tag", "")
        if "chelsea" in tag.lower(): it["tag"] = "Football"
        it["cover_image_url"] = make_hd_image_url(it.get("cover_image_url", ""))
        existing_by_link[link] = it

    logger.info(f"Loaded {len(existing_by_link)} existing stories from database.")

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    twelve_hours_ago = now_utc - datetime.timedelta(hours=12)

    # Feed definitions per category
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
            "https://www.gematsu.com/feed"
        ]
    }

    category_results = {}

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

        # For Gaming: if Gematsu feed gave fewer than 30 candidates, backfill from Gematsu homepage highlights
        if cat == "Gaming" and len(candidates) < TARGET_PER_CATEGORY:
            logger.info("Fetching additional Gematsu articles via homepage to ensure 30 gaming candidates...")
            try:
                jina_home = "https://r.jina.ai/https://www.gematsu.com/"
                req = urllib.request.Request(jina_home, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=12) as resp:
                    home_txt = resp.read().decode('utf-8', errors='ignore')
                raw_links = re.findall(r'https://www\.gematsu\.com/\d{4}/\d{2}/[^\s\)\"\']+', home_txt)
                for l in raw_links:
                    clean_l = l.split('#')[0]
                    if clean_l not in seen_links:
                        seen_links.add(clean_l)
                        # Derive title from slug
                        slug = clean_l.rstrip('/').split('/')[-1]
                        title = slug.replace('-', ' ').title()
                        candidates.append({
                            "title": title,
                            "link": clean_l,
                            "description": title,
                            "published_th": to_thailand_time(now_utc),
                            "category_hint": "Gaming",
                            "feed_image": "",
                            "_pub_dt": now_utc - datetime.timedelta(hours=2)
                        })
            except Exception as e:
                logger.warning(f"Could not backfill Gematsu homepage: {e}")

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

        # Limit to 30 candidates for this category
        selected_candidates = selected_candidates[:TARGET_PER_CATEGORY]

        # Separate into already-translated vs genuinely new
        final_category_articles = []
        new_items_to_translate = []

        for item in selected_candidates:
            link = item["link"]
            if link in existing_by_link:
                # Keep already translated article to save tokens
                existing_item = existing_by_link[link]
                # Ensure correct tag
                existing_item["tag"] = cat
                # Upgrade images
                existing_item["cover_image_url"] = make_hd_image_url(existing_item.get("cover_image_url", ""))
                final_category_articles.append(existing_item)
            else:
                new_items_to_translate.append(item)

        logger.info(f"[{cat}] Reusing {len(final_category_articles)} existing translated articles. Translating {len(new_items_to_translate)} new articles.")

        # Translate new items (if any)
        if new_items_to_translate:
            logger.info(f"[{cat}] Scraping full content and gallery for {len(new_items_to_translate)} articles...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                scraped_items = list(executor.map(scrape_article_details, new_items_to_translate))

            logger.info(f"[{cat}] Translating {len(scraped_items)} articles with Gemini Flash-Lite...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
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
                existing_by_link[item["link"]] = entry # remember for future deduplication

        # Ensure exactly up to 30 articles for this category
        category_results[cat] = final_category_articles[:TARGET_PER_CATEGORY]
        logger.info(f"[{cat}] Completed! Total articles: {len(category_results[cat])}/{TARGET_PER_CATEGORY}")

    # Combine all 3 categories (90 articles total)
    combined_db = []
    for cat in ["Football", "Technology", "Gaming"]:
        combined_db.extend(category_results.get(cat, []))

    # Save to data/news.json
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(combined_db, f, ensure_ascii=False, indent=2)

    # Update index.html and README.md
    update_ui_files(combined_db)

    logger.info("==================================================")
    logger.info(f"[✓] Pipeline complete! Maintained {len(combined_db)} total stories (30 Football, 30 Tech, 30 Gaming).")
    logger.info("==================================================")

if __name__ == "__main__":
    run()
