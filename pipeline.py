"""
Automated News Aggregator & Reader (GitHub Native)
Feeds:
1. Football: BBC Sport Football (http://feeds.bbci.co.uk/sport/football/rss.xml)
2. Technology: The Verge (https://www.theverge.com/rss/index.xml)
3. Gaming: Gematsu (https://www.gematsu.com/feed)

Features:
- Timezone: Converts all timestamps to Thailand Time (UTC+7).
- Crystal-Clear 1200px HD Images for all sources.
- Full multi-paragraph in-depth translation + bullet points.
- 1-Hour check interval via GitHub Actions.
- Tabs: ALL, ⚽ FOOTBALL, ⚡ TECH, 🎮 GAMING.
- Centered modern modal dialog.
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
TARGET_TOTAL_ARTICLES = 30
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

RSS_FEEDS = [
    {"category": "Football", "url": "http://feeds.bbci.co.uk/sport/football/rss.xml"},
    {"category": "Technology", "url": "https://www.theverge.com/rss/index.xml"},
    {"category": "Gaming", "url": "https://www.gematsu.com/feed"}
]

SYSTEM_PROMPT = """คุณคือนักข่าวและบรรณาธิการข่าวมืออาชีพ แปลและสรุปข่าวเป็นภาษาไทยอย่างถูกต้อง สละสลวย และตรงตามข้อเท็จจริง (Fact-based)
ข้อกำหนด:
1. ข่าวฟุตบอล: ทับศัพท์เทคนิคและชื่อนักเตะ/สโมสรอย่างถูกต้อง
2. ข่าวเทคโนโลยี: ทับศัพท์ชื่อระบบ ฮาร์ดแวร์ และแบรนด์อย่างถูกต้อง
3. ข่าวเกม (Gaming): ทับศัพท์ชื่อเกม แพลตฟอร์ม (PS5, Xbox, Switch, PC) และค่ายเกมอย่างถูกต้อง
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
    # Upgrade The Verge images
    if 'theverge.com' in url or 'platform.theverge' in url:
        if '?' in url:
            url = re.sub(r'w=\d+', 'w=1200', url)
            if 'w=' not in url: url += '&w=1200'
    return url

def scrape_full_article(link: str, default_desc: str) -> tuple[str, list[str]]:
    """Scrape article webpage to extract HD lead image and full paragraphs."""
    try:
        req = urllib.request.Request(link, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=8) as resp:
            content = resp.read().decode('utf-8', errors='ignore')

        # HD lead image from og:image
        img_m = re.search(r'property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', content) or \
                re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', content)
        hd_img = make_hd_image_url(img_m.group(1)) if img_m else None

        # Clean paragraphs
        clean_paras = []
        # Gematsu article paragraphs
        if 'gematsu.com' in link:
            article_match = re.search(r'<article[^>]*>(.*?)</article>', content, re.DOTALL)
            body_html = article_match.group(1) if article_match else content
            raw_p = re.findall(r'<p[^>]*>(.*?)</p>', body_html, re.DOTALL)
            for p in raw_p:
                c = clean_text(p)
                if len(c) > 30 and not any(c.startswith(x) for x in ['Source:', 'Thanks,', 'Watch the']):
                    clean_paras.append(c)

        # The Verge duet article paragraphs
        elif 'theverge.com' in link:
            duet = re.findall(r'<p[^>]*class=["\'][^"\']*duet--article[^"\']*["\'][^>]*>(.*?)</p>', content, re.DOTALL)
            for p in duet:
                c = clean_text(p)
                if len(c) > 30 and not any(x in c for x in ['Posts from this', 'Sign up for']):
                    clean_paras.append(c)

        # BBC Sport Football paragraphs
        if not clean_paras:
            raw_p = re.findall(r'<p[^>]*>(.*?)</p>', content, re.DOTALL)
            for p in raw_p:
                c = clean_text(p)
                if len(c) > 35 and not any(c.startswith(x) for x in ['Photo by', 'Follow ', 'Sign up', 'Share this', 'Published', 'Image source', 'Cast your']):
                    clean_paras.append(c)

        clean_paras = list(dict.fromkeys(clean_paras)) # deduplicate
        return hd_img or "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80", clean_paras[:12] if clean_paras else [default_desc]
    except Exception:
        return "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80", [default_desc]

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

    # 1. README.md
    readme_lines = [
        "# ⚡ Automated News Aggregator (GitHub Native)",
        "",
        "> **Live Automated News Reader:** Auto-scraped from BBC Sport Football, The Verge, and Gematsu (Gaming).",
        f"> *อัปเดตล่าสุด: `{now_th}`* • *จำนวนข่าวทั้งหมด: `{len(news_list)}/30 ข่าว`*",
        "",
        "### 🌐 [กดที่นี่เพื่อเปิดหน้าเว็บอ่านข่าว (GitHub Pages)](https://puresinghara.github.io/pure-news/)",
        "",
        "---",
        "",
        "### 📰 ตารางสรุปข่าวล่าสุด (30 เรื่องเด่น)",
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

    # 2. index.html (Centered luxury Verge reader with Gaming tab & Thailand time)
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
      <span class="hidden sm:inline">BBC FOOTBALL • THE VERGE TECH • GEMATSU GAMING</span>
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
          <p class="text-[11px] font-mono text-zinc-400 uppercase tracking-widest">Football • Technology • Gaming (30 Stories)</p>
        </div>
      </div>

      <!-- Tag Filters -->
      <div class="flex flex-wrap items-center gap-2">
        <button onclick="filterTag('ALL')" id="filterALL" class="px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded-sm transition">ALL</button>
        <button onclick="filterTag('Football')" id="filterFootball" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded-sm transition">⚽ FOOTBALL</button>
        <button onclick="filterTag('Technology')" id="filterTechnology" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded-sm transition">⚡ TECH</button>
        <button onclick="filterTag('Gaming')" id="filterGaming" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-amber-400 font-mono text-xs rounded-sm transition">🎮 GAMING</button>
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
        <!-- HD Image -->
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

    // Cache-busting: fetch newest json in background if served via http
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
            const textColor = t === 'Gaming' ? 'text-amber-400' : 'text-zinc-300';
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

      filtered.forEach((item, idx) => {{
        const div = document.createElement('div');
        div.className = 'bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition flex flex-col justify-between overflow-hidden rounded-sm cursor-pointer group';
        div.onclick = () => openModal(allNews.indexOf(item));
        
        const bulletsHtml = (item.bullet_points || []).slice(0, 3).map(b => 
          `<li class="flex items-start gap-1.5"><span class="text-[#ff3366] font-bold">›</span><span>${{b}}</span></li>`
        ).join('');

        let tagColor = 'text-[#ff3366]';
        if (item.tag === 'Technology') tagColor = 'text-cyan-400';
        else if (item.tag === 'Gaming') tagColor = 'text-amber-400';

        div.innerHTML = `
          <div>
            <div class="w-full h-44 overflow-hidden bg-zinc-950">
              <img src="${{item.cover_image_url}}" referrerpolicy="no-referrer" class="w-full h-full object-cover group-hover:scale-105 transition duration-500" onerror="this.onerror=null; this.src='https://images.unsplash.com/photo-1511512578047-dfb367046420?w=800&q=80';">
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

def run():
    logger.info("==================================================")
    logger.info("  STARTING 3-CATEGORY NEWS PIPELINE               ")
    logger.info("==================================================")

    os.makedirs(DATA_DIR, exist_ok=True)
    existing_news = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                existing_news = json.load(f)
        except Exception:
            pass

    existing_links = {it.get("link", "").strip() for it in existing_news}
    logger.info(f"Loaded {len(existing_news)} existing stories in repository.")

    # Fetch candidates from all 3 feeds
    raw_candidates = []
    seen = set()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    for feed_info in RSS_FEEDS:
        url = feed_info["url"]
        cat = feed_info["category"]
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                link = getattr(entry, 'link', '').strip()
                title = getattr(entry, 'title', '').strip()
                if not link or link in seen or any(x in title.lower() for x in ["who am i", "quiz", "football daily"]):
                    continue
                seen.add(link)

                pub_tuple = getattr(entry, 'published_parsed', None) or getattr(entry, 'updated_parsed', None)
                if pub_tuple:
                    pub_dt = datetime.datetime.fromtimestamp(time.mktime(pub_tuple), tz=datetime.timezone.utc)
                else:
                    pub_dt = now_utc

                raw_candidates.append({
                    "title": title,
                    "link": link,
                    "description": clean_text(getattr(entry, 'summary', '') or getattr(entry, 'description', '')),
                    "published_th": to_thailand_time(pub_dt),
                    "category_hint": cat,
                    "_pub_dt": pub_dt
                })
        except Exception as e:
            logger.error(f"Error reading feed {url}: {e}")

    # Sort newest first
    raw_candidates.sort(key=lambda x: x["_pub_dt"], reverse=True)

    # Filter to new items
    new_to_process = [c for c in raw_candidates if c["link"] not in existing_links]
    logger.info(f"Found {len(new_to_process)} brand NEW stories across Football, Tech, and Gaming.")

    # Limit new items to at most 15 per run
    new_to_process = new_to_process[:15]

    if new_to_process:
        # Scrape full article & HD images concurrently
        logger.info(f"Scraping detailed content concurrently for {len(new_to_process)} articles...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            def scrape_worker(item):
                img_url, paras = scrape_full_article(item["link"], item["description"])
                item["cover_image_url"] = img_url
                item["raw_paragraphs"] = paras
                return item

            new_to_process = list(executor.map(scrape_worker, new_to_process))

        # Parallel translation with Gemini
        logger.info(f"Translating {len(new_to_process)} articles with Gemini in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            translations = list(executor.map(call_gemini, new_to_process))

        processed_entries = []
        for item, trans in zip(new_to_process, translations):
            entry = {
                "id": int(time.time() * 1000) + len(processed_entries),
                "original_title": item["title"],
                "thai_title": trans.get("thai_title", item["title"]),
                "tag": trans.get("tag", item["category_hint"]),
                "bullet_points": trans.get("bullet_points", []),
                "full_paragraphs": trans.get("full_paragraphs", item["raw_paragraphs"][:4]),
                "link": item["link"],
                "cover_image_url": item["cover_image_url"],
                "published_th": item["published_th"],
                "category_hint": item["category_hint"]
            }
            processed_entries.append(entry)

        # Prepend new to existing
        updated_db = processed_entries + existing_news
    else:
        logger.info("No new articles. Upgrading images and timestamps on existing articles...")
        updated_db = existing_news

    # Purge any old entries with low-res 240px images or missing Thai times
    cleaned_db = []
    for it in updated_db:
        # Ensure HD image
        it["cover_image_url"] = make_hd_image_url(it.get("cover_image_url", ""))
        # Ensure published_th exists
        if "published_th" not in it and "published_at" in it:
            it["published_th"] = it["published_at"].replace("UTC", "เวลาไทย")
        # Replace any Chelsea FC tag with Football
        if it.get("tag") == "Chelsea FC":
            it["tag"] = "Football"
        cleaned_db.append(it)

    final_db = cleaned_db[:TARGET_TOTAL_ARTICLES]

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_db, f, ensure_ascii=False, indent=2)

    update_ui_files(final_db)
    logger.info(f"[✓] Pipeline complete! Maintained {len(final_db)} stories in database.")

if __name__ == "__main__":
    run()
