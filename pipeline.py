"""
Automated GitHub-Native News Pipeline (Zero Notion Required)
Features:
- STEP A: Scrape BBC Sport Football & The Verge RSS feeds (Filtered: Last 12 Hours, Max 30 items).
- STEP B: Deduplication against local data/news.json (Skips existing articles to save Gemini tokens).
- STEP C: Gemini AI Factual Translation & Auto-Tagging ([Football], [Technology], [Chelsea FC]).
- STEP D: Updates data/news.json (capped at 30 items), index.html (GitHub Pages), and README.md.
"""
import os
import json
import time
import datetime
import re
import html
import logging
import urllib.request
import feedparser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GitHubNewsPipeline")

# Directories and paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "news.json")
INDEX_HTML = os.path.join(BASE_DIR, "index.html")
README_FILE = os.path.join(BASE_DIR, "README.md")

# Load environment variable
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
if not GEMINI_API_KEY:
    # Try reading from .env if running locally
    env_file = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith("GEMINI_API_KEY="):
                    GEMINI_API_KEY = line.split("=", 1)[1].strip()

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip()
MAX_ARTICLES = int(os.getenv("MAX_ARTICLES", "30"))
LOOKBACK_HOURS = 12

RSS_FEEDS = [
    {"category_hint": "Football", "url": "http://feeds.bbci.co.uk/sport/football/rss.xml"},
    {"category_hint": "Technology", "url": "https://www.theverge.com/rss/index.xml"}
]

SYSTEM_PROMPT = """คุณคือนักข่าวสายเทคและกีฬา แปลและสรุปข่าวนี้เป็นภาษาไทยแบบสั้นกระชับเป็น Bullet points ตัดน้ำทิ้ง เอาแต่ Fact. หากเป็นข่าวฟุตบอล ให้ใช้ทับศัพท์ศัพท์เทคนิคฟุตบอล หากเป็นข่าวเทคโนโลยี ให้ทับศัพท์ชื่อระบบหรือฮาร์ดแวร์ ห้ามใส่อารมณ์ตื่นเต้นลงในเนื้อหา

กฎการจัดหมวดหมู่ (Tag Classification):
- หากเนื้อหาข่าวเกี่ยวข้องกับสโมสรเชลซี (Chelsea FC / Chelsea) ให้จัดแท็กเป็น: "Chelsea FC"
- หากเป็นข่าวฟุตบอลทั่วไป ให้จัดแท็กเป็น: "Football"
- หากเป็นข่าวเทคโนโลยี นวัตกรรม AI หรือฮาร์ดแวร์ ให้จัดแท็กเป็น: "Technology"
(เลือกได้เฉพาะ 1 แท็กจาก 3 ตัวเลือกนี้เท่านั้น)"""

def clean_html_text(raw_html: str) -> str:
    if not raw_html: return ""
    clean = re.sub(r'<[^>]+>', '', raw_html).strip()
    return " ".join(html.unescape(clean).split())

def extract_cover_image(entry) -> str:
    default_img = "https://images.unsplash.com/photo-1518770660439-4636190af475?w=1200&q=80"
    if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
        return html.unescape(entry.media_thumbnail[0].get('url', default_img))
    if hasattr(entry, 'media_content') and entry.media_content:
        return html.unescape(entry.media_content[0].get('url', default_img))
    if hasattr(entry, 'enclosures') and entry.enclosures:
        return html.unescape(entry.enclosures[0].get('href', default_img))
    if hasattr(entry, 'content') and entry.content:
        for c in entry.content:
            m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', c.get('value', ''))
            if m: return html.unescape(m.group(1))
    if hasattr(entry, 'summary') and entry.summary:
        m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', entry.summary)
        if m: return html.unescape(m.group(1))
    return default_img

def fetch_rss_candidates() -> list[dict]:
    """Fetch RSS feeds and filter to items within last 12 hours."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now_utc - datetime.timedelta(hours=LOOKBACK_HOURS)
    candidates = []
    seen = set()

    for feed_info in RSS_FEEDS:
        url = feed_info["url"]
        cat = feed_info["category_hint"]
        logger.info(f"Checking feed: {cat} ({url})")
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            logger.error(f"Error parsing feed {url}: {e}")
            continue

        for entry in feed.entries:
            link = getattr(entry, 'link', '').strip()
            if not link or link in seen: continue

            pub_tuple = getattr(entry, 'published_parsed', None) or getattr(entry, 'updated_parsed', None)
            if pub_tuple:
                pub_dt = datetime.datetime.fromtimestamp(time.mktime(pub_tuple), tz=datetime.timezone.utc)
                if pub_dt < cutoff:
                    continue
            else:
                pub_dt = now_utc

            title = getattr(entry, 'title', '').strip()
            if any(x in title.lower() for x in ["who am i", "quiz", "football daily"]):
                continue

            summary = clean_html_text(getattr(entry, 'summary', '') or getattr(entry, 'description', ''))
            img = extract_cover_image(entry)
            seen.add(link)

            candidates.append({
                "title": title,
                "link": link,
                "description": summary,
                "cover_image_url": img,
                "published_at": pub_dt.strftime("%Y-%m-%d %H:%M UTC"),
                "category_hint": cat,
                "_pub_dt": pub_dt
            })

    candidates.sort(key=lambda x: x["_pub_dt"], reverse=True)
    for c in candidates: del c["_pub_dt"]
    return candidates[:MAX_ARTICLES]

def call_gemini_api(title: str, description: str, category_hint: str) -> dict:
    """Call Gemini for factual summary and auto-tagging."""
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY is not set.")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    user_prompt = f"""หัวข้อข่าวต้นฉบับ: {title}
เนื้อหาเบื้องต้น: {description}
หมวดหมู่เบื้องต้น: {category_hint}

กรุณาตอบกลับเป็นโครงสร้าง JSON ตามรูปแบบนี้เท่านั้น:
{{
  "thai_title": "พาดหัวข่าวภาษาไทยที่กระชับและตรงตาม Fact",
  "tag": "Football | Technology | Chelsea FC",
  "bullet_points": [
    "ข้อเท็จจริงประเด็นที่ 1...",
    "ข้อเท็จจริงประเด็นที่ 2...",
    "ข้อเท็จจริงประเด็นที่ 3..."
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
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
                parsed = json.loads(raw_text)

                tag = parsed.get("tag", "").strip()
                if "chelsea" in tag.lower(): parsed["tag"] = "Chelsea FC"
                elif "tech" in tag.lower(): parsed["tag"] = "Technology"
                elif "foot" in tag.lower(): parsed["tag"] = "Football"
                else: parsed["tag"] = "Chelsea FC" if "chelsea" in title.lower() else category_hint
                return parsed
        except Exception as e:
            logger.warning(f"Gemini call attempt {attempt+1}/3 failed: {e}")
            time.sleep(2)

    fallback_tag = "Chelsea FC" if "chelsea" in title.lower() else category_hint
    return {
        "thai_title": title,
        "tag": fallback_tag,
        "bullet_points": [description] if description else ["ไม่มีรายละเอียดเพิ่มเติม"]
    }

def update_readme(news_list: list[dict]):
    """Generate dynamic README.md display table for GitHub viewers."""
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# ⚡ Automated News Aggregator (GitHub Native)",
        "",
        "> **Live Automated News Reader:** Auto-scraped, filtered within last 12 hours, deduplicated, and summarized into facts via Gemini AI.",
        f"> *Last updated: `{now_str}`* • *Total Stories: `{len(news_list)}/30`*",
        "",
        "### 🌐 [Click Here to Open Full Webview (GitHub Pages)](https://puresinghara.github.io/notion-news-pipeline/)",
        "",
        "---",
        "",
        "### 📰 Latest News Stories (12-Hour Window)",
        "",
        "| Tag | Headline & Facts | Published | Source |",
        "| :--- | :--- | :--- | :--- |"
    ]

    for item in news_list:
        tag_badge = f"`[{item['tag']}]`"
        bullets_md = "<br>".join([f"• {b}" for b in item.get('bullet_points', [])[:3]])
        headline = f"**{item['thai_title']}**<br>{bullets_md}"
        link = f"[Read Article]({item['link']})"
        pub = item.get('published_at', '')
        lines.append(f"| {tag_badge} | {headline} | {pub} | {link} |")

    lines.append("")
    lines.append("---")
    lines.append("*Automated 100% by GitHub Actions and Google Gemini AI.*")

    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    logger.info("Updated README.md with latest news!")

def update_index_html(news_list: list[dict]):
    """Update index.html with embedded JSON for instant offline & GitHub Pages rendering."""
    json_data_str = json.dumps(news_list, ensure_ascii=False)
    
    html_content = f'''<!DOCTYPE html>
<html lang="th" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>THE VERGE // GITHUB NATIVE LIVE NEWS READER</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@400;600;700&family=Prompt:wght@300;400;500;600&display=swap');
    body {{ font-family: 'Prompt', sans-serif; }}
    .verge-title {{ font-family: 'Chakra Petch', sans-serif; letter-spacing: -0.02em; }}
  </style>
</head>
<body class="bg-[#09090b] text-zinc-100 min-h-screen antialiased selection:bg-[#ff3366] selection:text-white">

  <!-- Top Announcement Bar -->
  <div class="border-b border-zinc-800 bg-zinc-950 px-4 py-2 text-xs font-mono flex items-center justify-between text-zinc-400">
    <div class="flex items-center gap-2">
      <span class="inline-block w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
      <span class="text-zinc-200 font-semibold">// LIVE GITHUB FEED:</span>
      <span>AUTOMATED 12H WINDOW • ZERO TOKENS WASTED</span>
    </div>
    <div class="flex items-center gap-4 text-[11px]">
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
            THE VERGE <span class="text-[#ff3366] text-xs font-mono font-bold tracking-widest px-2 py-0.5 border border-[#ff3366]/40 rounded-sm">GITHUB EDITION</span>
          </h1>
          <p class="text-[11px] font-mono text-zinc-400 uppercase tracking-widest">Football & Technology • Facts Summarization</p>
        </div>
      </div>

      <!-- Tag Filters -->
      <div class="flex flex-wrap items-center gap-2">
        <button onclick="filterTag('ALL')" id="filterALL" class="px-3 py-1 bg-white text-black font-mono font-bold text-xs rounded-sm transition">ALL</button>
        <button onclick="filterTag('Football')" id="filterFootball" class="px-3 py-1 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded-sm transition">⚽ FOOTBALL</button>
        <button onclick="filterTag('Technology')" id="filterTechnology" class="px-3 py-1 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded-sm transition">⚡ TECH</button>
        <button onclick="filterTag('Chelsea FC')" id="filterChelsea" class="px-3 py-1 bg-zinc-900 hover:bg-zinc-800 text-cyan-400 font-mono text-xs rounded-sm transition">🦁 CHELSEA FC</button>
      </div>

      <!-- Search -->
      <div>
        <input id="searchInput" type="text" oninput="handleSearch()" placeholder="ค้นหาข่าว..." class="bg-zinc-900 border border-zinc-800 text-xs px-3 py-1.5 rounded-sm text-zinc-200 placeholder-zinc-500 focus:outline-none focus:border-[#ff3366] w-48 font-sans">
      </div>
    </div>
  </header>

  <!-- Main Container -->
  <main class="max-w-7xl mx-auto px-4 sm:px-6 py-8">
    <div id="newsGrid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      <!-- Populated by JS -->
    </div>
  </main>

  <!-- Slide-Over Modal -->
  <div id="modalOverlay" class="fixed inset-0 bg-black/85 backdrop-blur-md z-50 hidden flex justify-end">
    <div class="w-full max-w-xl bg-zinc-950 border-l border-zinc-800 h-full p-6 md:p-8 overflow-y-auto flex flex-col justify-between">
      <div>
        <div class="flex items-center justify-between pb-4 border-b border-zinc-800 mb-6">
          <span id="modalTag" class="text-xs font-mono font-bold text-[#ff3366] uppercase"></span>
          <button onclick="closeModal()" class="text-zinc-400 hover:text-white text-3xl font-mono leading-none">&times;</button>
        </div>
        <div class="w-full h-64 overflow-hidden rounded-sm mb-6 bg-zinc-900">
          <img id="modalImg" src="" class="w-full h-full object-cover">
        </div>
        <h2 id="modalTitle" class="verge-title text-xl md:text-2xl font-bold text-white mb-4"></h2>
        <div id="modalDate" class="text-xs font-mono text-zinc-400 mb-6 pb-4 border-b border-zinc-800"></div>
        <div class="bg-black/60 border-l-2 border-[#ff3366] p-4 mb-6">
          <div class="text-xs font-mono text-[#ff3366] uppercase font-bold tracking-wider mb-2">📌 สรุปข้อเท็จจริง (KEY FACTS):</div>
          <ul id="modalBullets" class="space-y-2 text-sm text-zinc-200"></ul>
        </div>
      </div>
      <div class="pt-6 border-t border-zinc-800 flex items-center justify-between">
        <a id="modalSource" href="#" target="_blank" class="px-4 py-2 bg-zinc-900 border border-zinc-700 text-xs font-mono text-white rounded-sm hover:bg-zinc-800">
          อ่านต้นฉบับเต็ม →
        </a>
        <button onclick="closeModal()" class="px-4 py-2 bg-zinc-800 text-xs font-mono text-zinc-300 rounded-sm">ปิด</button>
      </div>
    </div>
  </div>

  <script>
    const allNews = {json_data_str};
    let currentTag = 'ALL';
    let searchQuery = '';

    function filterTag(tag) {{
      currentTag = tag;
      ['ALL', 'Football', 'Technology', 'Chelsea'].forEach(t => {{
        const btn = document.getElementById('filter' + t);
        if (btn) {{
          if ((t === 'Chelsea' && tag === 'Chelsea FC') || t === tag) {{
            btn.className = "px-3 py-1 bg-white text-black font-mono font-bold text-xs rounded-sm transition";
          }} else {{
            btn.className = "px-3 py-1 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 font-mono text-xs rounded-sm transition";
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
        grid.innerHTML = '<div class="col-span-3 text-center py-16 text-zinc-500 font-mono">ไม่มีข่าวในหมวดนี้ใน 12 ชั่วโมงล่าสุด</div>';
        return;
      }}

      filtered.forEach((item, idx) => {{
        const div = document.createElement('div');
        div.className = 'bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition flex flex-col justify-between overflow-hidden rounded-sm cursor-pointer group';
        div.onclick = () => openModal(allNews.indexOf(item));
        
        const bulletsHtml = (item.bullet_points || []).slice(0, 3).map(b => 
          `<li class="flex items-start gap-1.5"><span class="text-[#ff3366] font-bold">›</span><span>${{b}}</span></li>`
        ).join('');

        div.innerHTML = `
          <div>
            <div class="w-full h-44 overflow-hidden bg-zinc-950">
              <img src="${{item.cover_image_url}}" class="w-full h-full object-cover group-hover:scale-105 transition duration-500">
            </div>
            <div class="p-5">
              <div class="flex items-center gap-2 text-[11px] font-mono mb-2">
                <span class="text-[#ff3366] font-bold">${{item.tag}}</span>
                <span class="text-zinc-600">•</span>
                <span class="text-zinc-500">${{item.published_at}}</span>
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
            <span class="text-zinc-300 group-hover:text-white transition">อ่านรายละเอียด →</span>
            <span>VERGE FEED</span>
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
      document.getElementById('modalDate').textContent = `เผยแพร่: ${{item.published_at}}`;
      
      const bulletsList = document.getElementById('modalBullets');
      bulletsList.innerHTML = '';
      (item.bullet_points || []).forEach(b => {{
        const li = document.createElement('li');
        li.className = 'flex items-start gap-2';
        li.innerHTML = `<span class="text-[#ff3366] font-bold">›</span><span>${{b}}</span>`;
        bulletsList.appendChild(li);
      }});

      document.getElementById('modalSource').href = item.link;
      document.getElementById('modalOverlay').classList.remove('hidden');
    }}

    function closeModal() {{
      document.getElementById('modalOverlay').classList.add('hidden');
    }}

    render();
  </script>
</body>
</html>'''

    with open(INDEX_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info("Updated index.html for GitHub Pages!")

def run():
    logger.info("==================================================")
    logger.info("  STARTING GITHUB NATIVE NEWS PIPELINE            ")
    logger.info("==================================================")

    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Load existing news database from data/news.json
    existing_news = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                existing_news = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load existing data: {e}")

    existing_links = {it.get("link", "").strip() for it in existing_news}
    logger.info(f"Loaded {len(existing_news)} existing stories in repository.")

    # STEP A: Scrape & Filter (Last 12 hours)
    candidates = fetch_rss_candidates()
    logger.info(f"Scraped {len(candidates)} candidates within last {LOOKBACK_HOURS} hours.")

    new_processed = []
    skipped_count = 0

    for item in candidates:
        link = item["link"].strip()

        # STEP B: Deduplication against local repo database
        if link in existing_links:
            logger.info(f"  -> [SKIP] Already exists: '{item['title'][:40]}...' (Saving LLM tokens)")
            skipped_count += 1
            continue

        # STEP C: AI Processing via Gemini
        logger.info(f"  -> [NEW] Translating with Gemini: '{item['title'][:40]}...'")
        ai_res = call_gemini_api(item["title"], item["description"], item["category_hint"])
        
        new_entry = {
            "id": int(time.time() * 1000),
            "original_title": item["title"],
            "thai_title": ai_res.get("thai_title", item["title"]),
            "tag": ai_res.get("tag", item["category_hint"]),
            "bullet_points": ai_res.get("bullet_points", []),
            "link": link,
            "cover_image_url": item["cover_image_url"],
            "published_at": item["published_at"],
            "category_hint": item["category_hint"]
        }
        new_processed.append(new_entry)
        existing_links.add(link)
        time.sleep(1.0) # gentle rate limiter

    # Combine: newest first, capped to MAX_ARTICLES (30)
    updated_database = new_processed + existing_news
    updated_database = updated_database[:MAX_ARTICLES]

    # Save to data/news.json
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(updated_database, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(updated_database)} stories to {DATA_FILE}")

    # STEP D: Update README.md and index.html
    update_readme(updated_database)
    update_index_html(updated_database)

    logger.info("==================================================")
    logger.info("  PIPELINE EXECUTION SUMMARY                      ")
    logger.info(f"  12-Hour Candidates Scraped   : {len(candidates)}")
    logger.info(f"  Skipped Duplicates (Saved LLM): {skipped_count}")
    logger.info(f"  New Stories Processed via AI : {len(new_processed)}")
    logger.info(f"  Total Retained (Max 30)      : {len(updated_database)}")
    logger.info("==================================================")

if __name__ == "__main__":
    run()
