# 📋 Production Audit & Improvement Plan (`pure-news`)

> **Repository:** `https://github.com/puresinghara/pure-news` (`D:\Users\chumpon_s\Downloads\notion-news-pipeline`)  
> **Live Site:** `https://puresinghara.github.io/pure-news/`  
> **สถานะ:** Completed — ดำเนินการแก้ไขครบทั้ง 4 Phases และตรวจสอบผลลัพธ์เบื้องต้นแล้ว

---

## 🚨 ส่วนที่ 1: สรุปปัญหาที่พบจริงในโค้ดชุดปัจจุบัน (Critical Bugs Audit)

จากการตรวจสอบไฟล์ `.github/workflows/news_pipeline.yml`, `pipeline.py`, `index.html`, และข้อมูลจริงใน `data/news.json` (90 ข่าว) พบจุดบกพร่องสำคัญใน 5 ประเด็นหลักดังนี้:

---

### 1. GitHub Actions State & Git Sync (`.github/workflows/news_pipeline.yml`)
- **การเก็บ State ปัจจุบัน:** เก็บข้อมูลข่าวที่แปลแล้วไว้ใน `data/news.json` (90 ข่าว: หมวดละ 30 ข่าว) และฝังสำเนา JSON ทั้งก้อนซ้ำลงใน `index.html` (`let allNews = ...`) พร้อมเขียนตารางลง `README.md`
- **มี `permissions: contents: write` แล้ว** (บรรทัดที่ 11–12) และมี `git add` / `git commit` / `git push` (บรรทัดที่ 46–49)
- 🔴 **[CRITICAL] ไม่มี `concurrency` Group ป้องกันการรันชนกัน:**
  - ใน `.github/workflows/news_pipeline.yml` ไม่ได้ตั้งค่า `concurrency` หาก Cron รายชั่วโมง (`0 * * * *`) รันซ้อนกับ `workflow_dispatch` หรือรอบก่อนหน้ายังแปลข่าวไม่เสร็จ จะเกิดการรันซ้อน 2 Jobs พร้อมกัน เปลืองโควต้า Gemini API และเกิด Git Push ชนกัน
- 🔴 **[CRITICAL] ไม่มี `git pull --rebase` ก่อน `git push`:**
  - บรรทัดที่ 49 ใช้ `git diff --staged --quiet || (git commit -m "..." && git push)` โดยไม่มี `git pull --rebase origin main` (หรือ `--autostash`) ก่อน push ทั้งที่ `python pipeline.py` ใช้เวลารันหลายนาที หากมีการแก้โค้ดหรือมี commit อื่นเข้ามาระหว่างที่ Pipeline กำลังแปลข่าว ขั้นตอน `git push` จะล้มเหลวทันที (`! [rejected] main -> main (fetch first)`) และทำให้ข่าวที่แปลเสร็จในรอบนั้นสูญเปล่าทั้งหมด
- 🟠 **[HIGH] ไม่มี `timeout-minutes` ในระดับ Job:**
  - หากเว็บต้นทางหรือ API ค้าง Job จะรันค้างได้นานสุดถึง 6 ชั่วโมงตามค่าเริ่มต้นของ GitHub Actions
- 🟠 **[MEDIUM] การอ่านค่า `GEMINI_MODEL` มีโอกาสได้ค่าว่าง (`""`):**
  - ใน `pipeline.py` บรรทัดที่ 56: `GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip()` หากตัวแปรสภาพแวดล้อมถูกส่งมาเป็นสตริงว่าง จะทำให้ URL เรียกโมเดลพัง

---

### 2. Deduplication & Quota Leak (`pipeline.py`)
- 🔴 **[CRITICAL] บั๊กแปลข่าวเดิมซ้ำทุกชั่วโมงจากเงื่อนไข `is_thorough` (เผาโควต้าฟรี 19 ข่าว/ชั่วโมง!):**
  - ใน `pipeline.py` บรรทัดที่ 813–821:
    ```python
    paragraphs = existing_item.get("full_paragraphs", [])
    total_chars = sum(len(p) for p in paragraphs)
    is_thorough = len(paragraphs) >= 6 and total_chars >= 1800
    ```
  - **หลักฐานจาก `data/news.json` จริง:** มีข่าวที่แปลไทยเสร็จสมบูรณ์แล้วจำนวน **19 จาก 90 ข่าว** (เช่น ข่าวประกาศเกมใหม่จาก Gematsu, ข่าวสั้นจาก The Verge และ BBC ที่มี 6–11 ย่อหน้า แต่ความยาวรวมอยู่ที่ 1,019–1,781 ตัวอักษร) ที่ไม่ผ่านเกณฑ์ `total_chars >= 1800`
  - **ผลกระทบ:** ทุกๆ ชั่วโมงที่ GitHub Actions รัน ข่าวเดิมทั้ง 19 ชิ้นนี้จะถูกโยนทิ้งและส่งเข้า Gemini API เพื่อแปลใหม่ซ้ำแล้วซ้ำเล่าทุกชั่วโมงโดยไม่จำเป็น!
- 🔴 **[CRITICAL] ไม่ได้ใช้ `guid` (`entry.id`) และไม่ได้ทำ URL Canonicalization ก่อนเช็กข่าวซ้ำ:**
  - ใน `fetch_feed_entries()` (บรรทัดที่ 676–709) ดึงเฉพาะ `entry.link` ไม่ได้ดึง `entry.id` / `guid`
  - ใน `existing_by_link` (บรรทัดที่ 728–740 และ 806) เช็กด้วย URL ดิบตรงๆ โดย **ไม่ตัด Query Parameters** (เช่น `?at_medium=RSS&at_campaign=rss` ของ BBC หรือ `?utm_source=...`), ไม่ได้ปรับ `http://` เป็น `https://`, และไม่ตัด `/` ท้าย URL
  - หากสำนักข่าวเปลี่ยนพารามิเตอร์ท้ายลิงก์ หรือปรับ Slug เล็กน้อยโดยใช้ `guid` เดิม ระบบจะมองว่าเป็นข่าวใหม่และส่งแปลซ้ำทันที

---

### 3. Multi-Feed Fault Tolerance, Date Normalization & Image Extraction (`pipeline.py`)
- 🔴 **[CRITICAL] ข่าวใหม่ที่เพิ่งแปลเสร็จถูกดันไปอยู่ "ท้ายสุด" ของหมวดหมู่ (Sorting Bug):**
  - ใน `pipeline.py` บรรทัดที่ 801–851: ข่าวเก่าที่มีใน Cache (`existing_by_link`) จะถูก `append` เข้า `final_category_articles` ก่อน ส่วนข่าวใหม่ล่าสุด (`new_items_to_translate`) จะถูกนำไป Scrape + แปลภาษา แล้วนำมา `append` ต่อท้ายสุดของ `final_category_articles` **โดยไม่มีการ Sort ตามเวลาใหม่หลังแปลเสร็จ!**
  - **หลักฐานจาก `data/news.json` จริง:** ในหมวด `Football` ข่าววันที่ `13 พ.ย. 2026 • 07:48 น.` และ `07:23 น.` (ข่าวใหม่ล่าสุด) กลับไปตกอยู่อันดับที่ 29 และ 30 (ท้ายสุดของตาราง) ต่อท้ายข่าวเก่าของวันที่ `12 พ.ย. 2026 • 04:32 น.`!
- 🔴 **[CRITICAL] ถ้า RSS ฟีดล่มชั่วคราว ข่าวเก่าใน `data/news.json` ของหมวดนั้นจะถูกลบทิ้ง (Data Loss on Feed Failure):**
  - ใน `pipeline.py` บรรทัดที่ 775–871: `selected_candidates` สร้างจากรายการที่ดึงได้จาก RSS รอบปัจจุบันเท่านั้น หากฟีดของหมวดใดล่มหรือคืนค่ากลับมาไม่ครบ 30 ข่าว โค้ด **ไม่มีการ Backfill จาก `existing_news` (ข่าวที่เคยเซฟไว้แล้วใน `data/news.json`)** ทำให้ `category_results[cat]` มีจำนวนข่าวลดลงหรือกลายเป็น `[]` แล้วเขียนทับ `data/news.json` ทันที
- 🔴 **[CRITICAL] ดึงฟีดแบบ Sequential และใช้ `feedparser.parse(url)` โดยไม่มี Timeout และไม่มี Custom User-Agent:**
  - บรรทัดที่ 675 และ 775: เรียก `feedparser.parse(url)` ทีละฟีดแบบเรียงลำดับ (7 ฟีดของ The Verge + 3 ฟีดเกม + 1 ฟีดฟุตบอล = 11 ฟีด)
  - `feedparser.parse(url)` ใช้ `urllib` พื้นฐานโดย **ไม่มี HTTP Timeout** (หากเซิร์ฟเวอร์ปลายทางค้าง Pipeline จะค้างยาว) และส่ง User-Agent เป็น `FeedParser` ซึ่งเสี่ยงถูก Cloudflare (เช่น Eurogamer / Gematsu) บล็อก `403 Forbidden`
  - ไม่ได้ดึงฟีดแบบขนาน (`ThreadPoolExecutor` / `as_completed` เทียบเท่า `Promise.allSettled`) ที่แยก Fault Isolation ของแต่ละฟีดออกจากกัน
- 🟠 **[HIGH] การแปลงเวลา UTC ผิดพลาดด้วย `time.mktime()` และไม่เก็บ UTC Timestamp มาตรฐาน:**
  - บรรทัดที่ 684: `pub_dt = datetime.datetime.fromtimestamp(time.mktime(pub_tuple), tz=datetime.timezone.utc)`
  - `published_parsed` ของ `feedparser` เป็นเวลา **UTC** เสมอ แต่ `time.mktime()` ตีความ Tuple เป็น **เวลาท้องถิ่นของเครื่อง (Local Time)** แทนที่จะใช้ `calendar.timegm(pub_tuple)`
  - นอกจากนี้ ใน `data/news.json` เก็บแต่สตริงภาษาไทย `published_th` (ซึ่งมีฟอร์แมตปนกันระหว่าง `"2026-11-12 23:57 เวลาไทย"` กับ `"12 พ.ย. 2026 • 23:09 น."`) และไม่ได้เก็บ `published_ts` (Unix UTC Timestamp) หรือ ISO-8601 UTC ไว้ใช้เรียงลำดับอย่างแม่นยำ
- 🟠 **[MEDIUM] การดึงรูป Thumbnail จาก RSS/Atom ยังไม่ครบทุกมาตรฐาน:**
  - บรรทัดที่ 689–700: เช็กแค่ `media_content`, `enclosures[0].get('href')` และ `<img>` ใน `summary`/`description`
  - **ตกหล่น:** `media:thumbnail` (`entry.media_thumbnail`), `enclosures[0].get('url')` (พร้อมเช็ก MIME type ว่าเป็น `image/*`), `entry.content` (`<content:encoded>` หรือ Atom `<content>`), รวมถึง `twitter:image` ในหน้าเว็บ

---

### 4. Translation Quality & Rate Limit (`pipeline.py`)
- 🟠 **[HIGH] ใช้ Prompt เดียวกันทุกหมวดหมู่ ไม่ได้แยกบริบท (Football vs The Verge Tech vs Gaming):**
  - `SYSTEM_PROMPT` (บรรทัดที่ 73–86) และ `user_prompt` (บรรทัดที่ 229–259) ใช้คำสั่งรวมชุดเดียวสำหรับทุกหมวด
  - ขาดการกำหนดคลังคำศัพท์และสไตล์เฉพาะทาง:
    - **BBC Football:** ต้องใช้สำนวนข่าวกีฬาฟุตบอลที่ถูกต้อง (เช่น คลีนชีต, ตลาดซื้อขายนักเตะ, การลงสนามรับใช้ทีมชาติ แทนการแปลคำว่า `cap` หรือ `draw` ตรงตัว)
    - **The Verge (Tech):** ต้องรักษาคำศัพท์เทคนิค (API, SDK, LLM, Chipset, Benchmark, Cloud, Firmware) และโทนข่าวไอทีเชิงลึก
    - **Gaming News:** ต้องคงชื่อเกม ชื่อแพลตฟอร์ม (PS5, Xbox Series X|S, Switch 2, Steam) และศัพท์เกมเมอร์ (RPG, Roguelike, DLC, Patch, Frame Rate, Early Access) ไม่แปลชื่อเฉพาะของเกมเป็นภาษาไทยแบบผิดเพี้ยน
- 🔴 **[CRITICAL] ไม่มี Rate Limiter / Pacing ระหว่างยิง API และเมื่อติด Rate Limit แล้วเซฟข่าวภาษาอังกฤษดิบลง Database:**
  - บรรทัดที่ 833–834: รัน `ThreadPoolExecutor(max_workers=2)` ยิง `call_gemini` รัวต่อเนื่องทันทีโดยไม่มี Delay ขั้นต่ำระหว่าง Request และใน `call_gemini` (บรรทัดที่ 290–297) ใช้ Fixed Sleep (`6 * (attempt + 1)`) โดยไม่มี Random Jitter ทำให้ทั้ง 2 Threads ตื่นมาชน 429 พร้อมกันซ้ำ (Thundering Herd)
  - บรรทัดที่ 299–304: หาก Retry ครบ 4 ครั้งแล้วยังไม่สำเร็จ ฟังก์ชันคืนค่าหัวข้อและเนื้อหา **ภาษาอังกฤษดิบ** กลับไป แล้วบรรทัดที่ 850–851 นำไปบันทึกลง `data/news.json` ทันที!

---

### 5. Frontend Performance & UX Resilience (`index.html` & `pipeline.py`)
- 🔴 **[CRITICAL] โหลดข้อมูลซ้ำซ้อน ~1.88 MB ทุกครั้งที่เปิดเว็บ (Inline 941 KB + Fetch 943 KB ซ้ำทันที):**
  - `pipeline.py` (บรรทัดที่ 346, 472) ฝัง JSON ทั้ง 90 ข่าว (พร้อมเนื้อหายาว 6–20 ย่อหน้าของทุกข่าว) ลงในไฟล์ `index.html` ทำให้ `index.html` มีขนาดใหญ่ถึง **941 KB**
  - ทันทีที่เปิดหน้าเว็บ โค้ดบรรทัดที่ 478 สั่ง `fetch('data/news.json?t=' + Date.now())` โหลดไฟล์ `data/news.json` ขนาด **943 KB** ซ้ำอีกรอบโดยไม่ใช้ Cache แล้วสั่ง `render()` ล้าง DOM วาดการ์ดทั้งหมดใหม่ซ้ำเป็นรอบที่ 2!
- 🔴 **[CRITICAL] หน้าแรกเรนเดอร์ 90 การ์ดพร้อมกันโดยไม่มี `loading="lazy"` ใน `<img>`:**
  - ค่าเริ่มต้นเปิดที่แท็บ `ALL (90)` และแท็ก `<img>` ในการ์ด (บรรทัดที่ 539) ไม่มี `loading="lazy"` และ `decoding="async"` ทำให้เบราว์เซอร์สั่งโหลดรูปภาพความละเอียดสูง (1200px–1920px) พร้อมกันทันที 90 รูป กระทบความเร็วบนมือถืออย่างหนัก
- 🟠 **[HIGH] บั๊กจัดการรูปภาพเสีย (Broken Image Fallback) ใน Modal และ Gallery:**
  1. ใน Modal (`#modalImg` บรรทัดที่ 432): ค่าเริ่มต้นคือ `<img id="modalImg" src="" onerror="this.src='...unsplash...'">` การใส่ `src=""` ทำให้เบราว์เซอร์ยิง `onerror` ทันทีตั้งแต่เปิดหน้าเว็บ และไม่ได้ใส่ `this.onerror=null;` ซึ่งหาก Unsplash ล่มหรือถูกบล็อก จะเกิด **Infinite `onerror` Loop** จนหน้าเว็บค้าง!
  2. เวลาเปลี่ยนข่าวใน Modal รูปของข่าวเก่าจะค้างอยู่ชั่วขณะก่อนรูปใหม่โหลดเสร็จ และรูป Fallback ใช้รูปตู้เกม Unsplash รูปเดียวกันทุกหมวด (แม้จะเป็นข่าวฟุตบอลหรือข่าวเทค)
  3. ใน Gallery (`#modalGalleryGrid` บรรทัดที่ 606): หากรูปประกอบทุกรูปในข่าวนั้นโหลดไม่ขึ้น (`this.parentElement.style.display='none'`) หัวข้อ `📸 ภาพประกอบและ SCREENSHOTS ในข่าว :` จะยังคงแสดงค้างอยู่โดยไม่มีรูปข้างใต้
- 🟠 **[MEDIUM] ความปลอดภัย DOM (XSS / Unescaped HTML Injection):**
  - บรรทัดที่ 526, 549, 582 นำ `item.thai_title` และ `bullet_points` ใส่ใน `innerHTML` โดยตรงโดยไม่ผ่านการ Escape อักขระพิเศษ (`<`, `>`, `&`, `"`)

---

## 🛠️ ส่วนที่ 2: แผนการแก้ไขทีละ Phase (Step-by-Step Implementation Plan)

เพื่อยกระดับระบบให้เสถียรระดับ Production โดยไม่กระทบโครงสร้างเดิม แบ่งการดำเนินการออกเป็น **4 Phases** ดังนี้:

---

### 🔹 Phase 1: GitHub Actions Workflow, Concurrency & Git Sync Hardening
**ไฟล์ที่จะแก้ไข:**
- `.github/workflows/news_pipeline.yml`

**รายละเอียดการแก้ไข:**
1. เพิ่มบล็อก `concurrency` เพื่อป้องกันการรัน Pipeline ซ้อนกัน:
   ```yaml
   concurrency:
     group: pure-news-pipeline-${{ github.ref }}
     cancel-in-progress: false
   ```
2. ตั้งค่า `timeout-minutes: 25` ในระดับ Job (`scrape-and-update`) ป้องกัน Job ค้าง
3. ปรับปรุงขั้นตอน `Commit and Push Updates` ให้ปลอดภัยต่อการชนกันของ Git:
   - ใช้ `git add data/news.json README.md index.html`
   - เมื่อมีการเปลี่ยนแปลง ให้ `git commit` แล้วรัน `git pull --rebase origin main` (พร้อม fallback retry) ก่อน `git push origin HEAD:main`

---

### 🔹 Phase 2: Multi-Feed Fault Tolerance, Deduplication (`guid` + Canonical URL) & Timestamp Normalization
**ไฟล์ที่จะแก้ไข:**
- `pipeline.py`

**รายละเอียดการแก้ไข:**
1. **URL Canonicalization & `guid` Deduplication:**
   - สร้างฟังก์ชัน `normalize_url(url)` เพื่อตัด Query Tracking (`at_medium`, `at_campaign`, `utm_*`), ปรับ `http://` -> `https://`, และตัด `/` ท้าย URL
   - สร้าง `make_article_key(entry_or_item)` ที่ใช้ทั้ง `guid` (`entry.id`) และ Canonical URL ในการตรวจสอบข่าวซ้ำกับฐานข้อมูลเดิม (`existing_by_key` & `existing_by_link`)
2. **แก้บั๊กแปลซ้ำทุกชั่วโมง (`is_thorough`):**
   - ปรับเงื่อนไขการตรวจ Cache: หากข่าวนั้นเคยแปลเป็นภาษาไทยแล้ว (`has_thai` และมี `bullet_points` + `full_paragraphs` ที่ไม่ใช่ค่าว่าง/ไม่ใช่ Fallback ภาษาอังกฤษ) ให้ **Reuse ทันที 100%** โดยไม่บังคับ `total_chars >= 1800` ที่ทำให้ข่าวสั้นถูกแปลซ้ำทุกชั่วโมง
3. **Parallel Multi-Feed Fetching & Fault Isolation (เทียบเท่า `Promise.allSettled`):**
   - เปลี่ยนจากการใช้ `feedparser.parse(url)` แบบ Sequential ไม่มี Timeout มาเป็น `requests.get(url, headers=HEADERS, timeout=15)` ควบคู่กับ `ThreadPoolExecutor` + `as_completed`
   - หากฟีดใดฟีดหนึ่งในกลุ่ม Gaming หรือ Tech ล่ม/Timeout ให้บันทึก `logger.warning` แล้วทำงานต่อจากฟีดที่เหลือโดยไม่ทำให้ทั้งหมวดล่ม
4. **Database Backfill เมื่อ RSS คืนข่าวไม่ครบหรือล่มชั่วคราว:**
   - หากฟีดในหมวดใดมีปัญหาและได้ข่าวไม่ครบ 30 ชิ้น ให้ดึงข่าวเดิมของหมวดนั้นจาก `existing_news` (`data/news.json`) มาเติมจนครบ 30 ชิ้นเสมอ ป้องกันข่าวหายจากหน้าเว็บ
5. **UTC Timestamp Normalization (`calendar.timegm`) & Re-Sorting หลังแปลเสร็จ:**
   - ใช้ `calendar.timegm(pub_tuple)` แปลงเวลาจาก RSS/Atom เป็น UTC `datetime` และบันทึกค่า `published_ts` (Unix UTC Timestamp) + `published_at` (ISO-8601 UTC) + `published_th` (ฟอร์แมตไทยมาตรฐานเดียวกันทั้งหมด)
   - **เรียงลำดับ `final_category_articles` ใหม่ตาม `published_ts` (จากใหม่สุดไปเก่าสุด)** หลังจากรวมข่าวเก่าและข่าวที่เพิ่งแปลเสร็จ เพื่อแก้บั๊กข่าวใหม่ไปตกอยู่ท้ายหน้า
6. **Comprehensive Thumbnail Extraction:**
   - ดึงรูปตามลำดับความสำคัญ: `media:content` -> `media:thumbnail` -> `enclosure` (เฉพาะ `image/*`) -> `<img src>` ใน `content:encoded` / `summary` -> `og:image` / `twitter:image` จากหน้าเว็บต้นฉบับ พร้อมอัปเกรดเป็นความละเอียด HD

---

### 🔹 Phase 3: Category-Specific Translation Prompts & Smart Rate-Limit Protection
**ไฟล์ที่จะแก้ไข:**
- `pipeline.py`

**รายละเอียดการแก้ไข:**
1. **แยก System/Category Prompt ตามหมวดหมู่ (3 บุคลิกบรรณาธิการ):**
   - **Football (`BBC Football`):** บรรณาธิการข่าวฟุตบอลอังกฤษ ใช้ศัพท์ฟุตบอลสากลที่คนไทยคุ้นเคย ทับศัพท์ชื่อนักเตะ/สโมสร/ผู้จัดการทีมอย่างแม่นยำ เก็บคำสัมภาษณ์ครบถ้วน
   - **Technology (`The Verge`):** บรรณาธิการข่าวเทคโนโลยีและ AI รักษาศัพท์เทคนิค (API, LLM, GPU, Benchmark ฯลฯ) อธิบายบริบทเทคโนโลยีชัดเจน
   - **Gaming (`Gaming News`):** บรรณาธิการข่าวเกม คงชื่อเกม แพลตฟอร์ม ค่ายพัฒนา และศัพท์เกมเมอร์ (RPG, DLC, Patch, Frame Rate ฯลฯ) ไม่แปลชื่อเกมเป็นไทยแบบผิดความหมาย
2. **Rate Limiter + Exponential Backoff with Jitter:**
   - เพิ่มกลไก Rate Limiter (กำหนดระยะห่างขั้นต่ำระหว่างการยิง Gemini API แนะนำ Concurrency ปลอดภัย + Pacing Delay)
   - ใช้ Exponential Backoff ผสม Random Jitter (`base * (2 ** attempt) + random.uniform(0.5, 2.5)`) เมื่อเจอ HTTP `429` หรือ `503`
   - ป้องกันการนำข้อความภาษาอังกฤษดิบไปทับข่าวดีในกรณีที่ API ล่มชั่วคราว

---

### 🔹 Phase 4: Frontend Performance, 3-Tab UX & Broken Image Fallback (`index.html` Generator)
**ไฟล์ที่จะแก้ไข:**
- `pipeline.py` (ในฟังก์ชัน `update_ui_files()` ที่สร้าง `index.html`)
- `index.html` (อัปเดตเทมเพลตให้ตรงกับ `pipeline.py`)

**รายละเอียดการแก้ไข:**
1. **ยกเลิกการโหลดข้อมูลซ้ำ 2 รอบ (~1.88 MB -> โหลดครั้งเดียวอย่างรวดเร็ว):**
   - เลือกแนวทางที่เสถียรที่สุดสำหรับ GitHub Pages: ให้ `index.html` โหลดข้อมูลจาก `data/news.json` แบบ Async พร้อม Skeleton Loading ที่ลื่นไหล (หรือหากฝังข้อมูลเริ่มต้นไว้ ให้เช็ก Version/Timestamp ก่อน เพื่อไม่ต้อง `fetch` และ `render()` ซ้ำซ้อน 2 รอบในเวลาเดียวกัน)
2. **ปรับปรุงการแสดงผล 3 Tabs (`BBC Football`, `The Verge`, `Gaming News`) + `ALL`:**
   - จัดเรียงข่าวในแท็บ `ALL` ตามเวลาจริง (`published_ts` ใหม่สุดขึ้นก่อน ไม่ใช่เอา Football 30 ข่าวมาเรียงต่อกันก่อน Tech และ Gaming) และแสดงป้ายชื่อแท็บชัดเจน (`⚽ BBC FOOTBALL`, `⚡ THE VERGE`, `🎮 GAMING NEWS`)
3. **เพิ่ม `loading="lazy"` และ `decoding="async"` ให้รูปภาพทั้งหมด:**
   - ลดภาระการดาวน์โหลดรูปภาพพร้อมกัน 90 รูปตอนเปิดหน้าแรก
4. **Bulletproof Broken Image Fallback (零 Infinite Loop):**
   - สร้างฟังก์ชันจัดการรูปเสียที่ตั้งค่า `img.onerror = null` เสมอ พร้อม SVG/Thematic Fallback แยกตามหมวดหมู่ (`Football`, `Technology`, `Gaming`)
   - แก้ `#modalImg` ไม่ให้มี `src=""` ตอนเริ่มต้น และล้างรูปเก่าทันทีเมื่อเปิด Modal ข่าวใหม่
   - ใน `#modalGalleryGrid` หากรูปประกอบในข่าวโหลดไม่สำเร็จทั้งหมด ให้ซ่อนหัวข้อ `#modalGallerySection` อัตโนมัติ ไม่ปล่อยให้หัวข้อค้างว่างเปล่า
5. **HTML Escaping (`escapeHtml`):**
   - ป้องกันปัญหาอักขระพิเศษในพาดหัวข่าวหรือ Bullet Points ทำโครงสร้าง HTML เพี้ยน
