import re, json, time, tempfile, shutil, os, io, base64, threading, html, uuid
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import requests
import streamlit as st
from PIL import Image, ImageOps, ImageFilter
from src.core import (
    settings, logger, get_db_lock, db_conn, db_store_recipes, db_find_recipe_slug,
    db_delete_recipe, db_delete_recipe_by_slug, db_get_mapping, db_set_mapping,
    db_bulk_replace_mappings, clean_str, normalize_name, slugify, get_nested_name,
    extract_servings_number, safe_float, unique_by_name, safe_close_image,
    get_prompts_config, Recipe, MultiRecipeResponse, EditorRecipeResponse,
    _parse_pydantic_json, get_image_prompts, get_user_uploaded_recipe_rows,
    JSON_SCHEMA_HINT
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- IMPORTS ---
try: from google import genai; from google.genai import types
except ImportError: genai = None

try: 
    import yt_dlp
    VIDEO_IMPORT_AVAILABLE = True
except ImportError: 
    yt_dlp = None
    VIDEO_IMPORT_AVAILABLE = False

try: 
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError: 
    trafilatura = None
    TRAFILATURA_AVAILABLE = False

try: 
    import pytesseract
    try:
        pytesseract.get_tesseract_version()
        OCR_AVAILABLE = True
    except Exception:
        OCR_AVAILABLE = False
except ImportError: 
    pytesseract = None
    OCR_AVAILABLE = False


# --- UTILS ---
def infer_recipe_yield_from_text(text: str) -> str:
    raw = clean_str(text)
    if not raw: return ""
    for pattern in [
        r"(?:ergibt|für|macht|reicht für)\s*(\d+(?:\s*[-–]\s*\d+)?)\s*(?:portionen|personen|servings?)",
        r"(?:serves|makes|yield(?:s)?|portionen|portion|personen|servings?)\s*[:\-]?\s*(\d+(?:\s*[-–]\s*\d+)?)",
        r"(?:recipeyield|yield)\s*[:\-]?\s*(\d+(?:\s*[-–]\s*\d+)?)"
    ]:
        if match := re.search(pattern, raw, flags=re.IGNORECASE): return clean_str(match.group(1)).replace(" ", "")
    return ""


# --- REQUESTS & CACHE ---
@st.cache_resource
def get_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, connect=5, read=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter); s.mount("https://", adapter)
    return s

def get_auth_headers(api_key: str, json_content: bool = True) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {api_key}"}
    if json_content: h["Content-Type"] = "application/json"
    return h

def safe_mealie_request(method: str, url: str, headers: Dict[str, str], **kwargs) -> requests.Response:
    s = get_http_session()
    for attempt in range(4):
        try:
            resp = s.request(method, url, headers=headers, timeout=settings.request_timeout, **kwargs)
            if resp.status_code in (429, 503):
                time.sleep(float(resp.headers.get("Retry-After", 2 + attempt * 3)))
                continue
            if resp.status_code < 500: return resp
        except requests.exceptions.RequestException as exc: last_exc = exc
        time.sleep(2 + attempt * 3)
    raise last_exc

# --- MEALIE LOGIC ---
@st.cache_data(ttl=settings.recipe_cache_ttl)
def get_mealie_recipes(api_url: str, api_key: str) -> List[Dict[str, str]]:
    recipes = []
    try:
        resp = safe_mealie_request("GET", f"{api_url}/api/recipes?page=1&perPage=2000", headers=get_auth_headers(api_key, False))
        if resp.status_code == 200:
            items = resp.json().get("items", []) if isinstance(resp.json(), dict) else resp.json()
            for i in items:
                if isinstance(i, dict) and i.get("name") and i.get("slug"): recipes.append({"name": i["name"], "slug": i["slug"]})
            db_store_recipes(recipes)
    except Exception: pass
    return sorted(recipes, key=lambda x: x["name"].lower())

@st.cache_data(ttl=3600)
def get_mealie_user_id_by_email(api_url: str, api_key: str, email: str) -> Optional[str]:
    if not email: return None
    try:
        resp = safe_mealie_request("GET", f"{api_url}/api/users?page=1&perPage=200", headers=get_auth_headers(api_key, False))
        if resp.status_code == 200:
            for u in (resp.json().get("items", []) if isinstance(resp.json(), dict) else resp.json()):
                if u.get("email") and u.get("email").lower() == email.lower(): return u.get("id")
    except Exception: pass
    return None

@st.cache_data(ttl=settings.map_cache_ttl)
def get_mealie_data_maps(api_url: str, api_key: str) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]]:
    f_map, u_map, t_map, c_map, tl_map = {}, {}, {}, {}, {}
    headers = get_auth_headers(api_key, False)
    def fetch(ep: str, target: Dict):
        try:
            resp = safe_mealie_request("GET", f"{api_url}/api/{ep}?page=1&perPage=-1", headers=headers)
            if resp.status_code == 200:
                for i in (resp.json().get("items", []) if isinstance(resp.json(), dict) else resp.json()):
                    if isinstance(i, dict) and i.get("name") and i.get("id"): target[normalize_name(i["name"])] = i["id"]
                db_bulk_replace_mappings(ep, target)
        except Exception: pass
    fetch("foods", f_map); fetch("units", u_map); fetch("organizers/tags", t_map); fetch("organizers/categories", c_map)
    for ep in ["api/tools", "api/organizers/tools", "api/groups/tools"]:
        fetch(ep.replace("api/", ""), tl_map)
        if tl_map: break
    return f_map, u_map, t_map, c_map, tl_map

def get_recipe_by_slug(api_url: str, api_key: str, slug: str) -> Optional[Dict]:
    resp = safe_mealie_request("GET", f"{api_url}/api/recipes/{clean_str(slug)}", headers=get_auth_headers(api_key, False))
    return resp.json() if resp.status_code == 200 else None

def search_recipe_slug_by_name(api_url: str, api_key: str, recipe_name: str) -> Optional[str]:
    nn = normalize_name(recipe_name)
    for r in get_mealie_recipes(api_url, api_key):
        if normalize_name(r.get("name", "")) == nn: return r.get("slug")
    return None

def find_duplicate_recipe_slug(api_url: str, api_key: str, recipe_name: str) -> Optional[str]:
    db_slug = db_find_recipe_slug(recipe_name)
    if db_slug:
        if get_recipe_by_slug(api_url, api_key, db_slug): return db_slug
        db_delete_recipe(recipe_name); db_delete_recipe_by_slug(db_slug)
    starget = slugify(recipe_name)
    for r in get_mealie_recipes(api_url, api_key):
        if normalize_name(r.get("name", "")) == normalize_name(recipe_name) or slugify(r.get("name", "")) == starget: return r.get("slug")
    return None

_entity_lock = threading.Lock()
def get_or_create(ep: str, name: str, api_url: str, headers: Dict, dmap: Dict) -> Optional[str]:
    nn = normalize_name(name)
    if not nn: return None
    with _entity_lock:
        if nn in dmap: return dmap[nn]
        cached = db_get_mapping(ep, nn)
        if cached:
            dmap[nn] = cached; return cached
        resp = safe_mealie_request("POST", f"{api_url}/api/{ep}", headers=headers, json={"name": clean_str(name)})
        if resp.status_code in (200, 201):
            new_id = resp.json().get("id")
            if new_id:
                dmap[nn] = new_id; db_set_mapping(ep, nn, new_id); return new_id
    return None

def get_or_create_tool_robust(name: str, api_url: str, headers: Dict, dmap: Dict) -> Optional[str]:
    nn = normalize_name(name)
    if not nn: return None
    with _entity_lock:
        if nn in dmap: return dmap[nn]
        cached = db_get_mapping("tools", nn)
        if cached:
            dmap[nn] = cached; return cached
        for ep in ["api/tools", "api/organizers/tools", "api/groups/tools"]:
            resp = safe_mealie_request("POST", f"{api_url}/{ep}", headers=headers, json={"name": clean_str(name)})
            if resp.status_code in (200, 201):
                new_id = resp.json().get("id")
                if new_id:
                    dmap[nn] = new_id; db_set_mapping("tools", nn, new_id); return new_id
    return None

# --- WEB & VIDEO ---
def strip_html(html_text: str) -> str:
    t = re.sub(r"<(style|script(?![^>]*application/ld\+json)|noscript|header|footer|nav).*?</\1>", " ", html_text, flags=re.DOTALL | re.IGNORECASE)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", t))).strip()

def extract_recipe_jsonld_text(html_text: str) -> str:
    scripts = re.findall(r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", html_text, flags=re.DOTALL | re.IGNORECASE)
    parts = []
    def flatten(node):
        out = []
        if isinstance(node, dict):
            if isinstance(node.get("@graph"), list):
                for item in node["@graph"]: out.extend(flatten(item))
            else: out.append(node)
        elif isinstance(node, list):
            for item in node: out.extend(flatten(item))
        return out
    for raw in scripts:
        try:
            for item in flatten(json.loads(raw)):
                types = item.get("@type") if isinstance(item.get("@type"), list) else [item.get("@type")]
                if any(str(t).lower() == "recipe" for t in types if t):
                    if item.get("name"): parts.append(f"Titel: {item['name']}")
                    if item.get("description"): parts.append(f"Beschreibung: {item['description']}")
                    if item.get("recipeYield"): parts.append(f"Portionen: {item['recipeYield']}")
                    ings = item.get("recipeIngredient") or []
                    if ings: parts.extend(["Zutaten:"] + [f"- {clean_str(x)}" for x in ings if clean_str(x)])
                    steps = item.get("recipeInstructions") or []
                    if steps:
                        parts.append("Zubereitung:")
                        for idx, step in enumerate(steps, 1):
                            t = clean_str(step if isinstance(step, str) else step.get("text") or step.get("name"))
                            if t: parts.append(f"{idx}. {t}")
        except Exception: pass
    return "\n".join(parts).strip()

def fetch_url_text_and_image(url: str) -> Tuple[str, Optional[bytes]]:
    resp = get_http_session().get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    resp.raise_for_status()
    html_text, cover_img_bytes = resp.text, None
    im_match = re.search(r'<meta\s+(?:property|name)=[\"\'](?:og|twitter):image[\"\']\s+content=[\"\']([^\"\']+)[\"\']', html_text, re.IGNORECASE)
    if im_match:
        try:
            img_resp = get_http_session().get(urljoin(url, im_match.group(1)), headers={"User-Agent": "Mozilla/5.0"}, timeout=8, stream=True)
            if img_resp.status_code == 200:
                chunks, total = [], 0
                for chunk in img_resp.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        total += len(chunk)
                        if total > settings.max_cover_image_bytes: chunks = []; break
                        chunks.append(chunk)
                if chunks: cover_img_bytes = b"".join(chunks)
        except Exception: pass
    text = extract_recipe_jsonld_text(html_text)
    if trafilatura:
        try:
            ext = trafilatura.extract(html_text, include_comments=False, include_tables=True)
            text += "\n\n" + (ext if ext else strip_html(html_text))
        except Exception: text += "\n\n" + strip_html(html_text)
    else: text += "\n\n" + strip_html(html_text)
    return text.strip()[:50000], cover_img_bytes

def download_recipe_video(url: str) -> Dict:
    tdir = tempfile.mkdtemp()
    with yt_dlp.YoutubeDL({"format": "best[ext=mp4][height<=720]/best", "outtmpl": os.path.join(tdir, "video.%(ext)s"), "quiet": True, "writethumbnail": True, "get_comments": True, "max_comments": 40}) as ydl:
        info = ydl.extract_info(url, download=True)
        vpath = os.path.join(tdir, next((f for f in os.listdir(tdir) if f.endswith(('.mp4', '.webm', '.mkv'))), "video.mp4"))
        tb = None
        if info.get("thumbnail"):
            try: tb = get_http_session().get(info["thumbnail"], timeout=5).content
            except Exception: pass
        rt = f"Titel: {info.get('title', '')}\nBeschreibung: {info.get('description', '')}\n\n--- Kommentare ---\n" + "\n".join([f"Kommentar: {c.get('text', '')}" for c in info.get('comments', []) if c.get('text')])
        return {"video_path": vpath, "recipe_text": rt, "thumbnail_bytes": tb, "temp_dir": tdir}

def cleanup_video_bundle(bundle: Dict):
    if bundle.get("temp_dir") and os.path.exists(bundle["temp_dir"]): shutil.rmtree(bundle["temp_dir"], ignore_errors=True)

# --- IMAGE PROCESSING ---
def load_image(img_bytes: bytes) -> Image.Image:
    img = Image.open(io.BytesIO(img_bytes))
    img = ImageOps.exif_transpose(img)
    img.thumbnail((1600, 1600), getattr(Image, "Resampling", Image).LANCZOS)
    return img

def image_to_jpeg_bytes(img: Image.Image, quality: int = 90) -> bytes:
    temp = img.convert("RGB") if img.mode != "RGB" else img
    buf = io.BytesIO()
    temp.save(buf, format="JPEG", quality=quality)
    if temp is not img: safe_close_image(temp)
    return buf.getvalue()

def get_blur_placeholder(image_bytes: Optional[bytes]) -> Image.Image:
    if image_bytes:
        try:
            img = Image.open(io.BytesIO(image_bytes))
            if img.mode != 'RGB': img = img.convert('RGB')
            return img.filter(ImageFilter.GaussianBlur(30))
        except Exception: pass
    return Image.new('RGB', (800, 800), color=(233, 221, 255))

# --- AI & GEMINI ---
def get_prompt() -> str:
    fm, _, tm, cm, tlm = get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key)
    c = get_prompts_config()
    t = c.get("base_prompt", "").replace("{foods_str}", ", ".join(fm.keys()) or "Keine").replace("{tags_str}", ", ".join(tm.keys()) or "Keine").replace("{cats_str}", ", ".join(cm.keys()) or "Keine").replace("{tools_str}", ", ".join(tlm.keys()) or "Keine").replace("{json_schema_hint}", JSON_SCHEMA_HINT)
    return t.strip()

def get_pdf_prompt() -> str: return get_prompt() + get_prompts_config().get("pdf_prompt_addition", "")

def create_genai_client(api_key: str): return genai.Client(api_key=api_key) if genai else None

def analyze_content_with_gemini(client, prompt: str, images: Optional[List[Image.Image]] = None, text: Optional[str] = None) -> Dict:
    ocr, last_err = "", ""
    for attempt in range(3):
        try:
            contents = [prompt]
            if text: contents.append("Extrahierter Text:\n" + text)
            if ocr: contents.append("OCR-Fallback:\n" + ocr)
            if images: contents.extend(images)
            if last_err: contents.append(f"WARNING: Fix validation error: '{last_err}'")
            resp = client.models.generate_content(model=settings.gemini_model, contents=contents, config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=Recipe, temperature=0.1))
            pd = _parse_pydantic_json(Recipe, resp.text).model_dump(exclude_none=True)
            
            fallback_yield = infer_recipe_yield_from_text("\n".join([clean_str(text), clean_str(ocr)]))
            current_yield = clean_str(pd.get("recipeYield"))
            if fallback_yield and (not current_yield or current_yield in {"1", "1 Portion", "1 Portionen", "1 Person", "1 serving"}):
                pd["recipeYield"] = fallback_yield
                
            if not pd.get("recipeIngredient") and not pd.get("recipeInstructions") and attempt < 1: raise ValueError("Keine Zutaten gefunden.")
            return pd
        except Exception as exc:
            last_err = str(exc)
            if images and pytesseract and not ocr:
                ocr = "\n\n".join([clean_str(pytesseract.image_to_string(ImageOps.autocontrast(ImageOps.grayscale(i)))) for i in images])
            if attempt == 2: raise RuntimeError(f"Gemini fehlgeschlagen: {exc}")
            time.sleep(3)

def analyze_video_with_gemini(client, prompt: str, video_path: str, recipe_text: str) -> Dict:
    p = prompt + get_prompts_config().get("video_prompt_addition", "")
    last_err, f = "", None
    try:
        f = client.files.upload(file=video_path)
        while f.state.name == "PROCESSING": time.sleep(2); f = client.files.get(name=f.name)
        if f.state.name == "FAILED": raise RuntimeError("Video failed.")
        for attempt in range(3):
            try:
                contents = [f, p]
                if recipe_text: contents.append("Infos:\n" + recipe_text)
                if last_err: contents.append(f"WARNING: Fix validation error: '{last_err}'")
                resp = client.models.generate_content(model=settings.gemini_model, contents=contents, config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=Recipe, temperature=0.1))
                return _parse_pydantic_json(Recipe, resp.text).model_dump(exclude_none=True)
            except Exception as exc:
                last_err = str(exc)
                if attempt == 2: raise exc
                time.sleep(3)
    finally:
        if f:
            try: client.files.delete(name=f.name)
            except: pass

def analyze_pdf_with_gemini(client, prompt: str, pdf_path: str) -> List[Dict]:
    last_err, f = "", None
    try:
        f = client.files.upload(file=pdf_path)
        while f.state.name == "PROCESSING": time.sleep(2); f = client.files.get(name=f.name)
        for attempt in range(3):
            try:
                c = [f, prompt] + ([f"WARNING: '{last_err}'"] if last_err else [])
                resp = client.models.generate_content(model=settings.gemini_model, contents=c, config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=MultiRecipeResponse, temperature=0.1))
                return [r.model_dump(exclude_none=True) for r in _parse_pydantic_json(MultiRecipeResponse, resp.text).recipes]
            except Exception as exc:
                last_err = str(exc)
                if attempt == 2: raise exc
    finally:
        if f:
            try: client.files.delete(name=f.name)
            except: pass

def editor_transform_recipe(client, current_recipe: Dict, instruction: str) -> Tuple[Dict, str]:
    p = get_prompts_config().get("editor_prompt", "")
    t = json.dumps(current_recipe, ensure_ascii=False) + "\n\nAnweisung:\n" + instruction
    for attempt in range(3):
        try:
            resp = client.models.generate_content(model=settings.gemini_model, contents=[p, t], config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=EditorRecipeResponse, temperature=0.1))
            parsed = _parse_pydantic_json(EditorRecipeResponse, resp.text)
            return parsed.recipe.model_dump(exclude_none=True), parsed.explanation
        except Exception:
            if attempt == 2: return current_recipe, "Fehler bei Verarbeitung."
            time.sleep(2)

def generate_recipe_image_with_gemini(client, recipe_name: str, recipe_desc: str, image_model: str = "imagen-4.0-generate-001", custom_style: str = "") -> Optional[bytes]:
    sn = recipe_name.replace("'", "").replace('"', "")
    bp = f"A professional food photography shot of {sn}. {recipe_desc} CRITICAL: Accurately represent the exact type of food described. Do not over-embellish. If it is a simple rustic bake, loaf, or standard sheet cake, it MUST look authentic and rustic. NEVER generate a fancy, multi-layered decorated tort/layer-cake unless explicitly described."
    p = f"{bp} Style: {custom_style}" if custom_style.strip() else f"{bp} High quality, culinary magazine style."
    p += " Pure food photography, absolutely no typography, no letters, no writing, no labels, no watermarks, clean composition."
    try:
        resp = get_http_session().post(f"https://generativelanguage.googleapis.com/v1beta/models/{image_model}:predict?key={settings.gemini_api_key}", json={"instances": [{"prompt": p}], "parameters": {"sampleCount": 1}}, timeout=30)
        if resp.status_code == 200 and resp.json().get("predictions", [{}])[0].get("bytesBase64Encoded"): return base64.b64decode(resp.json()["predictions"][0]["bytesBase64Encoded"])
    except Exception as e: logger.error("imagen_err", error=str(e))
    return None

def auto_generate_cover_image(client, parsed_data: Dict, current_cover: Optional[bytes] = None, owner_label: str = "") -> Optional[bytes]:
    if current_cover: return current_cover
    style = ""
    try:
        for p in get_image_prompts():
            if p["is_default"] and (p["user_label"] == owner_label or "Lars Graf" in p["user_label"] or not style):
                style = p["text"]
                if p["user_label"] == owner_label: break
    except Exception: pass
    return generate_recipe_image_with_gemini(client, parsed_data.get("name", "Gericht"), parsed_data.get("description", ""), custom_style=style)

# --- MEALIE SAVE ---
def direct_save_to_mealie(parsed_data: Dict, api_url: str, api_key: str, cover_img_bytes: Optional[bytes] = None, preloaded_maps: Optional[Tuple] = None, target_slug: Optional[str] = None, org_url: str = "", audit_user_key: str = "", audit_user_label: str = "", audit_user_email: str = "", mealie_user_id: Optional[str] = None) -> Tuple[bool, str]:
    headers = get_auth_headers(api_key)
    fm, um, tm, cm, tlm = preloaded_maps or get_mealie_data_maps(api_url, api_key)
    name, slug = clean_str(parsed_data.get("name")) or "Unbenanntes Rezept", clean_str(target_slug)
    if not slug:
        dup = find_duplicate_recipe_slug(api_url, api_key, name)
        if dup and get_recipe_by_slug(api_url, api_key, dup): slug = dup
        else:
            db_delete_recipe(name); db_delete_recipe_by_slug(dup or "")
            c_resp = safe_mealie_request("POST", f"{api_url}/api/recipes", headers=headers, json={"name": name, "userId": mealie_user_id} if mealie_user_id else {"name": name})
            if c_resp.status_code not in (200, 201): return False, f"Fehler: {c_resp.text}"
            try: slug = c_resp.json().get("slug") or c_resp.json().get("recipeSlug")
            except Exception: slug = c_resp.text.strip('\"')
            if not slug or not get_recipe_by_slug(api_url, api_key, slug):
                for _ in range(4):
                    time.sleep(0.75)
                    fs = search_recipe_slug_by_name(api_url, api_key, name)
                    if fs and get_recipe_by_slug(api_url, api_key, fs): slug = fs; break
            if not slug: return False, "Slug-Auflösung fehlgeschlagen."
    
    db_r = get_recipe_by_slug(api_url, api_key, slug)
    if not db_r: return False, "Rezept konnte nicht geladen werden."
    
    db_r["name"], db_r["description"] = name, clean_str(parsed_data.get("description"))
    if mealie_user_id: db_r["userId"] = mealie_user_id
    if parsed_data.get("orgURL") or org_url: db_r["orgURL"] = parsed_data.get("orgURL") or org_url
    sn = extract_servings_number(parsed_data.get("recipeYield"))
    db_r["recipeYield"] = str(sn) if sn else clean_str(parsed_data.get("recipeYield"))
    db_r["recipeServings"] = sn or 1
    db_r["prepTime"], db_r["performTime"] = clean_str(parsed_data.get("prepTime")), clean_str(parsed_data.get("cookTime"))
    
    if nut := parsed_data.get("nutrition"):
        db_r["nutrition"] = {"calories": nut.get("calories", ""), "carbohydrateContent": nut.get("carbohydrateContent", ""), "proteinContent": nut.get("proteinContent", ""), "fatContent": nut.get("fatContent", "")}
        if any(clean_str(v) for v in nut.values()):
            db_r["showNutrition"] = True
            db_r.setdefault("settings", {})["showNutrition"] = True

    db_r["tags"] = [{"id": tid, "name": n, "slug": slugify(n)} for t in parsed_data.get("tags", []) if (n := clean_str(t.get("name"))) and (tid := get_or_create("organizers/tags", n, api_url, headers, tm))]
    db_r["recipeCategory"] = [{"id": cid, "name": n, "slug": slugify(n)} for c in parsed_data.get("recipeCategory", []) if (n := clean_str(c.get("name"))) and (cid := get_or_create("organizers/categories", n, api_url, headers, cm))]
    db_r["tools"] = [{"id": tlid, "name": n, "slug": slugify(n)} for t in parsed_data.get("tools", []) if (n := clean_str(t.get("name"))) and (tlid := get_or_create_tool_robust(n, api_url, headers, tlm))]

    r2u, f_ings = {}, []
    for ir in parsed_data.get("recipeIngredient", []):
        fv, ov = clean_str(get_nested_name(ir.get("food"))), clean_str(ir.get("originalText"))
        if not (ov or fv): continue
        uid, rid = str(uuid.uuid4()), clean_str(ir.get("referenceId"))
        if rid: r2u[rid] = uid
        ing = {"referenceId": uid, "originalText": ov or fv, "note": clean_str(ir.get("note")), "title": clean_str(ir.get("title")) or None}
        if (qty := safe_float(ir.get("quantity"))) is not None: ing["quantity"] = qty
        if (uv := clean_str(get_nested_name(ir.get("unit")))) and (uid2 := get_or_create("units", uv, api_url, headers, um)): ing["unit"] = {"id": uid2, "name": uv}
        if fv and (fid := get_or_create("foods", fv, api_url, headers, fm)): ing["food"] = {"id": fid, "name": fv}
        f_ings.append(ing)
    db_r["recipeIngredient"] = f_ings

    f_steps = []
    for idx, s in enumerate(parsed_data.get("recipeInstructions", [])):
        if txt := clean_str(s.get("text")):
            refs = [{"referenceId": r2u[rstr]} for r in s.get("ingredientReferences", []) if (rstr := clean_str(r.get("referenceId"))) in r2u]
            f_steps.append({"id": str(uuid.uuid4()), "title": clean_str(s.get("title")) or f"Schritt {idx+1}", "text": txt, "ingredientReferences": refs})
    db_r["recipeInstructions"] = f_steps

    u_resp = safe_mealie_request("PUT", f"{api_url}/api/recipes/{slug}", headers=headers, json=db_r)
    if u_resp.status_code not in (200, 201): return False, f"Fehler beim Aktualisieren: {u_resp.text}"
    
    if cover_img_bytes:
        try: safe_mealie_request("PUT", f"{api_url}/api/recipes/{slug}/image", headers=get_auth_headers(api_key, False), files={"image": ("cover.jpg", cover_img_bytes, "image/jpeg")}, data={"extension": "jpg"})
        except Exception: pass

    db_store_recipes([{"name": name, "slug": slug}])
    if audit_user_key: record_recipe_upload(audit_user_key, slug, name, audit_user_label, audit_user_email)
    try: get_user_stats_snapshot.clear()
    except Exception: pass
    return True, slug

def fetch_mealie_recipe_text(slug: str, api_url: str, api_key: str) -> str:
    recipe = get_recipe_by_slug(api_url, api_key, slug)
    if not recipe: raise RuntimeError(f"Rezept {slug} konnte nicht geladen werden.")
    ings = recipe.get("recipeIngredient", [])
    if any(clean_str(get_nested_name(i.get("food", {}))) or clean_str(i.get("originalText")) for i in ings):
        cr = {"name": recipe.get("name"), "description": recipe.get("description"), "recipeYield": recipe.get("recipeYield"), "prepTime": recipe.get("prepTime"), "cookTime": recipe.get("performTime"), "tags": [{"name": clean_str(t.get("name"))} for t in recipe.get("tags", [])], "recipeCategory": [{"name": clean_str(c.get("name"))} for c in recipe.get("recipeCategory", [])], "tools": [{"name": clean_str(t.get("name"))} for t in recipe.get("tools", [])], "nutrition": recipe.get("nutrition", {}), "recipeIngredient": [{"referenceId": clean_str(i.get("referenceId")), "originalText": clean_str(i.get("originalText")), "title": clean_str(i.get("title")), "note": clean_str(i.get("note")), "quantity": safe_float(i.get("quantity")), "unit": {"name": clean_str(i.get("unit", {}).get("name"))} if i.get("unit") else None, "food": {"name": clean_str(i.get("food", {}).get("name"))} if i.get("food") else None} for i in ings], "recipeInstructions": [{"title": clean_str(s.get("title")), "text": clean_str(s.get("text")), "ingredientReferences": s.get("ingredientReferences", [])} for s in recipe.get("recipeInstructions", [])]}
        return "WICHTIGE ANWEISUNG: Dies ist ein bereits geparstes Rezept als JSON. Es herrscht ein striktes ÄNDERUNGSVERBOT für das Array 'recipeIngredient'. Du musst alle Zutaten, 'originalText' und 'food' 1:1 kopieren!\n\n" + json.dumps(cr, ensure_ascii=False)
    
    rp = [f"Titel: {recipe.get('name', '')}", f"Beschreibung: {recipe.get('description', '')}"]
    if ings: rp.extend(["\nZUTATEN (STRIKTES ÄNDERUNGSVERBOT):"] + [f"- {v}" for i in ings if (v := clean_str(i.get("originalText")) or clean_str(i.get("note")) or clean_str(i.get("display")))])
    if insts := recipe.get("recipeInstructions", []): rp.extend(["\nZubereitung:"] + [clean_str(s.get("text", "")) for s in insts])
    return "\n".join(rp)

@st.cache_data(ttl=300)
def get_user_stats_snapshot(api_url: str, api_key: str, user_key: str) -> Dict[str, Any]:
    u_rows = get_user_uploaded_recipe_rows(user_key)
    u_map = {r["recipe_slug"]: r for r in u_rows}
    i_cnt, scanned = {}, 0
    for r in list(u_map.values())[:150]:
        if full := get_recipe_by_slug(api_url, api_key, r["recipe_slug"]):
            scanned += 1
            seen = set()
            for i in full.get("recipeIngredient", []):
                n = normalize_name(clean_str(get_nested_name(i.get("food"))) or clean_str(i.get("originalText")))
                if n and n not in seen and not any(ex == n or ex in n.split() for ex in ["salz", "wasser", "pfeffer", "meersalz"]) and not ("salz" in n and "pfeffer" in n):
                    i_cnt[n] = i_cnt.get(n, 0) + 1; seen.add(n)
    t_ings = [{"name": k, "hits": v, "share": v/scanned*100} for k, v in sorted(i_cnt.items(), key=lambda x: x[1], reverse=True)[:5]] if scanned else []
    
    with get_db_lock(), db_conn() as conn:
        lb = [{"label": r[0] or "Anonym", "count": r[1]} for r in conn.execute("SELECT user_label, COUNT(recipe_slug) as c FROM uploads WHERE source = 'snap_to_mealie' GROUP BY user_key ORDER BY c DESC LIMIT 10").fetchall()]
        t_row = conn.execute("SELECT COUNT(*) FROM uploads WHERE source = 'snap_to_mealie'").fetchone()
    
    return {"personal_count": len(u_map), "scanned_count": scanned, "top_ingredients": t_ings, "hours_saved": round(len(u_map)*10/60, 1), "upload_rows": u_rows, "leaderboard": lb, "total_app_uploads": t_row[0] if t_row else 0}
