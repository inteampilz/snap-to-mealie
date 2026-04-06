import os, re, json, sqlite3, threading, time, sys, io, zipfile, uuid, math
from typing import Any, Dict, List, Optional, Type, TypeVar
from textwrap import dedent
import logging
import structlog
import streamlit as st
import streamlit.components.v1 as components
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field
from PIL import Image, ImageDraw, ImageFont

# --- CONFIG & LOGGING ---
class AppSettings(BaseSettings):
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    oidc_client_id: Optional[str] = Field(default=None, alias="OIDC_CLIENT_ID")
    oidc_client_secret: str = Field(default="", alias="OIDC_CLIENT_SECRET")
    oidc_discovery_url: str = Field(default="", alias="OIDC_DISCOVERY_URL")
    oidc_redirect_uri: str = Field(default="", alias="OIDC_REDIRECT_URI")
    oidc_cookie_secret: str = Field(default="", alias="OIDC_COOKIE_SECRET")
    mealie_url: str = Field(default="http://localhost:9000", alias="MEALIE_URL")
    mealie_api_key: str = Field(default="", alias="MEALIE_API_KEY")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-2.5-flash", alias="GEMINI_MODEL")
    gemini_rpm: int = Field(default=1000, alias="GEMINI_RPM")
    batch_max_workers: int = Field(default=8, alias="BATCH_MAX_WORKERS")
    request_timeout: int = Field(default=20, alias="REQUEST_TIMEOUT")
    snap_cache_db: str = Field(default=".streamlit/snap_to_mealie_vnext.sqlite3", alias="SNAP_CACHE_DB")
    map_cache_ttl: int = Field(default=300, alias="MAP_CACHE_TTL")
    recipe_cache_ttl: int = Field(default=120, alias="RECIPE_CACHE_TTL")
    max_cover_image_bytes: int = Field(default=15 * 1024 * 1024, alias="MAX_COVER_IMAGE_BYTES")
    active_user_ttl_sec: int = Field(default=900, alias="ACTIVE_USER_TTL_SEC")
    admin_users: str = Field(default="", alias="ADMIN_USERS")
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = AppSettings()
settings.mealie_url = settings.mealie_url.rstrip("/")
ADMIN_USERS_LIST = [s.strip().lower() for s in settings.admin_users.split(",") if s.strip()]

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=settings.log_level.upper())
structlog.configure(
    processors=[structlog.stdlib.add_log_level, structlog.stdlib.add_logger_name, structlog.processors.TimeStamper(fmt="iso"), structlog.dev.ConsoleRenderer()],
    context_class=dict, logger_factory=structlog.stdlib.LoggerFactory(), wrapper_class=structlog.stdlib.BoundLogger, cache_logger_on_first_use=True
)
logger = structlog.get_logger("snap-to-mealie")

STATIC_ROOT = os.path.join(os.getcwd(), "static")
PROMPTS_FILE = os.path.join(STATIC_ROOT, "prompts.json")
PWA_APP_NAME = "Snap-to-Mealie"
PWA_SHORT_NAME = "SnapMealie"

def _create_app_icon(size: int, path: str) -> None:
    img = Image.new("RGBA", (size, size), (103, 80, 164, 255))
    draw = ImageDraw.Draw(img)
    inset = max(12, size // 10)
    draw.rounded_rectangle((inset, inset, size - inset, size - inset), radius=size // 5, fill=(255, 251, 254, 255))
    try: font = ImageFont.truetype("DejaVuSans-Bold.ttf", size // 2)
    except Exception: font = ImageFont.load_default()
    text = "S"
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text(((size - tw) / 2, (size - th) / 2 - size * 0.04), text, font=font, fill=(103, 80, 164, 255))
    img.save(path, format="PNG")
    img.close()

def ensure_streamlit_config() -> None:
    os.makedirs(".streamlit", exist_ok=True)
    config_path = os.path.join(".streamlit", "config.toml")
    cfg = dedent(
        """
        [server]
        enableStaticServing = true
        headless = true
        """
    ).strip()
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f: existing = f.read()
        except Exception: existing = ""
        if "enableStaticServing" not in existing:
            with open(config_path, "a", encoding="utf-8") as f: f.write("\n\n" + cfg + "\n")
    else:
        with open(config_path, "w", encoding="utf-8") as f: f.write(cfg + "\n")

    if settings.oidc_client_id:
        cookie_secret = settings.oidc_cookie_secret.strip()
        if not cookie_secret or cookie_secret == "super-secret":
            raise RuntimeError("OIDC_COOKIE_SECRET fehlt oder ist unsicher. Setze einen zufälligen String im Environment!")

        secrets_path = os.path.join(".streamlit", "secrets.toml")
        if not os.path.exists(secrets_path):
            with open(secrets_path, "w", encoding="utf-8") as f:
                f.write(
                    f'[auth]\nredirect_uri = "{settings.oidc_redirect_uri}"\n'
                    f'cookie_secret = "{cookie_secret}"\n\n'
                    f'[auth.custom]\nclient_id = "{settings.oidc_client_id}"\n'
                    f'client_secret = "{settings.oidc_client_secret}"\n'
                    f'server_metadata_url = "{settings.oidc_discovery_url}"\n'
                )

def ensure_pwa_assets() -> None:
    os.makedirs(STATIC_ROOT, exist_ok=True)
    icons_dir = os.path.join(STATIC_ROOT, "icons")
    os.makedirs(icons_dir, exist_ok=True)
    icon_192 = os.path.join(icons_dir, "icon-192.png")
    icon_512 = os.path.join(icons_dir, "icon-512.png")
    if not os.path.exists(icon_192): _create_app_icon(192, icon_192)
    if not os.path.exists(icon_512): _create_app_icon(512, icon_512)

    manifest = {
        "name": PWA_APP_NAME, "short_name": PWA_SHORT_NAME, "description": "Rezepte aus Bildern, PDFs, URLs und Mealie direkt importieren.",
        "start_url": "/?pwa=1", "id": "/?pwa=1", "display": "standalone", "scope": "/",
        "background_color": "#121116", "theme_color": "#6750a4",
        "icons": [
            {"src": "/app/static/icons/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/app/static/icons/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
        ],
        "share_target": {"action": "/", "method": "GET", "params": {"title": "title", "text": "text", "url": "shared_url"}}
    }
    with open(os.path.join(STATIC_ROOT, "manifest.json"), "w", encoding="utf-8") as f: json.dump(manifest, f, ensure_ascii=False, indent=2)

    sw = dedent(
        """
        self.addEventListener('install', event => { self.skipWaiting(); });
        self.addEventListener('activate', event => { event.waitUntil(self.clients.claim()); });
        self.addEventListener('fetch', event => { event.respondWith(fetch(event.request).catch(() => caches.match(event.request))); });
        """
    ).strip()
    with open(os.path.join(STATIC_ROOT, "sw.js"), "w", encoding="utf-8") as f: f.write(sw)

def inject_pwa_bootstrap() -> None:
    components.html(
        dedent(
            """
            <script>
            const parentDoc = window.parent.document;
            if (!parentDoc.querySelector('link[rel="manifest"]')) {
                const link = parentDoc.createElement('link');
                link.rel = 'manifest';
                link.href = '/app/static/manifest.json?v=6';
                parentDoc.head.appendChild(link);
            }
            if (!parentDoc.querySelector('meta[name="theme-color"]')) {
                const meta = parentDoc.createElement('meta');
                meta.name = 'theme-color';
                meta.content = '#6750a4';
                parentDoc.head.appendChild(meta);
            }
            if ('serviceWorker' in window.parent.navigator) {
                window.parent.navigator.serviceWorker.register('/app/static/sw.js').catch(() => {});
            }
            </script>
            """
        ),
        height=0,
    )


# --- TASKS & STATE MANAGEMENT ---
@st.cache_resource
def get_task_lock() -> threading.Lock: 
    return threading.Lock()

@st.cache_resource
def get_task_registry() -> Dict[str, Dict[str, Any]]: 
    return {}

# --- PYDANTIC MODELS ---
class Tag(BaseModel): name: str
class Category(BaseModel): name: str
class Tool(BaseModel): name: str
class Nutrition(BaseModel):
    calories: Optional[str] = None
    carbohydrateContent: Optional[str] = None
    proteinContent: Optional[str] = None
    fatContent: Optional[str] = None
class Unit(BaseModel): name: str
class Food(BaseModel): name: str
class RecipeIngredient(BaseModel):
    referenceId: str
    title: Optional[str] = None
    quantity: Optional[float] = None
    unit: Optional[Unit] = None
    food: Optional[Food] = None
    note: Optional[str] = ""
    originalText: str
class IngredientReference(BaseModel): referenceId: str
class RecipeInstruction(BaseModel):
    title: str
    text: str
    ingredientReferences: List[IngredientReference] = Field(default_factory=list)
class Recipe(BaseModel):
    name: str
    description: str
    recipeYield: str
    prepTime: str
    cookTime: str
    tags: List[Tag] = Field(default_factory=list)
    recipeCategory: List[Category] = Field(default_factory=list)
    tools: List[Tool] = Field(default_factory=list)
    nutrition: Optional[Nutrition] = None
    recipeIngredient: List[RecipeIngredient] = Field(default_factory=list)
    recipeInstructions: List[RecipeInstruction] = Field(default_factory=list)
class MultiRecipeResponse(BaseModel): recipes: List[Recipe]
class EditorRecipeResponse(BaseModel):
    explanation: str
    recipe: Recipe

T = TypeVar('T', bound=BaseModel)
def _parse_pydantic_json(model_class: Type[T], text: str) -> T:
    clean_text = text.strip()
    bt = chr(96) * 3
    if clean_text.startswith(bt + "json"): clean_text = clean_text[7:]
    elif clean_text.startswith(bt): clean_text = clean_text[3:]
    if clean_text.endswith(bt): clean_text = clean_text[:-3]
    return model_class.model_validate_json(clean_text.strip())

# --- UTILS ---
def clean_str(val: Any) -> str:
    s = str(val).strip() if val else ""
    return "" if s.lower() in {"none", "null", "n/a", "na", "-", "nan", "leer"} else s
def normalize_name(text: str) -> str:
    s = text.lower().strip() if text else ""
    return re.sub(r'[^a-z0-9]', '', s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"))
def slugify(text: str) -> str:
    t = clean_str(text).lower().replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"[^a-z0-9]+", "-", t).strip("-")
def get_nested_name(obj: Any) -> str: return obj.get("name", "") if isinstance(obj, dict) else str(obj) if obj else ""
def extract_servings_number(value: Any) -> Optional[int]:
    match = re.search(r"[0-9]+", clean_str(value))
    return max(1, int(match.group(0))) if match else None
def safe_float(val: Any) -> Optional[float]:
    try:
        num = float(str(val).replace(",", "."))
        return None if math.isnan(num) or math.isinf(num) else num
    except Exception: return None
def unique_by_name(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen, out = set(), []
    for item in items:
        n = clean_str(item.get("name"))
        if n and normalize_name(n) not in seen:
            seen.add(normalize_name(n))
            out.append({"name": n})
    return out
def format_duration(seconds: Optional[float]) -> str:
    if seconds is None: return "wird berechnet"
    m, s = divmod(max(0, int(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m" if h > 0 else f"{m}m {s}s" if m > 0 else f"{s}s"

def safe_close_image(img: Optional[Image.Image]) -> None:
    if img is None: return
    try: img.close()
    except Exception: pass

def close_images(images: Optional[List[Image.Image]]) -> None:
    if not images: return
    for img in images: safe_close_image(img)

# --- PROMPTS ---
JSON_SCHEMA_HINT = """
{
  "name": "Name des Rezepts",
  "description": "Kurze Beschreibung des Gerichts",
  "recipeYield": "4",
  "prepTime": "15 Minuten",
  "cookTime": "30 Minuten",
  "tags": [{"name": "vegan"}],
  "recipeCategory": [{"name": "Hauptgericht"}],
  "tools": [{"name": "Bratpfanne"}],
  "nutrition": {"calories": "450 kcal", "carbohydrateContent": "50 g", "proteinContent": "20 g", "fatContent": "15 g"},
  "recipeIngredient": [{"referenceId":"ing1","title":"Für die Sauce","quantity":1,"unit":{"name":"EL"},"food":{"name":"Olivenöl"},"note":"","originalText":"1 EL Olivenöl"}],
  "recipeInstructions": [{"title":"Schritt 1","text":"Öl erhitzen.","ingredientReferences":[{"referenceId":"ing1"}]}]
}
"""

def get_prompts_config() -> Dict[str, str]:
    defaults = {
        "base_prompt": dedent("""\
            Du bist ein professionelles Rezept-Analysemodell. Du extrahierst aus einem oder mehreren Bildern, Videos ODER Texten/JSON-Dateien zuverlässig alle Rezeptdetails.
            Antworte IMMER ausschließlich mit einem strikt validen JSON und NICHTS anderem.
            Übersetze alle Inhalte ins Deutsche.

            Datenbank-Abgleich:
            Bekannte Zutaten: [{foods_str}]
            Bekannte Tags: [{tags_str}]
            Bekannte Kategorien: [{cats_str}]
            Bekannte Werkzeuge: [{tools_str}]

            ABSOLUT KRITISCHE REGELN FÜR ZUTATEN:
            - ZUTATENLISTE HAT PRIORITÄT: Ziehe die Zutaten immer primär aus der expliziten Zutatenliste (in Text/Beschreibung/Kommentaren).
            - ORIGINALTEXT ZWINGEND ERHALTEN: Das Feld 'originalText' muss IMMER die exakte, vollständige Zeile der Zutat aus der Quelle enthalten (z.B. "250g Kirschtomaten, halbiert"). Nichts weglassen!
            - BEI MEALIE-UPDATES: Wenn du ein bestehendes Rezept als JSON erhältst, MÜSSEN die Felder 'originalText' und 'food.name' 1:1 übernommen werden!
            - KEINE VERFÄLSCHUNG: Du darfst NIEMALS konkrete Zutaten (z.B. "Zucchini") durch generische Oberbegriffe (z.B. "Gemüse") ersetzen.

            Weitere Regeln:
            - note darf nur Eigenschaften enthalten, z.B. gehackt, weich, geschmolzen, warm.
            - Jede Zutat bekommt eine stabile Kurz-ID wie ing1, ing2.
            - ingredientReferences dürfen nur auf existierende referenceIds verweisen.
            - Wenn Mengen fehlen, quantity=null.
            - recipeYield / Portionen müssen exakt erkannt werden. Setze niemals pauschal 1.
            - SCHÄTZE NÄHRWERTE: Wenn keine Nährwerte im Text stehen, schätze die Nährwerte (Kalorien, Kohlenhydrate, Eiweiß, Fett) pro Portion realistisch ab.
            - title-Sektionen nur auf der ersten Zutat einer Sektion setzen.
            - Zutaten chronologisch nach Verwendung sortieren.
            - Erzeuge IMMER sinnvolle, kleinschrittige Überschriften (title) für Arbeitsschritte.
            - Extrahiere alle benötigten Werkzeuge/Utensilien als Array in das Feld 'tools'.
            - Zeiten als lesbarer Text, niemals ISO8601.
            - Antwort nur als JSON im folgenden Schema:
            {json_schema_hint}""").strip(),
        "pdf_prompt_addition": "\n\nZUSATZ-REGEL FÜR DOKUMENTE:\nDas übergebene Dokument kann MEHRERE Rezepte enthalten. Extrahiere ALLE Rezepte, die du im Dokument findest, und gib sie als Liste im JSON zurück.",
        "video_prompt_addition": "\n\nSPEZIELLE VIDEO-REGELN:\n1. TEXT ZUERST: Die genauen Zutaten und Mengen stehen oft in den angehängten Text-Metadaten. Wenn du dort eine Zutatenliste findest, hat diese ABSOLUTE PRIORITÄT und muss 1:1 übernommen werden.\n2. VIDEO-FALLBACK: NUR WENN in den Text-Metadaten absolut keine Zutaten zu finden sind, analysiere das Video, um die Zutaten und Mengen aus Bild und Ton zu extrahieren.\n3. ZUBEREITUNG: Die Arbeitsschritte leitest du immer primär aus dem Video ab.",
        "editor_prompt": dedent("""\
            Du bist ein KI-Sous-Chef. Du erhältst ein bestehendes Rezept als JSON und eine Nutzeranweisung. Verändere das Rezept präzise nach der Anweisung.

            WICHTIGE REGELN:
            1. Ändere NIEMALS das Feld 'originalText' oder 'referenceId' von BEREITS BESTEHENDEN Zutaten! Diese müssen exakt gleich bleiben.
            2. Passe Mengen (quantity), Einheiten (unit), Portionen (recipeYield) und Schritte sinnvoll an.
            3. Passe auch die Nährwerte ('nutrition') linear zu den Portionen an, falls welche vorhanden sind.
            4. Wenn du komplett neue Zutaten hinzufügst, erzeuge für diese einen sinnvollen 'originalText'.
            5. Fasse in 1-2 kurzen, freundlichen Sätzen zusammen, was du gemacht hast (im Feld 'explanation').""").strip()
    }
    
    try:
        if not os.path.exists(PROMPTS_FILE):
            os.makedirs(os.path.dirname(PROMPTS_FILE), exist_ok=True)
            with open(PROMPTS_FILE, "w", encoding="utf-8") as f:
                json.dump(defaults, f, ensure_ascii=False, indent=4)
            return defaults
        
        with open(PROMPTS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            for k, v in defaults.items():
                if k not in loaded:
                    loaded[k] = v
            return loaded
    except Exception as e:
        logger.error("error_loading_prompts", error=str(e))
        return defaults


# --- DATABASE ---
_db_local = threading.local()

import contextlib
@contextlib.contextmanager
def db_conn():
    if not hasattr(_db_local, "sqlite_conn"):
        os.makedirs(os.path.dirname(settings.snap_cache_db), exist_ok=True)
        conn = sqlite3.connect(settings.snap_cache_db, check_same_thread=False, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        _db_local.sqlite_conn = conn
    yield _db_local.sqlite_conn

def init_cache_db() -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS mappings (endpoint TEXT NOT NULL, name TEXT NOT NULL, item_id TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(endpoint, name))")
        conn.execute("CREATE TABLE IF NOT EXISTS recipes (norm_name PRIMARY KEY, recipe_name TEXT NOT NULL, slug TEXT NOT NULL, updated_at INTEGER NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS uploads (user_key TEXT NOT NULL, recipe_slug TEXT NOT NULL, recipe_name TEXT NOT NULL, user_label TEXT, user_email TEXT, source TEXT NOT NULL, first_uploaded_at INTEGER NOT NULL, last_uploaded_at INTEGER NOT NULL, upload_count INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(user_key, recipe_slug))")
        conn.execute("CREATE TABLE IF NOT EXISTS image_prompts (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, prompt_text TEXT NOT NULL, is_default INTEGER NOT NULL DEFAULT 0)")
        try: conn.execute("ALTER TABLE image_prompts ADD COLUMN user_label TEXT DEFAULT ''")
        except sqlite3.OperationalError: pass
        conn.execute("CREATE TABLE IF NOT EXISTS editor_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, user_key TEXT NOT NULL, recipe_name TEXT NOT NULL, recipe_data TEXT NOT NULL, cover_image BLOB, created_at INTEGER NOT NULL)")
        try: conn.execute("DELETE FROM image_prompts WHERE prompt_text LIKE 'http://%' OR prompt_text LIKE 'https://%'")
        except Exception: pass
        conn.commit()

# Expose standard DB operations
def get_image_prompts(user_label: str = "") -> List[Dict[str, Any]]:
    with get_db_lock(), db_conn() as conn:
        q = "SELECT id, name, prompt_text, is_default, user_label FROM image_prompts WHERE user_label = ? OR user_label = '' OR (user_label LIKE '%Lars Graf%' AND is_default = 1) ORDER BY name" if user_label else "SELECT id, name, prompt_text, is_default, user_label FROM image_prompts ORDER BY name"
        rows = conn.execute(q, (user_label,) if user_label else ()).fetchall()
    return [{"id": r[0], "name": r[1], "text": r[2], "is_default": bool(r[3]), "user_label": r[4] if len(r) > 4 else ""} for r in rows]

def save_image_prompt(name: str, text: str, user_label: str = "", is_default: bool = False) -> None:
    with get_db_lock(), db_conn() as conn:
        if is_default: conn.execute("UPDATE image_prompts SET is_default = 0 WHERE user_label = ?", (user_label,))
        conn.execute("INSERT INTO image_prompts (name, prompt_text, is_default, user_label) VALUES (?, ?, ?, ?)", (name, text, int(is_default), user_label))
        conn.commit()

def set_default_image_prompt(prompt_id: int, user_label: str = "") -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("UPDATE image_prompts SET is_default = 0 WHERE user_label = ?", (user_label,))
        conn.execute("UPDATE image_prompts SET is_default = 1 WHERE id = ?", (prompt_id,))
        conn.commit()

def delete_image_prompt(prompt_id: int) -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("DELETE FROM image_prompts WHERE id = ?", (prompt_id,))
        conn.commit()

def add_to_editor_queue(user_key: str, recipe_data: Dict[str, Any], cover_image: Optional[bytes] = None) -> None:
    name = clean_str(recipe_data.get("name", "Unbenanntes Rezept"))
    with get_db_lock(), db_conn() as conn:
        conn.execute("INSERT INTO editor_queue (user_key, recipe_name, recipe_data, cover_image, created_at) VALUES (?, ?, ?, ?, ?)", (user_key, name, json.dumps(recipe_data, ensure_ascii=False), cover_image, int(time.time())))
        conn.commit()

def get_editor_queue(user_key: str) -> List[Dict[str, Any]]:
    with get_db_lock(), db_conn() as conn:
        rows = conn.execute("SELECT id, recipe_name, recipe_data, cover_image, created_at FROM editor_queue WHERE user_key = ? ORDER BY created_at ASC", (user_key,)).fetchall()
    return [{"id": r[0], "recipe_name": r[1], "recipe_data": json.loads(r[2]), "cover_image": r[3], "created_at": r[4]} for r in rows]

def delete_from_editor_queue(item_id: int) -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("DELETE FROM editor_queue WHERE id = ?", (item_id,))
        conn.commit()

def db_find_recipe_slug(recipe_name: str) -> Optional[str]:
    k = normalize_name(recipe_name)
    with get_db_lock(), db_conn() as conn:
        row = conn.execute("SELECT slug FROM recipes WHERE norm_name = ?", (k,)).fetchone()
    return row[0] if row else None

def db_delete_recipe(recipe_name: str) -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("DELETE FROM recipes WHERE norm_name = ?", (normalize_name(recipe_name),))
        conn.commit()

def db_delete_recipe_by_slug(slug: str) -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("DELETE FROM recipes WHERE slug = ?", (clean_str(slug),))
        conn.commit()

def db_store_recipes(recipes: List[Dict[str, str]]) -> None:
    rows = [(normalize_name(r.get("name")), clean_str(r.get("name")), clean_str(r.get("slug")), int(time.time())) for r in recipes if r.get("name") and r.get("slug")]
    if rows:
        with get_db_lock(), db_conn() as conn:
            conn.executemany("INSERT INTO recipes(norm_name, recipe_name, slug, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(norm_name) DO UPDATE SET recipe_name=excluded.recipe_name, slug=excluded.slug, updated_at=excluded.updated_at", rows)
            conn.commit()

def db_get_mapping(endpoint: str, name: str) -> Optional[str]:
    with get_db_lock(), db_conn() as conn:
        row = conn.execute("SELECT item_id FROM mappings WHERE endpoint = ? AND name = ?", (endpoint, normalize_name(name))).fetchone()
    return row[0] if row else None

def db_set_mapping(endpoint: str, name: str, item_id: str) -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("INSERT INTO mappings(endpoint, name, item_id, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(endpoint, name) DO UPDATE SET item_id=excluded.item_id, updated_at=excluded.updated_at", (endpoint, normalize_name(name), clean_str(item_id), int(time.time())))
        conn.commit()

def db_bulk_replace_mappings(endpoint: str, mapping: Dict[str, str]) -> None:
    rows = [(endpoint, normalize_name(k), clean_str(v), int(time.time())) for k, v in mapping.items() if normalize_name(k) and clean_str(v)]
    if rows:
        with get_db_lock(), db_conn() as conn:
            conn.executemany("INSERT INTO mappings(endpoint, name, item_id, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(endpoint, name) DO UPDATE SET item_id=excluded.item_id, updated_at=excluded.updated_at", rows)
            conn.commit()

# --- AUTH & USERS ---
def has_streamlit_auth() -> bool: return all(hasattr(st, attr) for attr in ["login", "logout"]) and hasattr(st, "user")
def is_streamlit_user_logged_in() -> bool: return True if getattr(st, "user", None) is None else bool(getattr(st.user, "is_logged_in", False))
def get_current_user_email() -> str: return clean_str(getattr(getattr(st, "user", None), "email", ""))
def get_current_user_label() -> str:
    user_obj = getattr(st, "user", None)
    for attr in ["name", "preferred_username", "email", "id"]:
        val = clean_str(getattr(user_obj, attr, ""))
        if val: return val
    return "Anonym"
def get_current_user_key() -> str: return get_current_user_email().lower() or get_current_user_label().lower() or "anonym"
def is_admin_user() -> bool: return True if not ADMIN_USERS_LIST else (get_current_user_email().lower() in ADMIN_USERS_LIST or get_current_user_label().lower() in ADMIN_USERS_LIST)

@st.cache_resource
def get_active_user_registry() -> Dict[str, Dict[str, Any]]: return {}

def register_active_user() -> None:
    if "_snap_session_id" not in st.session_state: st.session_state._snap_session_id = str(uuid.uuid4())
    get_active_user_registry()[st.session_state._snap_session_id] = {"label": get_current_user_label(), "email": get_current_user_email(), "last_seen": time.time()}

def get_active_users_snapshot() -> List[Dict[str, Any]]:
    cutoff = time.time() - settings.active_user_ttl_sec
    reg = get_active_user_registry()
    for sid in list(reg.keys()):
        if reg[sid].get("last_seen", 0) < cutoff: del reg[sid]
    deduped = {}
    for user in reg.values():
        k = (clean_str(user.get("email")) or clean_str(user.get("label")) or "anonym").lower()
        if k not in deduped or user.get("last_seen", 0) > deduped[k].get("last_seen", 0): deduped[k] = user
    return sorted(deduped.values(), key=lambda x: (x.get("label") or "", x.get("email") or ""))

def record_recipe_upload(user_key: str, recipe_slug: str, recipe_name: str, user_label: str, user_email: str) -> None:
    with get_db_lock(), db_conn() as conn:
        conn.execute("INSERT INTO uploads(user_key, recipe_slug, recipe_name, user_label, user_email, source, first_uploaded_at, last_uploaded_at, upload_count) VALUES (?, ?, ?, ?, ?, 'snap_to_mealie', ?, ?, 1) ON CONFLICT(user_key, recipe_slug) DO UPDATE SET recipe_name=excluded.recipe_name, user_label=excluded.user_label, user_email=excluded.user_email, source=excluded.source, last_uploaded_at=excluded.last_uploaded_at, upload_count=uploads.upload_count + 1", (clean_str(user_key).lower(), clean_str(recipe_slug), clean_str(recipe_name), clean_str(user_label), clean_str(user_email), int(time.time()), int(time.time())))
        conn.commit()

def get_user_uploaded_recipe_rows(user_key: str) -> List[Dict[str, Any]]:
    with get_db_lock(), db_conn() as conn:
        rows = conn.execute("SELECT recipe_slug, recipe_name, user_label, user_email, first_uploaded_at, last_uploaded_at, upload_count FROM uploads WHERE user_key = ? AND source = 'snap_to_mealie' ORDER BY last_uploaded_at DESC", (clean_str(user_key).lower(),)).fetchall()
    return [{"recipe_slug": r[0], "recipe_name": r[1], "user_label": r[2], "user_email": r[3], "first_uploaded_at": r[4], "last_uploaded_at": r[5], "upload_count": r[6]} for r in rows]

def get_all_uploaded_recipe_rows(limit: int = 100) -> List[Dict[str, Any]]:
    with get_db_lock(), db_conn() as conn:
        rows = conn.execute("SELECT recipe_slug, recipe_name, user_label, user_email, first_uploaded_at, last_uploaded_at, upload_count FROM uploads WHERE source = 'snap_to_mealie' ORDER BY last_uploaded_at DESC LIMIT ?", (limit,)).fetchall()
    return [{"recipe_slug": r[0], "recipe_name": r[1], "user_label": r[2] or "Anonym", "user_email": r[3], "first_uploaded_at": r[4], "last_uploaded_at": r[5], "upload_count": r[6]} for r in rows]

def generate_extension_zip() -> bytes:
    manifest = '{"manifest_version":3,"name":"Snap-to-Mealie Sender","version":"1.0","permissions":["activeTab","storage"],"action":{"default_title":"An Snap-to-Mealie senden"},"options_page":"options.html","background":{"service_worker":"background.js"}}'
    bg_js = 'chrome.action.onClicked.addListener((tab) => { chrome.storage.sync.get(["snapUrl"], function(result) { if (!result.snapUrl) { chrome.runtime.openOptionsPage(); return; } chrome.tabs.create({ url: result.snapUrl.replace(/\\/$/, "") + "/?shared_url=" + encodeURIComponent(tab.url) }); }); });'
    opt_html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Setup</title><style>body{font-family:sans-serif;padding:20px;max-width:400px}input{width:100%;padding:8px;margin:10px 0}button{background:#6750a4;color:#fff;border:none;padding:10px;width:100%;cursor:pointer}.status{color:green;margin-top:10px}</style></head><body><h2>⚙️ Setup</h2><input type="text" id="urlInput" placeholder="https://..."><button id="saveBtn">Speichern</button><div id="status" class="status"></div><script src="options.js"></script></body></html>'
    opt_js = 'document.addEventListener("DOMContentLoaded",()=>{chrome.storage.sync.get(["snapUrl"],r=>{if(r.snapUrl)document.getElementById("urlInput").value=r.snapUrl});document.getElementById("saveBtn").addEventListener("click",()=>{const url=document.getElementById("urlInput").value.trim();if(url){chrome.storage.sync.set({snapUrl:url},()=>{const s=document.getElementById("status");s.textContent="✅ Gespeichert!";setTimeout(()=>s.textContent="",2000)})}})});'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for n, d in [("manifest.json", manifest), ("background.js", bg_js), ("options.html", opt_html), ("options.js", opt_js)]: z.writestr(n, d)
    return buf.getvalue()
