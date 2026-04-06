import base64
import gc
import html
import asyncio
import subprocess
import io
import json
import math
import os
import re
import sqlite3
import threading
import time
import sys
import uuid
import tempfile
import shutil
import zipfile
import logging
from contextlib import contextmanager
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar
from urllib.parse import urljoin, urlparse

import requests
import structlog
import streamlit as st
import streamlit.components.v1 as components
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, ValidationError
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# Config / Environment Validation via Pydantic
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Structured Logging Setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=settings.log_level.upper(),
)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger("snap-to-mealie")


# -----------------------------------------------------------------------------
# Library Import Checks
# -----------------------------------------------------------------------------
try:
    from google import genai
    from google.genai import types
except ImportError:
    st.error("google-genai SDK fehlt. Bitte prüfe die requirements/docker-compose.")
    st.stop()

try:
    import pytesseract
    try:
        pytesseract.get_tesseract_version()
        OCR_AVAILABLE = True
    except Exception:
        OCR_AVAILABLE = False
except Exception:
    pytesseract = None
    OCR_AVAILABLE = False

try:
    import yt_dlp
    VIDEO_IMPORT_AVAILABLE = True
except Exception:
    yt_dlp = None
    VIDEO_IMPORT_AVAILABLE = False

try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except Exception:
    trafilatura = None
    TRAFILATURA_AVAILABLE = False

STATIC_ROOT = os.path.join(os.getcwd(), "static")
PROMPTS_FILE = os.path.join(STATIC_ROOT, "prompts.json")
PWA_APP_NAME = "Snap-to-Mealie"
PWA_SHORT_NAME = "SnapMealie"
LOTTIE_URL = "https://assets5.lottiefiles.com/packages/lf20_q8m8hb2a.json"


# -----------------------------------------------------------------------------
# Pydantic Models for Structured Output
# -----------------------------------------------------------------------------
class Tag(BaseModel):
    name: str

class Category(BaseModel):
    name: str

class Tool(BaseModel):
    name: str

class Nutrition(BaseModel):
    calories: Optional[str] = None
    carbohydrateContent: Optional[str] = None
    proteinContent: Optional[str] = None
    fatContent: Optional[str] = None

class Unit(BaseModel):
    name: str

class Food(BaseModel):
    name: str

class RecipeIngredient(BaseModel):
    referenceId: str = Field(description="Unique string ID for this ingredient, e.g. 'ing1'")
    title: Optional[str] = None
    quantity: Optional[float] = None
    unit: Optional[Unit] = None
    food: Optional[Food] = None
    note: Optional[str] = ""
    originalText: str

class IngredientReference(BaseModel):
    referenceId: str

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

class MultiRecipeResponse(BaseModel):
    recipes: List[Recipe]

class EditorRecipeResponse(BaseModel):
    explanation: str = Field(description="Kurze Zusammenfassung der Änderungen in 1-2 Sätzen.")
    recipe: Recipe

T = TypeVar('T', bound=BaseModel)

def _parse_pydantic_json(model_class: Type[T], text: str) -> T:
    """Extrahiert JSON sicher aus der LLM-Antwort und validiert es gegen Pydantic."""
    clean_text = text.strip()
    if clean_text.startswith("```json"):
        clean_text = clean_text[7:]
    elif clean_text.startswith("```"):
        clean_text = clean_text[3:]
    if clean_text.endswith("```"):
        clean_text = clean_text[:-3]
    return model_class.model_validate_json(clean_text.strip())


# -----------------------------------------------------------------------------
# Prompt Versioning (JSON Config)
# -----------------------------------------------------------------------------
def get_prompts_config() -> Dict[str, str]:
    defaults = {
        "base_prompt": dedent("""\
            Du bist ein professionelles Rezept-Analysemodell. Du extrahierst aus einem oder mehreren Bildern, Videos ODER Texten/JSON-Dateien zuverlässig alle Rezeptdetails.
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
            - Zeiten als lesbarer Text, niemals ISO8601.""").strip(),
        "pdf_prompt_addition": "\n\nZUSATZ-REGEL FÜR DOKUMENTE:\nDas übergebene Dokument kann MEHRERE Rezepte enthalten. Extrahiere ALLE Rezepte, die du im Dokument findest.",
        "video_prompt_addition": "\n\nSPEZIELLE VIDEO-REGELN:\n1. TEXT ZUERST: Die genauen Zutaten und Mengen stehen oft in den angehängten Text-Metadaten. Wenn du dort eine Zutatenliste findest, hat diese ABSOLUTE PRIORITÄT und muss 1:1 übernommen werden.\n2. VIDEO-FALLBACK: NUR WENN in den Text-Metadaten absolut keine Zutaten zu finden sind, analysiere das Video, um die Zutaten und Mengen aus Bild und Ton zu extrahieren.\n3. ZUBEREITUNG: Die Arbeitsschritte leitest du immer primär aus dem Video ab.",
        "editor_prompt": dedent("""\
            Du bist ein KI-Sous-Chef. Du erhältst ein bestehendes Rezept als JSON und eine Nutzeranweisung. Verändere das Rezept präzise nach der Anweisung.

            WICHTIGE REGELN:
            1. Ändere NIEMALS das Feld 'originalText' oder 'referenceId' von BEREITS BESTEHENDEN Zutaten! Diese müssen exakt gleich bleiben.
            2. Passe Mengen (quantity), Einheiten (unit), Portionen (recipeYield) und Schritte sinnvoll an.
            3. Passe auch die Nährwerte ('nutrition') linear zu den Portionen an, falls welche vorhanden sind.
            4. Wenn du komplett neue Zutaten hinzufügst, erzeuge für diese einen sinnvollen 'originalText'.""").strip()
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

def get_prompt() -> str:
    foods_map, _, tags_map, cats_map, tools_map = get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key)
    foods_str = ", ".join(list(foods_map.keys())) if foods_map else "Keine"
    tags_str = ", ".join(list(tags_map.keys())) if tags_map else "Keine"
    cats_str = ", ".join(list(cats_map.keys())) if cats_map else "Keine"
    tools_str = ", ".join(list(tools_map.keys())) if tools_map else "Keine"

    config = get_prompts_config()
    template = config.get("base_prompt", "")
    template = template.replace("{foods_str}", foods_str)
    template = template.replace("{tags_str}", tags_str)
    template = template.replace("{cats_str}", cats_str)
    template = template.replace("{tools_str}", tools_str)
    return template.strip()

def get_pdf_prompt() -> str:
    return get_prompt() + get_prompts_config().get("pdf_prompt_addition", "")


# -----------------------------------------------------------------------------
# Extension ZIP Generator
# -----------------------------------------------------------------------------
def generate_extension_zip() -> bytes:
    """Generiert die Chrome-Extension 'on the fly' als ZIP-Datei."""
    manifest = """{
  "manifest_version": 3,
  "name": "Snap-to-Mealie Sender",
  "version": "1.0",
  "description": "Sendet die aktuelle Webseite (Rezepte/Videos) mit einem Klick an deinen Snap-to-Mealie Server.",
  "permissions": ["activeTab", "storage"],
  "action": {
    "default_title": "An Snap-to-Mealie senden"
  },
  "options_page": "options.html",
  "background": {
    "service_worker": "background.js"
  }
}"""
    bg_js = """chrome.action.onClicked.addListener((tab) => {
    chrome.storage.sync.get(['snapUrl'], function(result) {
        if (!result.snapUrl) {
            chrome.runtime.openOptionsPage();
            return;
        }
        const snapBaseUrl = result.snapUrl.replace(/\\/$/, "");
        const shareUrl = snapBaseUrl + "/?shared_url=" + encodeURIComponent(tab.url);
        chrome.tabs.create({ url: shareUrl });
    });
});"""
    opt_html = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Snap-to-Mealie Optionen</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; padding: 20px; max-width: 400px; }
    input { width: 100%; padding: 8px; margin: 10px 0; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
    button { background-color: #6750a4; color: white; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; font-weight: bold; width: 100%; }
    button:hover { background-color: #55418a; }
    .status { margin-top: 10px; color: green; font-weight: bold; }
  </style>
</head>
<body>
  <h2>⚙️ Snap-to-Mealie Setup</h2>
  <p>Trage hier die Adresse deines Snap-to-Mealie Servers ein (inkl. https://):</p>
  <input type="text" id="urlInput" placeholder="z. B. [https://snap.deinedomain.de](https://snap.deinedomain.de)">
  <button id="saveBtn">Speichern</button>
  <div id="status" class="status"></div>
  <script src="options.js"></script>
</body>
</html>"""
    opt_js = """document.addEventListener('DOMContentLoaded', () => {
    chrome.storage.sync.get(['snapUrl'], function(result) {
        if (result.snapUrl) {
            document.getElementById('urlInput').value = result.snapUrl;
        }
    });

    document.getElementById('saveBtn').addEventListener('click', () => {
        const url = document.getElementById('urlInput').value.trim();
        if (url) {
            chrome.storage.sync.set({ snapUrl: url }, function() {
                const status = document.getElementById('status');
                status.textContent = "✅ Gespeichert!";
                setTimeout(() => status.textContent = "", 2000);
            });
        }
    });
});"""

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", manifest)
        z.writestr("background.js", bg_js)
        z.writestr("options.html", opt_html)
        z.writestr("options.js", opt_js)
    return buf.getvalue()


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------
def clean_str(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    banned_words = {"none", "null", "n/a", "na", "-", "nan", "leer"}
    return "" if s.lower() in banned_words else s

def normalize_name(text: str) -> str:
    if not text: return ""
    s = text.lower().strip()
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r'[^a-z0-9]', '', s)

def slugify(text: str) -> str:
    text = clean_str(text).lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")

def get_nested_name(obj: Any) -> str:
    if isinstance(obj, dict):
        return obj.get("name", "")
    return str(obj) if obj else ""

def extract_servings_number(value: Any) -> Optional[int]:
    raw = clean_str(value)
    if not raw:
        return None
    match = re.search(r"[0-9]+", raw)
    if match:
        try:
            return max(1, int(match.group(0)))
        except Exception:
            return None
    return None

def safe_float(val: Any) -> Optional[float]:
    if val is None or str(val).strip() == "":
        return None
    try:
        num = float(str(val).replace(",", "."))
        if math.isnan(num) or math.isinf(num):
            return None
        return num
    except Exception:
        return None

def unique_by_name(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for item in items:
        name = clean_str(item.get("name"))
        if not name:
            continue
        key = normalize_name(name)
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": name})
    return out

def safe_close_image(img: Optional[Image.Image]) -> None:
    if img is None:
        return
    try:
        img.close()
    except Exception:
        pass

def close_images(images: Optional[List[Image.Image]]) -> None:
    if not images:
        return
    for img in images:
        safe_close_image(img)

def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "wird berechnet"
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"

def toast(message: str, icon: str = "ℹ️") -> None:
    if hasattr(st, "toast"):
        st.toast(message, icon=icon)
    else:
        st.info(f"{icon} {message}")

def get_blur_placeholder(image_bytes: Optional[bytes]) -> Image.Image:
    if image_bytes:
        try:
            img = Image.open(io.BytesIO(image_bytes))
            if img.mode != 'RGB':
                img = img.convert('RGB')
            return img.filter(ImageFilter.GaussianBlur(30))
        except Exception:
            pass
    return Image.new('RGB', (800, 800), color=(233, 221, 255))


# -----------------------------------------------------------------------------
# State Management
# -----------------------------------------------------------------------------
def init_session() -> None:
    defaults = {
        "recipe_data": None,
        "upload_success": [],
        "collected_images": [],
        "collected_pdfs": [],
        "cover_image_bytes": None,
        "target_slug": None,
        "cropper_open": False,
        "sous_chef_history": [],
        "shared_urls_input": "",
        "shared_video_input": "",
        "shared_mealie_input": "",
        "switch_to_tab": None,
        "current_queue_id": None,
        "theme_mode": "dark"
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


# -----------------------------------------------------------------------------
# PWA / static assets
# -----------------------------------------------------------------------------
def _create_app_icon(size: int, path: str) -> None:
    img = Image.new("RGBA", (size, size), (103, 80, 164, 255))
    draw = ImageDraw.Draw(img)
    inset = max(12, size // 10)
    draw.rounded_rectangle((inset, inset, size - inset, size - inset), radius=size // 5, fill=(255, 251, 254, 255))
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", size // 2)
    except Exception:
        font = ImageFont.load_default()
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
            with open(config_path, "r", encoding="utf-8") as f:
                existing = f.read()
        except Exception:
            existing = ""
        if "enableStaticServing" not in existing:
            with open(config_path, "a", encoding="utf-8") as f:
                f.write("\n\n" + cfg + "\n")
    else:
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(cfg + "\n")

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
    if not os.path.exists(icon_192):
        _create_app_icon(192, icon_192)
    if not os.path.exists(icon_512):
        _create_app_icon(512, icon_512)

    manifest = {
        "name": PWA_APP_NAME,
        "short_name": PWA_SHORT_NAME,
        "description": "Rezepte aus Bildern, PDFs, URLs und Mealie direkt importieren.",
        "start_url": "/?pwa=1",
        "id": "/?pwa=1",
        "display": "standalone",
        "scope": "/",
        "background_color": "#121116",
        "theme_color": "#6750a4",
        "icons": [
            {"src": "/app/static/icons/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/app/static/icons/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
        ],
        "share_target": {
            "action": "/",
            "method": "GET",
            "params": {
                "title": "title",
                "text": "text",
                "url": "shared_url"
            }
        }
    }
    with open(os.path.join(STATIC_ROOT, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    sw = dedent(
        """
        self.addEventListener('install', event => {
          self.skipWaiting();
        });
        self.addEventListener('activate', event => {
          event.waitUntil(self.clients.claim());
        });
        self.addEventListener('fetch', event => {
          event.respondWith(fetch(event.request).catch(() => caches.match(event.request)));
        });
        """
    ).strip()
    with open(os.path.join(STATIC_ROOT, "sw.js"), "w", encoding="utf-8") as f:
        f.write(sw)

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


# -----------------------------------------------------------------------------
# Streamlit setup / theme
# -----------------------------------------------------------------------------
def inject_ui(theme_mode: str = "dark") -> None:
    is_dark = theme_mode == "dark"
    surface = "#221f2b" if is_dark else "#fffbfe"
    text = "#f4eff4" if is_dark else "#1d1b20"
    text_muted = "#cfc6d6" if is_dark else "#49454f"
    outline = "rgba(202,196,208,0.20)" if is_dark else "rgba(121,116,126,0.14)"
    bg = (
        "radial-gradient(900px 420px at 0% 0%, rgba(103,80,164,0.16), transparent 45%),"
        " radial-gradient(900px 420px at 100% 0%, rgba(142,108,199,0.14), transparent 42%),"
        " linear-gradient(180deg, #121116 0%, #17151d 30%, #1b1822 100%)"
    ) if is_dark else (
        "radial-gradient(900px 420px at 0% 0%, rgba(103,80,164,0.10), transparent 45%),"
        " radial-gradient(900px 420px at 100% 0%, rgba(142,108,199,0.10), transparent 42%),"
        " linear-gradient(180deg, #f7f2fa 0%, #fffbfe 30%, #f8f4fb 100%)"
    )
    sidebar = "linear-gradient(180deg, #18161f 0%, #1d1a24 100%)" if is_dark else "linear-gradient(180deg, #fcf8ff 0%, #f8f4fb 100%)"
    css = f"""
    <style>
    :root {{
      --primary:#6750a4; --on-primary:#fff; --surface:{surface}; --text:{text}; --text-muted:{text_muted};
      --outline:{outline}; --surface-soft:{'#2a2632' if is_dark else '#f3edf7'};
      --shadow-1:0 1px 2px rgba(0,0,0,.10),0 1px 3px 1px rgba(0,0,0,.08);
      --shadow-2:0 2px 6px rgba(0,0,0,.12),0 1px 2px rgba(0,0,0,.08);
    }}
    .stApp {{ background:{bg}; }}
    .block-container {{ max-width:1200px; padding-top:1rem; padding-bottom:2rem; }}
    header[data-testid="stHeader"] {{ background:rgba(0,0,0,0)!important; border-bottom:none!important; }}
    #MainMenu {{ visibility:hidden!important; }}
    [data-testid="collapsedControl"] {{ position:fixed!important; top:.8rem!important; left:.9rem!important; z-index:1001!important; }}
    button[kind="header"], [data-testid="collapsedControl"] button {{
      background:#000!important; border:3px solid #fff!important; border-radius:16px!important; box-shadow:0 0 0 4px rgba(0,0,0,.22), var(--shadow-2)!important;
      color:#fff!important; min-width:46px!important; min-height:46px!important;
    }}
    button[kind="header"] svg, [data-testid="collapsedControl"] button svg {{ fill:#fff!important; color:#fff!important; opacity:1!important; }}
    [data-testid="stSidebar"] {{ background:{sidebar}; border-right:1px solid var(--outline); }}
    [data-testid="stSidebar"] * {{ color:{text}!important; }}
    [data-testid="stSidebar"] .stButton>button, [data-testid="stSidebar"] [data-testid="stExpander"] {{ background:{surface}!important; border:1px solid var(--outline)!important; }}
    .snap-appbar {{ display:flex; align-items:center; justify-content:space-between; gap:1rem; margin-bottom:.8rem; }}
    .snap-appbar h1 {{ margin:0; font-size:2rem; line-height:1; color:var(--text); text-transform:lowercase; letter-spacing:-.03em; }}
    .snap-badges {{ display:flex; gap:.45rem; flex-wrap:wrap; justify-content:flex-end; align-items:center; min-height:34px; }}
    .snap-badge {{ display:inline-flex; align-items:center; min-height:34px; padding:.35rem .7rem; border-radius:999px; background:{'#3b2f59' if is_dark else '#e9ddff'}; color:{'#f1e7ff' if is_dark else '#22005d'}; font-size:.78rem; font-weight:700; box-shadow:var(--shadow-1); }}
    [data-testid="stPopover"] button {{ min-width:34px!important; min-height:34px!important; height:34px!important; background:transparent!important; border:none!important; box-shadow:none!important; color:var(--text)!important; font-size:1.35rem!important; padding:0!important; }}
    [data-testid="stPopover"] button::after {{ content:none!important; display:none!important; }}
    [data-testid="stPopover"] button svg:last-of-type {{ display:none!important; }}
    .snap-card {{ border:1px solid var(--outline); background:{surface}; border-radius:26px; box-shadow:var(--shadow-1); padding:1rem 1.1rem; margin-bottom:.9rem; }}
    .snap-card h3 {{ margin:0; color:var(--text); font-size:1rem; }}
    .snap-card p {{ margin:.35rem 0 0; color:var(--text-muted); line-height:1.58; }}
    .stTabs [data-baseweb="tab-list"] {{ gap:.5rem; background:{'#2a2632' if is_dark else '#f3edf7'}; border:1px solid var(--outline); border-radius:999px; padding:.35rem; box-shadow:var(--shadow-1); }}
    .stTabs [data-baseweb="tab"] {{ height:46px; border-radius:999px; color:var(--text-muted); font-weight:700; }}
    .stTabs [aria-selected="true"] {{ background:var(--primary)!important; color:#fff!important; box-shadow:var(--shadow-2)!important; }}
    .stButton>button, .stForm button[type="submit"] {{ min-height:44px; border-radius:999px!important; border:1px solid var(--outline)!important; background:{surface}!important; color:var(--text)!important; font-weight:700!important; box-shadow:var(--shadow-1)!important; }}
    .stButton>button[kind="primary"], .stForm button[type="submit"] {{ background:linear-gradient(135deg,#7356b6 0%, var(--primary) 100%)!important; color:#fff!important; border-color:transparent!important; }}
    div[data-testid="stFileUploader"] section {{ background:{'linear-gradient(180deg, rgba(103,80,164,0.22), rgba(34,31,43,0.88))' if is_dark else 'linear-gradient(180deg, rgba(233,221,255,0.22), rgba(255,251,254,0.82))'}!important; border:1.5px dashed {'rgba(233,221,255,0.28)' if is_dark else 'rgba(103,80,164,0.24)'}!important; border-radius:22px!important; }}
    div[data-testid="stFileUploader"] button {{ background:linear-gradient(135deg,#7356b6 0%, var(--primary) 100%)!important; color:#fff!important; border:none!important; }}
    div[data-testid="stTextArea"] textarea, div[data-testid="stTextInput"] input {{ background:{surface}!important; color:var(--text)!important; border:1px solid var(--outline)!important; border-radius:20px!important; }}
    [data-testid="stExpander"] {{ border:1px solid var(--outline)!important; border-radius:22px!important; background:{surface}!important; box-shadow:var(--shadow-1)!important; }}
    .stAlert {{ border-radius:18px!important; }}
    .stProgress > div > div > div > div {{ background:linear-gradient(90deg,#8065bf, var(--primary))!important; }}
    div[data-testid="stImage"] img {{ border-radius:18px; border:1px solid var(--outline); box-shadow:var(--shadow-1); }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def render_theme_menu() -> None:
    options = ["Hell", "Dunkel"]
    current_index = 0 if st.session_state.get("theme_mode", "dark") == "light" else 1
    trigger_label = "☰"
    if hasattr(st, "popover"):
        with st.popover(trigger_label):
            selected = st.radio("Design", options, index=current_index, key="theme_mode_menu")
            new_mode = "light" if selected == "Hell" else "dark"
            if new_mode != st.session_state.get("theme_mode", "dark"):
                st.session_state.theme_mode = new_mode
                st.rerun()
    else:
        selected = st.selectbox("Design", options, index=current_index, key="theme_mode_menu_fallback")
        new_mode = "light" if selected == "Hell" else "dark"
        if new_mode != st.session_state.get("theme_mode", "dark"):
            st.session_state.theme_mode = new_mode
            st.rerun()

def render_header() -> None:
    left, right = st.columns([0.48, 0.52], vertical_alignment="center")
    with left:
        st.markdown("<div class='snap-appbar'><h1>snap to mealie</h1></div>", unsafe_allow_html=True)
    with right:
        a, b = st.columns([0.9, 0.1], vertical_alignment="center")
        with a:
            st.markdown(f"<div class='snap-badges'><span class='snap-badge'>{html.escape(settings.gemini_model)}</span></div>", unsafe_allow_html=True)
        with b:
            render_theme_menu()

def ui_card(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"<div class='snap-card'><h3>{html.escape(title)}</h3>{f'<p>{html.escape(subtitle)}</p>' if subtitle else ''}</div>",
        unsafe_allow_html=True,
    )

def render_lottie_loading(height: int = 120) -> None:
    components.html(
        dedent(
            f"""
            <script src="[https://unpkg.com/@lottiefiles/lottie-player@latest/dist/lottie-player.js](https://unpkg.com/@lottiefiles/lottie-player@latest/dist/lottie-player.js)"></script>
            <div style="display:flex;justify-content:center;align-items:center;height:{height}px;">
              <lottie-player src="{LOTTIE_URL}" background="transparent" speed="1" style="width: {height}px; height: {height}px;" loop autoplay></lottie-player>
            </div>
            """
        ),
        height=height,
    )


# -----------------------------------------------------------------------------
# DB/cache / Thread-Local SQLite WAL Proxy
# -----------------------------------------------------------------------------
@st.cache_resource
def get_db_lock() -> threading.Lock:
    return threading.Lock()

# Thread-Local Connection Pool to avoid repeatedly reopening SQLite WAL
_db_local = threading.local()

@contextmanager
def db_conn():
    if not hasattr(_db_local, "sqlite_conn"):
        os.makedirs(os.path.dirname(settings.snap_cache_db), exist_ok=True)
        conn = sqlite3.connect(settings.snap_cache_db, check_same_thread=False, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-10000;") # 10MB Cache
        _db_local.sqlite_conn = conn
    try:
        yield _db_local.sqlite_conn
    except Exception:
        raise

def ensure_uploads_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS uploads ("
        "user_key TEXT NOT NULL, "
        "recipe_slug TEXT NOT NULL, "
        "recipe_name TEXT NOT NULL, "
        "user_label TEXT, "
        "user_email TEXT, "
        "source TEXT NOT NULL, "
        "first_uploaded_at INTEGER NOT NULL, "
        "last_uploaded_at INTEGER NOT NULL, "
        "upload_count INTEGER NOT NULL DEFAULT 1, "
        "PRIMARY KEY(user_key, recipe_slug)"
        ")"
    )

def ensure_image_prompts_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS image_prompts ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "name TEXT NOT NULL, "
        "prompt_text TEXT NOT NULL, "
        "is_default INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    # Safe static ALTER statement to upgrade older databases
    try:
        conn.execute("ALTER TABLE image_prompts ADD COLUMN user_label TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass

def ensure_editor_queue_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS editor_queue ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "user_key TEXT NOT NULL, "
        "recipe_name TEXT NOT NULL, "
        "recipe_data TEXT NOT NULL, "
        "cover_image BLOB, "
        "created_at INTEGER NOT NULL"
        ")"
    )

def init_cache_db() -> None:
    with get_db_lock():
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("CREATE TABLE IF NOT EXISTS mappings (endpoint TEXT NOT NULL, name TEXT NOT NULL, item_id TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(endpoint, name))")
            cur.execute("CREATE TABLE IF NOT EXISTS recipes (norm_name PRIMARY KEY, recipe_name TEXT NOT NULL, slug TEXT NOT NULL, updated_at INTEGER NOT NULL)")
            ensure_uploads_table(conn)
            ensure_image_prompts_table(conn)
            ensure_editor_queue_table(conn)
            
            try:
                conn.execute("DELETE FROM image_prompts WHERE prompt_text LIKE 'http://%' OR prompt_text LIKE 'https://%'")
            except Exception:
                pass
                
            conn.commit()
            logger.debug("sqlite_initialized")

def get_image_prompts(user_label: str = "") -> List[Dict[str, Any]]:
    with get_db_lock():
        with db_conn() as conn:
            ensure_image_prompts_table(conn)
            if user_label:
                rows = conn.execute("SELECT id, name, prompt_text, is_default, user_label FROM image_prompts WHERE user_label = ? OR user_label = '' OR (user_label LIKE '%Lars Graf%' AND is_default = 1) ORDER BY name", (user_label,)).fetchall()
            else:
                rows = conn.execute("SELECT id, name, prompt_text, is_default, user_label FROM image_prompts ORDER BY name").fetchall()
    return [{"id": r[0], "name": r[1], "text": r[2], "is_default": bool(r[3]), "user_label": r[4] if len(r) > 4 else ""} for r in rows]

def save_image_prompt(name: str, text: str, user_label: str = "", is_default: bool = False) -> None:
    with get_db_lock():
        with db_conn() as conn:
            ensure_image_prompts_table(conn)
            if is_default:
                conn.execute("UPDATE image_prompts SET is_default = 0 WHERE user_label = ?", (user_label,))
            conn.execute("INSERT INTO image_prompts (name, prompt_text, is_default, user_label) VALUES (?, ?, ?, ?)", (name, text, int(is_default), user_label))
            conn.commit()

def set_default_image_prompt(prompt_id: int, user_label: str = "") -> None:
    with get_db_lock():
        with db_conn() as conn:
            ensure_image_prompts_table(conn)
            conn.execute("UPDATE image_prompts SET is_default = 0 WHERE user_label = ?", (user_label,))
            conn.execute("UPDATE image_prompts SET is_default = 1 WHERE id = ?", (prompt_id,))
            conn.commit()

def delete_image_prompt(prompt_id: int) -> None:
    with get_db_lock():
        with db_conn() as conn:
            ensure_image_prompts_table(conn)
            conn.execute("DELETE FROM image_prompts WHERE id = ?", (prompt_id,))
            conn.commit()


# --- Editor Queue Helpers ---
def add_to_editor_queue(user_key: str, recipe_data: Dict[str, Any], cover_image: Optional[bytes] = None) -> None:
    name = clean_str(recipe_data.get("name", "Unbenanntes Rezept"))
    data_json = json.dumps(recipe_data, ensure_ascii=False)
    with get_db_lock(), db_conn() as conn:
        ensure_editor_queue_table(conn)
        conn.execute(
            "INSERT INTO editor_queue (user_key, recipe_name, recipe_data, cover_image, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_key, name, data_json, cover_image, int(time.time()))
        )
        conn.commit()
    logger.info("editor_queue_added", recipe_name=name, user_key=user_key)

def get_editor_queue(user_key: str) -> List[Dict[str, Any]]:
    with get_db_lock(), db_conn() as conn:
        ensure_editor_queue_table(conn)
        rows = conn.execute("SELECT id, recipe_name, recipe_data, cover_image, created_at FROM editor_queue WHERE user_key = ? ORDER BY created_at ASC", (user_key,)).fetchall()
    return [{"id": r[0], "recipe_name": r[1], "recipe_data": json.loads(r[2]), "cover_image": r[3], "created_at": r[4]} for r in rows]

def delete_from_editor_queue(item_id: int) -> None:
    with get_db_lock(), db_conn() as conn:
        ensure_editor_queue_table(conn)
        conn.execute("DELETE FROM editor_queue WHERE id = ?", (item_id,))
        conn.commit()


def db_find_recipe_slug(recipe_name: str) -> Optional[str]:
    key = normalize_name(recipe_name)
    if not key:
        return None
    with get_db_lock():
        with db_conn() as conn:
            row = conn.execute("SELECT slug FROM recipes WHERE norm_name = ?", (key,)).fetchone()
    return row[0] if row else None

def db_delete_recipe(recipe_name: str) -> None:
    key = normalize_name(recipe_name)
    if not key:
        return
    with get_db_lock():
        with db_conn() as conn:
            conn.execute("DELETE FROM recipes WHERE norm_name = ?", (key,))
            conn.commit()

def db_delete_recipe_by_slug(slug: str) -> None:
    slug = clean_str(slug)
    if not slug:
        return
    with get_db_lock():
        with db_conn() as conn:
            conn.execute("DELETE FROM recipes WHERE slug = ?", (slug,))
            conn.commit()

def db_store_recipes(recipes: List[Dict[str, str]]) -> None:
    now = int(time.time())
    rows = []
    for r in recipes:
        name = clean_str(r.get("name"))
        slug = clean_str(r.get("slug"))
        if name and slug:
            rows.append((normalize_name(name), name, slug, now))
    if not rows:
        return
    with get_db_lock():
        with db_conn() as conn:
            conn.executemany(
                "INSERT INTO recipes(norm_name, recipe_name, slug, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(norm_name) DO UPDATE SET recipe_name=excluded.recipe_name, slug=excluded.slug, updated_at=excluded.updated_at",
                rows,
            )
            conn.commit()

def db_get_mapping(endpoint: str, name: str) -> Optional[str]:
    norm = normalize_name(name)
    if not norm:
        return None
    with get_db_lock():
        with db_conn() as conn:
            row = conn.execute("SELECT item_id FROM mappings WHERE endpoint = ? AND name = ?", (endpoint, norm)).fetchone()
    return row[0] if row else None

def db_set_mapping(endpoint: str, name: str, item_id: str) -> None:
    norm = normalize_name(name)
    item_id = clean_str(item_id)
    if not norm or not item_id:
        return
    with get_db_lock():
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO mappings(endpoint, name, item_id, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(endpoint, name) DO UPDATE SET item_id=excluded.item_id, updated_at=excluded.updated_at",
                (endpoint, norm, item_id, int(time.time())),
            )
            conn.commit()

def db_bulk_replace_mappings(endpoint: str, mapping: Dict[str, str]) -> None:
    rows = [(endpoint, normalize_name(k), clean_str(v), int(time.time())) for k, v in mapping.items() if normalize_name(k) and clean_str(v)]
    if not rows:
        return
    with get_db_lock():
        with db_conn() as conn:
            conn.executemany(
                "INSERT INTO mappings(endpoint, name, item_id, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(endpoint, name) DO UPDATE SET item_id=excluded.item_id, updated_at=excluded.updated_at",
                rows,
            )
            conn.commit()


# -----------------------------------------------------------------------------
# Shared runtime resources / Optimized Request Pooling
# -----------------------------------------------------------------------------
@st.cache_resource
def get_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

class RateLimiter:
    def __init__(self, rpm: int):
        self.interval = max(0.01, 60.0 / max(1, rpm))
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            target = max(now, self.next_allowed)
            sleep_for = max(0.0, target - now)
            self.next_allowed = target + self.interval
        if sleep_for > 0:
            time.sleep(sleep_for)

@st.cache_resource
def get_gemini_rate_limiter() -> RateLimiter:
    return RateLimiter(settings.gemini_rpm)

@st.cache_resource
def get_task_registry() -> Dict[str, Dict[str, Any]]:
    return {}

@st.cache_resource
def get_active_user_registry() -> Dict[str, Dict[str, Any]]:
    return {}

@st.cache_resource
def get_task_lock() -> threading.Lock:
    return threading.Lock()

@st.cache_resource
def get_entity_lock() -> threading.Lock:
    return threading.Lock()


# -----------------------------------------------------------------------------
# Auth / user helpers
# -----------------------------------------------------------------------------
def has_streamlit_auth() -> bool:
    return all(hasattr(st, attr) for attr in ["login", "logout"]) and hasattr(st, "user")

def is_streamlit_user_logged_in() -> bool:
    user_obj = getattr(st, "user", None)
    if user_obj is None:
        return True
    return bool(getattr(user_obj, "is_logged_in", False))

def get_current_user_email() -> str:
    user_obj = getattr(st, "user", None)
    return clean_str(getattr(user_obj, "email", ""))

def get_current_user_label() -> str:
    user_obj = getattr(st, "user", None)
    for attr in ["name", "preferred_username", "email", "id"]:
        value = clean_str(getattr(user_obj, attr, ""))
        if value:
            return value
    return "Anonym"

def get_current_user_key() -> str:
    email = get_current_user_email().lower()
    if email:
        return email
    label = get_current_user_label().lower()
    return label or "anonym"

def is_admin_user() -> bool:
    if not ADMIN_USERS_LIST:
        return True
    email = get_current_user_email().lower()
    label = get_current_user_label().lower()
    return email in ADMIN_USERS_LIST or label in ADMIN_USERS_LIST

def register_active_user() -> None:
    if "_snap_session_id" not in st.session_state:
        st.session_state._snap_session_id = str(uuid.uuid4())
    get_active_user_registry()[st.session_state._snap_session_id] = {
        "label": get_current_user_label(),
        "email": get_current_user_email(),
        "last_seen": time.time(),
    }

def cleanup_inactive_users() -> None:
    cutoff = time.time() - settings.active_user_ttl_sec
    registry = get_active_user_registry()
    for sid, entry in list(registry.items()):
        if entry.get("last_seen", 0) < cutoff:
            del registry[sid]

def get_active_users_snapshot() -> List[Dict[str, Any]]:
    cleanup_inactive_users()
    deduped: Dict[str, Dict[str, Any]] = {}
    for user in get_active_user_registry().values():
        key = (clean_str(user.get("email")) or clean_str(user.get("label")) or "anonym").lower()
        if key not in deduped or user.get("last_seen", 0) > deduped[key].get("last_seen", 0):
            deduped[key] = user
    users = list(deduped.values())
    users.sort(key=lambda x: (x.get("label") or "", x.get("email") or ""))
    return users

def record_recipe_upload(user_key: str, recipe_slug: str, recipe_name: str, user_label: str, user_email: str, source: str = "snap_to_mealie") -> None:
    user_key = clean_str(user_key).lower()
    recipe_slug = clean_str(recipe_slug)
    recipe_name = clean_str(recipe_name)
    if not user_key or not recipe_slug or not recipe_name:
        return
    now = int(time.time())
    with get_db_lock():
        with db_conn() as conn:
            ensure_uploads_table(conn)
            conn.execute(
                "INSERT INTO uploads(user_key, recipe_slug, recipe_name, user_label, user_email, source, first_uploaded_at, last_uploaded_at, upload_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1) "
                "ON CONFLICT(user_key, recipe_slug) DO UPDATE SET "
                "recipe_name=excluded.recipe_name, user_label=excluded.user_label, user_email=excluded.user_email, "
                "source=excluded.source, last_uploaded_at=excluded.last_uploaded_at, upload_count=uploads.upload_count + 1",
                (user_key, recipe_slug, recipe_name, clean_str(user_label), clean_str(user_email), source, now, now),
            )
            conn.commit()

def get_user_uploaded_recipe_rows(user_key: str) -> List[Dict[str, Any]]:
    user_key = clean_str(user_key).lower()
    if not user_key:
        return []
    with get_db_lock():
        with db_conn() as conn:
            ensure_uploads_table(conn)
            rows = conn.execute(
                "SELECT recipe_slug, recipe_name, user_label, user_email, first_uploaded_at, last_uploaded_at, upload_count "
                "FROM uploads WHERE user_key = ? AND source = 'snap_to_mealie' ORDER BY last_uploaded_at DESC",
                (user_key,),
            ).fetchall()
    return [
        {
            "recipe_slug": row[0],
            "recipe_name": row[1],
            "user_label": row[2],
            "user_email": row[3],
            "first_uploaded_at": row[4],
            "last_uploaded_at": row[5],
            "upload_count": row[6],
        }
        for row in rows
    ]

def get_all_uploaded_recipe_rows(limit: int = 100) -> List[Dict[str, Any]]:
    """Gibt die historisch neuesten Uploads ALLER Nutzer zurück (für Admins)."""
    with get_db_lock():
        with db_conn() as conn:
            ensure_uploads_table(conn)
            rows = conn.execute(
                "SELECT recipe_slug, recipe_name, user_label, user_email, first_uploaded_at, last_uploaded_at, upload_count "
                "FROM uploads WHERE source = 'snap_to_mealie' ORDER BY last_uploaded_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [
        {
            "recipe_slug": row[0],
            "recipe_name": row[1],
            "user_label": row[2] or "Anonym",
            "user_email": row[3],
            "first_uploaded_at": row[4],
            "last_uploaded_at": row[5],
            "upload_count": row[6],
        }
        for row in rows
    ]


# -----------------------------------------------------------------------------
# API helpers
# -----------------------------------------------------------------------------
def get_auth_headers(api_key: str, json_content: bool = True) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {api_key}"}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers

def safe_mealie_request(method: str, url: str, headers: Dict[str, str], **kwargs) -> requests.Response:
    session = get_http_session()
    last_exc = None
    for attempt in range(4):
        try:
            resp = session.request(method, url, headers=headers, timeout=settings.request_timeout, **kwargs)
            if resp.status_code in (429, 503):
                retry_after = resp.headers.get("Retry-After")
                sleep_time = float(retry_after) if retry_after and retry_after.isdigit() else (2 + attempt * 3)
                time.sleep(sleep_time)
                continue
            if resp.status_code < 500:
                return resp
        except requests.exceptions.RequestException as exc:
            last_exc = exc
        time.sleep(2 + attempt * 3)
    if last_exc:
        raise last_exc
    return session.request(method, url, headers=headers, timeout=settings.request_timeout, **kwargs)


# -----------------------------------------------------------------------------
# Mealie caches / duplicate fallback
# -----------------------------------------------------------------------------
@st.cache_data(ttl=settings.recipe_cache_ttl)
def get_mealie_recipes(api_url: str, api_key: str) -> List[Dict[str, str]]:
    recipes: List[Dict[str, str]] = []
    if not api_url or not api_key:
        return recipes
    try:
        resp = safe_mealie_request("GET", f"{api_url}/api/recipes?page=1&perPage=2000", headers=get_auth_headers(api_key, json_content=False))
        if resp.status_code == 200:
            payload = resp.json()
            items = payload.get("items", []) if isinstance(payload, dict) else payload
            for item in items:
                if isinstance(item, dict) and item.get("name") and item.get("slug"):
                    recipes.append({"name": item["name"], "slug": item["slug"]})
            db_store_recipes(recipes)
    except Exception:
        pass
    return sorted(recipes, key=lambda x: x["name"].lower())

@st.cache_data(ttl=3600)
def get_mealie_user_id_by_email(api_url: str, api_key: str, email: str) -> Optional[str]:
    """Sucht die Mealie userId basierend auf der E-Mail-Adresse."""
    if not email: return None
    try:
        resp = safe_mealie_request("GET", f"{api_url}/api/users?orderByNullPosition=first&orderDirection=desc&page=1&perPage=200", headers=get_auth_headers(api_key, False))
        if resp.status_code == 200:
            data = resp.json()
            users = data.get("items", []) if isinstance(data, dict) else data
            for u in users:
                u_email = u.get("email")
                if u_email and u_email.lower() == email.lower():
                    logger.debug("mealie_user_id_found", email=email, id=u.get('id'))
                    return u.get("id")
    except Exception as e: 
        logger.error("error_querying_mealie_user", error=str(e))
    return None


@st.cache_data(ttl=settings.map_cache_ttl)
def get_mealie_data_maps(api_url: str, api_key: str) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]]:
    foods_map: Dict[str, str] = {}
    units_map: Dict[str, str] = {}
    tags_map: Dict[str, str] = {}
    cats_map: Dict[str, str] = {}
    tools_map: Dict[str, str] = {}
    if not api_url or not api_key:
        return foods_map, units_map, tags_map, cats_map, tools_map
    headers = get_auth_headers(api_key, json_content=False)

    def fetch(endpoint: str, target: Dict[str, str]) -> None:
        try:
            resp = safe_mealie_request("GET", f"{api_url}/api/{endpoint}?page=1&perPage=-1", headers=headers)
            if resp.status_code == 200:
                payload = resp.json()
                items = payload.get("items", []) if isinstance(payload, dict) else payload
                for item in items:
                    if isinstance(item, dict) and item.get("name") and item.get("id"):
                        target[normalize_name(item["name"])] = item["id"]
                db_bulk_replace_mappings(endpoint, target)
        except Exception:
            pass

    fetch("foods", foods_map)
    fetch("units", units_map)
    fetch("organizers/tags", tags_map)
    fetch("organizers/categories", cats_map)
    
    for ep in ["api/tools", "api/organizers/tools", "api/groups/tools"]:
        try:
            resp = safe_mealie_request("GET", f"{api_url}/{ep}?page=1&perPage=-1", headers=headers)
            if resp.status_code == 200:
                payload = resp.json()
                items = payload.get("items", []) if isinstance(payload, dict) else payload
                for item in items:
                    if isinstance(item, dict) and item.get("name") and item.get("id"):
                        tools_map[normalize_name(item["name"])] = item["id"]
                db_bulk_replace_mappings("tools", tools_map)
                break
        except Exception:
            pass

    return foods_map, units_map, tags_map, cats_map, tools_map

def get_recipe_by_slug(api_url: str, api_key: str, slug: str) -> Optional[Dict[str, Any]]:
    slug = clean_str(slug)
    if not slug:
        return None
    resp = safe_mealie_request("GET", f"{api_url}/api/recipes/{slug}", headers=get_auth_headers(api_key, json_content=False))
    if resp.status_code == 200:
        try:
            return resp.json()
        except Exception:
            return None
    return None

def search_recipe_slug_by_name(api_url: str, api_key: str, recipe_name: str) -> Optional[str]:
    recipe_name_norm = normalize_name(recipe_name)
    for recipe in get_mealie_recipes(api_url, api_key):
        if normalize_name(recipe.get("name", "")) == recipe_name_norm:
            return recipe.get("slug")
    return None

def find_duplicate_recipe_slug(api_url: str, api_key: str, recipe_name: str) -> Optional[str]:
    recipe_name = clean_str(recipe_name)
    if not recipe_name:
        return None
    db_slug = db_find_recipe_slug(recipe_name)
    if db_slug:
        if get_recipe_by_slug(api_url, api_key, db_slug):
            return db_slug
        db_delete_recipe(recipe_name)
        db_delete_recipe_by_slug(db_slug)
    slug_target = slugify(recipe_name)
    for recipe in get_mealie_recipes(api_url, api_key):
        name_existing = recipe.get("name", "")
        slug_existing = recipe.get("slug", "")
        if normalize_name(name_existing) == normalize_name(recipe_name):
            return slug_existing
        if slugify(name_existing) == slug_target:
            return slug_existing
    return None

def get_or_create(endpoint: str, name: str, api_url: str, headers: Dict[str, str], data_map: Dict[str, str]) -> Optional[str]:
    norm_name = normalize_name(name)
    if not norm_name:
        return None
    with get_entity_lock():
        if norm_name in data_map:
            return data_map[norm_name]
        cached_id = db_get_mapping(endpoint, norm_name)
        if cached_id:
            data_map[norm_name] = cached_id
            return cached_id
        resp = safe_mealie_request("POST", f"{api_url}/api/{endpoint}", headers=headers, json={"name": clean_str(name)})
        if resp.status_code in (200, 201):
            new_id = resp.json().get("id")
            if new_id:
                data_map[norm_name] = new_id
                db_set_mapping(endpoint, norm_name, new_id)
                return new_id
    return None

def get_or_create_tool_robust(name: str, api_url: str, headers: Dict[str, str], data_map: Dict[str, str]) -> Optional[str]:
    norm_name = normalize_name(name)
    if not norm_name:
        return None
    with get_entity_lock():
        if norm_name in data_map:
            return data_map[norm_name]
        cached_id = db_get_mapping("tools", norm_name)
        if cached_id:
            data_map[norm_name] = cached_id
            return cached_id
            
        endpoints = ["api/tools", "api/organizers/tools", "api/groups/tools"]
        for ep in endpoints:
            resp = safe_mealie_request("POST", f"{api_url}/{ep}", headers=headers, json={"name": clean_str(name)})
            if resp.status_code in (200, 201):
                new_id = resp.json().get("id")
                if new_id:
                    data_map[norm_name] = new_id
                    db_set_mapping("tools", norm_name, new_id)
                    return new_id
    return None


# -----------------------------------------------------------------------------
# Video download helpers
# -----------------------------------------------------------------------------
def download_recipe_video(url: str) -> Dict[str, Any]:
    if not VIDEO_IMPORT_AVAILABLE or not yt_dlp:
        raise RuntimeError("yt_dlp ist nicht installiert oder verfügbar.")
        
    temp_dir = tempfile.mkdtemp()
    outtmpl = os.path.join(temp_dir, "video.%(ext)s")
    
    ydl_opts = {
        "format": "best[ext=mp4][height<=720]/best[ext=mp4]/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
        "writethumbnail": True,
        "get_comments": True,
        "max_comments": 40,
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        video_path = ydl.prepare_filename(info)
        downloaded_files = os.listdir(temp_dir)
        vid_file = next((f for f in downloaded_files if f.endswith(('.mp4', '.webm', '.mkv'))), None)
        if vid_file:
            video_path = os.path.join(temp_dir, vid_file)
            
        thumbnail_bytes = None
        if info.get("thumbnail"):
            try:
                resp = get_http_session().get(info["thumbnail"], timeout=5)
                if resp.status_code == 200:
                    thumbnail_bytes = resp.content
            except Exception:
                pass
        
        comments_list = info.get('comments', [])
        comment_text = "\n".join([f"Kommentar: {c.get('text', '')}" for c in comments_list if c.get('text')])
        
        recipe_text = (
            f"Titel: {info.get('title', '')}\n"
            f"Beschreibung: {info.get('description', '')}\n\n"
            f"--- Kommentare ---\n"
            f"{comment_text}"
        )
        
        logger.info("video_downloaded", title=info.get('title'))
        return {
            "video_path": video_path,
            "recipe_text": recipe_text,
            "thumbnail_bytes": thumbnail_bytes,
            "temp_dir": temp_dir
        }

def cleanup_video_bundle(bundle: Dict[str, Any]) -> None:
    temp_dir = bundle.get("temp_dir")
    if temp_dir and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)


# -----------------------------------------------------------------------------
# Content extraction
# -----------------------------------------------------------------------------
def strip_html(html_text: str) -> str:
    text = re.sub(r"<style.*?</style>", " ", html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script(?![^>]*application/ld\+json).*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<noscript.*?</noscript>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<header.*?</header>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<footer.*?</footer>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<nav.*?</nav>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()

def extract_recipe_jsonld_text(html_text: str) -> str:
    scripts = re.findall(r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", html_text, flags=re.DOTALL | re.IGNORECASE)
    parts: List[str] = []
    def flatten(node: Any) -> List[Dict[str, Any]]:
        out = []
        if isinstance(node, dict):
            if isinstance(node.get("@graph"), list):
                for item in node["@graph"]:
                    out.extend(flatten(item))
            else:
                out.append(node)
        elif isinstance(node, list):
            for item in node:
                out.extend(flatten(item))
        return out
    for raw in scripts:
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for item in flatten(data):
            types = item.get("@type") if isinstance(item.get("@type"), list) else [item.get("@type")]
            if not any(str(t).lower() == "recipe" for t in types if t):
                continue
            if item.get("name"):
                parts.append(f"Titel: {item['name']}")
            if item.get("description"):
                parts.append(f"Beschreibung: {item['description']}")
            if item.get("recipeYield"):
                parts.append(f"Portionen: {item['recipeYield']}")
            ingredients = item.get("recipeIngredient") or []
            if ingredients:
                parts.append("Zutaten:")
                parts.extend([f"- {clean_str(x)}" for x in ingredients if clean_str(x)])
            steps = item.get("recipeInstructions") or []
            if steps:
                parts.append("Zubereitung:")
                idx = 1
                for step in steps:
                    text = clean_str(step if isinstance(step, str) else step.get("text") or step.get("name"))
                    if text:
                        parts.append(f"{idx}. {text}")
                        idx += 1
    return "\n".join(parts).strip()

def fetch_url_text_and_image(url: str) -> Tuple[str, Optional[bytes]]:
    headers = {"User-Agent": "Mozilla/5.0"}
    session = get_http_session()
    resp = session.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    html_text = resp.text
    cover_img_bytes = None
    image_match = (
        re.search(r'<meta\s+(?:property|name)=[\"\']og:image[\"\']\s+content=[\"\']([^\"\']+)[\"\']', html_text, re.IGNORECASE)
        or re.search(r'<meta\s+(?:property|name)=[\"\']twitter:image[\"\']\s+content=[\"\']([^\"\']+)[\"\']', html_text, re.IGNORECASE)
    )
    if image_match:
        img_url = urljoin(url, image_match.group(1))
        try:
            img_resp = session.get(img_url, headers=headers, timeout=8, stream=True)
            if img_resp.status_code == 200:
                content_length = int(img_resp.headers.get("content-length", 0) or 0)
                if content_length == 0 or content_length <= settings.max_cover_image_bytes:
                    chunks = []
                    total = 0
                    for chunk in img_resp.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        total += len(chunk)
                        if total > settings.max_cover_image_bytes:
                            chunks = []
                            break
                        chunks.append(chunk)
                    if chunks:
                        cover_img_bytes = b"".join(chunks)
        except Exception:
            pass
            
    text = extract_recipe_jsonld_text(html_text)
    
    if TRAFILATURA_AVAILABLE:
        try:
            extracted_main = trafilatura.extract(html_text, include_comments=False, include_tables=True)
            if extracted_main:
                text += "\n\n" + extracted_main
            else:
                text += "\n\n" + strip_html(html_text)
        except Exception:
            text += "\n\n" + strip_html(html_text)
    else:
        text += "\n\n" + strip_html(html_text)
        
    logger.debug("url_extraction_finished", url=url)
    return text.strip()[:50000], cover_img_bytes

def load_image(img_bytes: bytes) -> Image.Image:
    img = Image.open(io.BytesIO(img_bytes))
    img = ImageOps.exif_transpose(img)
    resample = getattr(Image, "Resampling", Image).LANCZOS
    img.thumbnail((1600, 1600), resample)
    return img

def image_to_jpeg_bytes(img: Image.Image, quality: int = 90) -> bytes:
    temp = img.convert("RGB") if img.mode != "RGB" else img
    buf = io.BytesIO()
    temp.save(buf, format="JPEG", quality=quality)
    if temp is not img:
        safe_close_image(temp)
    return buf.getvalue()

def fetch_mealie_recipe_text(slug: str, api_url: str, api_key: str) -> str:
    recipe = get_recipe_by_slug(api_url, api_key, slug)
    if not recipe:
        raise RuntimeError(f"Rezept {slug} konnte nicht geladen werden.")
    
    ings = recipe.get("recipeIngredient", [])
    is_parsed = False
    if ings:
        is_parsed = any(clean_str(get_nested_name(ing.get("food", {}))) or clean_str(ing.get("originalText")) for ing in ings)

    if is_parsed:
        clean_recipe = {
            "name": recipe.get("name"),
            "description": recipe.get("description"),
            "recipeYield": recipe.get("recipeYield"),
            "prepTime": recipe.get("prepTime"),
            "cookTime": recipe.get("performTime"),
            "tags": [{"name": clean_str(t.get("name"))} for t in recipe.get("tags", []) if clean_str(t.get("name"))],
            "recipeCategory": [{"name": clean_str(c.get("name"))} for c in recipe.get("recipeCategory", []) if clean_str(c.get("name"))],
            "tools": [{"name": clean_str(t.get("name"))} for t in recipe.get("tools", []) if clean_str(t.get("name"))],
            "nutrition": recipe.get("nutrition", {}),
            "recipeIngredient": [],
            "recipeInstructions": []
        }
        
        for ing in ings:
            clean_recipe["recipeIngredient"].append({
                "referenceId": clean_str(ing.get("referenceId")),
                "originalText": clean_str(ing.get("originalText")),
                "title": clean_str(ing.get("title")),
                "note": clean_str(ing.get("note")),
                "quantity": safe_float(ing.get("quantity")),
                "unit": {"name": clean_str(ing.get("unit", {}).get("name"))} if ing.get("unit") else None,
                "food": {"name": clean_str(ing.get("food", {}).get("name"))} if ing.get("food") else None,
            })
            
        for step in recipe.get("recipeInstructions", []):
            clean_recipe["recipeInstructions"].append({
                "title": clean_str(step.get("title")),
                "text": clean_str(step.get("text")),
                "ingredientReferences": step.get("ingredientReferences", [])
            })
            
        return "WICHTIGE ANWEISUNG: Dies ist ein bereits geparstes Rezept als JSON. Es herrscht ein striktes ÄNDERUNGSVERBOT für das Array 'recipeIngredient'. Du musst alle Zutaten, 'originalText' und 'food' 1:1 kopieren! Verändere niemals Zutaten (wie Zucchini zu Gemüse) aufgrund der Zubereitungsschritte!\n\n" + json.dumps(clean_recipe, ensure_ascii=False)
    else:
        raw_parts = [f"Titel: {recipe.get('name', '')}", f"Beschreibung: {recipe.get('description', '')}"]
        if ings:
            raw_parts.append("\nZUTATEN (STRIKTES ÄNDERUNGSVERBOT: Übernimm diese Zutaten exakt so, wie sie hier stehen. Verändere oder kombiniere sie nicht anhand der Zubereitungsschritte!):")
            for ing in ings:
                text_val = clean_str(ing.get("originalText")) or clean_str(ing.get("note")) or clean_str(ing.get("display"))
                if text_val:
                    raw_parts.append(f"- {text_val}")
        insts = recipe.get("recipeInstructions", [])
        if insts:
            raw_parts.append("\nZubereitung / Rohtext:")
            for step in insts:
                raw_parts.append(clean_str(step.get("text", "")))
        return "\n".join(raw_parts)

def run_ocr_on_images(images: List[Image.Image]) -> str:
    if not OCR_AVAILABLE:
        return ""
    texts = []
    for img in images:
        try:
            gray = ImageOps.autocontrast(ImageOps.grayscale(img))
            text = pytesseract.image_to_string(gray)
            if clean_str(text):
                texts.append(clean_str(text))
            safe_close_image(gray)
        except Exception:
            continue
    return "\n\n".join(texts)

def infer_recipe_yield_from_text(text: str) -> str:
    raw = clean_str(text)
    if not raw:
        return ""
    patterns = [
        r"(?:ergibt|für|macht|reicht für)\s*(\d+(?:\s*[-–]\s*\d+)?)\s*(?:portionen|personen|servings?)",
        r"(?:serves|makes|yield(?:s)?|portionen|portion|personen|servings?)\s*[:\-]?\s*(\d+(?:\s*[-–]\s*\d+)?)",
        r"(?:recipeyield|yield)\s*[:\-]?\s*(\d+(?:\s*[-–]\s*\d+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return clean_str(match.group(1)).replace(" ", "")
    return ""


# -----------------------------------------------------------------------------
# Gemini (Using google-genai Pydantic Schema feature natively)
# -----------------------------------------------------------------------------
def create_genai_client(api_key: str):
    return genai.Client(api_key=api_key)

def build_model_contents(prompt: str, images: Optional[List[Image.Image]] = None, text: Optional[str] = None, ocr_text: Optional[str] = None) -> List[Any]:
    parts: List[Any] = [prompt]
    if text:
        parts.append("Hier ist extrahierter Rezepttext. Ignoriere Rauschen und liefere nur das Rezept als JSON.\n\n" + text)
    if ocr_text:
        parts.append("Zusätzlicher OCR-Fallback-Text. Nur ergänzend verwenden.\n\n" + ocr_text)
    if images:
        parts.extend(images)
    return parts

def analyze_content_with_gemini(client_bundle: Any, prompt: str, images: Optional[List[Image.Image]] = None, text: Optional[str] = None) -> Dict[str, Any]:
    limiter = get_gemini_rate_limiter()
    client = client_bundle
    ocr_text = ""
    last_error_msg = ""
    logger.info("Starte Inhaltsanalyse mit Gemini.")
    
    for attempt in range(3):
        try:
            limiter.wait()
            contents = build_model_contents(prompt, images=images, text=text, ocr_text=ocr_text or None)
            
            if last_error_msg:
                contents.append(f"WARNING: Your previous response failed validation with error: '{last_error_msg}'. Please correct the format and return valid JSON.")
            
            response = client.models.generate_content(
                model=settings.gemini_model,
                contents=contents,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=Recipe,
                    temperature=0.1
                )
            )
            
            parsed_recipe = _parse_pydantic_json(Recipe, response.text)
            parsed_dict = parsed_recipe.model_dump(exclude_none=True)
            
            fallback_yield = infer_recipe_yield_from_text("\n".join([clean_str(text), clean_str(ocr_text)]))
            current_yield = clean_str(parsed_dict.get("recipeYield"))
            if fallback_yield and (not current_yield or current_yield in {"1", "1 Portion", "1 Portionen", "1 Person", "1 serving"}):
                parsed_dict["recipeYield"] = fallback_yield
                
            if not parsed_dict.get("recipeIngredient") and not parsed_dict.get("recipeInstructions"):
                if attempt < 1:
                    raise ValueError("Weder Zutaten noch Zubereitung auf dieser Seite gefunden.")
                    
            logger.info("gemini_analysis_success", attempt=attempt+1)
            return parsed_dict
        except Exception as exc:
            last_error_msg = str(exc)
            logger.warning("gemini_analysis_attempt_failed", attempt=attempt+1, error=str(exc))
            if images and OCR_AVAILABLE and not ocr_text:
                ocr_text = run_ocr_on_images(images)
            if attempt == 2:
                raise RuntimeError(f"Gemini-Analyse fehlgeschlagen nach 3 Versuchen: {exc}")
            time.sleep(3 + 2 * attempt)
    raise RuntimeError("Gemini-Analyse fehlgeschlagen")


def analyze_video_with_gemini(client_bundle: Any, prompt: str, video_path: str, recipe_text: str) -> Dict[str, Any]:
    client = client_bundle
    limiter = get_gemini_rate_limiter()
    uploaded_file = None
    logger.info("gemini_video_analysis_started")
    
    config = get_prompts_config()
    enhanced_prompt = prompt + config.get("video_prompt_addition", "")
    last_error_msg = ""
    
    try:
        uploaded_file = client.files.upload(file=video_path)
        while uploaded_file.state.name == "PROCESSING":
            time.sleep(2)
            uploaded_file = client.files.get(name=uploaded_file.name)
        
        if uploaded_file.state.name == "FAILED":
            raise RuntimeError("Video processing failed.")
            
        for attempt in range(3):
            try:
                limiter.wait()
                contents = [uploaded_file, enhanced_prompt]
                if recipe_text:
                    contents.append("Zusätzliche Infos aus dem Video (Titel, Beschreibung, Kommentare):\n" + recipe_text)
                if last_error_msg:
                    contents.append(f"WARNING: Your previous response failed validation with error: '{last_error_msg}'. Please correct the format.")

                response = client.models.generate_content(
                    model=settings.gemini_model,
                    contents=contents,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=Recipe,
                        temperature=0.1
                    )
                )
                
                parsed_recipe = _parse_pydantic_json(Recipe, response.text)
                return parsed_recipe.model_dump(exclude_none=True)
            except Exception as exc:
                last_error_msg = str(exc)
                if attempt == 2:
                    raise exc
                time.sleep(3 + 2 * attempt)
    finally:
        if uploaded_file:
            try:
                client.files.delete(name=uploaded_file.name)
            except Exception:
                pass
    raise RuntimeError("Gemini-Videoanalyse fehlgeschlagen")

def analyze_pdf_with_gemini(client_bundle: Any, prompt: str, pdf_path: str) -> List[Dict[str, Any]]:
    client = client_bundle
    limiter = get_gemini_rate_limiter()
    uploaded_file = None
    logger.info("gemini_pdf_analysis_started")
    
    last_error_msg = ""
    try:
        uploaded_file = client.files.upload(file=pdf_path)
        while uploaded_file.state.name == "PROCESSING":
            time.sleep(2)
            uploaded_file = client.files.get(name=uploaded_file.name)
        
        if uploaded_file.state.name == "FAILED":
            raise RuntimeError("PDF processing failed.")
            
        for attempt in range(3):
            try:
                limiter.wait()
                contents = [uploaded_file, prompt]
                if last_error_msg:
                    contents.append(f"WARNING: Your previous response failed validation with error: '{last_error_msg}'. Please correct the format.")

                response = client.models.generate_content(
                    model=settings.gemini_model,
                    contents=contents,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=MultiRecipeResponse,
                        temperature=0.1
                    )
                )
                
                parsed_multi = _parse_pydantic_json(MultiRecipeResponse, response.text)
                return [r.model_dump(exclude_none=True) for r in parsed_multi.recipes]
            except Exception as exc:
                last_error_msg = str(exc)
                if attempt == 2:
                    raise exc
                time.sleep(3 + 2 * attempt)
    finally:
        if uploaded_file:
            try:
                client.files.delete(name=uploaded_file.name)
            except Exception:
                pass
    return []

def editor_transform_recipe(client_bundle: Any, current_recipe: Dict[str, Any], instruction: str) -> Tuple[Dict[str, Any], str]:
    config = get_prompts_config()
    prompt = config.get("editor_prompt", "")
    text = "Aktuelles Rezept-JSON:\n" + json.dumps(current_recipe, ensure_ascii=False) + "\n\nNutzeranweisung:\n" + instruction
    
    client = client_bundle
    limiter = get_gemini_rate_limiter()
    last_error_msg = ""
    
    for attempt in range(3):
        try:
            limiter.wait()
            contents = [prompt, text]
            if last_error_msg:
                contents.append(f"WARNING: Your previous response failed validation with error: '{last_error_msg}'. Please correct the format.")

            response = client.models.generate_content(
                model=settings.gemini_model, 
                contents=contents, 
                config=types.GenerateContentConfig(
                    response_mime_type="application/json", 
                    response_schema=EditorRecipeResponse,
                    temperature=0.1
                )
            )
            
            parsed_editor = _parse_pydantic_json(EditorRecipeResponse, response.text)
            return parsed_editor.recipe.model_dump(exclude_none=True), parsed_editor.explanation
            
        except Exception as exc:
            last_error_msg = str(exc)
            if attempt == 2:
                raise exc
            time.sleep(2 + attempt)
            
    return current_recipe, "Es gab einen Fehler bei der Verarbeitung."

def generate_recipe_image_with_gemini(client_bundle: Any, recipe_name: str, recipe_desc: str, image_model: str = "imagen-4.0-generate-001", custom_style: str = "") -> Optional[bytes]:
    """Generiert ein atemberaubendes Food-Bild mit Imagen."""
    safe_name = recipe_name.replace("'", "").replace('"', "")
    
    base_prompt = f"A professional food photography shot of {safe_name}. {recipe_desc} "
    base_prompt += "CRITICAL: Accurately represent the exact type of food described. Do not over-embellish. If it is a simple rustic bake, loaf, or standard sheet cake, it MUST look authentic and rustic. NEVER generate a fancy, multi-layered decorated tort/layer-cake unless explicitly described. "
    
    if custom_style.strip():
        prompt = f"{base_prompt} Style and modifiers: {custom_style.strip()}"
    else:
        prompt = f"{base_prompt} High quality, culinary magazine style, studio lighting, appetizing, perfectly plated, photorealistic."
    
    prompt += " Pure food photography, absolutely no typography, no letters, no writing, no labels, no watermarks, clean composition."
    
    url = f"[https://generativelanguage.googleapis.com/v1beta/models/](https://generativelanguage.googleapis.com/v1beta/models/){image_model}:predict?key={settings.gemini_api_key}"
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {"sampleCount": 1}
    }
    
    try:
        resp = get_http_session().post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            b64_img = data.get("predictions", [{}])[0].get("bytesBase64Encoded")
            if b64_img:
                return base64.b64decode(b64_img)
        else:
            logger.error("imagen_api_error", status_code=resp.status_code, response=resp.text)
    except Exception as e:
        logger.error("imagen_request_error", error=str(e))
    return None

def auto_generate_cover_image(client_bundle: Any, parsed_data: Dict[str, Any], current_cover: Optional[bytes] = None, owner_label: str = "") -> Optional[bytes]:
    """Prüft, ob ein Cover existiert. Falls nicht, wird per Imagen 4 eins mit dem Standard-Prompt generiert."""
    if current_cover is not None:
        return current_cover
    
    name_val = parsed_data.get("name", "Leckeres Gericht")
    desc_val = parsed_data.get("description", "")
    
    style = ""
    try:
        with get_db_lock(), db_conn() as conn:
            ensure_image_prompts_table(conn)
            
            if owner_label:
                row = conn.execute("SELECT prompt_text FROM image_prompts WHERE is_default = 1 AND user_label = ?", (owner_label,)).fetchone()
                if row:
                    style = row[0]
            
            if not style:
                row_lars = conn.execute("SELECT prompt_text FROM image_prompts WHERE is_default = 1 AND user_label LIKE '%Lars Graf%'").fetchone()
                if row_lars:
                    style = row_lars[0]
            
            if not style:
                row_any = conn.execute("SELECT prompt_text FROM image_prompts WHERE is_default = 1").fetchone()
                if row_any:
                    style = row_any[0]
                    
    except Exception as e:
        logger.error("error_loading_standard_prompts", error=str(e))
        
    return generate_recipe_image_with_gemini(client_bundle, name_val, desc_val, image_model="imagen-4.0-generate-001", custom_style=style)


# -----------------------------------------------------------------------------
# Save to Mealie
# -----------------------------------------------------------------------------
def direct_save_to_mealie(
    parsed_data: Dict[str, Any],
    api_url: str,
    api_key: str,
    cover_img_bytes: Optional[bytes] = None,
    preloaded_maps: Optional[Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]]] = None,
    target_slug: Optional[str] = None,
    org_url: str = "",
    audit_user_key: str = "",
    audit_user_label: str = "",
    audit_user_email: str = "",
    mealie_user_id: Optional[str] = None,
) -> Tuple[bool, str]:
    headers = get_auth_headers(api_key)
    foods_map, units_map, tags_map, cats_map, tools_map = preloaded_maps or get_mealie_data_maps(api_url, api_key)
    name = clean_str(parsed_data.get("name")) or "Unbenanntes Rezept"
    slug = clean_str(target_slug)

    logger.info("mealie_save_started", recipe_name=name)

    if not slug:
        duplicate_slug = find_duplicate_recipe_slug(api_url, api_key, name)
        if duplicate_slug:
            existing_recipe = get_recipe_by_slug(api_url, api_key, duplicate_slug)
            if existing_recipe:
                slug = duplicate_slug
            else:
                db_delete_recipe(name)
                db_delete_recipe_by_slug(duplicate_slug)
                slug = None
        if not slug:
            payload = {"name": name}
            if mealie_user_id:
                payload["userId"] = mealie_user_id
                
            create_resp = safe_mealie_request("POST", f"{api_url}/api/recipes", headers=headers, json=payload)
            if create_resp.status_code not in (200, 201):
                logger.error("mealie_create_failed", recipe_name=name, response=create_resp.text)
                return False, f"Fehler beim Erstellen des Rezepts: {create_resp.text}"
            create_payload = None
            try:
                create_payload = create_resp.json()
            except Exception:
                pass
            candidates: List[str] = []
            if isinstance(create_payload, str):
                candidates.append(create_payload.strip().strip('"'))
            elif isinstance(create_payload, dict):
                for key in ("slug", "recipeSlug"):
                    value = clean_str(create_payload.get(key))
                    if value:
                        candidates.append(value)
                if clean_str(create_payload.get("name")) and not candidates:
                    candidates.append(slugify(create_payload.get("name")))
            raw_text_slug = clean_str(create_resp.text.strip().strip('"'))
            if raw_text_slug:
                candidates.append(raw_text_slug)
            candidates.append(slugify(name))
            resolved = None
            for candidate in candidates:
                if get_recipe_by_slug(api_url, api_key, candidate):
                    resolved = candidate
                    break
            if not resolved:
                for _ in range(4):
                    time.sleep(0.75)
                    found_slug = search_recipe_slug_by_name(api_url, api_key, name)
                    if found_slug and get_recipe_by_slug(api_url, api_key, found_slug):
                        resolved = found_slug
                        break
            if not resolved:
                logger.error("mealie_slug_resolution_failed", recipe_name=name)
                return False, "Rezept wurde vermutlich erstellt, aber der Slug konnte nicht sicher aufgelöst werden. Bitte Rezeptliste/Cache prüfen."
            slug = resolved

    db_recipe = get_recipe_by_slug(api_url, api_key, slug)
    if not db_recipe:
        db_delete_recipe(name)
        db_delete_recipe_by_slug(slug)
        return False, f"Rezept-Slug '{slug}' konnte in Mealie nicht geladen werden."

    db_recipe["name"] = name
    db_recipe["description"] = clean_str(parsed_data.get("description"))
    if mealie_user_id:
        db_recipe["userId"] = mealie_user_id
        
    if parsed_data.get("orgURL"):
        db_recipe["orgURL"] = parsed_data.get("orgURL")
    elif org_url:
        db_recipe["orgURL"] = org_url
    servings_number = extract_servings_number(parsed_data.get("recipeYield"))
    db_recipe["recipeYield"] = str(servings_number) if servings_number is not None else clean_str(parsed_data.get("recipeYield"))
    db_recipe["recipeServings"] = servings_number if servings_number is not None else 1
    db_recipe["prepTime"] = clean_str(parsed_data.get("prepTime"))
    db_recipe["performTime"] = clean_str(parsed_data.get("cookTime"))

    nut = parsed_data.get("nutrition")
    if nut:
        db_recipe["nutrition"] = {
            "calories": nut.get("calories", ""),
            "carbohydrateContent": nut.get("carbohydrateContent", ""),
            "proteinContent": nut.get("proteinContent", ""),
            "fatContent": nut.get("fatContent", "")
        }
        
        if any(clean_str(v) for v in nut.values()):
            db_recipe["showNutrition"] = True
            if "settings" not in db_recipe or not isinstance(db_recipe["settings"], dict):
                db_recipe["settings"] = {}
            db_recipe["settings"]["showNutrition"] = True

    final_tags = []
    for tag in parsed_data.get("tags", []):
        tag_name = clean_str(tag.get("name"))
        if tag_name:
            tag_id = get_or_create("organizers/tags", tag_name, api_url, headers, tags_map)
            if tag_id:
                final_tags.append({"id": tag_id, "name": tag_name, "slug": slugify(tag_name)})
    db_recipe["tags"] = final_tags

    final_cats = []
    for cat in parsed_data.get("recipeCategory", []):
        cat_name = clean_str(cat.get("name"))
        if cat_name:
            cat_id = get_or_create("organizers/categories", cat_name, api_url, headers, cats_map)
            if cat_id:
                final_cats.append({"id": cat_id, "name": cat_name, "slug": slugify(cat_name)})
    db_recipe["recipeCategory"] = final_cats

    final_tools = []
    for tool in parsed_data.get("tools", []):
        tool_name = clean_str(tool.get("name"))
        if tool_name:
            tool_id = get_or_create_tool_robust(tool_name, api_url, headers, tools_map)
            if tool_id:
                final_tools.append({
                    "id": tool_id, 
                    "name": tool_name, 
                    "slug": slugify(tool_name)
                })
    db_recipe["tools"] = final_tools

    ref_to_uuid: Dict[str, str] = {}
    final_ings = []
    for ing_raw in parsed_data.get("recipeIngredient", []):
        food_val = clean_str(get_nested_name(ing_raw.get("food")))
        orig_val = clean_str(ing_raw.get("originalText")) or food_val
        if not orig_val and not food_val:
            continue
        real_uuid = str(uuid.uuid4())
        ref_id_val = clean_str(ing_raw.get("referenceId"))
        if ref_id_val:
            ref_to_uuid[ref_id_val] = real_uuid
        ing: Dict[str, Any] = {
            "referenceId": real_uuid,
            "originalText": orig_val,
            "note": clean_str(ing_raw.get("note")),
            "title": clean_str(ing_raw.get("title")) or None,
        }
        qty = safe_float(ing_raw.get("quantity"))
        if qty is not None:
            ing["quantity"] = qty
        unit_val = clean_str(get_nested_name(ing_raw.get("unit")))
        if unit_val:
            unit_id = get_or_create("units", unit_val, api_url, headers, units_map)
            if unit_id:
                ing["unit"] = {"id": unit_id, "name": unit_val}
        if food_val:
            food_id = get_or_create("foods", food_val, api_url, headers, foods_map)
            if food_id:
                ing["food"] = {"id": food_id, "name": food_val}
        final_ings.append(ing)
    db_recipe["recipeIngredient"] = final_ings

    final_steps = []
    for idx, step in enumerate(parsed_data.get("recipeInstructions", [])):
        text = clean_str(step.get("text"))
        if not text:
            continue
        refs = []
        for ref in step.get("ingredientReferences", []):
            ref_str = clean_str(ref.get("referenceId"))
            if ref_str and ref_str in ref_to_uuid:
                refs.append({"referenceId": ref_to_uuid[ref_str]})
        final_steps.append({"id": str(uuid.uuid4()), "title": clean_str(step.get("title")) or f"Schritt {idx+1}", "text": text, "ingredientReferences": refs})
    db_recipe["recipeInstructions"] = final_steps

    update_resp = safe_mealie_request("PUT", f"{api_url}/api/recipes/{slug}", headers=headers, json=db_recipe)
    if update_resp.status_code not in (200, 201):
        logger.error("mealie_update_failed", slug=slug, response=update_resp.text)
        return False, f"Fehler beim Aktualisieren: {update_resp.text}"

    if cover_img_bytes:
        try:
            img_headers = get_auth_headers(api_key, json_content=False)
            files = {"image": ("cover.jpg", cover_img_bytes, "image/jpeg")}
            safe_mealie_request("PUT", f"{api_url}/api/recipes/{slug}/image", headers=img_headers, files=files, data={"extension": "jpg"})
            logger.info("mealie_image_uploaded", slug=slug)
        except Exception as e:
            logger.warning("mealie_image_upload_failed", slug=slug, error=str(e))

    db_store_recipes([{"name": name, "slug": slug}])
    if audit_user_key:
        record_recipe_upload(audit_user_key, slug, name, audit_user_label, audit_user_email, source="snap_to_mealie")
        
    try:
        get_user_stats_snapshot.clear()
    except Exception:
        pass
        
    logger.info("mealie_save_success", recipe_name=name)
    return True, slug


# -----------------------------------------------------------------------------
# Tasks / sidebar monitor
# -----------------------------------------------------------------------------
def task_update(task_id: str, **changes) -> None:
    with get_task_lock():
        task = get_task_registry().get(task_id)
        if task:
            task.update(changes)

def task_append(task_id: str, key: str, message: str) -> None:
    with get_task_lock():
        task = get_task_registry().get(task_id)
        if task:
            task.setdefault(key, []).append(message)

def task_inc(task_id: str) -> None:
    with get_task_lock():
        task = get_task_registry().get(task_id)
        if task:
            task["current"] = task.get("current", 0) + 1

def task_set_detail(task_id: str, detail: str) -> None:
    task_update(task_id, last_detail=detail, last_detail_at=time.time())

def compute_task_metrics(task: Dict[str, Any]) -> Dict[str, Any]:
    started_at = float(task.get("started_at", time.time()) or time.time())
    elapsed = max(0.0, time.time() - started_at)
    completed = int(task.get("current", 0) or 0)
    total = int(task.get("total", 0) or 0)
    rpm = (completed / elapsed * 60.0) if elapsed > 0 and completed > 0 else 0.0
    remaining = max(0, total - completed)
    eta_seconds = (remaining / rpm * 60.0) if rpm > 0 and remaining > 0 else (0.0 if remaining == 0 else None)
    return {"elapsed": elapsed, "rpm": rpm, "eta_seconds": eta_seconds}

def get_running_tasks_snapshot() -> List[Dict[str, Any]]:
    tasks = [task for task in get_task_registry().values() if task.get("status") == "running"]
    tasks.sort(key=lambda x: x.get("started_at", 0), reverse=True)
    return tasks

def make_task(name: str, total: int) -> Tuple[str, Dict[str, Any]]:
    task_id = str(uuid.uuid4())
    user_label = get_current_user_label()
    user_key = get_current_user_key()
    user_email = get_current_user_email()
    
    m_user_id = get_mealie_user_id_by_email(settings.mealie_url, settings.mealie_api_key, user_email)
    
    task = {
        "name": name,
        "owner": user_label,
        "owner_key": user_key,
        "owner_email": user_email,
        "mealie_user_id": m_user_id,
        "started_at": time.time(),
        "total": total,
        "current": 0,
        "status": "running",
        "logs": [],
        "errors": [],
        "stop_requested": False,
        "last_detail": "Task angelegt",
    }
    with get_task_lock():
        get_task_registry()[task_id] = task
    logger.info("task_created", task_name=name, user_label=user_label)
    return task_id, task

def _render_task_monitor_body() -> None:
    if st.button("🗑️ Historie leeren", use_container_width=True):
        with get_task_lock():
            to_delete = [tid for tid, task in get_task_registry().items() if task["status"] in ["abgeschlossen", "abgebrochen"]]
            for tid in to_delete:
                del get_task_registry()[tid]
        st.rerun()

    for t_id, task in list(get_task_registry().items()):
        if task["status"] == "running":
            icon = "⏳"
        elif task["status"] == "abgebrochen":
            icon = "🛑"
        elif task.get("errors"):
            if task.get("total", 0) > 0 and len(task.get("errors", [])) >= task.get("total", 0):
                icon = "❌"
            else:
                icon = "⚠️"
        else:
            icon = "✅"
            
        owner_text = f" · {task.get('owner', 'Unbekannt')}" if task.get("owner") else ""
        metrics = compute_task_metrics(task)
        with st.expander(f"{icon} {task['name']} ({task['current']}/{task['total']}){owner_text}"):
            if task["total"] > 0:
                st.progress(task["current"] / task["total"])
            st.caption(
                f"Status: {task.get('last_detail', 'Wird vorbereitet')} · "
                f"Tempo: {metrics['rpm']:.2f} Rezepte/Min · "
                f"ETA: {format_duration(metrics['eta_seconds'])} · "
                f"Laufzeit: {format_duration(metrics['elapsed'])}"
            )
            if task["status"] == "running":
                ct1, ct2 = st.columns(2)
                if ct1.button("🛑 Stoppen", key=f"stop_{t_id}", use_container_width=True):
                    with get_task_lock():
                        get_task_registry()[t_id]["stop_requested"] = True
                        get_task_registry()[t_id]["last_detail"] = "Task wird gestoppt"
                    st.rerun()
                if ct2.button("🗑️ Löschen", key=f"delr_{t_id}", use_container_width=True):
                    with get_task_lock():
                        if t_id in get_task_registry():
                            del get_task_registry()[t_id]
                    st.rerun()
            for err in task.get("errors", []):
                st.error(err)
            for log in task.get("logs", []):
                st.success(log)
            if task["status"] in ["abgeschlossen", "abgebrochen"]:
                if st.button("Eintrag ausblenden", key=f"del_{t_id}", use_container_width=True):
                    with get_task_lock():
                        if t_id in get_task_registry():
                            del get_task_registry()[t_id]
                    st.rerun()

    if is_admin_user():
        with st.expander("🛡️ Adminpanel", expanded=False):
            st.markdown("### 🧩 Browser-Erweiterung")
            st.write("Lade das Addon für Chrome/Edge herunter, um Rezepte direkt von jeder Website an Snap-to-Mealie zu senden.")
            st.download_button(
                label="📦 Addon herunterladen (.zip)",
                data=generate_extension_zip(),
                file_name="snap-to-mealie-addon.zip",
                mime="application/zip",
                use_container_width=True
            )
            
            st.divider()
            active_users = get_active_users_snapshot()
            running_tasks = get_running_tasks_snapshot()
            st.markdown("**Aktive Nutzer**")
            if active_users:
                for user in active_users:
                    label = user.get("label") or "Unbekannt"
                    email = user.get("email") or ""
                    seen_ago = max(0, int(time.time() - user.get("last_seen", time.time())))
                    if email and email != label:
                        st.write(f"• {label} — {email} · aktiv vor {seen_ago}s")
                    else:
                        st.write(f"• {label} · aktiv vor {seen_ago}s")
            else:
                st.info("Keine aktiven Nutzer erkannt.")
            st.markdown("**Laufende Rezeptumwandlungen**")
            if running_tasks:
                for task in running_tasks:
                    owner = task.get("owner", "Unbekannt")
                    metrics = compute_task_metrics(task)
                    st.write(f"• {task.get('name', 'Task')} — {owner} ({task.get('current', 0)}/{task.get('total', 0)}) · {metrics['rpm']:.2f} Rezepte/Min · ETA {format_duration(metrics['eta_seconds'])}")
                    st.caption(task.get("last_detail", "Wird vorbereitet"))
            else:
                st.info("Aktuell laufen keine Rezeptumwandlungen.")
                
            st.divider()
            st.markdown("### 🧪 System-Tests")
            st.write("Prüfe die Systemintegrität mit der automatisierten Test-Suite.")
            if st.button("▶️ Test-Suite ausführen", use_container_width=True):
                with st.spinner("Führe Tests aus... (Das kann ein paar Sekunden dauern)"):
                    try:
                        test_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_app.py")
                        result = subprocess.run(
                            [sys.executable, "-m", "pytest", test_file_path, "-v", "--disable-warnings"], 
                            capture_output=True, text=True, timeout=60
                        )
                        if result.returncode == 0:
                            st.success("✅ Alle Tests erfolgreich bestanden!")
                        else:
                            st.error("❌ Einige Tests sind fehlgeschlagen oder es gab einen Fehler im Test-Code.")
                        with st.expander("Test-Log (Konsolenausgabe)", expanded=True):
                            st.code(result.stdout + "\n" + result.stderr, language="text")
                    except FileNotFoundError:
                        st.error("❌ Konnte 'pytest' nicht ausführen. Ist es installiert? (`pip install pytest`)")
                    except Exception as e:
                        st.error(f"❌ Unerwarteter Fehler beim Ausführen der Tests: {e}")


if hasattr(st, "fragment"):
    @st.fragment(run_every="3s")
    def render_task_monitor() -> None:
        _render_task_monitor_body()
else:
    def render_task_monitor() -> None:
        _render_task_monitor_body()


# -----------------------------------------------------------------------------
# Background processors (ASYNC MIGRATION)
# -----------------------------------------------------------------------------
def _process_single_image_batch_item(task_id: str, idx: int, chunk: List[bytes], pair_mode: bool, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct") -> None:
    with get_task_lock():
        task_data = get_task_registry().get(task_id)
        if not task_data or task_data.get("stop_requested"):
            return
            
    label = f"Paar {idx + 1}" if pair_mode else f"Bild {idx + 1}"
    client_bundle = create_genai_client(gemini_api_key)
    images_pil: List[Image.Image] = []
    cover_img_pil: Optional[Image.Image] = None
    recipe_img_pil: Optional[Image.Image] = None
    try:
        task_set_detail(task_id, f"{label}: Bilder werden geladen")
        images_pil = [load_image(img_bytes) for img_bytes in chunk]
        if pair_mode and len(images_pil) == 2:
            cover_img_pil = images_pil[0]
            recipe_img_pil = images_pil[1]
            task_set_detail(task_id, f"{label}: Rezepttext wird analysiert")
            parsed_data = analyze_content_with_gemini(client_bundle, prompt, images=[recipe_img_pil])
            cover_img_bytes = image_to_jpeg_bytes(cover_img_pil)
        else:
            task_set_detail(task_id, f"{label}: Bild wird analysiert")
            parsed_data = analyze_content_with_gemini(client_bundle, prompt, images=images_pil)
            task_set_detail(task_id, f"{label}: Generiere Cover-Bild")
            cover_img_bytes = auto_generate_cover_image(client_bundle, parsed_data, None, task_data.get("owner", ""))
            
        if target_mode == "editor":
            task_set_detail(task_id, f"{label}: Füge zur Editor-Warteschlange hinzu")
            add_to_editor_queue(task_data.get("owner_key", ""), parsed_data, cover_img_bytes)
            task_append(task_id, "logs", f"✅ {label}: In Editor-Warteschlange gelegt")
            task_set_detail(task_id, f"{label}: Erfolgreich in Warteschlange")
        else:
            task_set_detail(task_id, f"{label}: Speichere nach Mealie")
            success, result = direct_save_to_mealie(
                parsed_data,
                mealie_url,
                mealie_api_key,
                cover_img_bytes,
                preloaded_maps,
                audit_user_key=task_data.get("owner_key", ""),
                audit_user_label=task_data.get("owner", ""),
                audit_user_email=task_data.get("owner_email", ""),
                mealie_user_id=task_data.get("mealie_user_id")
            )
            if success:
                task_append(task_id, "logs", f"✅ {label}: {result}")
                task_set_detail(task_id, f"{label}: Erfolgreich gespeichert als {result}")
            else:
                task_append(task_id, "errors", f"❌ {label}: {result}")
                task_set_detail(task_id, f"{label}: Fehler beim Speichern")
    except Exception as exc:
        task_append(task_id, "errors", f"❌ {label}: {exc}")
        task_set_detail(task_id, f"{label}: Abgebrochen wegen Fehler")
    finally:
        close_images(images_pil)
        safe_close_image(cover_img_pil)
        safe_close_image(recipe_img_pil)
        gc.collect()
        task_inc(task_id)


def background_image_batch_process(task_id: str, image_bytes_list: List[bytes], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, pair_mode: bool = False, is_batch: bool = True, target_mode: str = "direct"):
    if is_batch:
        chunks = [image_bytes_list[i:i + 2] for i in range(0, len(image_bytes_list), 2)] if pair_mode else [[img] for img in image_bytes_list]
    else:
        chunks = [image_bytes_list]
        
    task_set_detail(task_id, f"Async Start · {len(chunks)} Elemente in der Warteschlange")
    
    async def run_tasks():
        semaphore = asyncio.Semaphore(settings.batch_max_workers)
        
        async def sem_task(idx, chunk):
            async with semaphore:
                await asyncio.to_thread(_process_single_image_batch_item, task_id, idx, chunk, pair_mode, mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
                
        tasks = [sem_task(idx, chunk) for idx, chunk in enumerate(chunks)]
        await asyncio.gather(*tasks)
        
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"):
                get_task_registry()[task_id]["status"] = "abgebrochen"
                get_task_registry()[task_id]["last_detail"] = "Vom Benutzer gestoppt"
                return
        task_update(task_id, status="abgeschlossen", last_detail="Alle Bildaufgaben abgeschlossen")

    asyncio.run(run_tasks())


def _process_single_pdf_batch_item(task_id: str, idx: int, pdf_bytes: bytes, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct") -> None:
    with get_task_lock():
        task_data = get_task_registry().get(task_id)
        if not task_data or task_data.get("stop_requested"):
            return
    client_bundle = create_genai_client(gemini_api_key)
    
    fd, temp_path = tempfile.mkstemp(suffix=".pdf")
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(pdf_bytes)
            
        task_set_detail(task_id, f"PDF {idx + 1}/{total}: Lade PDF zu Gemini hoch")
        recipes = analyze_pdf_with_gemini(client_bundle, prompt, temp_path)
        
        task_set_detail(task_id, f"PDF {idx + 1}/{total}: {len(recipes)} Rezepte gefunden. Verarbeite...")
        
        saved_count = 0
        for r_idx, recipe_data in enumerate(recipes):
            task_set_detail(task_id, f"PDF {idx + 1}/{total}: Generiere KI-Bild für Rezept {r_idx+1}")
            cover_img_bytes = auto_generate_cover_image(client_bundle, recipe_data, None, task_data.get("owner", ""))
            
            if target_mode == "editor":
                add_to_editor_queue(task_data.get("owner_key", ""), recipe_data, cover_img_bytes)
                task_append(task_id, "logs", f"✅ PDF {idx+1} (Rezept {r_idx+1}): In Editor-Warteschlange gelegt")
                saved_count += 1
            else:
                success, result = direct_save_to_mealie(
                    recipe_data,
                    mealie_url,
                    mealie_api_key,
                    cover_img_bytes,
                    preloaded_maps,
                    audit_user_key=task_data.get("owner_key", ""),
                    audit_user_label=task_data.get("owner", ""),
                    audit_user_email=task_data.get("owner_email", ""),
                    mealie_user_id=task_data.get("mealie_user_id")
                )
                if success:
                    task_append(task_id, "logs", f"✅ PDF {idx+1} (Rezept {r_idx+1}): {result}")
                    saved_count += 1
                else:
                    task_append(task_id, "errors", f"❌ PDF {idx+1} (Rezept {r_idx+1}): {result}")
                
        task_set_detail(task_id, f"PDF {idx + 1}/{total}: {saved_count} Rezepte erfolgreich verarbeitet.")
    except Exception as exc:
        task_append(task_id, "errors", f"❌ PDF {idx+1}: {exc}")
        task_set_detail(task_id, f"PDF {idx + 1}/{total}: Abgebrochen wegen Fehler")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        task_inc(task_id)
        gc.collect()

def background_pdf_batch_process(task_id: str, pdf_bytes_list: List[bytes], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(pdf_bytes_list)} PDF(s) in der Warteschlange")
    
    async def run_tasks():
        semaphore = asyncio.Semaphore(settings.batch_max_workers)
        
        async def sem_task(idx, pdf_bytes):
            async with semaphore:
                await asyncio.to_thread(_process_single_pdf_batch_item, task_id, idx, pdf_bytes, len(pdf_bytes_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
                
        tasks = [sem_task(idx, pdf_bytes) for idx, pdf_bytes in enumerate(pdf_bytes_list)]
        await asyncio.gather(*tasks)
        
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"):
                get_task_registry()[task_id]["status"] = "abgebrochen"
                get_task_registry()[task_id]["last_detail"] = "Vom Benutzer gestoppt"
                return
        task_update(task_id, status="abgeschlossen", last_detail="Alle PDF-Aufgaben abgeschlossen")

    asyncio.run(run_tasks())


def _process_single_url_batch_item(task_id: str, idx: int, url: str, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct") -> None:
    with get_task_lock():
        task_data = get_task_registry().get(task_id)
        if not task_data or task_data.get("stop_requested"):
            return
    client_bundle = create_genai_client(gemini_api_key)
    try:
        task_set_detail(task_id, f"URL {idx + 1}/{total}: Lade Webseite")
        text, cover_img_bytes = fetch_url_text_and_image(url)
        task_set_detail(task_id, f"URL {idx + 1}/{total}: Analysiere Rezept")
        parsed_data = analyze_content_with_gemini(client_bundle, prompt, text=text)
        parsed_data["orgURL"] = url
        
        task_set_detail(task_id, f"URL {idx + 1}/{total}: Prüfe/Generiere Cover-Bild")
        cover_img_bytes = auto_generate_cover_image(client_bundle, parsed_data, cover_img_bytes, task_data.get("owner", ""))
        
        if target_mode == "editor":
            task_set_detail(task_id, f"URL {idx + 1}/{total}: Füge zur Editor-Warteschlange hinzu")
            add_to_editor_queue(task_data.get("owner_key", ""), parsed_data, cover_img_bytes)
            task_append(task_id, "logs", f"✅ {url}: In Editor-Warteschlange gelegt")
            task_set_detail(task_id, f"URL {idx + 1}/{total}: Erfolgreich in Warteschlange")
        else:
            task_set_detail(task_id, f"URL {idx + 1}/{total}: Speichere nach Mealie")
            success, result = direct_save_to_mealie(
                parsed_data,
                mealie_url,
                mealie_api_key,
                cover_img_bytes,
                preloaded_maps,
                org_url=url,
                audit_user_key=task_data.get("owner_key", ""),
                audit_user_label=task_data.get("owner", ""),
                audit_user_email=task_data.get("owner_email", ""),
                mealie_user_id=task_data.get("mealie_user_id")
            )
            if success:
                task_append(task_id, "logs", f"✅ {url}: {result}")
                task_set_detail(task_id, f"URL {idx + 1}/{total}: Erfolgreich gespeichert als {result}")
            else:
                task_append(task_id, "errors", f"❌ {url}: {result}")
                task_set_detail(task_id, f"URL {idx + 1}/{total}: Fehler beim Speichern")
    except Exception as exc:
        task_append(task_id, "errors", f"❌ {url}: {exc}")
        task_set_detail(task_id, f"URL {idx + 1}/{total}: Abgebrochen wegen Fehler")
    finally:
        task_inc(task_id)
        gc.collect()


def background_url_batch_process(task_id: str, url_list: List[str], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(url_list)} URLs in der Warteschlange")
    
    async def run_tasks():
        semaphore = asyncio.Semaphore(settings.batch_max_workers)
        
        async def sem_task(idx, url):
            async with semaphore:
                await asyncio.to_thread(_process_single_url_batch_item, task_id, idx, url, len(url_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
                
        tasks = [sem_task(idx, url) for idx, url in enumerate(url_list)]
        await asyncio.gather(*tasks)
        
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"):
                get_task_registry()[task_id]["status"] = "abgebrochen"
                get_task_registry()[task_id]["last_detail"] = "Vom Benutzer gestoppt"
                return
        task_update(task_id, status="abgeschlossen", last_detail="Alle URL-Aufgaben abgeschlossen")

    asyncio.run(run_tasks())


def _process_single_video_batch_item(task_id: str, idx: int, url: str, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct") -> None:
    with get_task_lock():
        task_data = get_task_registry().get(task_id)
        if not task_data or task_data.get("stop_requested"):
            return
    client_bundle = create_genai_client(gemini_api_key)
    bundle: Dict[str, Any] = {}
    try:
        task_set_detail(task_id, f"Video {idx + 1}/{total}: Lade Video herunter")
        bundle = download_recipe_video(url)
        task_set_detail(task_id, f"Video {idx + 1}/{total}: Analysiere Video in Gemini")
        parsed_data = analyze_video_with_gemini(client_bundle, prompt, bundle.get("video_path", ""), bundle.get("recipe_text", ""))
        parsed_data["orgURL"] = url
        
        task_set_detail(task_id, f"Video {idx + 1}/{total}: Prüfe/Generiere Cover-Bild")
        cover_img_bytes = auto_generate_cover_image(client_bundle, parsed_data, bundle.get("thumbnail_bytes"), task_data.get("owner", ""))
        
        if target_mode == "editor":
            task_set_detail(task_id, f"Video {idx + 1}/{total}: Füge zur Editor-Warteschlange hinzu")
            add_to_editor_queue(task_data.get("owner_key", ""), parsed_data, cover_img_bytes)
            task_append(task_id, "logs", f"✅ {url}: In Editor-Warteschlange gelegt")
            task_set_detail(task_id, f"Video {idx + 1}/{total}: Erfolgreich in Warteschlange")
        else:
            task_set_detail(task_id, f"Video {idx + 1}/{total}: Speichere nach Mealie")
            success, result = direct_save_to_mealie(
                parsed_data,
                mealie_url,
                mealie_api_key,
                cover_img_bytes,
                preloaded_maps,
                org_url=url,
                audit_user_key=task_data.get("owner_key", ""),
                audit_user_label=task_data.get("owner", ""),
                audit_user_email=task_data.get("owner_email", ""),
                mealie_user_id=task_data.get("mealie_user_id")
            )
            if success:
                task_append(task_id, "logs", f"✅ {url}: {result}")
                task_set_detail(task_id, f"Video {idx + 1}/{total}: Erfolgreich gespeichert als {result}")
            else:
                task_append(task_id, "errors", f"❌ {url}: {result}")
                task_set_detail(task_id, f"Video {idx + 1}/{total}: Fehler beim Speichern")
    except Exception as exc:
        task_append(task_id, "errors", f"❌ {url}: {exc}")
        task_set_detail(task_id, f"Video {idx + 1}/{total}: Abgebrochen wegen Fehler")
    finally:
        cleanup_video_bundle(bundle)
        task_inc(task_id)
        gc.collect()


def background_video_batch_process(task_id: str, url_list: List[str], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(url_list)} Videos in der Warteschlange")
    
    async def run_tasks():
        semaphore = asyncio.Semaphore(settings.batch_max_workers)
        
        async def sem_task(idx, url):
            async with semaphore:
                await asyncio.to_thread(_process_single_video_batch_item, task_id, idx, url, len(url_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
                
        tasks = [sem_task(idx, url) for idx, url in enumerate(url_list)]
        await asyncio.gather(*tasks)
        
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"):
                get_task_registry()[task_id]["status"] = "abgebrochen"
                get_task_registry()[task_id]["last_detail"] = "Vom Benutzer gestoppt"
                return
        task_update(task_id, status="abgeschlossen", last_detail="Alle Video-Aufgaben abgeschlossen")

    asyncio.run(run_tasks())


def _process_single_mealie_batch_item(task_id: str, idx: int, slug: str, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct") -> None:
    with get_task_lock():
        task_data = get_task_registry().get(task_id)
        if not task_data or task_data.get("stop_requested"):
            return
    client_bundle = create_genai_client(gemini_api_key)
    try:
        task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Lade Mealie-Rezept {slug}")
        text = fetch_mealie_recipe_text(slug, mealie_url, mealie_api_key)
        task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Analysiere Rezept")
        parsed_data = analyze_content_with_gemini(client_bundle, prompt, text=text)
        
        task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Prüfe/Generiere Cover-Bild")
        existing_recipe = get_recipe_by_slug(mealie_url, mealie_api_key, slug)
        cover_img_bytes = None
        if existing_recipe and not existing_recipe.get("image"):
            cover_img_bytes = auto_generate_cover_image(client_bundle, parsed_data, None, task_data.get("owner", ""))
            
        if target_mode == "editor":
            task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Füge zur Editor-Warteschlange hinzu")
            add_to_editor_queue(task_data.get("owner_key", ""), parsed_data, cover_img_bytes)
            task_append(task_id, "logs", f"✅ {slug}: In Editor-Warteschlange gelegt")
            task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Erfolgreich in Warteschlange")
        else:
            task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Schreibe Änderungen zurück")
            success, result = direct_save_to_mealie(
                parsed_data,
                mealie_url,
                mealie_api_key,
                cover_img_bytes,
                preloaded_maps,
                target_slug=slug,
                audit_user_key=task_data.get("owner_key", ""),
                audit_user_label=task_data.get("owner", ""),
                audit_user_email=task_data.get("owner_email", ""),
                mealie_user_id=task_data.get("mealie_user_id")
            )
            if success:
                task_append(task_id, "logs", f"✅ {slug}: {result}")
                task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Erfolgreich aktualisiert")
            else:
                task_append(task_id, "errors", f"❌ {slug}: {result}")
                task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Fehler beim Aktualisieren")
    except Exception as exc:
        task_append(task_id, "errors", f"❌ {slug}: {exc}")
        task_set_detail(task_id, f"Rezept {idx + 1}/{total}: Abgebrochen wegen Fehler")
    finally:
        task_inc(task_id)
        gc.collect()


def background_mealie_batch_process(task_id: str, slug_list: List[str], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(slug_list)} Mealie-Rezepte in der Warteschlange")
    
    async def run_tasks():
        semaphore = asyncio.Semaphore(settings.batch_max_workers)
        
        async def sem_task(idx, slug):
            async with semaphore:
                await asyncio.to_thread(_process_single_mealie_batch_item, task_id, idx, slug, len(slug_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
                
        tasks = [sem_task(idx, slug) for idx, slug in enumerate(slug_list)]
        await asyncio.gather(*tasks)
        
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"):
                get_task_registry()[task_id]["status"] = "abgebrochen"
                get_task_registry()[task_id]["last_detail"] = "Vom Benutzer gestoppt"
                return
        task_update(task_id, status="abgeschlossen", last_detail="Alle Mealie-Aufgaben abgeschlossen")

    asyncio.run(run_tasks())

# -----------------------------------------------------------------------------
# Stats
# -----------------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_user_stats_snapshot(api_url: str, api_key: str, user_key: str) -> Dict[str, Any]:
    uploaded_rows = get_user_uploaded_recipe_rows(user_key)
    unique_rows = {row["recipe_slug"]: row for row in uploaded_rows}
    
    ingredient_counter: Dict[str, int] = {}
    scanned = 0
    
    for row in list(unique_rows.values())[:150]:
        full = get_recipe_by_slug(api_url, api_key, row["recipe_slug"])
        if not full:
            continue
        scanned += 1
        seen = set()
        for ing in full.get("recipeIngredient", []):
            name = clean_str(get_nested_name(ing.get("food"))) or clean_str(ing.get("originalText"))
            name = normalize_name(name)
            is_excluded = any(ex == name or ex in name.split() for ex in ["salz", "wasser", "pfeffer", "meersalz"]) or ("salz" in name and "pfeffer" in name)
            
            if name and name not in seen and not is_excluded:
                ingredient_counter[name] = ingredient_counter.get(name, 0) + 1
                seen.add(name)
                
    top_ingredients = []
    if ingredient_counter and scanned > 0:
        sorted_ings = sorted(ingredient_counter.items(), key=lambda x: x[1], reverse=True)
        for ing, hits in sorted_ings[:5]:
            top_ingredients.append({
                "name": ing,
                "hits": hits,
                "share": hits / scanned * 100
            })

    leaderboard = []
    total_app_uploads = 0
    with get_db_lock():
        with db_conn() as conn:
            ensure_uploads_table(conn)
            lb_rows = conn.execute(
                "SELECT user_label, COUNT(recipe_slug) as c "
                "FROM uploads WHERE source = 'snap_to_mealie' "
                "GROUP BY user_key ORDER BY c DESC LIMIT 10"
            ).fetchall()
            leaderboard = [{"label": r[0] or "Anonym", "count": r[1]} for r in lb_rows]
            
            tot_row = conn.execute("SELECT COUNT(*) FROM uploads WHERE source = 'snap_to_mealie'").fetchone()
            total_app_uploads = tot_row[0] if tot_row else 0
            
    return {
        "personal_count": len(unique_rows),
        "scanned_count": scanned,
        "top_ingredients": top_ingredients,
        "hours_saved": round(len(unique_rows) * 10 / 60, 1),
        "upload_rows": uploaded_rows,
        "leaderboard": leaderboard,
        "total_app_uploads": total_app_uploads
    }


# -----------------------------------------------------------------------------
# Shift image functions
# -----------------------------------------------------------------------------
def shift_image(idx: int, direction: int) -> None:
    imgs = st.session_state.collected_images
    if direction == -1 and idx > 0:
        imgs[idx], imgs[idx-1] = imgs[idx-1], imgs[idx]
    elif direction == 1 and idx < len(imgs) - 1:
        imgs[idx], imgs[idx+1] = imgs[idx+1], imgs[idx]
    st.session_state.switch_to_tab = 0

def swap_pair(idx: int) -> None:
    imgs = st.session_state.collected_images
    if idx + 1 < len(imgs):
        imgs[idx], imgs[idx+1] = imgs[idx+1], imgs[idx]
    st.session_state.switch_to_tab = 0

def remove_image(idx: int) -> None:
    if 0 <= idx < len(st.session_state.collected_images):
        st.session_state.collected_images.pop(idx)
    st.session_state.switch_to_tab = 0


# -----------------------------------------------------------------------------
# App init
# -----------------------------------------------------------------------------
ensure_streamlit_config()
ensure_pwa_assets()
init_cache_db()
st.set_page_config(page_title="Snap-to-Mealie", page_icon="📸", layout="wide", initial_sidebar_state="expanded")
init_session()
inject_ui(st.session_state.theme_mode)
inject_pwa_bootstrap()

if settings.oidc_client_id and has_streamlit_auth():
    if not is_streamlit_user_logged_in():
        st.title("🔒 Authentifizierung erforderlich")
        st.login("custom")
        st.stop()
    else:
        if st.sidebar.button("Abmelden"):
            st.logout()
elif settings.oidc_client_id and not has_streamlit_auth():
    st.sidebar.warning("OIDC ist gesetzt, aber diese Streamlit-Version unterstützt st.user/st.login nicht vollständig.")

if not settings.mealie_api_key or not settings.gemini_api_key:
    st.error("API-Schlüssel fehlen.")
    st.stop()

client = create_genai_client(settings.gemini_api_key)
register_active_user()


# -----------------------------------------------------------------------------
# PWA Share Target Handling
# -----------------------------------------------------------------------------
try:
    _shared_content = f"{st.query_params.get('title', '')} {st.query_params.get('text', '')} {st.query_params.get('shared_url', '')}"
    _extracted_urls = re.findall(r'https?://[^\s]+', _shared_content)
    if _extracted_urls:
        _mealie_domain = settings.mealie_url.split("://")[-1].split(":")[0]
        _known_slugs = [r["slug"] for r in get_mealie_recipes(settings.mealie_url, settings.mealie_api_key)]
        
        _added_mealie = False
        _added_video = False
        _added_url = False

        for u in _extracted_urls:
            possible_slug = u.strip('/').split('/')[-1].split('?')[0]
            if any(domain in u for domain in ["youtube.com", "youtu.be", "instagram.com"]):
                _current = st.session_state.get("shared_video_input", "")
                if u not in _current:
                    st.session_state.shared_video_input = (_current + "\n" + u).strip()
                    _added_video = True
            elif _mealie_domain in u or "mealie" in u.lower():
                resolved_slug = None
                if "/recipe/" in u:
                    resolved_slug = possible_slug
                elif possible_slug in _known_slugs:
                    resolved_slug = possible_slug
                else:
                    try:
                        _resp = get_http_session().get(u, timeout=5)
                        _jsonld = extract_recipe_jsonld_text(_resp.text)
                        _t_match = re.search(r"Titel:\s*(.+)", _jsonld)
                        _title = _t_match.group(1).strip() if _t_match else None
                        if not _title:
                            _t_match2 = re.search(r"<title>(.*?)</title>", _resp.text, re.IGNORECASE)
                            if _t_match2:
                                _title = _t_match2.group(1).split("|")[0].strip()
                        if _title:
                            resolved_slug = search_recipe_slug_by_name(settings.mealie_url, settings.mealie_api_key, _title)
                    except Exception:
                        pass
                if resolved_slug and resolved_slug in _known_slugs:
                    _current = st.session_state.get("shared_mealie_input", "")
                    val_to_add = f"{settings.mealie_url}/recipe/{resolved_slug}"
                    if val_to_add not in _current:
                        st.session_state.shared_mealie_input = (_current + "\n" + val_to_add).strip()
                        _added_mealie = True
                else:
                    _current = st.session_state.get("shared_urls_input", "")
                    if u not in _current:
                        st.session_state.shared_urls_input = (_current + "\n" + u).strip()
                        _added_url = True
            else:
                _current = st.session_state.get("shared_urls_input", "")
                if u not in _current:
                    st.session_state.shared_urls_input = (_current + "\n" + u).strip()
                    _added_url = True

        if _added_mealie:
            toast("🔗 Mealie-Rezept erkannt und aufgelöst!", icon="🔄")
            st.session_state.switch_to_tab = 4
        elif _added_video:
            toast("🔗 Video-URL empfangen!", icon="🎥")
            st.session_state.switch_to_tab = 3
        elif _added_url:
            toast("🔗 Web-URL empfangen!", icon="🌐")
            st.session_state.switch_to_tab = 2
        st.query_params.clear()
except Exception:
    pass


def reset_editor_state() -> None:
    st.session_state.recipe_data = None
    st.session_state.cover_image_bytes = None
    st.session_state.target_slug = None
    st.session_state.sous_chef_history = []
    st.session_state.current_queue_id = None
    # Zwingt die UI dazu, die gespeicherten Prompts bei einem neuen Rezept komplett neu zu bewerten
    for key in ["prompt_selector", "img_style_txt", "last_prompt_selector"]:
        if key in st.session_state:
            del st.session_state[key]


def clear_images_from_state() -> None:
    st.session_state.collected_images = []
    gc.collect()


def run_analysis_with_animation(fn):
    with st.status("Analyse läuft...", expanded=True) as status:
        render_lottie_loading(100)
        result = fn()
        status.update(label="Analyse abgeschlossen", state="complete", expanded=False)
        return result


# -----------------------------------------------------------------------------
# Sidebar monitor
# -----------------------------------------------------------------------------
with st.sidebar:
    render_task_monitor()


# -----------------------------------------------------------------------------
# Header / main UI
# -----------------------------------------------------------------------------
render_header()

if not st.session_state.get("recipe_data"):
    if st.session_state.get("shared_mealie_input"):
        st.info("🔗 **Mealie-Rezept empfangen!** Klicke auf '🪄 An Editor senden'.", icon="📲")
    elif st.session_state.get("shared_video_input"):
        st.info("🔗 **Video-Link empfangen!** Klicke auf '🪄 An Editor senden'.", icon="📲")
    elif st.session_state.get("shared_urls_input"):
        st.info("🔗 **Web-URL empfangen!** Klicke auf '🪄 An Editor senden'.", icon="📲")


if st.session_state.upload_success:
    for item in st.session_state.upload_success:
        if item == "BACKGROUND_TASK_STARTED":
            toast("Hintergrund-Aufgabe gestartet", "🚀")
        else:
            toast(f"Rezept gespeichert: {item}", "🍳")
            st.success(f"🎉 Gespeichert! [Hier ansehen]({settings.mealie_url}/recipe/{item})")
    st.session_state.upload_success = []


def set_active_tab(idx: int):
    st.session_state.switch_to_tab = idx

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📁 Datei-Import", "📷 Kamera", "🌐 URL Import", "🎥 Video Import", "🔄 Mealie Rezept", "📊 Statistiken"])

if st.session_state.get("switch_to_tab") is not None:
    tab_idx = st.session_state.switch_to_tab
    components.html(
        f"""
        <script>
        const parentDoc = window.parent.document;
        setTimeout(() => {{
            const tabs = parentDoc.querySelectorAll('.stTabs [role="tab"]');
            if (tabs.length > {tab_idx}) {{
                tabs[{tab_idx}].click();
            }}
        }}, 150);
        </script>
        """,
        height=0
    )
    st.session_state.switch_to_tab = None

with tab1:
    ui_card("Datei-Import (Bilder & PDFs)", "Ziehe Fotos von Rezepten oder ganze PDF-Kochbücher hinein.")
    uploaded_files = st.file_uploader("Dateien hinzufügen", type=["jpg", "jpeg", "png", "pdf"], accept_multiple_files=True, on_change=set_active_tab, args=(0,))
    
    if uploaded_files:
        for file in sorted(uploaded_files, key=lambda x: x.name):
            data = file.getvalue()
            if file.name.lower().endswith(".pdf"):
                if data not in st.session_state.collected_pdfs:
                    st.session_state.collected_pdfs.append(data)
            else:
                if data not in st.session_state.collected_images:
                    st.session_state.collected_images.append(data)

with tab2:
    ui_card("Kamera", "Fotografiere direkt mit dem Gerät.")
    if not st.session_state.get("camera_active"):
        st.info("Klicke auf den Button, um die Kamera zu starten. Erst dann wird nach der Berechtigung gefragt.")
        if st.button("📷 Kamera aktivieren", use_container_width=True):
            st.session_state.camera_active = True
            st.session_state.switch_to_tab = 1
            st.rerun()
    else:
        camera_image = st.camera_input("Foto aufnehmen", on_change=set_active_tab, args=(1,))
        if camera_image:
            data = camera_image.getvalue()
            if data not in st.session_state.collected_images:
                st.session_state.collected_images.append(data)
                toast("Foto zur Sammlung hinzugefügt!", "📸")
        if st.button("❌ Kamera wieder schließen", use_container_width=True):
            st.session_state.camera_active = False
            st.session_state.switch_to_tab = 1
            st.rerun()

if st.session_state.collected_pdfs:
    st.divider()
    ui_card("Aktuelle PDF-Sammlung", f"Es liegen {len(st.session_state.collected_pdfs)} PDF-Dokument(e) bereit.")
    c1, c2, c3 = st.columns(3)
    if c1.button("🪄 An Editor senden (PDF)", use_container_width=True, type="primary"):
        st.session_state.switch_to_tab = 0
        task_id, _ = make_task("Hintergrund: PDF -> Editor", len(st.session_state.collected_pdfs))
        threading.Thread(target=background_pdf_batch_process, args=(task_id, list(st.session_state.collected_pdfs), settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_pdf_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
        st.session_state.collected_pdfs = []
        st.rerun()
    if c2.button("📚 Direkt-Import (PDF)", use_container_width=True):
        st.session_state.switch_to_tab = 0
        task_id, _ = make_task("Hintergrund: PDF -> Mealie", len(st.session_state.collected_pdfs))
        threading.Thread(target=background_pdf_batch_process, args=(task_id, list(st.session_state.collected_pdfs), settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_pdf_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "direct"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
        st.session_state.collected_pdfs = []
        st.rerun()
    if c3.button("🗑️ PDFs verwerfen", use_container_width=True):
        st.session_state.switch_to_tab = 0
        st.session_state.collected_pdfs = []
        st.rerun()

if st.session_state.collected_images:
    st.divider()
    ui_card("Aktuelle Bildsammlung", "Ordne die Bilder in der korrekten Reihenfolge.")
    pair_mode = st.checkbox("🤝 Bilder paarweise verarbeiten (1. Bild = Cover, 2. Bild = Text)", value=True, on_change=set_active_tab, args=(0,))
    if pair_mode:
        for i in range(0, len(st.session_state.collected_images), 2):
            with st.container(border=True):
                st.markdown(f"**Rezept { (i // 2) + 1 }**")
                cols = st.columns([0.42, 0.16, 0.42], vertical_alignment="center")
                with cols[0]:
                    st.image(st.session_state.collected_images[i], use_container_width=True, caption="🖼️ Cover-Bild")
                    c_a, c_b, c_c = st.columns(3)
                    c_a.button("⬅️", key=f"l_{i}", on_click=shift_image, args=(i, -1), disabled=(i==0), use_container_width=True)
                    c_b.button("🗑️", key=f"d_{i}", on_click=remove_image, args=(i,), use_container_width=True)
                    c_c.button("➡️", key=f"r_{i}", on_click=shift_image, args=(i, 1), disabled=(i==len(st.session_state.collected_images)-1), use_container_width=True)
                with cols[1]:
                    if i + 1 < len(st.session_state.collected_images):
                        st.button("🔄 Tauschen", key=f"swap_{i}", on_click=swap_pair, args=(i,), use_container_width=True)
                with cols[2]:
                    if i + 1 < len(st.session_state.collected_images):
                        st.image(st.session_state.collected_images[i + 1], use_container_width=True, caption="📝 Rezept-Text")
                        c_a, c_b, c_c = st.columns(3)
                        c_a.button("⬅️", key=f"l_{i+1}", on_click=shift_image, args=(i+1, -1), disabled=(i+1==0), use_container_width=True)
                        c_b.button("🗑️", key=f"d_{i+1}", on_click=remove_image, args=(i+1,), use_container_width=True)
                        c_c.button("➡️", key=f"r_{i+1}", on_click=shift_image, args=(i+1, 1), disabled=(i+1==len(st.session_state.collected_images)-1), use_container_width=True)
    else:
        cols = st.columns(3)
        for idx, img in enumerate(st.session_state.collected_images):
            with cols[idx % 3]:
                st.image(img, use_container_width=True, caption=f"Bild {idx + 1}")
                ca, cb, cc = st.columns(3)
                ca.button("⬅️", key=f"sl_{idx}", on_click=shift_image, args=(idx, -1), disabled=(idx==0), use_container_width=True)
                cb.button("🗑️", key=f"sd_{idx}", on_click=remove_image, args=(idx,), use_container_width=True)
                cc.button("➡️", key=f"sr_{idx}", on_click=shift_image, args=(idx, 1), disabled=(idx==len(st.session_state.collected_images)-1), use_container_width=True)

    st.markdown("### 1 Rezept aus allen Bilder")
    if st.button("🪄 An Editor senden (als 1 Rezept)", type="primary", use_container_width=True):
        st.session_state.switch_to_tab = 0
        task_id, _ = make_task("Hintergrund: Bild -> Editor", 1)
        threading.Thread(target=background_image_batch_process, args=(task_id, list(st.session_state.collected_images), settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), pair_mode, False, "editor"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
        clear_images_from_state()
        st.rerun()

    st.markdown("### Jedes Bild / Paar als eigenes Rezept")
    c1, c2, c3 = st.columns(3)
    if c1.button("📚 Editor-Stapel", use_container_width=True):
        st.session_state.switch_to_tab = 0
        total = math.ceil(len(st.session_state.collected_images) / 2) if pair_mode else len(st.session_state.collected_images)
        task_id, _ = make_task("Hintergrund-Stapel: Bilder -> Editor", total)
        threading.Thread(target=background_image_batch_process, args=(task_id, list(st.session_state.collected_images), settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), pair_mode, True, "editor"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
        clear_images_from_state()
        st.rerun()
    if c2.button("📚 Direkt-Stapel", use_container_width=True):
        st.session_state.switch_to_tab = 0
        total = math.ceil(len(st.session_state.collected_images) / 2) if pair_mode else len(st.session_state.collected_images)
        task_id, _ = make_task("Hintergrund-Stapel: Bilder -> Mealie", total)
        threading.Thread(target=background_image_batch_process, args=(task_id, list(st.session_state.collected_images), settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), pair_mode, True, "direct"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
        clear_images_from_state()
        st.rerun()
    if c3.button("🗑️ Verwerfen", use_container_width=True):
        st.session_state.switch_to_tab = 0
        clear_images_from_state()
        reset_editor_state()
        st.rerun()

if not st.session_state.collected_images and not st.session_state.collected_pdfs:
    ui_card("Noch keine Dateien geladen", "Starte mit Datei-Upload oder Kamera.")

with tab3:
    ui_card("URL-Import", "Füge eine oder mehrere Rezept-URLs ein.")
    url_area = st.text_area("URLs (eine pro Zeile):", key="shared_urls_input", placeholder="[https://example.com/rezept-1](https://example.com/rezept-1)", on_change=set_active_tab, args=(2,))
    raw_urls = [u.strip() for u in url_area.split("\n") if u.strip()]
    urls = []
    invalid_urls = []
    for u in raw_urls:
        check_u = u if u.startswith(('http://', 'https://')) else f"https://{u}"
        try:
            parsed = urlparse(check_u)
            if parsed.scheme in ["http", "https"] and parsed.netloc and "." in parsed.netloc:
                urls.append(check_u)
            else:
                invalid_urls.append(u)
        except Exception:
            invalid_urls.append(u)
    if invalid_urls:
        st.warning(f"Ignoriere {len(invalid_urls)} ungültige Einträge.")
    if urls:
        c1, c2, c3 = st.columns(3)
        if c1.button("🪄 An Editor senden (1 URL)", disabled=len(urls) > 1, use_container_width=True):
            st.session_state.switch_to_tab = 2
            task_id, _ = make_task("Hintergrund: URL -> Editor", 1)
            threading.Thread(target=background_url_batch_process, args=(task_id, [urls[0]], settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()
        if c2.button("📚 Editor-Stapel", use_container_width=True):
            st.session_state.switch_to_tab = 2
            task_id, _ = make_task("Hintergrund-Stapel: URLs -> Editor", len(urls))
            threading.Thread(target=background_url_batch_process, args=(task_id, urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()
        if c3.button("📚 Direkt-Stapel", use_container_width=True):
            st.session_state.switch_to_tab = 2
            task_id, _ = make_task("Hintergrund-Stapel: URLs -> Mealie", len(urls))
            threading.Thread(target=background_url_batch_process, args=(task_id, urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "direct"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()

with tab4:
    ui_card("Video-Import", "YouTube- oder Instagram-Links.")
    if not VIDEO_IMPORT_AVAILABLE:
        st.warning("yt-dlp fehlt.")
    video_area = st.text_area("Video Links (eine pro Zeile):", key="shared_video_input", placeholder="[https://www.youtube.com/watch?v=](https://www.youtube.com/watch?v=)...", on_change=set_active_tab, args=(3,))
    raw_vurls = [u.strip() for u in video_area.split("\n") if u.strip()]
    video_urls = []
    invalid_vurls = []
    for u in raw_vurls:
        check_u = u if u.startswith(('http://', 'https://')) else f"https://{u}"
        try:
            parsed = urlparse(check_u)
            if parsed.scheme in ["http", "https"] and parsed.netloc and "." in parsed.netloc:
                video_urls.append(check_u)
            else:
                invalid_vurls.append(u)
        except Exception:
            invalid_vurls.append(u)
    if invalid_vurls:
        st.warning(f"Ignoriere {len(invalid_vurls)} ungültige Einträge.")
    if video_urls:
        c1, c2, c3 = st.columns(3)
        if c1.button("🪄 An Editor senden (1 Video)", disabled=(len(video_urls) > 1 or not VIDEO_IMPORT_AVAILABLE), use_container_width=True):
            st.session_state.switch_to_tab = 3
            task_id, _ = make_task("Hintergrund: Video -> Editor", 1)
            threading.Thread(target=background_video_batch_process, args=(task_id, [video_urls[0]], settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()
        if c2.button("📚 Editor-Stapel (Video)", disabled=not VIDEO_IMPORT_AVAILABLE, use_container_width=True):
            st.session_state.switch_to_tab = 3
            task_id, _ = make_task("Hintergrund-Stapel: Videos -> Editor", len(video_urls))
            threading.Thread(target=background_video_batch_process, args=(task_id, video_urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()
        if c3.button("📚 Direkt-Stapel (Video)", disabled=not VIDEO_IMPORT_AVAILABLE, use_container_width=True):
            st.session_state.switch_to_tab = 3
            task_id, _ = make_task("Hintergrund-Stapel: Videos -> Mealie", len(video_urls))
            threading.Thread(target=background_video_batch_process, args=(task_id, video_urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "direct"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()

with tab5:
    ui_card("Mealie-Rezepte überarbeiten", "Rezepte aus Mealie sichten und verbessern.")
    mealie_area = st.text_area("Mealie Links:", key="shared_mealie_input", placeholder="[https://mealie.example.com/recipe/pasta](https://mealie.example.com/recipe/pasta)", on_change=set_active_tab, args=(4,))
    recipe_list = get_mealie_recipes(settings.mealie_url, settings.mealie_api_key)
    mealie_selected = st.multiselect("Dropdown:", options=[r["slug"] for r in recipe_list], format_func=lambda x: next((r["name"] for r in recipe_list if r["slug"] == x), x), on_change=set_active_tab, args=(4,))
    all_slugs = list(set(mealie_selected + [u.strip().rstrip("/").split("/")[-1] for u in mealie_area.split("\n") if u.strip()]))
    if all_slugs:
        c1, c2, c3 = st.columns(3)
        if c1.button("📥 Laden (Original)", disabled=len(all_slugs) > 1, use_container_width=True):
            st.session_state.switch_to_tab = 4
            task_id, _ = make_task("Vordergrund: Laden", 1)
            try:
                with st.status("Lade Rezept..."):
                    st.session_state.target_slug = all_slugs[0]
                    recipe = get_recipe_by_slug(settings.mealie_url, settings.mealie_api_key, all_slugs[0])
                    if recipe:
                        reset_editor_state() # Löscht die alten Prompts aus dem Speicher
                        st.session_state.target_slug = all_slugs[0]
                        st.session_state.recipe_data = {
                            "name": recipe.get("name"), "description": recipe.get("description"), "orgURL": recipe.get("orgURL"),
                            "recipeYield": recipe.get("recipeYield"), "prepTime": recipe.get("prepTime"), "cookTime": recipe.get("performTime"),
                            "tags": recipe.get("tags", []), "recipeCategory": recipe.get("recipeCategory", []),
                            "tools": recipe.get("tools", []), "nutrition": recipe.get("nutrition", {}), "recipeIngredient": recipe.get("recipeIngredient", []),
                            "recipeInstructions": recipe.get("recipeInstructions", [])
                        }
                        st.session_state.cover_image_bytes = None
                    task_update(task_id, status="abgeschlossen", current=1)
            except Exception as e:
                st.error(str(e))
            st.rerun()
        if c2.button("✨ KI-Analyse an Editor (1)", disabled=len(all_slugs) > 1, use_container_width=True):
            st.session_state.switch_to_tab = 4
            task_id, _ = make_task("Hintergrund: Mealie -> Editor", 1)
            threading.Thread(target=background_mealie_batch_process, args=(task_id, [all_slugs[0]], settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()
        if c3.button("📚 Editor-Stapel (Mealie)", use_container_width=True):
            st.session_state.switch_to_tab = 4
            task_id, _ = make_task("Hintergrund-Stapel: Mealie -> Editor", len(all_slugs))
            threading.Thread(target=background_mealie_batch_process, args=(task_id, all_slugs, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]
            st.rerun()

with tab6:
    ui_card("Statistiken & Leaderboard", "Nutzungsauswertung.")
    stats = get_user_stats_snapshot(settings.mealie_url, settings.mealie_api_key, get_current_user_key())
    c1, c2, c3 = st.columns(3)
    c1.metric("Gesamt via App", stats["total_app_uploads"])
    c2.metric("Deine Uploads", stats["personal_count"])
    c3.metric("Ersparte Zeit", f"{stats['hours_saved']} h")
    st.divider()
    col_lb, col_ing = st.columns(2)
    with col_lb:
        st.markdown("### 🏆 Leaderboard")
        for idx, user_stat in enumerate(stats.get("leaderboard", [])):
            medal = "🥇" if idx == 0 else "🥈" if idx == 1 else "🥉" if idx == 2 else f"{idx+1}."
            st.write(f"**{medal} {user_stat['label']}** — {user_stat['count']} Rezepte")
    with col_ing:
        st.markdown("### 🍳 Deine Top 5 Zutaten")
        if stats.get("top_ingredients"):
            for i, ing in enumerate(stats["top_ingredients"]):
                st.write(f"{i+1}. **{ing['name'].title()}** — in {ing['share']:.0f}%")
    st.divider()
    if stats.get("upload_rows"):
        with st.expander("Meine Historie", expanded=False):
            for row in stats["upload_rows"][:50]:
                uploaded = time.strftime("%Y-%m-%d %H:%M", time.localtime(row["last_uploaded_at"]))
                st.markdown(f"• [{row['recipe_name']}]({settings.mealie_url}/recipe/{row['recipe_slug']}) — zuletzt {uploaded}")
    
    if is_admin_user():
        st.divider()
        st.markdown("### 🛡️ Globale Historie")
        global_rows = get_all_uploaded_recipe_rows(100)
        if global_rows:
            with st.expander("Alle Nutzer Uploads ansehen", expanded=False):
                for row in global_rows:
                    uploaded = time.strftime("%Y-%m-%d %H:%M", time.localtime(row["last_uploaded_at"]))
                    label = row["user_label"] if row["user_label"] != "Anonym" else (row["user_email"] or "Anonym")
                    st.markdown(f"• **{label}**: [{row['recipe_name']}]({settings.mealie_url}/recipe/{row['recipe_slug']}) — {uploaded}")
        
        st.divider()
        st.markdown("### 🧪 System-Tests")
        st.write("Prüfe die Systemintegrität mit der automatisierten Test-Suite.")
        if st.button("▶️ Test-Suite ausführen", use_container_width=True):
            with st.spinner("Führe Tests aus... (Das kann ein paar Sekunden dauern)"):
                try:
                    test_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_app.py")
                    result = subprocess.run(
                        [sys.executable, "-m", "pytest", test_file_path, "-v", "--disable-warnings"], 
                        capture_output=True, text=True, timeout=60
                    )
                    if result.returncode == 0:
                        st.success("✅ Alle Tests erfolgreich bestanden!")
                    else:
                        st.error("❌ Einige Tests sind fehlgeschlagen oder es gab einen Fehler im Test-Code.")
                    
                    with st.expander("Test-Log (Konsolenausgabe)", expanded=True):
                        st.code(result.stdout + "\n" + result.stderr, language="text")
                except FileNotFoundError:
                    st.error("❌ Konnte 'pytest' nicht ausführen. Ist es installiert? (`pip install pytest`)")
                except Exception as e:
                    st.error(f"❌ Unerwarteter Fehler beim Ausführen der Tests: {e}")

@st.fragment(run_every="3s")
def render_editor_queue():
    editor_queue_items = get_editor_queue(get_current_user_key())
    if editor_queue_items and not st.session_state.get("recipe_data"):
        st.divider()
        ui_card("📝 Editor-Warteschlange", f"{len(editor_queue_items)} Rezept(e) bereit.")
        for q_item in editor_queue_items:
            with st.container(border=True):
                cq1, cq2, cq3 = st.columns([0.7, 0.15, 0.15])
                time_str = time.strftime("%d.%m. %H:%M", time.localtime(q_item["created_at"]))
                cq1.write(f"**{q_item['recipe_name']}** ({time_str})")
                if cq2.button("✏️ Laden", key=f"lq_{q_item['id']}", use_container_width=True):
                    reset_editor_state() # WICHTIG: Setzt Prompt und Cache für neues Rezept zurück!
                    st.session_state.recipe_data = q_item['recipe_data']
                    st.session_state.cover_image_bytes = q_item['cover_image']
                    st.session_state.current_queue_id = q_item['id']
                    st.session_state.target_slug = None
                    st.rerun()
                if cq3.button("🗑️", key=f"dq_{q_item['id']}", use_container_width=True):
                    delete_from_editor_queue(q_item['id'])
                    st.rerun()

render_editor_queue()

if st.session_state.recipe_data:
    st.divider()
    ui_card("Editor", "Prüfe und speichere dein Rezept.")
    d = st.session_state.recipe_data
    col_img, col_form = st.columns([0.25, 0.75])
    
    with col_img:
        img_placeholder = st.empty()
        if st.session_state.cover_image_bytes:
            img_placeholder.image(st.session_state.cover_image_bytes, use_container_width=True)
            
        if st.button("🗑️ Bild", use_container_width=True):
            st.session_state.cover_image_bytes = None
            st.rerun()
            
        with st.expander("🎨 KI-Bild", expanded=False):
            st.selectbox("Modell", ["imagen-4.0-generate-001", "imagen-3.0-generate-001"], key="img_model_sel")
            
            current_user_lbl = get_current_user_label()
            prompts = get_image_prompts(current_user_lbl)
            
            p_names = ["✏️ Manuell"] + [f"{p['name']} {'⭐' if p['is_default'] else ''}" for p in prompts]
            
            if "prompt_selector" not in st.session_state:
                default_idx = 0
                
                for i, p in enumerate(prompts):
                    if p["is_default"] and p.get("user_label", "") == current_user_lbl:
                        default_idx = i + 1
                        break
                        
                if default_idx == 0:
                    for i, p in enumerate(prompts):
                        if p["is_default"] and "Lars Graf" in p.get("user_label", ""):
                            default_idx = i + 1
                            break
                            
                if default_idx == 0:
                    for i, p in enumerate(prompts):
                        if p["is_default"]:
                            default_idx = i + 1
                            break

                st.session_state.prompt_selector = p_names[default_idx]
                if default_idx > 0:
                    st.session_state.img_style_txt = prompts[default_idx - 1]["text"]
                else:
                    st.session_state.img_style_txt = ""
            
            selected_prompt_name = st.selectbox("Stil-Vorlage", p_names, key="prompt_selector")
            
            if st.session_state.get("last_prompt_selector") != selected_prompt_name:
                st.session_state.last_prompt_selector = selected_prompt_name
                if selected_prompt_name != "✏️ Manuell":
                    for p in prompts:
                        if selected_prompt_name == f"{p['name']} {'⭐' if p['is_default'] else ''}":
                            st.session_state.img_style_txt = p["text"]
                            break
                else:
                    st.session_state.img_style_txt = ""

            selected_id = -1
            for p in prompts:
                if selected_prompt_name == f"{p['name']} {'⭐' if p['is_default'] else ''}":
                    selected_id = p["id"]
                    break
            
            style_txt = st.text_area("Prompt", key="img_style_txt")
            
            st.markdown("---")
            new_prompt_name = st.text_input("Name für Vorlage (zum Speichern)", placeholder="z.B. Düster & Rustikal")
            
            c_btn1, c_btn2 = st.columns(2)
            with c_btn1:
                if st.button("💾 Vorlage speichern", use_container_width=True):
                    if new_prompt_name and style_txt:
                        if "http://" in style_txt.lower() or "https://" in style_txt.lower():
                            st.error("❌ Das sieht aus wie ein Web-Link! Bitte gib einen echten Beschreibungstext für das KI-Bild ein.")
                        else:
                            save_image_prompt(new_prompt_name, style_txt, current_user_lbl)
                            toast("Vorlage gespeichert!", "✅")
                            if "prompt_selector" in st.session_state:
                                del st.session_state["prompt_selector"]
                            st.rerun()
            with c_btn2:
                if selected_id != -1:
                    if st.button("⭐ Als Standard", use_container_width=True):
                        set_default_image_prompt(selected_id, current_user_lbl)
                        toast("Als Standard markiert!", "⭐")
                        if "prompt_selector" in st.session_state:
                            del st.session_state["prompt_selector"]
                        st.rerun()
                        
            if selected_id != -1:
                if st.button("🗑️ Diese Vorlage löschen", use_container_width=True):
                    delete_image_prompt(selected_id)
                    if "prompt_selector" in st.session_state:
                        del st.session_state["prompt_selector"]
                    st.session_state.img_style_txt = ""
                    if "last_prompt_selector" in st.session_state:
                        del st.session_state["last_prompt_selector"]
                    toast("Vorlage gelöscht!", "🗑️")
                    st.rerun()

            if st.button("✨ Generieren", use_container_width=True, type="primary"):
                blurred_img = get_blur_placeholder(st.session_state.cover_image_bytes)
                img_placeholder.image(blurred_img, use_container_width=True, caption="✨ Bild wird generiert...")
                
                with st.spinner("Koche Bild..."):
                    client_bundle = create_genai_client(settings.gemini_api_key)
                    selected_model = st.session_state.get("img_model_sel", "imagen-4.0-generate-001")
                    new_img = generate_recipe_image_with_gemini(client_bundle, d.get("name"), "", image_model=selected_model, custom_style=style_txt)
                    if new_img:
                        st.session_state.cover_image_bytes = new_img
                        st.rerun()
                    else:
                        st.error("Bild konnte nicht generiert werden. Bitte prüfe die Docker-Logs für Details.")
                        img_placeholder.empty()
                        if st.session_state.cover_image_bytes:
                            img_placeholder.image(st.session_state.cover_image_bytes, use_container_width=True)
                        
        if st.button("💾 Speichern", use_container_width=True, type="primary"):
            u_email = get_current_user_email()
            m_uid = get_mealie_user_id_by_email(settings.mealie_url, settings.mealie_api_key, u_email)
            success, result = direct_save_to_mealie(d, settings.mealie_url, settings.mealie_api_key, st.session_state.cover_image_bytes, audit_user_key=get_current_user_key(), audit_user_label=get_current_user_label(), audit_user_email=u_email, mealie_user_id=m_uid)
            if success:
                st.session_state.upload_success = [result]
                if st.session_state.get("current_queue_id"):
                    delete_from_editor_queue(st.session_state.current_queue_id)
                reset_editor_state()
                st.rerun()
                
    with col_form:
        st.session_state.recipe_data["name"] = st.text_input("Name", d.get("name"))
        st.session_state.recipe_data["description"] = st.text_area("Beschreibung", d.get("description"))
        
        c1, c2, c3 = st.columns(3)
        recipe_yield = c1.text_input("Portionen", str(extract_servings_number(d.get("recipeYield")) or d.get("recipeYield", "")))
        prep = c2.text_input("Vorbereitung", d.get("prepTime", ""))
        cook = c3.text_input("Zubereitung", d.get("cookTime", ""))
        
        st.session_state.recipe_data["recipeYield"] = str(extract_servings_number(recipe_yield) or recipe_yield)
        st.session_state.recipe_data["prepTime"] = prep
        st.session_state.recipe_data["cookTime"] = cook
        
        # --- Nährwerte Editor ---
        st.markdown("#### 📊 Geschätzte Nährwerte (pro Portion)")
        nut = d.get("nutrition", {})
        n_cols = st.columns(4)
        kcal = n_cols[0].text_input("Kalorien (kcal)", nut.get("calories", ""))
        carbs = n_cols[1].text_input("Kohlenhydrate (g)", nut.get("carbohydrateContent", ""))
        prot = n_cols[2].text_input("Eiweiß (g)", nut.get("proteinContent", ""))
        fat = n_cols[3].text_input("Fett (g)", nut.get("fatContent", ""))
        
        st.session_state.recipe_data["nutrition"] = {
            "calories": kcal, 
            "carbohydrateContent": carbs,
            "proteinContent": prot, 
            "fatContent": fat
        }

        st.markdown("#### 🍎 Zutaten")
        ing_list = []
        for idx, i in enumerate(d.get("recipeIngredient", [])):
            ing_list.append({
                "RefID": i.get("referenceId") or f"ing{idx+1}",
                "Sektion": i.get("title", ""),
                "Menge": i.get("quantity"), 
                "Einheit": get_nested_name(i.get("unit")), 
                "Zutat": get_nested_name(i.get("food")),
                "Notiz": i.get("note", ""),
                "Original": i.get("originalText", "")
            })
        edited_ings = st.data_editor(ing_list, num_rows="dynamic", use_container_width=True, key="ings")
        st.session_state.recipe_data["recipeIngredient"] = [
            {
                "referenceId": clean_str(r.get("RefID")) or f"ing{idx+1}",
                "title": clean_str(r.get("Sektion")) or None, 
                "quantity": r.get("Menge"), 
                "unit": {"name": clean_str(r.get("Einheit"))} if clean_str(r.get("Einheit")) else None, 
                "food": {"name": clean_str(r.get("Zutat"))} if clean_str(r.get("Zutat")) else None, 
                "note": clean_str(r.get("Notiz")), 
                "originalText": clean_str(r.get("Original"))
            } 
            for idx, r in enumerate(edited_ings)
        ]

        st.markdown("#### 👨‍🍳 Zubereitung")
        step_list = []
        for idx, s in enumerate(d.get("recipeInstructions", [])):
            step_list.append({
                "Titel": s.get("title", "") or f"Schritt {idx+1}",
                "Text": s.get("text", ""),
                "Refs": ", ".join(
                    clean_str(ref.get("referenceId"))
                    for ref in s.get("ingredientReferences", [])
                    if clean_str(ref.get("referenceId"))
                )
            })
        edited_steps = st.data_editor(step_list, num_rows="dynamic", use_container_width=True, key="steps")
        st.session_state.recipe_data["recipeInstructions"] = [
            {
                "title": clean_str(r.get("Titel")) or f"Schritt {idx+1}", 
                "text": clean_str(r.get("Text")),
                "ingredientReferences": [
                    {"referenceId": ref.strip()}
                    for ref in str(r.get("Refs", "")).split(",")
                    if ref.strip()
                ]
            } 
            for idx, r in enumerate(edited_steps)
            if clean_str(r.get("Text"))
        ]

        st.markdown("🧑‍🍳 **KI-Sous-Chef**")
        for msg in st.session_state.sous_chef_history:
            with st.chat_message(msg["role"]): st.write(msg["content"])
        chef_prompt = st.chat_input("Anweisung an Sous-Chef...")
        if chef_prompt:
            st.session_state.sous_chef_history.append({"role": "user", "content": chef_prompt})
            client_bundle = create_genai_client(settings.gemini_api_key)
            updated, expl = editor_transform_recipe(client_bundle, st.session_state.recipe_data, chef_prompt)
            st.session_state.recipe_data = updated
            st.session_state.sous_chef_history.append({"role": "assistant", "content": expl})
            st.rerun()
            
    if st.button("❌ Verwerfen", use_container_width=True):
        reset_editor_state()
        st.rerun()
