import base64
import html
import logging
import threading
import time
import os
import re
import math
import subprocess
import sys
import gc
from urllib.parse import urlparse

import streamlit as st
import streamlit.components.v1 as components
from typing import Any, Dict, List

# Core imports
from src.core import (
    settings, logger, ensure_streamlit_config, ensure_pwa_assets, init_cache_db,
    inject_pwa_bootstrap, has_streamlit_auth, is_streamlit_user_logged_in, 
    get_current_user_email, get_current_user_label, get_current_user_key, is_admin_user,
    register_active_user, get_active_users_snapshot, generate_extension_zip,
    get_image_prompts, save_image_prompt, set_default_image_prompt, delete_image_prompt,
    get_editor_queue, delete_from_editor_queue, clean_str, extract_servings_number, format_duration,
    get_all_uploaded_recipe_rows, get_nested_name
)

# Services imports
from src.services import (
    get_mealie_recipes, get_mealie_data_maps, get_mealie_user_id_by_email, get_recipe_by_slug,
    create_genai_client, editor_transform_recipe, generate_recipe_image_with_gemini,
    direct_save_to_mealie, get_user_stats_snapshot, get_blur_placeholder,
    get_prompt, get_pdf_prompt, VIDEO_IMPORT_AVAILABLE, extract_mealie_slug
)

# Tasks imports
from src.tasks import (
    get_task_registry, get_task_lock, compute_task_metrics, get_running_tasks_snapshot, make_task,
    background_image_batch_process, background_url_batch_process, background_video_batch_process,
    background_pdf_batch_process, background_mealie_batch_process
)

MAX_BATCH_RECIPES = 40


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
    :root {{ --primary:#6750a4; --on-primary:#fff; --surface:{surface}; --text:{text}; --text-muted:{text_muted}; --outline:{outline}; --surface-soft:{'#2a2632' if is_dark else '#f3edf7'}; --shadow-1:0 1px 2px rgba(0,0,0,.10),0 1px 3px 1px rgba(0,0,0,.08); --shadow-2:0 2px 6px rgba(0,0,0,.12),0 1px 2px rgba(0,0,0,.08); }}
    .stApp {{ background:{bg}; }}
    .block-container {{ max-width:1200px; padding-top:1rem; padding-bottom:2rem; }}
    header[data-testid="stHeader"] {{ background:rgba(0,0,0,0)!important; border-bottom:none!important; }}
    #MainMenu {{ visibility:hidden!important; }}
    [data-testid="collapsedControl"] {{ position:fixed!important; top:.8rem!important; left:.9rem!important; z-index:1001!important; }}
    button[kind="header"], [data-testid="collapsedControl"] button {{ background:#000!important; border:3px solid #fff!important; border-radius:16px!important; box-shadow:0 0 0 4px rgba(0,0,0,.22), var(--shadow-2)!important; color:#fff!important; min-width:46px!important; min-height:46px!important; }}
    button[kind="header"] svg, [data-testid="collapsedControl"] button svg {{ fill:#fff!important; color:#fff!important; opacity:1!important; }}
    [data-testid="stSidebar"] {{ background:{sidebar}; border-right:1px solid var(--outline); }}
    [data-testid="stSidebar"] * {{ color:{text}!important; }}
    [data-testid="stSidebar"] .stButton>button, [data-testid="stSidebar"] [data-testid="stExpander"] {{ background:{surface}!important; border:1px solid var(--outline)!important; }}
    .snap-appbar {{ display:flex; align-items:center; justify-content:space-between; gap:1rem; margin-bottom:.8rem; }}
    .snap-appbar h1 {{ margin:0; font-size:2rem; line-height:1; color:var(--text); text-transform:lowercase; letter-spacing:-.03em; }}
    .snap-badges {{ display:flex; gap:.45rem; flex-wrap:wrap; justify-content:flex-end; align-items:center; min-height:34px; }}
    .snap-badge {{ display:inline-flex; align-items:center; min-height:34px; padding:.35rem .7rem; border-radius:999px; background:{'#3b2f59' if is_dark else '#e9ddff'}; color:{'#f1e7ff' if is_dark else '#22005d'}; font-size:.78rem; font-weight:700; box-shadow:var(--shadow-1); }}
    [data-testid="stPopover"] button {{ min-width:34px!important; min-height:34px!important; height:34px!important; background:transparent!important; border:none!important; box-shadow:none!important; color:var(--text)!important; font-size:1.35rem!important; padding:0!important; }}
    [data-testid="stPopover"] button::after, [data-testid="stPopover"] button svg:last-of-type {{ display:none!important; }}
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
    if hasattr(st, "popover"):
        with st.popover("☰"):
            selected = st.radio("Design", options, index=current_index, key="theme_mode_menu")
            if (new_mode := "light" if selected == "Hell" else "dark") != st.session_state.get("theme_mode", "dark"):
                st.session_state.theme_mode = new_mode; st.rerun()
    else:
        selected = st.selectbox("Design", options, index=current_index, key="theme_mode_menu_fallback")
        if (new_mode := "light" if selected == "Hell" else "dark") != st.session_state.get("theme_mode", "dark"):
            st.session_state.theme_mode = new_mode; st.rerun()

def render_header() -> None:
    left, right = st.columns([0.48, 0.52], vertical_alignment="center")
    with left: st.markdown("<div class='snap-appbar'><h1>snap to mealie</h1></div>", unsafe_allow_html=True)
    with right:
        a, b = st.columns([0.9, 0.1], vertical_alignment="center")
        with a: st.markdown(f"<div class='snap-badges'><span class='snap-badge'>{html.escape(settings.gemini_model)}</span></div>", unsafe_allow_html=True)
        with b: render_theme_menu()

def ui_card(title: str, subtitle: str = "") -> None:
    st.markdown(f"<div class='snap-card'><h3>{html.escape(title)}</h3>{f'<p>{html.escape(subtitle)}</p>' if subtitle else ''}</div>", unsafe_allow_html=True)

def toast(message: str, icon: str = "ℹ️") -> None:
    if hasattr(st, "toast"): st.toast(message, icon=icon)
    else: st.info(f"{icon} {message}")


# -----------------------------------------------------------------------------
# Init & Routing helpers
# -----------------------------------------------------------------------------
def init_session() -> None:
    for key, default in {"recipe_data": None, "upload_success": [], "collected_images": [], "collected_pdfs": [], "cover_image_bytes": None, "target_slug": None, "cropper_open": False, "sous_chef_history": [], "shared_urls_input": "", "shared_video_input": "", "shared_mealie_input": "", "switch_to_tab": None, "current_queue_id": None, "theme_mode": "dark"}.items():
        if key not in st.session_state: st.session_state[key] = default

def reset_editor_state() -> None:
    st.session_state.update({"recipe_data": None, "cover_image_bytes": None, "target_slug": None, "sous_chef_history": [], "current_queue_id": None})
    for key in ["prompt_selector", "img_style_txt", "last_prompt_selector"]:
        if key in st.session_state: del st.session_state[key]

def clear_images_from_state() -> None: st.session_state.collected_images = []; gc.collect()

def shift_image(idx: int, direction: int) -> None:
    imgs = st.session_state.collected_images
    if direction == -1 and idx > 0: imgs[idx], imgs[idx-1] = imgs[idx-1], imgs[idx]
    elif direction == 1 and idx < len(imgs) - 1: imgs[idx], imgs[idx+1] = imgs[idx+1], imgs[idx]
    st.session_state.switch_to_tab = 0

def swap_pair(idx: int) -> None:
    imgs = st.session_state.collected_images
    if idx + 1 < len(imgs): imgs[idx], imgs[idx+1] = imgs[idx+1], imgs[idx]
    st.session_state.switch_to_tab = 0

def remove_image(idx: int) -> None:
    if 0 <= idx < len(st.session_state.collected_images): st.session_state.collected_images.pop(idx)
    st.session_state.switch_to_tab = 0

def set_active_tab(idx: int): st.session_state.switch_to_tab = idx

def save_all_editor_queue_to_mealie() -> None:
    queue_items = get_editor_queue(get_current_user_key())
    if not queue_items:
        toast("Keine Rezepte in der Warteschlange.", "ℹ️")
        return

    u_email, u_label = get_current_user_email(), get_current_user_label()
    m_uid = get_mealie_user_id_by_email(settings.mealie_url, settings.mealie_api_key, u_email)
    saved_slugs, errors = [], []

    with st.spinner(f"Speichere {len(queue_items)} Rezept(e) nach Mealie..."):
        for item in queue_items:
            ok, res = direct_save_to_mealie(
                item["recipe_data"],
                settings.mealie_url,
                settings.mealie_api_key,
                item.get("cover_image"),
                audit_user_key=get_current_user_key(),
                audit_user_label=u_label,
                audit_user_email=u_email,
                mealie_user_id=m_uid,
            )
            if ok:
                saved_slugs.append(res)
                delete_from_editor_queue(item["id"])
            else:
                errors.append(f"{item.get('recipe_name', 'Unbekannt')}: {res}")

    if saved_slugs:
        st.session_state.upload_success = list(saved_slugs)
        toast(f"{len(saved_slugs)} Rezept(e) gespeichert.", "✅")
    if errors:
        for err in errors[:8]:
            st.error(f"❌ {err}")
        if len(errors) > 8:
            st.warning(f"... und {len(errors) - 8} weitere Fehler.")

def render_editor_queue_body() -> None:
    if (eq := get_editor_queue(get_current_user_key())) and not st.session_state.get("recipe_data"):
        st.divider()
        ui_card("📝 Editor-Warteschlange", f"{len(eq)} Rezept(e) bereit.")
        if st.button("💾 Alle direkt nach Mealie speichern", use_container_width=True, type="primary", key="save_all_editor_queue"):
            save_all_editor_queue_to_mealie()
            st.rerun()
        for q in eq:
            with st.container(border=True):
                cq1, cq2, cq3 = st.columns([0.7, 0.15, 0.15])
                cq1.write(f"**{q['recipe_name']}** ({time.strftime('%d.%m. %H:%M', time.localtime(q['created_at']))})")
                if cq2.button("✏️ Laden", key=f"lq_{q['id']}", use_container_width=True):
                    reset_editor_state(); st.session_state.update({"recipe_data": q['recipe_data'], "cover_image_bytes": q['cover_image'], "current_queue_id": q['id'], "target_slug": None}); st.rerun()
                if cq3.button("🗑️", key=f"dq_{q['id']}", use_container_width=True): delete_from_editor_queue(q['id']); st.rerun()


def _render_task_monitor_body() -> None:
    max_visible_logs = 8
    max_visible_errors = 5

    if st.button("🗑️ Historie leeren", use_container_width=True):
        with get_task_lock():
            for tid in [tid for tid, t in get_task_registry().items() if t["status"] in ["abgeschlossen", "abgebrochen"]]: del get_task_registry()[tid]
        st.rerun()
    if st.button("🧹 Hängende Tasks löschen", use_container_width=True):
        with get_task_lock():
            for tid in [tid for tid, t in get_task_registry().items() if t.get("status") == "running"]: del get_task_registry()[tid]
        toast("Laufende Tasks wurden aus der Sidebar entfernt.", "🧹")
        st.rerun()

    for t_id, task in list(get_task_registry().items()):
        icon = "⏳" if task["status"] == "running" else "🛑" if task["status"] == "abgebrochen" else "❌" if task.get("errors") and task.get("total", 0) > 0 and len(task.get("errors", [])) >= task.get("total", 0) else "⚠️" if task.get("errors") else "✅"
        metrics = compute_task_metrics(task)
        with st.expander(f"{icon} {task['name']} ({task['current']}/{task['total']}){f' · {task.get(chr(111)+chr(119)+chr(110)+chr(101)+chr(114))}' if task.get('owner') else ''}"):
            if task["total"] > 0: st.progress(task["current"] / task["total"])
            st.caption(f"Status: {task.get('last_detail', 'Wird vorbereitet')} · Tempo: {metrics['rpm']:.2f} Rezepte/Min · ETA: {format_duration(metrics['eta_seconds'])} · Laufzeit: {format_duration(metrics['elapsed'])}")
            if task["status"] == "running":
                ct1, ct2 = st.columns(2)
                if ct1.button("🛑 Stoppen", key=f"stop_{t_id}", use_container_width=True):
                    with get_task_lock():
                        get_task_registry()[t_id]["stop_requested"] = True; get_task_registry()[t_id]["last_detail"] = "Task wird gestoppt"
                    st.rerun()
                if ct2.button("🗑️ Löschen", key=f"delr_{t_id}", use_container_width=True):
                    with get_task_lock():
                        if t_id in get_task_registry(): del get_task_registry()[t_id]
                    st.rerun()
            errors = task.get("errors", [])
            logs = task.get("logs", [])
            hidden_errors = max(0, len(errors) - max_visible_errors)
            hidden_logs = max(0, len(logs) - max_visible_logs)
            if hidden_errors:
                st.caption(f"… {hidden_errors} ältere Fehler ausgeblendet (Performance).")
            for err in errors[-max_visible_errors:]:
                st.error(err)
            if hidden_logs:
                st.caption(f"… {hidden_logs} ältere Erfolgsmeldungen ausgeblendet (Performance).")
            for log in logs[-max_visible_logs:]:
                st.success(log)
            if task["status"] in ["abgeschlossen", "abgebrochen"] and st.button("Eintrag ausblenden", key=f"del_{t_id}", use_container_width=True):
                with get_task_lock():
                    if t_id in get_task_registry(): del get_task_registry()[t_id]
                st.rerun()

    if is_admin_user():
        with st.expander("🛡️ Adminpanel", expanded=False):
            st.markdown("### 🧩 Browser-Erweiterung")
            st.download_button(label="📦 Addon herunterladen (.zip)", data=generate_extension_zip(), file_name="snap-to-mealie-addon.zip", mime="application/zip", use_container_width=True)
            st.divider()
            st.markdown("**Aktive Nutzer**")
            if active_users := get_active_users_snapshot():
                for u in active_users:
                    lbl = u.get('label') or 'Unbekannt'
                    eml = u.get('email')
                    eml_str = f" — {eml}" if eml and eml != lbl else ""
                    ago = max(0, int(time.time() - u.get('last_seen', time.time())))
                    st.write(f"• {lbl}{eml_str} · aktiv vor {ago}s")
            else: st.info("Keine aktiven Nutzer erkannt.")
            st.markdown("**Laufende Rezeptumwandlungen**")
            if running_tasks := get_running_tasks_snapshot():
                for task in running_tasks:
                    m = compute_task_metrics(task)
                    st.write(f"• {task.get('name', 'Task')} — {task.get('owner', 'Unbekannt')} ({task.get('current', 0)}/{task.get('total', 0)}) · {m['rpm']:.2f} Rezepte/Min · ETA {format_duration(m['eta_seconds'])}")
            else: st.info("Aktuell laufen keine Rezeptumwandlungen.")

if hasattr(st, "fragment"):
    @st.fragment(run_every="3s")
    def render_task_monitor() -> None: _render_task_monitor_body()
    @st.fragment(run_every="3s")
    def render_editor_queue(): render_editor_queue_body()
else:
    def render_task_monitor() -> None: _render_task_monitor_body()
    def render_editor_queue(): render_editor_queue_body()

# -----------------------------------------------------------------------------
# APP INIT
# -----------------------------------------------------------------------------
ensure_streamlit_config()
ensure_pwa_assets()
init_cache_db()
st.set_page_config(page_title="Snap-to-Mealie", page_icon="📸", layout="wide", initial_sidebar_state="expanded")
init_session()
inject_ui(st.session_state.theme_mode)
inject_pwa_bootstrap()

if settings.oidc_client_id and has_streamlit_auth():
    if not is_streamlit_user_logged_in(): st.title("🔒 Authentifizierung erforderlich"); st.login("custom"); st.stop()
    elif st.sidebar.button("Abmelden"): st.logout()
elif settings.oidc_client_id and not has_streamlit_auth():
    st.sidebar.warning("OIDC ist gesetzt, aber diese Streamlit-Version unterstützt st.user/st.login nicht vollständig.")

if not settings.mealie_api_key or not settings.gemini_api_key: st.error("API-Schlüssel fehlen."); st.stop()

client = create_genai_client(settings.gemini_api_key)
register_active_user()

try:
    _shared_content = f"{st.query_params.get('title', '')} {st.query_params.get('text', '')} {st.query_params.get('shared_url', '')}"
    if _extracted_urls := re.findall(r'https?://[^\s]+', _shared_content):
        _mealie_domain = settings.mealie_url.split("://")[-1].split(":")[0]
        _known_slugs = [r["slug"] for r in get_mealie_recipes(settings.mealie_url, settings.mealie_api_key)]
        _added_mealie, _added_video, _added_url = False, False, False

        for u in _extracted_urls:
            possible_slug = extract_mealie_slug(u)
            if any(d in u for d in ["youtube.com", "youtu.be", "instagram.com"]):
                if u not in st.session_state.get("shared_video_input", ""): st.session_state.shared_video_input = (st.session_state.get("shared_video_input", "") + "\n" + u).strip(); _added_video = True
            elif _mealie_domain in u or "mealie" in u.lower():
                resolved_slug = possible_slug if "/recipe/" in u or possible_slug in _known_slugs else None
                if not resolved_slug:
                    try:
                        import requests
                        _resp = requests.get(u, timeout=5)
                        from src.services import extract_recipe_jsonld_text
                        _jsonld = extract_recipe_jsonld_text(_resp.text)
                        if _t_match := re.search(r"Titel:\s*(.+)", _jsonld) or re.search(r"<title>(.*?)</title>", _resp.text, re.IGNORECASE):
                            from src.services import search_recipe_slug_by_name
                            resolved_slug = search_recipe_slug_by_name(settings.mealie_url, settings.mealie_api_key, _t_match.group(1).split("|")[0].strip() if hasattr(_t_match, 'group') else None)
                    except Exception: pass
                if resolved_slug and resolved_slug in _known_slugs:
                    if (val := f"{settings.mealie_url}/recipe/{resolved_slug}") not in st.session_state.get("shared_mealie_input", ""): st.session_state.shared_mealie_input = (st.session_state.get("shared_mealie_input", "") + "\n" + val).strip(); _added_mealie = True
                else:
                    if u not in st.session_state.get("shared_urls_input", ""): st.session_state.shared_urls_input = (st.session_state.get("shared_urls_input", "") + "\n" + u).strip(); _added_url = True
            else:
                if u not in st.session_state.get("shared_urls_input", ""): st.session_state.shared_urls_input = (st.session_state.get("shared_urls_input", "") + "\n" + u).strip(); _added_url = True

        if _added_mealie: toast("🔗 Mealie-Rezept erkannt!", icon="🔄"); st.session_state.switch_to_tab = 4
        elif _added_video: toast("🔗 Video-URL empfangen!", icon="🎥"); st.session_state.switch_to_tab = 3
        elif _added_url: toast("🔗 Web-URL empfangen!", icon="🌐"); st.session_state.switch_to_tab = 2
        st.query_params.clear()
except Exception: pass

with st.sidebar: render_task_monitor()

render_header()

if not st.session_state.get("recipe_data"):
    if st.session_state.get("shared_mealie_input"): st.info("🔗 **Mealie-Rezept empfangen!** Klicke auf '🪄 An Editor senden'.", icon="📲")
    elif st.session_state.get("shared_video_input"): st.info("🔗 **Video-Link empfangen!** Klicke auf '🪄 An Editor senden'.", icon="📲")
    elif st.session_state.get("shared_urls_input"): st.info("🔗 **Web-URL empfangen!** Klicke auf '🪄 An Editor senden'.", icon="📲")

if st.session_state.upload_success:
    for item in st.session_state.upload_success:
        if item == "BACKGROUND_TASK_STARTED": toast("Hintergrund-Aufgabe gestartet", "🚀")
        else: toast(f"Rezept gespeichert: {item}", "🍳"); st.success(f"🎉 Gespeichert! [Hier ansehen]({settings.mealie_url}/recipe/{item})")
    st.session_state.upload_success = []

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📁 Datei-Import", "📷 Kamera", "🌐 URL Import", "🎥 Video Import", "🔄 Mealie Rezept", "📊 Statistiken"])

if st.session_state.get("switch_to_tab") is not None:
    tab_idx = st.session_state.switch_to_tab
    components.html(f"<script>setTimeout(() => {{ window.parent.document.querySelectorAll('.stTabs [role=\"tab\"]')[{tab_idx}].click(); }}, 150);</script>", height=0)
    st.session_state.switch_to_tab = None

with tab1:
    ui_card("Datei-Import (Bilder & PDFs)", "Ziehe Fotos von Rezepten oder ganze PDF-Kochbücher hinein.")
    if uploaded_files := st.file_uploader("Dateien hinzufügen", type=["jpg", "jpeg", "png", "pdf"], accept_multiple_files=True, on_change=set_active_tab, args=(0,)):
        for f in sorted(uploaded_files, key=lambda x: x.name):
            data = f.getvalue()
            if f.name.lower().endswith(".pdf") and data not in st.session_state.collected_pdfs: st.session_state.collected_pdfs.append(data)
            elif not f.name.lower().endswith(".pdf") and data not in st.session_state.collected_images: st.session_state.collected_images.append(data)

with tab2:
    ui_card("Kamera", "Fotografiere direkt mit dem Gerät.")
    if not st.session_state.get("camera_active"):
        st.info("Klicke auf den Button, um die Kamera zu starten. Erst dann wird nach der Berechtigung gefragt.")
        if st.button("📷 Kamera aktivieren", use_container_width=True): st.session_state.camera_active = True; st.session_state.switch_to_tab = 1; st.rerun()
    else:
        if camera_image := st.camera_input("Foto aufnehmen", on_change=set_active_tab, args=(1,)):
            if (data := camera_image.getvalue()) not in st.session_state.collected_images: st.session_state.collected_images.append(data); toast("Foto zur Sammlung hinzugefügt!", "📸")
        if st.button("❌ Kamera wieder schließen", use_container_width=True): st.session_state.camera_active = False; st.session_state.switch_to_tab = 1; st.rerun()

if st.session_state.collected_pdfs:
    st.divider(); ui_card("Aktuelle PDF-Sammlung", f"Es liegen {len(st.session_state.collected_pdfs)} PDF-Dokument(e) bereit.")
    c1, c2, c3 = st.columns(3)
    if c1.button("🪄 An Editor senden (PDF)", use_container_width=True, type="primary"):
        selected_pdfs = list(st.session_state.collected_pdfs)[:MAX_BATCH_RECIPES]
        if len(st.session_state.collected_pdfs) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} Dateien verarbeitet.")
        st.session_state.switch_to_tab = 0; task_id, _ = make_task("Hintergrund: PDF -> Editor", len(selected_pdfs))
        import threading; threading.Thread(target=background_pdf_batch_process, args=(task_id, selected_pdfs, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_pdf_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.session_state.collected_pdfs = []; st.rerun()
    if c2.button("📚 Direkt-Import (PDF)", use_container_width=True):
        selected_pdfs = list(st.session_state.collected_pdfs)[:MAX_BATCH_RECIPES]
        if len(st.session_state.collected_pdfs) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} Dateien verarbeitet.")
        st.session_state.switch_to_tab = 0; task_id, _ = make_task("Hintergrund: PDF -> Mealie", len(selected_pdfs))
        import threading; threading.Thread(target=background_pdf_batch_process, args=(task_id, selected_pdfs, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_pdf_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "direct"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.session_state.collected_pdfs = []; st.rerun()
    if c3.button("🗑️ PDFs verwerfen", use_container_width=True): st.session_state.switch_to_tab = 0; st.session_state.collected_pdfs = []; st.rerun()

if st.session_state.collected_images:
    st.divider(); ui_card("Aktuelle Bildsammlung", "Ordne die Bilder in der korrekten Reihenfolge.")
    pair_mode = st.checkbox("🤝 Bilder paarweise verarbeiten (1. Bild = Cover, 2. Bild = Text)", value=True, on_change=set_active_tab, args=(0,))
    if pair_mode:
        for i in range(0, len(st.session_state.collected_images), 2):
            with st.container(border=True):
                st.markdown(f"**Rezept { (i // 2) + 1 }**"); cols = st.columns([0.42, 0.16, 0.42], vertical_alignment="center")
                with cols[0]:
                    st.image(st.session_state.collected_images[i], use_container_width=True, caption="🖼️ Cover-Bild")
                    c_a, c_b, c_c = st.columns(3)
                    c_a.button("⬅️", key=f"l_{i}", on_click=shift_image, args=(i, -1), disabled=(i==0), use_container_width=True)
                    c_b.button("🗑️", key=f"d_{i}", on_click=remove_image, args=(i,), use_container_width=True)
                    c_c.button("➡️", key=f"r_{i}", on_click=shift_image, args=(i, 1), disabled=(i==len(st.session_state.collected_images)-1), use_container_width=True)
                with cols[1]:
                    if i + 1 < len(st.session_state.collected_images): st.button("🔄 Tauschen", key=f"swap_{i}", on_click=swap_pair, args=(i,), use_container_width=True)
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
        st.session_state.switch_to_tab = 0; task_id, _ = make_task("Hintergrund: Bild -> Editor", 1)
        import threading; threading.Thread(target=background_image_batch_process, args=(task_id, list(st.session_state.collected_images), settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), pair_mode, False, "editor"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; clear_images_from_state(); st.rerun()

    st.markdown("### Jedes Bild / Paar als eigenes Rezept")
    c1, c2, c3 = st.columns(3)
    if c1.button("📚 Editor-Stapel", use_container_width=True):
        selected_images = list(st.session_state.collected_images)[:MAX_BATCH_RECIPES * (2 if pair_mode else 1)]
        total = math.ceil(len(selected_images) / 2) if pair_mode else len(selected_images)
        if total > MAX_BATCH_RECIPES:
            selected_images = selected_images[:MAX_BATCH_RECIPES * 2]
            total = MAX_BATCH_RECIPES
        if (math.ceil(len(st.session_state.collected_images) / 2) if pair_mode else len(st.session_state.collected_images)) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} verarbeitet.")
        st.session_state.switch_to_tab = 0; task_id, _ = make_task("Hintergrund-Stapel: Bilder -> Editor", total)
        import threading; threading.Thread(target=background_image_batch_process, args=(task_id, selected_images, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), pair_mode, True, "editor"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; clear_images_from_state(); st.rerun()
    if c2.button("📚 Direkt-Stapel", use_container_width=True):
        selected_images = list(st.session_state.collected_images)[:MAX_BATCH_RECIPES * (2 if pair_mode else 1)]
        total = math.ceil(len(selected_images) / 2) if pair_mode else len(selected_images)
        if total > MAX_BATCH_RECIPES:
            selected_images = selected_images[:MAX_BATCH_RECIPES * 2]
            total = MAX_BATCH_RECIPES
        if (math.ceil(len(st.session_state.collected_images) / 2) if pair_mode else len(st.session_state.collected_images)) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} verarbeitet.")
        st.session_state.switch_to_tab = 0; task_id, _ = make_task("Hintergrund-Stapel: Bilder -> Mealie", total)
        import threading; threading.Thread(target=background_image_batch_process, args=(task_id, selected_images, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), pair_mode, True, "direct"), daemon=False).start()
        st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; clear_images_from_state(); st.rerun()
    if c3.button("🗑️ Verwerfen", use_container_width=True): st.session_state.switch_to_tab = 0; clear_images_from_state(); reset_editor_state(); st.rerun()

if not st.session_state.collected_images and not st.session_state.collected_pdfs: ui_card("Noch keine Dateien geladen", "Starte mit Datei-Upload oder Kamera.")

with tab3:
    ui_card("URL-Import", "Füge eine oder mehrere Rezept-URLs ein.")
    url_area = st.text_area("URLs (eine pro Zeile):", key="shared_urls_input", placeholder="https://example.com/rezept-1", on_change=set_active_tab, args=(2,))
    urls, invalid_urls = [], []
    for u in [u.strip() for u in url_area.split("\n") if u.strip()]:
        check_u = u if u.startswith(('http://', 'https://')) else f"https://{u}"
        try:
            parsed = urlparse(check_u)
            urls.append(check_u) if parsed.scheme in ["http", "https"] and parsed.netloc and "." in parsed.netloc else invalid_urls.append(u)
        except Exception: invalid_urls.append(u)
    if invalid_urls: st.warning(f"Ignoriere {len(invalid_urls)} ungültige Einträge.")
    if urls:
        c1, c2, c3 = st.columns(3)
        if c1.button("🪄 An Editor senden (1 URL)", disabled=len(urls) > 1, use_container_width=True):
            st.session_state.switch_to_tab = 2; task_id, _ = make_task("Hintergrund: URL -> Editor", 1)
            import threading; threading.Thread(target=background_url_batch_process, args=(task_id, [urls[0]], settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()
        if c2.button("📚 Editor-Stapel", use_container_width=True):
            selected_urls = urls[:MAX_BATCH_RECIPES]
            if len(urls) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} URLs verarbeitet.")
            st.session_state.switch_to_tab = 2; task_id, _ = make_task("Hintergrund-Stapel: URLs -> Editor", len(selected_urls))
            import threading; threading.Thread(target=background_url_batch_process, args=(task_id, selected_urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()
        if c3.button("📚 Direkt-Stapel", use_container_width=True):
            selected_urls = urls[:MAX_BATCH_RECIPES]
            if len(urls) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} URLs verarbeitet.")
            st.session_state.switch_to_tab = 2; task_id, _ = make_task("Hintergrund-Stapel: URLs -> Mealie", len(selected_urls))
            import threading; threading.Thread(target=background_url_batch_process, args=(task_id, selected_urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "direct"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()

with tab4:
    ui_card("Video-Import", "YouTube- oder Instagram-Links.")
    if not VIDEO_IMPORT_AVAILABLE: st.warning("yt-dlp fehlt.")
    video_area = st.text_area("Video Links (eine pro Zeile):", key="shared_video_input", placeholder="https://www.youtube.com/watch?v=...", on_change=set_active_tab, args=(3,))
    video_urls, invalid_vurls = [], []
    for u in [u.strip() for u in video_area.split("\n") if u.strip()]:
        check_u = u if u.startswith(('http://', 'https://')) else f"https://{u}"
        try:
            parsed = urlparse(check_u)
            video_urls.append(check_u) if parsed.scheme in ["http", "https"] and parsed.netloc and "." in parsed.netloc else invalid_vurls.append(u)
        except Exception: invalid_vurls.append(u)
    if invalid_vurls: st.warning(f"Ignoriere {len(invalid_vurls)} ungültige Einträge.")
    if video_urls:
        c1, c2, c3 = st.columns(3)
        if c1.button("🪄 An Editor senden (1 Video)", disabled=(len(video_urls) > 1 or not VIDEO_IMPORT_AVAILABLE), use_container_width=True):
            st.session_state.switch_to_tab = 3; task_id, _ = make_task("Hintergrund: Video -> Editor", 1)
            import threading; threading.Thread(target=background_video_batch_process, args=(task_id, [video_urls[0]], settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()
        if c2.button("📚 Editor-Stapel (Video)", disabled=not VIDEO_IMPORT_AVAILABLE, use_container_width=True):
            selected_video_urls = video_urls[:MAX_BATCH_RECIPES]
            if len(video_urls) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} Video-Links verarbeitet.")
            st.session_state.switch_to_tab = 3; task_id, _ = make_task("Hintergrund-Stapel: Videos -> Editor", len(selected_video_urls))
            import threading; threading.Thread(target=background_video_batch_process, args=(task_id, selected_video_urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()
        if c3.button("📚 Direkt-Stapel (Video)", disabled=not VIDEO_IMPORT_AVAILABLE, use_container_width=True):
            selected_video_urls = video_urls[:MAX_BATCH_RECIPES]
            if len(video_urls) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} Video-Links verarbeitet.")
            st.session_state.switch_to_tab = 3; task_id, _ = make_task("Hintergrund-Stapel: Videos -> Mealie", len(selected_video_urls))
            import threading; threading.Thread(target=background_video_batch_process, args=(task_id, selected_video_urls, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "direct"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()

with tab5:
    ui_card("Mealie-Rezepte überarbeiten", "Rezepte aus Mealie sichten und verbessern.")
    mealie_area = st.text_area("Mealie Links:", key="shared_mealie_input", placeholder="https://mealie.example.com/recipe/pasta", on_change=set_active_tab, args=(4,))
    recipe_list = get_mealie_recipes(settings.mealie_url, settings.mealie_api_key)
    mealie_selected = st.multiselect("Dropdown:", options=[r["slug"] for r in recipe_list], format_func=lambda x: next((r["name"] for r in recipe_list if r["slug"] == x), x), on_change=set_active_tab, args=(4,))
    slugs_from_text = [extract_mealie_slug(u) for u in mealie_area.split("\n") if u.strip()]
    all_slugs = list(dict.fromkeys([*mealie_selected, *[s for s in slugs_from_text if s]]))
    if all_slugs:
        c1, c2, c3 = st.columns(3)
        if c1.button("📥 Laden (Original)", disabled=len(all_slugs) > 1, use_container_width=True):
            st.session_state.switch_to_tab = 4; task_id, _ = make_task("Vordergrund: Laden", 1)
            try:
                with st.status("Lade Rezept..."):
                    if recipe := get_recipe_by_slug(settings.mealie_url, settings.mealie_api_key, all_slugs[0]):
                        reset_editor_state(); st.session_state.target_slug = all_slugs[0]
                        st.session_state.recipe_data = {"name": recipe.get("name"), "description": recipe.get("description"), "orgURL": recipe.get("orgURL"), "recipeYield": recipe.get("recipeYield"), "prepTime": recipe.get("prepTime"), "cookTime": recipe.get("performTime"), "tags": recipe.get("tags", []), "recipeCategory": recipe.get("recipeCategory", []), "tools": recipe.get("tools", []), "nutrition": recipe.get("nutrition", {}), "recipeIngredient": recipe.get("recipeIngredient", []), "recipeInstructions": recipe.get("recipeInstructions", [])}
                        st.session_state.cover_image_bytes = None
                    import src.tasks as t; t.task_update(task_id, status="abgeschlossen", current=1)
            except Exception as e: st.error(str(e))
            st.rerun()
        if c2.button("✨ KI-Analyse an Editor (1)", disabled=len(all_slugs) > 1, use_container_width=True):
            st.session_state.switch_to_tab = 4; task_id, _ = make_task("Hintergrund: Mealie -> Editor", 1)
            import threading; threading.Thread(target=background_mealie_batch_process, args=(task_id, [all_slugs[0]], settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()
        if c3.button("📚 Editor-Stapel (Mealie)", use_container_width=True):
            selected_slugs = all_slugs[:MAX_BATCH_RECIPES]
            if len(all_slugs) > MAX_BATCH_RECIPES: st.warning(f"Maximal {MAX_BATCH_RECIPES} Rezepte pro Stapel. Es werden nur die ersten {MAX_BATCH_RECIPES} Mealie-Rezepte verarbeitet.")
            st.session_state.switch_to_tab = 4; task_id, _ = make_task("Hintergrund-Stapel: Mealie -> Editor", len(selected_slugs))
            import threading; threading.Thread(target=background_mealie_batch_process, args=(task_id, selected_slugs, settings.mealie_url, settings.mealie_api_key, settings.gemini_api_key, get_prompt(), get_mealie_data_maps(settings.mealie_url, settings.mealie_api_key), "editor"), daemon=False).start()
            st.session_state.upload_success = ["BACKGROUND_TASK_STARTED"]; st.rerun()

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
        if stats.get("leaderboard"):
            for idx, user_stat in enumerate(stats["leaderboard"]): 
                medal = '🥇' if idx == 0 else '🥈' if idx == 1 else '🥉' if idx == 2 else f'{idx+1}.'
                st.write(f"**{medal} {user_stat['label']}** — {user_stat['count']} Rezepte")
        else:
            st.info("Noch keine Einträge.")
            
    with col_ing:
        st.markdown("### 🍳 Deine Top 5 Zutaten")
        if stats.get("top_ingredients"):
            for i, ing in enumerate(stats["top_ingredients"]): 
                st.write(f"{i+1}. **{ing['name'].title()}** — in {ing['share']:.0f}%")
        else:
            st.info("Noch keine Zutaten analysiert.")
            
    st.divider()
    st.markdown("### 📖 Meine Historie")
    if stats.get("upload_rows"):
        with st.expander("Letzte 50 Uploads ansehen", expanded=False):
            for row in stats["upload_rows"][:50]: 
                st.markdown(f"• [{row['recipe_name']}]({settings.mealie_url}/recipe/{row['recipe_slug']}) — zuletzt {time.strftime('%Y-%m-%d %H:%M', time.localtime(row['last_uploaded_at']))}")
    else:
        st.info("Du hast noch keine Rezepte hochgeladen.")

    if is_admin_user():
        st.divider()
        st.markdown("### 🛡️ Globale Historie (Admin)")
        global_rows = get_all_uploaded_recipe_rows(100)
        if global_rows:
            with st.expander("Letzte 100 globale Uploads ansehen", expanded=False):
                for row in global_rows:
                    uploaded = time.strftime("%Y-%m-%d %H:%M", time.localtime(row["last_uploaded_at"]))
                    label = row["user_label"] if row["user_label"] != "Anonym" else (row["user_email"] or "Anonym")
                    st.markdown(f"• **{label}**: [{row['recipe_name']}]({settings.mealie_url}/recipe/{row['recipe_slug']}) — {uploaded}")
        else:
            st.info("Noch keine globalen Uploads im System.")
        
        st.divider()
        st.markdown("### 🧪 System-Tests")
        st.write("Prüfe die Systemintegrität mit der automatisierten Test-Suite.")
        if st.button("▶️ Test-Suite ausführen", key="test_suite_tab6", use_container_width=True):
            with st.spinner("Führe Tests aus... (Das kann ein paar Sekunden dauern)"):
                try:
                    res = subprocess.run([sys.executable, "-m", "pytest", os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_app.py"), "-v", "--disable-warnings"], capture_output=True, text=True, timeout=60)
                    if res.returncode == 0: st.success("✅ Alle Tests erfolgreich bestanden!")
                    else: st.error("❌ Einige Tests sind fehlgeschlagen.")
                    with st.expander("Test-Log", expanded=True): st.code(res.stdout + "\n" + res.stderr, language="text")
                except Exception as e: st.error(f"❌ Fehler: {e}")

render_editor_queue()

if st.session_state.recipe_data:
    st.divider()
    ui_card("Editor", "Prüfe und speichere dein Rezept.")
    d = st.session_state.recipe_data
    col_img, col_form = st.columns([0.25, 0.75])
    
    with col_img:
        img_placeholder = st.empty()
        if st.session_state.cover_image_bytes: img_placeholder.image(st.session_state.cover_image_bytes, use_container_width=True)
        if st.button("🗑️ Bild", use_container_width=True): st.session_state.cover_image_bytes = None; st.rerun()
            
        with st.expander("🎨 KI-Bild", expanded=False):
            st.selectbox("Modell", ["imagen-4.0-generate-001", "imagen-3.0-generate-001"], key="img_model_sel")
            current_user_lbl = get_current_user_label()
            prompts = get_image_prompts(current_user_lbl)
            p_names = ["✏️ Manuell"] + [f"{p['name']} {'⭐' if p['is_default'] else ''}" for p in prompts]
            
            if "prompt_selector" not in st.session_state:
                default_idx = 0
                for i, p in enumerate(prompts):
                    if p["is_default"] and p.get("user_label", "") == current_user_lbl: default_idx = i + 1; break
                if default_idx == 0:
                    for i, p in enumerate(prompts):
                        if p["is_default"] and "Lars Graf" in p.get("user_label", ""): default_idx = i + 1; break
                if default_idx == 0:
                    for i, p in enumerate(prompts):
                        if p["is_default"]: default_idx = i + 1; break
                st.session_state.prompt_selector = p_names[default_idx]
                st.session_state.img_style_txt = prompts[default_idx - 1]["text"] if default_idx > 0 else ""
            
            selected_prompt_name = st.selectbox("Stil-Vorlage", p_names, key="prompt_selector")
            if st.session_state.get("last_prompt_selector") != selected_prompt_name:
                st.session_state.last_prompt_selector = selected_prompt_name
                st.session_state.img_style_txt = next((p["text"] for p in prompts if selected_prompt_name == f"{p['name']} {'⭐' if p['is_default'] else ''}"), "") if selected_prompt_name != "✏️ Manuell" else ""

            selected_id = next((p["id"] for p in prompts if selected_prompt_name == f"{p['name']} {'⭐' if p['is_default'] else ''}"), -1)
            style_txt = st.text_area("Prompt", key="img_style_txt")
            st.markdown("---")
            new_prompt_name = st.text_input("Name für Vorlage (zum Speichern)", placeholder="z.B. Düster & Rustikal")
            
            c_btn1, c_btn2 = st.columns(2)
            with c_btn1:
                if st.button("💾 Vorlage speichern", use_container_width=True):
                    if new_prompt_name and style_txt:
                        if "http://" in style_txt.lower() or "https://" in style_txt.lower(): st.error("❌ Das sieht aus wie ein Web-Link!")
                        else:
                            save_image_prompt(new_prompt_name, style_txt, current_user_lbl); toast("Vorlage gespeichert!", "✅")
                            if "prompt_selector" in st.session_state: del st.session_state["prompt_selector"]
                            st.rerun()
            with c_btn2:
                if selected_id != -1 and st.button("⭐ Als Standard", use_container_width=True):
                    set_default_image_prompt(selected_id, current_user_lbl); toast("Als Standard markiert!", "⭐")
                    if "prompt_selector" in st.session_state: del st.session_state["prompt_selector"]
                    st.rerun()
            if selected_id != -1 and st.button("🗑️ Diese Vorlage löschen", use_container_width=True):
                delete_image_prompt(selected_id)
                for k in ["prompt_selector", "last_prompt_selector"]:
                    if k in st.session_state: del st.session_state[k]
                st.session_state.img_style_txt = ""; toast("Vorlage gelöscht!", "🗑️"); st.rerun()

            if st.button("✨ Generieren", use_container_width=True, type="primary"):
                img_placeholder.image(get_blur_placeholder(st.session_state.cover_image_bytes), use_container_width=True, caption="✨ Bild wird generiert...")
                with st.spinner("Koche Bild..."):
                    new_img = generate_recipe_image_with_gemini(create_genai_client(settings.gemini_api_key), d.get("name"), "", image_model=st.session_state.get("img_model_sel", "imagen-4.0-generate-001"), custom_style=style_txt)
                    if new_img: st.session_state.cover_image_bytes = new_img; st.rerun()
                    else:
                        st.error("Bild konnte nicht generiert werden."); img_placeholder.empty()
                        if st.session_state.cover_image_bytes: img_placeholder.image(st.session_state.cover_image_bytes, use_container_width=True)
                        
        if st.button("💾 Speichern", use_container_width=True, type="primary"):
            u_email, u_label = get_current_user_email(), get_current_user_label()
            m_uid = get_mealie_user_id_by_email(settings.mealie_url, settings.mealie_api_key, u_email)
            success, result = direct_save_to_mealie(d, settings.mealie_url, settings.mealie_api_key, st.session_state.cover_image_bytes, audit_user_key=get_current_user_key(), audit_user_label=u_label, audit_user_email=u_email, mealie_user_id=m_uid)
            if success:
                st.session_state.upload_success = [result]
                if st.session_state.get("current_queue_id"): delete_from_editor_queue(st.session_state.current_queue_id)
                reset_editor_state(); st.rerun()
                
    with col_form:
        st.session_state.recipe_data["name"] = st.text_input("Name", d.get("name"))
        st.session_state.recipe_data["description"] = st.text_area("Beschreibung", d.get("description"))
        
        c1, c2, c3 = st.columns(3)
        st.session_state.recipe_data["recipeYield"] = str(extract_servings_number(c1.text_input("Portionen", str(extract_servings_number(d.get("recipeYield")) or d.get("recipeYield", "")))) or c1.text_input("Portionen", str(extract_servings_number(d.get("recipeYield")) or d.get("recipeYield", ""))))
        st.session_state.recipe_data["prepTime"] = c2.text_input("Vorbereitung", d.get("prepTime", ""))
        st.session_state.recipe_data["cookTime"] = c3.text_input("Zubereitung", d.get("cookTime", ""))
        
        st.markdown("#### 📊 Geschätzte Nährwerte (pro Portion)")
        nut = d.get("nutrition", {})
        n_cols = st.columns(4)
        st.session_state.recipe_data["nutrition"] = {"calories": n_cols[0].text_input("Kalorien (kcal)", nut.get("calories", "")), "carbohydrateContent": n_cols[1].text_input("Kohlenhydrate (g)", nut.get("carbohydrateContent", "")), "proteinContent": n_cols[2].text_input("Eiweiß (g)", nut.get("proteinContent", "")), "fatContent": n_cols[3].text_input("Fett (g)", nut.get("fatContent", ""))}

        st.markdown("#### 🍎 Zutaten")
        edited_ings = st.data_editor([{"RefID": i.get("referenceId") or f"ing{idx+1}", "Sektion": i.get("title", ""), "Menge": i.get("quantity"), "Einheit": get_nested_name(i.get("unit")), "Zutat": get_nested_name(i.get("food")), "Notiz": i.get("note", ""), "Original": i.get("originalText", "")} for idx, i in enumerate(d.get("recipeIngredient", []))], num_rows="dynamic", use_container_width=True, key="ings")
        st.session_state.recipe_data["recipeIngredient"] = [{"referenceId": clean_str(r.get("RefID")) or f"ing{idx+1}", "title": clean_str(r.get("Sektion")) or None, "quantity": r.get("Menge"), "unit": {"name": clean_str(r.get("Einheit"))} if clean_str(r.get("Einheit")) else None, "food": {"name": clean_str(r.get("Zutat"))} if clean_str(r.get("Zutat")) else None, "note": clean_str(r.get("Notiz")), "originalText": clean_str(r.get("Original"))} for idx, r in enumerate(edited_ings)]

        st.markdown("#### 👨‍🍳 Zubereitung")
        edited_steps = st.data_editor([{"Titel": s.get("title", "") or f"Schritt {idx+1}", "Text": s.get("text", ""), "Refs": ", ".join(clean_str(ref.get("referenceId")) for ref in s.get("ingredientReferences", []) if clean_str(ref.get("referenceId")))} for idx, s in enumerate(d.get("recipeInstructions", []))], num_rows="dynamic", use_container_width=True, key="steps")
        st.session_state.recipe_data["recipeInstructions"] = [{"title": clean_str(r.get("Titel")) or f"Schritt {idx+1}", "text": clean_str(r.get("Text")), "ingredientReferences": [{"referenceId": ref.strip()} for ref in str(r.get("Refs", "")).split(",") if ref.strip()]} for idx, r in enumerate(edited_steps) if clean_str(r.get("Text"))]

        st.markdown("🧑‍🍳 **KI-Sous-Chef**")
        for msg in st.session_state.sous_chef_history:
            with st.chat_message(msg["role"]): st.write(msg["content"])
        if chef_prompt := st.chat_input("Anweisung an Sous-Chef..."):
            st.session_state.sous_chef_history.append({"role": "user", "content": chef_prompt})
            updated, expl = editor_transform_recipe(create_genai_client(settings.gemini_api_key), st.session_state.recipe_data, chef_prompt)
            st.session_state.recipe_data = updated; st.session_state.sous_chef_history.append({"role": "assistant", "content": expl}); st.rerun()
            
    if st.button("❌ Verwerfen", use_container_width=True): reset_editor_state(); st.rerun()
