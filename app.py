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
    get_all_uploaded_recipe_rows
)

# Services imports
from src.services import (
# Weiter unten in der Datei beim Tab 6:

with tab6:
    ui_card("Statistiken & Leaderboard", "Nutzungsauswertung.")
    stats = get_user_stats_snapshot(settings.mealie_url, settings.mealie_api_key, get_current_user_key())
    c1, c2, c3 = st.columns(3)
    c1.metric("Gesamt via App", stats["total_app_uploads"])
    c2.metric("Deine Uploads", stats["personal_count"])
    c3.metric("Ersparte Zeit", f"{stats['hours_saved']} h")
    st.divider(); col_lb, col_ing = st.columns(2)
    with col_lb:
        st.markdown("### 🏆 Leaderboard")
        for idx, user_stat in enumerate(stats.get("leaderboard", [])): st.write(f"**{'🥇' if idx == 0 else '🥈' if idx == 1 else '🥉' if idx == 2 else f'{idx+1}.'} {user_stat['label']}** — {user_stat['count']} Rezepte")
    with col_ing:
        st.markdown("### 🍳 Deine Top 5 Zutaten")
        for i, ing in enumerate(stats.get("top_ingredients", [])): st.write(f"{i+1}. **{ing['name'].title()}** — in {ing['share']:.0f}%")
    st.divider()
    if stats.get("upload_rows"):
        with st.expander("Meine Historie", expanded=False):
            for row in stats["upload_rows"][:50]: st.markdown(f"• [{row['recipe_name']}]({settings.mealie_url}/recipe/{row['recipe_slug']}) — zuletzt {time.strftime('%Y-%m-%d %H:%M', time.localtime(row['last_uploaded_at']))}")

    # --- WIEDER HINZUGEFÜGT: Admin-Bereich im Statistik-Tab ---
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
