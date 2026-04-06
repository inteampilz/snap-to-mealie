import asyncio, time, uuid, gc
from typing import Any, Dict, List, Tuple
import streamlit as st
from PIL import Image
from src.core import settings, logger, get_task_registry, get_task_lock, format_duration, get_current_user_label, get_current_user_key, get_current_user_email, add_to_editor_queue
from src.services import get_mealie_user_id_by_email, create_genai_client, load_image, image_to_jpeg_bytes, analyze_content_with_gemini, auto_generate_cover_image, direct_save_to_mealie, safe_close_image, download_recipe_video, analyze_video_with_gemini, cleanup_video_bundle, analyze_pdf_with_gemini, fetch_url_text_and_image, fetch_mealie_recipe_text

def task_update(task_id: str, **changes) -> None:
    with get_task_lock():
        if t := get_task_registry().get(task_id): t.update(changes)

def task_append(task_id: str, key: str, message: str) -> None:
    with get_task_lock():
        if t := get_task_registry().get(task_id): t.setdefault(key, []).append(message)

def task_inc(task_id: str) -> None:
    with get_task_lock():
        if t := get_task_registry().get(task_id): t["current"] = t.get("current", 0) + 1

def task_set_detail(task_id: str, detail: str) -> None: task_update(task_id, last_detail=detail, last_detail_at=time.time())

def compute_task_metrics(task: Dict[str, Any]) -> Dict[str, Any]:
    el = max(0.0, time.time() - float(task.get("started_at", time.time()) or time.time()))
    c, t = int(task.get("current", 0) or 0), int(task.get("total", 0) or 0)
    rpm = (c / el * 60.0) if el > 0 and c > 0 else 0.0
    rem = max(0, t - c)
    return {"elapsed": el, "rpm": rpm, "eta_seconds": (rem / rpm * 60.0) if rpm > 0 and rem > 0 else (0.0 if rem == 0 else None)}

def get_running_tasks_snapshot() -> List[Dict[str, Any]]:
    return sorted([t for t in get_task_registry().values() if t.get("status") == "running"], key=lambda x: x.get("started_at", 0), reverse=True)

def make_task(name: str, total: int) -> Tuple[str, Dict[str, Any]]:
    t_id = str(uuid.uuid4())
    ul, uk, ue = get_current_user_label(), get_current_user_key(), get_current_user_email()
    task = {
        "name": name, "owner": ul, "owner_key": uk, "owner_email": ue, 
        "mealie_user_id": get_mealie_user_id_by_email(settings.mealie_url, settings.mealie_api_key, ue),
        "started_at": time.time(), "total": total, "current": 0, "status": "running", 
        "logs": [], "errors": [], "stop_requested": False, "last_detail": "Task angelegt"
    }
    with get_task_lock(): get_task_registry()[t_id] = task
    logger.info("task_created", task_name=name, user_label=ul)
    return t_id, task

def _process_single_image_batch_item(task_id: str, idx: int, chunk: List[bytes], pair_mode: bool, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str) -> None:
    with get_task_lock():
        td = get_task_registry().get(task_id)
        if not td or td.get("stop_requested"): return
            
    lbl = f"Paar {idx + 1}" if pair_mode else f"Bild {idx + 1}"
    cb = create_genai_client(gemini_api_key)
    ipil, cpil, rpil = [], None, None
    try:
        task_set_detail(task_id, f"{lbl}: Lade Bilder")
        ipil = [load_image(b) for b in chunk]
        if pair_mode and len(ipil) == 2:
            cpil, rpil = ipil[0], ipil[1]
            task_set_detail(task_id, f"{lbl}: Analysiere Text")
            pd = analyze_content_with_gemini(cb, prompt, images=[rpil])
            cib = image_to_jpeg_bytes(cpil)
        else:
            task_set_detail(task_id, f"{lbl}: Analysiere Bild")
            pd = analyze_content_with_gemini(cb, prompt, images=ipil)
            task_set_detail(task_id, f"{lbl}: Generiere Cover")
            cib = auto_generate_cover_image(cb, pd, None, td.get("owner", ""))
            
        if target_mode == "editor":
            add_to_editor_queue(td.get("owner_key", ""), pd, cib)
            task_append(task_id, "logs", f"✅ {lbl}: In Editor-Warteschlange")
        else:
            task_set_detail(task_id, f"{lbl}: Speichere nach Mealie")
            ok, res = direct_save_to_mealie(pd, mealie_url, mealie_api_key, cib, preloaded_maps, audit_user_key=td.get("owner_key", ""), audit_user_label=td.get("owner", ""), audit_user_email=td.get("owner_email", ""), mealie_user_id=td.get("mealie_user_id"))
            task_append(task_id, "logs" if ok else "errors", f"{'✅' if ok else '❌'} {lbl}: {res}")
    except Exception as exc: task_append(task_id, "errors", f"❌ {lbl}: {exc}")
    finally:
        for i in ipil: safe_close_image(i)
        task_inc(task_id); gc.collect()

def background_image_batch_process(task_id: str, image_bytes_list: List[bytes], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, pair_mode: bool = False, is_batch: bool = True, target_mode: str = "direct"):
    chunks = [image_bytes_list[i:i + 2] for i in range(0, len(image_bytes_list), 2)] if pair_mode and is_batch else [[img] for img in image_bytes_list] if is_batch else [image_bytes_list]
    task_set_detail(task_id, f"Async Start · {len(chunks)} Elemente")
    
    async def run_tasks():
        sem = asyncio.Semaphore(settings.batch_max_workers)
        async def sem_task(idx, chunk):
            async with sem: await asyncio.to_thread(_process_single_image_batch_item, task_id, idx, chunk, pair_mode, mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
        await asyncio.gather(*[sem_task(idx, c) for idx, c in enumerate(chunks)])
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"):
                task_update(task_id, status="abgeschlossen", last_detail="Gestoppt"); return
        task_update(task_id, status="abgeschlossen", last_detail="Fertig")
    asyncio.run(run_tasks())

def _process_single_pdf_batch_item(task_id: str, idx: int, pdf_bytes: bytes, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str) -> None:
    with get_task_lock():
        td = get_task_registry().get(task_id)
        if not td or td.get("stop_requested"): return
    cb = create_genai_client(gemini_api_key)
    fd, tpath = tempfile.mkstemp(suffix=".pdf")
    try:
        with os.fdopen(fd, 'wb') as f: f.write(pdf_bytes)
        task_set_detail(task_id, f"PDF {idx+1}/{total}: Analyse")
        recipes = analyze_pdf_with_gemini(cb, prompt, tpath)
        
        for ri, rdata in enumerate(recipes):
            task_set_detail(task_id, f"PDF {idx+1}/{total} (R{ri+1}): Cover")
            cib = auto_generate_cover_image(cb, rdata, None, td.get("owner", ""))
            if target_mode == "editor":
                add_to_editor_queue(td.get("owner_key", ""), rdata, cib)
                task_append(task_id, "logs", f"✅ PDF {idx+1} (R{ri+1}): Queue")
            else:
                ok, res = direct_save_to_mealie(rdata, mealie_url, mealie_api_key, cib, preloaded_maps, audit_user_key=td.get("owner_key", ""), audit_user_label=td.get("owner", ""), audit_user_email=td.get("owner_email", ""), mealie_user_id=td.get("mealie_user_id"))
                task_append(task_id, "logs" if ok else "errors", f"{'✅' if ok else '❌'} PDF {idx+1} (R{ri+1}): {res}")
    except Exception as exc: task_append(task_id, "errors", f"❌ PDF {idx+1}: {exc}")
    finally:
        if os.path.exists(tpath): os.remove(tpath)
        task_inc(task_id); gc.collect()

def background_pdf_batch_process(task_id: str, pdf_bytes_list: List[bytes], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(pdf_bytes_list)} PDFs")
    async def run_tasks():
        sem = asyncio.Semaphore(settings.batch_max_workers)
        async def sem_task(idx, pb):
            async with sem: await asyncio.to_thread(_process_single_pdf_batch_item, task_id, idx, pb, len(pdf_bytes_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
        await asyncio.gather(*[sem_task(idx, pb) for idx, pb in enumerate(pdf_bytes_list)])
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"): task_update(task_id, status="abgeschlossen", last_detail="Gestoppt"); return
        task_update(task_id, status="abgeschlossen", last_detail="Fertig")
    asyncio.run(run_tasks())

def _process_single_url_batch_item(task_id: str, idx: int, url: str, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str) -> None:
    with get_task_lock():
        td = get_task_registry().get(task_id)
        if not td or td.get("stop_requested"): return
    cb = create_genai_client(gemini_api_key)
    try:
        task_set_detail(task_id, f"URL {idx+1}/{total}: Lade Web")
        txt, cib = fetch_url_text_and_image(url)
        pd = analyze_content_with_gemini(cb, prompt, text=txt)
        pd["orgURL"] = url
        task_set_detail(task_id, f"URL {idx+1}/{total}: Cover")
        cib = auto_generate_cover_image(cb, pd, cib, td.get("owner", ""))
        
        if target_mode == "editor":
            add_to_editor_queue(td.get("owner_key", ""), pd, cib)
            task_append(task_id, "logs", f"✅ {url}: Queue")
        else:
            ok, res = direct_save_to_mealie(pd, mealie_url, mealie_api_key, cib, preloaded_maps, org_url=url, audit_user_key=td.get("owner_key", ""), audit_user_label=td.get("owner", ""), audit_user_email=td.get("owner_email", ""), mealie_user_id=td.get("mealie_user_id"))
            task_append(task_id, "logs" if ok else "errors", f"{'✅' if ok else '❌'} {url}: {res}")
    except Exception as exc: task_append(task_id, "errors", f"❌ {url}: {exc}")
    finally: task_inc(task_id); gc.collect()

def background_url_batch_process(task_id: str, url_list: List[str], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(url_list)} URLs")
    async def run_tasks():
        sem = asyncio.Semaphore(settings.batch_max_workers)
        async def sem_task(idx, u):
            async with sem: await asyncio.to_thread(_process_single_url_batch_item, task_id, idx, u, len(url_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
        await asyncio.gather(*[sem_task(idx, u) for idx, u in enumerate(url_list)])
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"): task_update(task_id, status="abgeschlossen", last_detail="Gestoppt"); return
        task_update(task_id, status="abgeschlossen", last_detail="Fertig")
    asyncio.run(run_tasks())

def _process_single_video_batch_item(task_id: str, idx: int, url: str, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str) -> None:
    with get_task_lock():
        td = get_task_registry().get(task_id)
        if not td or td.get("stop_requested"): return
    cb = create_genai_client(gemini_api_key)
    bundle = {}
    try:
        task_set_detail(task_id, f"Video {idx+1}/{total}: Lade Video")
        bundle = download_recipe_video(url)
        pd = analyze_video_with_gemini(cb, prompt, bundle.get("video_path", ""), bundle.get("recipe_text", ""))
        pd["orgURL"] = url
        task_set_detail(task_id, f"Video {idx+1}/{total}: Cover")
        cib = auto_generate_cover_image(cb, pd, bundle.get("thumbnail_bytes"), td.get("owner", ""))
        
        if target_mode == "editor":
            add_to_editor_queue(td.get("owner_key", ""), pd, cib)
            task_append(task_id, "logs", f"✅ {url}: Queue")
        else:
            ok, res = direct_save_to_mealie(pd, mealie_url, mealie_api_key, cib, preloaded_maps, org_url=url, audit_user_key=td.get("owner_key", ""), audit_user_label=td.get("owner", ""), audit_user_email=td.get("owner_email", ""), mealie_user_id=td.get("mealie_user_id"))
            task_append(task_id, "logs" if ok else "errors", f"{'✅' if ok else '❌'} {url}: {res}")
    except Exception as exc: task_append(task_id, "errors", f"❌ {url}: {exc}")
    finally: cleanup_video_bundle(bundle); task_inc(task_id); gc.collect()

def background_video_batch_process(task_id: str, url_list: List[str], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(url_list)} Videos")
    async def run_tasks():
        sem = asyncio.Semaphore(settings.batch_max_workers)
        async def sem_task(idx, u):
            async with sem: await asyncio.to_thread(_process_single_video_batch_item, task_id, idx, u, len(url_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
        await asyncio.gather(*[sem_task(idx, u) for idx, u in enumerate(url_list)])
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"): task_update(task_id, status="abgeschlossen", last_detail="Gestoppt"); return
        task_update(task_id, status="abgeschlossen", last_detail="Fertig")
    asyncio.run(run_tasks())

def _process_single_mealie_batch_item(task_id: str, idx: int, slug: str, total: int, mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str) -> None:
    with get_task_lock():
        td = get_task_registry().get(task_id)
        if not td or td.get("stop_requested"): return
    cb = create_genai_client(gemini_api_key)
    try:
        task_set_detail(task_id, f"Rezept {idx+1}/{total}: Lade {slug}")
        txt = fetch_mealie_recipe_text(slug, mealie_url, mealie_api_key)
        pd = analyze_content_with_gemini(cb, prompt, text=txt)
        task_set_detail(task_id, f"Rezept {idx+1}/{total}: Cover")
        from src.services import get_recipe_by_slug
        er = get_recipe_by_slug(mealie_url, mealie_api_key, slug)
        cib = auto_generate_cover_image(cb, pd, None, td.get("owner", "")) if er and not er.get("image") else None
        
        if target_mode == "editor":
            add_to_editor_queue(td.get("owner_key", ""), pd, cib)
            task_append(task_id, "logs", f"✅ {slug}: Queue")
        else:
            ok, res = direct_save_to_mealie(pd, mealie_url, mealie_api_key, cib, preloaded_maps, target_slug=slug, audit_user_key=td.get("owner_key", ""), audit_user_label=td.get("owner", ""), audit_user_email=td.get("owner_email", ""), mealie_user_id=td.get("mealie_user_id"))
            task_append(task_id, "logs" if ok else "errors", f"{'✅' if ok else '❌'} {slug}: {res}")
    except Exception as exc: task_append(task_id, "errors", f"❌ {slug}: {exc}")
    finally: task_inc(task_id); gc.collect()

def background_mealie_batch_process(task_id: str, slug_list: List[str], mealie_url: str, mealie_api_key: str, gemini_api_key: str, prompt: str, preloaded_maps, target_mode: str = "direct"):
    task_set_detail(task_id, f"Async Start · {len(slug_list)} Mealie-Rezepte")
    async def run_tasks():
        sem = asyncio.Semaphore(settings.batch_max_workers)
        async def sem_task(idx, slug):
            async with sem: await asyncio.to_thread(_process_single_mealie_batch_item, task_id, idx, slug, len(slug_list), mealie_url, mealie_api_key, gemini_api_key, prompt, preloaded_maps, target_mode)
        await asyncio.gather(*[sem_task(idx, slug) for idx, slug in enumerate(slug_list)])
        with get_task_lock():
            if get_task_registry().get(task_id, {}).get("stop_requested"): task_update(task_id, status="abgeschlossen", last_detail="Gestoppt"); return
        task_update(task_id, status="abgeschlossen", last_detail="Fertig")
    asyncio.run(run_tasks())
