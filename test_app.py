import os
import tempfile
import json
import zipfile
import io
import time
import pytest
from unittest.mock import patch, MagicMock

# -----------------------------------------------------------------------------
# Setup: Sichere temporäre Datenbank für alle Tests
# -----------------------------------------------------------------------------
temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite3")
os.environ["SNAP_CACHE_DB"] = temp_db.name

# Importiere nun alle Module der neuen Service-Architektur
from src import core
from src import services
from src import tasks

@pytest.fixture(autouse=True)
def setup_db():
    core.init_cache_db()
    yield

# -----------------------------------------------------------------------------
# 1. UNIT TESTS: Strings & Utility
# -----------------------------------------------------------------------------
def test_clean_str():
    assert core.clean_str("  Hallo Welt  ") == "Hallo Welt"
    assert core.clean_str("N/A") == ""
    assert core.clean_str("null") == ""
    assert core.clean_str(None) == ""
    assert core.clean_str("-") == ""

def test_normalize_name():
    assert core.normalize_name("Äpfel & Birnen!") == "aepfelbirnen"
    assert core.normalize_name("  Tomaten-Mark ") == "tomatenmark"
    assert core.normalize_name("") == ""

def test_slugify():
    assert core.slugify("Spaghetti Bolognese!") == "spaghetti-bolognese"
    assert core.slugify("Äpfel & Birnen") == "aepfel-birnen"
    assert core.slugify("  Süß-Sauer  ") == "suess-sauer"

def test_get_nested_name():
    assert core.get_nested_name({"name": "Tomate"}) == "Tomate"
    assert core.get_nested_name("Salz") == "Salz"
    assert core.get_nested_name(None) == ""

def test_extract_servings_number():
    assert core.extract_servings_number("4 Portionen") == 4
    assert core.extract_servings_number("Für 2-3 Personen") == 2
    assert core.extract_servings_number("Einige") is None
    assert core.extract_servings_number("") is None

def test_safe_float():
    assert core.safe_float("1.5") == 1.5
    assert core.safe_float("2,5") == 2.5
    assert core.safe_float("abc") is None

def test_unique_by_name():
    items = [{"name": "Salz"}, {"name": "Pfeffer"}, {"name": "salz"}]
    unique_items = core.unique_by_name(items)
    assert len(unique_items) == 2
    assert unique_items[0]["name"] == "Salz"

def test_format_duration():
    assert core.format_duration(45) == "45s"
    assert core.format_duration(65) == "1m 5s"
    assert core.format_duration(3665) == "1h 1m"
    assert core.format_duration(None) == "wird berechnet"

def test_infer_recipe_yield_from_text():
    assert services.infer_recipe_yield_from_text("Ein leckeres Rezept. Reicht für 4 Portionen.") == "4"
    assert services.infer_recipe_yield_from_text("Yield: 12 servings. Bake for 20 mins.") == "12"


# -----------------------------------------------------------------------------
# 2. UNIT TESTS: Pydantic Validation & Parser
# -----------------------------------------------------------------------------
def test_pydantic_parsing():
    raw_json = '{"name": "Pizza", "description": "Lecker", "recipeYield": "2", "prepTime": "10m", "cookTime": "15m", "recipeIngredient": [{"referenceId": "i1", "originalText": "Käse"}], "recipeInstructions": [{"title": "1", "text": "Backen", "ingredientReferences": []}]}'
    recipe = core._parse_pydantic_json(core.Recipe, raw_json)
    
    assert recipe.name == "Pizza"
    assert recipe.recipeIngredient[0].originalText == "Käse"
    assert recipe.recipeIngredient[0].referenceId == "i1"
    assert recipe.recipeInstructions[0].title == "1"


# -----------------------------------------------------------------------------
# 3. UNIT TESTS: Web Scraping & HTML
# -----------------------------------------------------------------------------
def test_strip_html():
    html_content = """
    <html>
        <head><style>body {color: red;}</style></head>
        <body>
            <header>Menu</header>
            <p>Leckeres <b>Rezept</b>!</p>
            <script>alert('test');</script>
        </body>
    </html>
    """
    clean_text = services.strip_html(html_content)
    assert "Leckeres" in clean_text
    assert "Rezept" in clean_text
    assert "Menu" not in clean_text
    assert "alert" not in clean_text
    assert "body" not in clean_text

def test_extract_recipe_jsonld_text():
    html_content = """
    <script type="application/ld+json">
    {
      "@type": "Recipe",
      "name": "Spaghetti",
      "recipeYield": "2",
      "recipeIngredient": ["Nudeln", "Sauce"],
      "recipeInstructions": [{"text": "Kochen"}]
    }
    </script>
    """
    extracted = services.extract_recipe_jsonld_text(html_content)
    assert "Titel: Spaghetti" in extracted
    assert "Portionen: 2" in extracted
    assert "- Nudeln" in extracted
    assert "1. Kochen" in extracted


# -----------------------------------------------------------------------------
# 4. DATENBANK TESTS: Lokale Speicherung & State
# -----------------------------------------------------------------------------
def test_editor_queue_operations():
    user_key = "test_user"
    core.add_to_editor_queue(user_key, {"name": "Queue Rezept"}, cover_image=b"bytes")
    
    items = core.get_editor_queue(user_key)
    assert len(items) >= 1
    assert items[-1]["recipe_name"] == "Queue Rezept"
    assert items[-1]["cover_image"] == b"bytes"
    
    core.delete_from_editor_queue(items[-1]["id"])
    assert len(core.get_editor_queue(user_key)) == len(items) - 1

def test_image_prompts_db():
    core.save_image_prompt("Test Style", "Test Prompt", user_label="test_user", is_default=True)
    prompts = core.get_image_prompts("test_user")
    
    assert any(p["name"] == "Test Style" and p["is_default"] for p in prompts)
    
    core.save_image_prompt("Zweiter Style", "Text2", user_label="test_user", is_default=False)
    prompts_after = core.get_image_prompts("test_user")
    second_id = next(p["id"] for p in prompts_after if p["name"] == "Zweiter Style")
    
    core.set_default_image_prompt(second_id, "test_user")
    prompts_final = core.get_image_prompts("test_user")
    
    assert next(p["is_default"] for p in prompts_final if p["id"] == second_id) is True
    
    core.delete_image_prompt(second_id)
    assert not any(p["id"] == second_id for p in core.get_image_prompts("test_user"))

def test_mappings_and_recipes_db():
    core.db_set_mapping("foods", "Äpfel", "id-apfel")
    assert core.db_get_mapping("foods", "äpfel") == "id-apfel"
    assert core.db_get_mapping("foods", "Aepfel") == "id-apfel"
    
    core.db_store_recipes([{"name": "Kuchen", "slug": "kuchen-123"}])
    assert core.db_find_recipe_slug("Kuchen") == "kuchen-123"
    core.db_delete_recipe_by_slug("kuchen-123")
    assert core.db_find_recipe_slug("Kuchen") is None

def test_upload_history():
    core.record_recipe_upload("user1", "slug1", "Rezept 1", "User Eins", "user1@test.com")
    rows = core.get_user_uploaded_recipe_rows("user1")
    assert len(rows) == 1
    assert rows[0]["recipe_slug"] == "slug1"
    
    all_rows = core.get_all_uploaded_recipe_rows(10)
    assert len(all_rows) > 0

# -----------------------------------------------------------------------------
# 5. UNIT TESTS: Tasks & ZIP Generierung
# -----------------------------------------------------------------------------
@patch('src.tasks.get_mealie_user_id_by_email', return_value="mealie-id-123")
def test_task_registry_and_metrics(mock_mealie_id):
    task_id, task = tasks.make_task("Test-Task", 10)
    
    assert task["name"] == "Test-Task"
    assert task["status"] == "running"
    
    tasks.task_inc(task_id)
    tasks.task_set_detail(task_id, "Detail Info")
    tasks.task_append(task_id, "logs", "Log Eintrag")
    
    registry = core.get_task_registry()
    updated_task = registry[task_id]
    
    assert updated_task["current"] == 1
    assert updated_task["last_detail"] == "Detail Info"
    assert "Log Eintrag" in updated_task["logs"]
    
    updated_task["started_at"] = time.time() - 60 
    metrics = tasks.compute_task_metrics(updated_task)
    assert metrics["rpm"] > 0
    assert metrics["eta_seconds"] > 0

def test_generate_extension_zip():
    zip_bytes = core.generate_extension_zip()
    assert isinstance(zip_bytes, bytes)
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        file_list = zf.namelist()
        assert "manifest.json" in file_list
        assert "background.js" in file_list
        assert "options.html" in file_list

# -----------------------------------------------------------------------------
# 6. INTEGRATION TESTS (MOCKED): APIs
# -----------------------------------------------------------------------------
@patch('src.services.safe_mealie_request')
def test_get_mealie_recipes_mocked(mock_request):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"items": [{"name": "Lasagne", "slug": "lasagne-123"}]}
    mock_request.return_value = mock_resp
    
    recipes = services.get_mealie_recipes("http://fake-mealie", "fake-key")
    assert len(recipes) == 1
    assert recipes[0]["name"] == "Lasagne"

@patch('requests.Session.get')
def test_fetch_url_text_and_image_mocked(mock_session_get):
    fake_html = """
    <html>
      <head><meta property="og:image" content="https://example.com/image.jpg"></head>
      <body>
        <script type="application/ld+json">
        {"@context": "https://schema.org/", "@type": "Recipe", "name": "Super Suppe"}
        </script>
        <p>Ein bisschen Text auf der Seite.</p>
      </body>
    </html>
    """
    mock_resp_html = MagicMock()
    mock_resp_html.text = fake_html
    mock_resp_html.raise_for_status = MagicMock()
    
    mock_resp_img = MagicMock()
    mock_resp_img.status_code = 200
    mock_resp_img.headers = {"content-length": "100"}
    mock_resp_img.iter_content.return_value = [b"fake_image_data"]
    
    mock_session_get.side_effect = [mock_resp_html, mock_resp_img]
    
    text, img_bytes = services.fetch_url_text_and_image("https://example.com/suppe")
    assert "Titel: Super Suppe" in text
    assert img_bytes == b"fake_image_data"

@patch('src.services.generate_recipe_image_with_gemini')
def test_auto_generate_cover_image_fallback_mocked(mock_generate):
    mock_generate.return_value = b"fake_cover_bytes"
    client_bundle = {"client": MagicMock(), "backend": "mock"}
    
    core.save_image_prompt("Lars Style", "Lars Prompt Text", user_label="Lars Graf", is_default=True)
    services.auto_generate_cover_image(client_bundle, {"name": "Test"}, None, owner_label="Max")
    mock_generate.assert_called_with(client_bundle, "Test", "", custom_style="Lars Prompt Text")
    
    core.save_image_prompt("Max Style", "Max Prompt Text", user_label="Max", is_default=True)
    services.auto_generate_cover_image(client_bundle, {"name": "Test"}, None, owner_label="Max")
    mock_generate.assert_called_with(client_bundle, "Test", "", custom_style="Max Prompt Text")

if __name__ == "__main__":
    import sys
    print("Starte Pytest automatisch...")
    pytest.main(["-v", os.path.abspath(__file__)])
