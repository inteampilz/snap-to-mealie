import os, re, json, sqlite3, threading, time, sys, io, zipfile, uuid
from typing import Any, Dict, List, Optional, Type, TypeVar
from textwrap import dedent
import logging
import structlog
import streamlit as st
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field

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
    if clean_text.startswith("
