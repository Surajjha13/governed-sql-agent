from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import asyncio
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from dotenv import load_dotenv

load_dotenv()

from app.schema_service.api import router as schema_router
from app.query_service.api import router as query_router
from app.connection_service.api import router as connection_router
from app.auth.api import router as auth_router
import app.app_state as app_state

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _load_cors_origins():
    configured = os.getenv("ALLOWED_ORIGINS")
    if configured:
        return [origin.strip() for origin in configured.split(",") if origin.strip()]
    return [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]

async def monitor_idle_connections():
    while True:
        try:
            app_state.check_and_disconnect()
        except Exception as e:
            logger.error(f"Error in idle connection monitor: {e}")
        await asyncio.sleep(60)

async def preload_models():
    """Background task to preload heavy models to avoid hangs on first connection."""
    try:
        def _load_embedding_model():
            from app.semantic_service.vector_index import get_embedding_model
            return get_embedding_model()

        logger.info("Pre-loading embedding model...")
        await asyncio.to_thread(_load_embedding_model)
        logger.info("Embedding model ready.")
    except Exception as e:
        logger.error(f"Failed to pre-load embedding model: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor_task = asyncio.create_task(monitor_idle_connections())
    preload_task = asyncio.create_task(preload_models())
    logger.info("Application starting up... Idle connection monitor started.")
    yield
    monitor_task.cancel()
    preload_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        logger.info("Idle connection monitor stopped.")

from fastapi.responses import JSONResponse
from fastapi import Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])

app = FastAPI(title="SQL Agent", lifespan=lifespan)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error on {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "An unexpected critical error occurred. Please contact an administrator."}
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=_load_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication API
app.include_router(
    auth_router,
    prefix="/auth",
    tags=["Authentication"]
)

app.include_router(
    connection_router,
    tags=["Connection Management"]
)

app.include_router(
    schema_router,
    tags=["Schema Discovery"]
)

app.include_router(
    query_router,
    tags=["Query"]
)

@app.get("/health", tags=["System"])
async def health_check():
    return {
        "status": "healthy",
        "timestamp": asyncio.get_event_loop().time(),
        "total_active_sessions": len(app_state.sessions),
        "connected_sessions": len([s for s in app_state.sessions.values() if s.current_connection])
    }

frontend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "frontend"))
if os.path.exists(frontend_path):
    app.mount("/css", StaticFiles(directory=os.path.join(frontend_path, "css")), name="css")
    app.mount("/js", StaticFiles(directory=os.path.join(frontend_path, "js")), name="js")
    app.mount("/llm_svgs", StaticFiles(directory=os.path.join(frontend_path, "llm_svgs")), name="llm_svgs")
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_path, "assets")), name="assets")

    @app.get("/favicon.ico", include_in_schema=False)
    async def serve_favicon():
        return FileResponse(
            os.path.join(frontend_path, "assets", "favicon.svg"),
            media_type="image/svg+xml",
        )

    @app.get("/favicon.svg", include_in_schema=False)
    async def serve_favicon_svg():
        return FileResponse(
            os.path.join(frontend_path, "assets", "favicon.svg"),
            media_type="image/svg+xml",
        )

    @app.get("/")
    async def serve_root():
        return FileResponse(os.path.join(frontend_path, "landing.html"))

    @app.get("/workspace")
    @app.get("/index.html")
    @app.get("/solo")
    async def serve_workspace():
        return FileResponse(os.path.join(frontend_path, "index.html"))

    @app.get("/login")
    @app.get("/login.html")
    async def serve_login():
        return FileResponse(os.path.join(frontend_path, "login.html"))

    @app.get("/admin")
    @app.get("/admin.html")
    async def serve_admin():
        return FileResponse(os.path.join(frontend_path, "admin.html"))

    @app.get("/landing")
    @app.get("/landing.html")
    async def serve_landing():
        return FileResponse(os.path.join(frontend_path, "landing.html"))

    @app.get("/how-it-works")
    @app.get("/how-it-works.html")
    async def serve_how_it_works():
        return RedirectResponse(url="/landing.html#how-it-works", status_code=307)
# Reload trigger for security update
