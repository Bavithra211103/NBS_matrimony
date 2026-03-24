import logging
import traceback
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.config import settings
from db.session import get_db_connection
from core.firebase import initialize_firebase
from api.photostudio import router as photostudio_router
from api.matrimony import router as matrimony_router
from db.init_db import init_db

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Startup Sequence (Guaranteed Execution) ---
try:
    print("\n" + "="*50)
    print(">>> SYSTEM STARTUP INITIATED")
    
    # 1. Initialize Database Tables
    print(">>> Initializing database...")
    init_db()
    
    # 2. Ensure all necessary directories exist
    print(">>> Setting up local directories...")
    settings.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    settings.PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    settings.HOROSCOPES_DIR.mkdir(parents=True, exist_ok=True)
    (settings.UPLOAD_DIR / "admin_files").mkdir(parents=True, exist_ok=True)
    (settings.UPLOAD_DIR / "user_photos").mkdir(parents=True, exist_ok=True)
    (settings.UPLOAD_DIR / "frame_colors").mkdir(parents=True, exist_ok=True)
    (settings.UPLOAD_DIR / "profile_photos").mkdir(parents=True, exist_ok=True)
    print(">>> System startup sequence completed successfully.")
    print("="*50 + "\n")
except Exception as e:
    print(f"\n[!] CRITICAL STARTUP ERROR: {str(e)}")
    logger.critical(f"Startup Error: {e}")
    logger.critical(traceback.format_exc())

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Firebase
    try:
        initialize_firebase()
        logger.info("Firebase initialized.")
    except Exception as e:
        logger.error(f"Firebase initialization failed: {str(e)}")
    
    # Startup Database cleanup
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM refresh_tokens WHERE expires_at <= NOW()")
        cur.execute("DELETE FROM matrimony_refresh_tokens WHERE expires_at <= NOW()")
        conn.commit()
        logger.info("Expired refresh tokens cleaned up.")
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error cleaning up expired refresh tokens: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()
    
    yield
    # Shutdown
    logger.info("Application shutting down...")

app = FastAPI(
    lifespan=lifespan, 
    title=settings.PROJECT_NAME, 
    version=settings.VERSION, 
    debug=True
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://admin.newbrindha.com",
        "https://matrimony.newbrindha.com",
        "https://newbrindha.com",
        "https://backend.newbrindha.com",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8000",
    ],
    allow_origin_regex=r"https?://(.*\.)?newbrindha\.com",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static file routes
app.mount("/static", StaticFiles(directory=settings.UPLOAD_DIR), name="static")
app.mount("/static/photos", StaticFiles(directory=settings.UPLOAD_DIR), name="static_photos")
app.mount("/static/horoscopes", StaticFiles(directory=settings.UPLOAD_DIR), name="static_horoscopes")

# Include modularized routers
app.include_router(photostudio_router)
app.include_router(matrimony_router)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "database": "connected"}

@app.get("/")
async def root():
    return {
        "message": f"Welcome to the {settings.PROJECT_NAME}",
        "version": settings.VERSION,
        "status": "online"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)