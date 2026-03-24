import os
import platform
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Settings:
    PROJECT_NAME = "Photo Studio & Matrimony API"
    VERSION = "1.0.0"
    SECRET_KEY = os.getenv("SECRET_KEY", "annularSecretKey")
    REFRESH_SECRET_KEY = os.getenv("REFRESH_SECRET_KEY", "annularRefreshSecretKey")
    ALGORITHM = os.getenv("ALGORITHM", "HS512")
    ACCESS_TOKEN_EXPIRE_MINUTES = 120
    REFRESH_TOKEN_EXPIRE_DAYS = 7
    OTP_EXPIRE_MINUTES = 5
    
    # Base Directory (Project Root)
    BASE_DIR = Path(__file__).resolve().parent.parent

    UPLOAD_DIR = BASE_DIR / os.getenv("UPLOAD_DIR", "uploads")
    PHOTOS_DIR = BASE_DIR / os.getenv("PHOTOS_DIR", "myapp/uploaded_photos")
    HOROSCOPES_DIR = BASE_DIR / os.getenv("HOROSCOPES_DIR", "myapp/uploaded_horoscopes")
    
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432")
    }
    
    BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

    AWS_CONFIG = {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region": os.getenv("AWS_REGION"),
        "bucket_name": os.getenv("AWS_S3_BUCKET")
    }

settings = Settings()
