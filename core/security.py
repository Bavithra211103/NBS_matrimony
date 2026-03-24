import os
import random
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from core.config import settings
from db.session import get_db_connection
from psycopg2.extras import DictCursor

# Logging
logger = logging.getLogger(__name__)

# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security_scheme = HTTPBearer()

def verify_password(plain_password, hashed_password):
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception as e:
        logger.error(f"Error verifying password: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def get_password_hash(password):
    try:
        return pwd_context.hash(password)
    except Exception as e:
        logger.error(f"Error hashing password: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error hashing password")

def generate_otp(length=6):
    return ''.join([str(random.randint(0, 9)) for _ in range(length)])

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    try:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
        return encoded_jwt
    except Exception as e:
        logger.error(f"Error creating access token: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error creating access token")

def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None):
    try:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + (expires_delta or timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, settings.REFRESH_SECRET_KEY, algorithm=settings.ALGORITHM)
        return encoded_jwt
    except Exception as e:
        logger.error(f"Error creating refresh token: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error creating refresh token")

def is_user_blocked(matrimony_id: str) -> bool:
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM blocked_users WHERE blocked_matrimony_id = %s
        """, (matrimony_id,))
        return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking if user is blocked: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    finally:
        if cur: cur.close()
        if conn: conn.close()

async def get_current_user(auth: HTTPAuthorizationCredentials = Depends(security_scheme)) -> Dict[str, Any]:
    token = auth.credentials
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        email = payload.get("sub")
        user_type = payload.get("user_type")
        
        if email is None:
            logger.warning(f"Token payload missing 'sub': {payload}")
            raise HTTPException(status_code=401, detail="Invalid authentication token")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            if user_type == "admin":
                cur.execute("SELECT id, email, user_type FROM users WHERE email = %s", (email,))
            else:
                cur.execute(
                    "SELECT id, email, user_type FROM users WHERE email = %s UNION SELECT id, email, user_type FROM matrimony_profiles WHERE email = %s",
                    (email, email)
                )
            
            user = cur.fetchone()
            if not user:
                logger.warning(f"User not found for email: {email}")
                raise HTTPException(status_code=401, detail="User not found")
            
            return {"id": user[0], "email": user[1], "user_type": user[2]}
        finally:
            cur.close()
            conn.close()
            
    except JWTError as e:
        logger.warning(f"JWT Decode error: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_current_user: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error during authentication")

async def get_current_user_matrimony(auth: HTTPAuthorizationCredentials = Depends(security_scheme)) -> Dict[str, Any]:
    token = auth.credentials
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id = payload.get("sub")
        user_type = payload.get("user_type")
        
        if not user_id or not user_type:
            logger.warning(f"Token payload missing 'sub' or 'user_type': {payload}")
            raise HTTPException(status_code=401, detail="Invalid authentication token")
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        try:
            if user_type == "admin":
                cur.execute("SELECT id, email, user_type FROM users WHERE email = %s", (user_id,))
            else:
                cur.execute("SELECT * FROM matrimony_profiles WHERE matrimony_id = %s", (user_id,))
            
            user = cur.fetchone()
            if not user:
                logger.warning(f"User not found for id/email: {user_id}")
                raise HTTPException(status_code=401, detail="User not found")
            
            user_dict = dict(user)
            user_dict["user_type"] = user_type      # always use token value
            user_dict["matrimony_id"] = user_id     # ensure matrimony_id is set
            return user_dict
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()
            
    except JWTError as e:
        logger.warning(f"JWT Decode error: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_current_user_matrimony: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error during authentication")
