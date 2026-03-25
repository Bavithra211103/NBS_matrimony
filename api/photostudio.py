from fastapi import APIRouter, Depends, HTTPException, Form, File, UploadFile, Query
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import json
import traceback
from googletrans import Translator

logger = logging.getLogger(__name__)

from core.config import settings
from db.session import get_db_connection
from datetime import datetime, timedelta, timezone
from core.security import (
    get_current_user, create_access_token, create_refresh_token, verify_password, get_password_hash
)
from models.schemas import (
    GetFileUpdate, FileSelectionsRequest, UserCreate, UserLogin, Token, 
    EventForm, RefreshToken
)
from utils.file_handler import file_handler
router = APIRouter(prefix="/photostudio", tags=["Photostudio"])

@router.post("/admin/register", response_model=Dict[str, Any])
async def register(user: UserCreate):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM users WHERE email = %s", (user.email,))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        
        hashed_password = get_password_hash(user.password)
        cur.execute(
            "INSERT INTO users (email, password_hash, user_type) VALUES (%s, %s, %s) RETURNING id",
            (user.email, hashed_password, user.user_type)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"New admin registered: {user.email}")
        return {"message": "Registration successful", "user_id": user_id}
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error in admin register: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/admin/login", response_model=Token)
async def login(user: UserLogin):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, email, password_hash, user_type FROM users WHERE email = %s",
            (user.email,)
        )
        db_user = cur.fetchone()
        if not db_user or not verify_password(user.password, db_user[2]):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        access_token = create_access_token(
            {"sub": db_user[1], "user_type": db_user[3]},
            timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        refresh_token = create_refresh_token(
            {"sub": db_user[1], "user_type": db_user[3]},
            timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
        )
        
        cur.execute(
            "INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES (%s, %s, %s)",
            (refresh_token, db_user[0], datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
        )
        conn.commit()
        
        logger.info(f"Admin logged in: {user.email}")
        return {
            "message": "Login successful",
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "email": db_user[1],
            "user_type": db_user[3]
        }
    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error in admin login: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/user/eventform", response_model=Dict[str, Any])
async def create_event_form(event: EventForm):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO event_forms (name, contact, event_date, event_time, event_type) VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (event.name, event.contact, event.event_date, event.event_time, event.event_type)
        )
        event_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"Event form submitted: {event.name}")
        return {
            "message": f"{event.name},{event.event_date},{event.event_time},{event.event_type} Event form submitted successfully",
            "event_id": event_id
        }
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error creating event form: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.get("/admin/eventform", response_model=List[Dict[str, Any]])
async def get_event_forms():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT name, contact, event_date, event_time, event_type, created_at FROM event_forms ORDER BY created_at DESC")
        rows = cur.fetchall()
        for row in rows:
            row["message"] = f"{row['name']}, {row['event_date']}, {row['event_time']}, {row['event_type']} Event form submitted successfully"
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error retrieving event forms: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/refresh-token", response_model=Token)
async def refresh_token(token: RefreshToken):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        from jose import jwt
        payload = jwt.decode(token.refresh_token, settings.REFRESH_SECRET_KEY, algorithms=[settings.ALGORITHM])
        email = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        
        cur.execute("SELECT user_id, expires_at FROM refresh_tokens WHERE token = %s AND expires_at > NOW()", (token.refresh_token,))
        db_token = cur.fetchone()
        if not db_token:
            raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
        
        access_token = create_access_token({"sub": email}, timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
        new_refresh_token = create_refresh_token({"sub": email}, timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
        
        cur.execute("INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES (%s, %s, %s)", (new_refresh_token, db_token[0], datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)))
        cur.execute("DELETE FROM refresh_tokens WHERE token = %s", (token.refresh_token,))
        conn.commit()
        
        return {"access_token": access_token, "refresh_token": new_refresh_token, "token_type": "bearer", "email": email, "user_type": "user", "message": "Token refreshed"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@router.post("/admin/fileupload", response_model=Dict[str, Any])
async def admin_upload_files(
    files: List[UploadFile] = File(...),
    category: str = Form(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can access this endpoint")
    
    uploaded_files = []
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        logger.info(f"Received {len(files)} files for upload in category: {category}")

        for file in files:
            try:
                file_url = file_handler.upload_file(file, "admin_files")
                if not file_url:
                    logger.warning(f"File upload failed for {file.filename}")
                    continue
            except Exception as e:
                logger.error(f"S3 Upload Error for {file.filename}: {str(e)}")
                logger.error(traceback.format_exc())
                continue
            
            file_type = file.content_type  

            try:
                cur.execute(
                    """
                    INSERT INTO files (filename, file_type, category, file_url, uploaded_by, uploaded_at)
                    VALUES (%s, %s, %s, %s, %s, NOW()) RETURNING id;
                    """,
                    (file.filename, file_type, category, file_url, current_user["id"])
                )
                file_id = cur.fetchone()
                
                if not file_id:
                    logger.warning(f"Failed to get file ID for {file.filename} after DB insert")
                    continue

                uploaded_files.append({
                    "id": file_id[0],
                    "filename": file.filename,
                    "file_type": file_type,
                    "category": category,
                    "uploaded_by": current_user["id"],
                    "file_url": file_url
                })

            except psycopg2.Error as e:
                if 'conn' in locals(): conn.rollback()
                logger.error(f"Database Error for {file.filename}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

        conn.commit()

        if not uploaded_files:
            logger.warning("No files were successfully uploaded")
            raise HTTPException(status_code=500, detail="No files were uploaded successfully")

        logger.info(f"Successfully uploaded {len(uploaded_files)} files to category: {category}")
        return {
            "message": "Files uploaded successfully by admin",
            "file_urls": [file["file_url"] for file in uploaded_files],
            "uploaded_files": uploaded_files
        }

    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Unexpected error in admin file upload: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.get("/user/fileupload", response_model=List[Dict[str, Any]])
async def get_uploaded_files(
    category: str = Query(..., description="Category of the uploaded files"),
    limit: Optional[int] = Query(10, description="Number of files per page"),
    offset: Optional[int] = Query(0, description="Page offset"),
    language: str = Query("en", description="Language for response (e.g., 'en', 'ta')")
):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            SELECT id, category, filename, file_url, uploaded_by, uploaded_at
            FROM files
            WHERE category = %s
            LIMIT %s OFFSET %s
            """,
            (category, limit, offset)
        )
        files = cur.fetchall()
        
        if not files:
            raise HTTPException(status_code=404, detail="No files found")
        
        if language == "ta":
            translator = Translator()
            category = translator.translate(category, dest="ta").text
        
        uploaded_files = [
            {
                "id": file[0],
                "category": category,
                "filename": file[2],
                "file_url": file[3],
                "uploaded_by": file[4],
                "uploaded_at": file[5]
            }
            for file in files
        ]
        
        return uploaded_files
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving files for category {category}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/admin/private/fileupload", response_model=Dict[str, Any])
async def admin_upload_private_files(
    files: List[UploadFile] = File(...),
    category: str = Form(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "user":
        logger.warning(f"Unauthorized private upload attempt by user type: {current_user.get('user_type')}")
        raise HTTPException(status_code=403, detail="Only user can access this endpoint")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        logger.info(f"Received {len(files)} private files for category: {category} from user: {current_user['id']}")
        cur.execute(
            "SELECT private_files_id FROM private_files WHERE uploaded_by = %s AND category = %s",
            (current_user["id"], category)
        )
        result = cur.fetchone()

        if result:
            private_files_id = result[0]
        else:
            cur.execute(
                "INSERT INTO private_files (uploaded_by, category) VALUES (%s, %s) RETURNING private_files_id",
                (current_user["id"], category)
            )
            private_files_id = cur.fetchone()[0]

        uploaded_file_info = []

        for file in files:
            try:
                file_url = file_handler.upload_file(file, "admin_files")
                if not file_url:
                    continue

                file_type = file.content_type

                cur.execute(
                    """
                    INSERT INTO private_files_url (private_files_id, file_type, file_url, uploaded_at)
                    VALUES (%s, %s, %s, NOW())
                    RETURNING id, file_url, file_type, uploaded_at
                    """,
                    (private_files_id, file_type, file_url)
                )
                uploaded = cur.fetchone()
                uploaded_file_info.append({
                    "url": uploaded[1],
                    "file_type": uploaded[2],
                    "uploaded_at": uploaded[3]
                })

            except Exception as e:
                logger.error(f"Upload Error for {file.filename}: {str(e)}")
                logger.error(traceback.format_exc())
            
        if not uploaded_file_info:
            logger.warning("No private files were uploaded successfully")
            raise HTTPException(status_code=400, detail="No files were uploaded successfully")

        conn.commit()

        logger.info(f"Successfully uploaded {len(uploaded_file_info)} private files for category: {category}")
        return {
            "message": "Files uploaded successfully by admin",
            "private_files_id": private_files_id,
            "uploaded_by": current_user["id"],
            "category": category,
            "uploaded_files": uploaded_file_info
        }

    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Unexpected error in private file upload: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.get("/admin/private/get_files", response_model=Dict[str, Any])
async def get_user_uploaded_files(
    user_id: int = Query(..., alias="user_id"),
    file_id: int = Query(None, alias="file_id")
):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT private_files_id, category 
            FROM private_files 
            WHERE uploaded_by = %s
            """,
            (user_id,)
        )
        private_file_records = cur.fetchall()

        if not private_file_records:
            return {"message": "No selected files found", "selected_files": []}

        all_files_data = []

        for private_files_id, category_value in private_file_records:
            query = """
                SELECT file_url, file_type, user_selected_files, uploaded_at, id
                FROM private_files_url
                WHERE private_files_id = %s
            """
            params = [private_files_id]

            if file_id:
                query += " AND id = %s"
                params.append(file_id)

            cur.execute(query + " ORDER BY uploaded_at DESC", tuple(params))
            files = cur.fetchall()

            if files:
                files_data = [
                    {
                        "file_url": row[0],
                        "file_type": row[1],
                        "user_selected_files": row[2],
                        "uploaded_at": row[3],
                        "id": row[4],
                        "category": category_value,
                        "private_files_id": private_files_id
                    }
                    for row in files
                ]
                all_files_data.extend(files_data)

        if not all_files_data:
            raise HTTPException(status_code=404, detail="No files found for this user")

        return {
            "uploaded_by": user_id,
            "uploaded_files": all_files_data
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving uploaded files for user {user_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.put("/admin/private/fileupdate", response_model=Dict[str, Any])
async def update_uploaded_file(
    request_data: GetFileUpdate,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "user":
        raise HTTPException(status_code=403, detail="Only user can access this endpoint")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT pf.private_files_id 
            FROM private_files_url pfu
            JOIN private_files pf ON pf.private_files_id = pfu.private_files_id
            WHERE pfu.id = %s AND pf.uploaded_by = %s AND pf.category = %s
            """,
            (request_data.file_id, current_user["id"], request_data.category)
        )
        result = cur.fetchone()

        if not result:
            raise HTTPException(status_code=200, detail="File not found or not accessible")

        private_files_id = result[0]

        cur.execute(
            """
            UPDATE private_files_url 
            SET file_url = %s, file_type = %s
            WHERE id = %s
            """,
            (request_data.file_url, request_data.file_type, request_data.file_id)
        )
        conn.commit()

        return {
            "message": "File updated successfully",
            "updated_file": {
                "file_url": request_data.file_url,
                "file_type": request_data.file_type,
                "file_id": request_data.file_id,
                "private_files_id": private_files_id,
                "category": request_data.category
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating uploaded file {request_data.file_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.delete("/admin/private/filedelete", response_model=Dict[str, Any])
async def delete_uploaded_file(
    file_id: int = Query(...),
    category: str = Query(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "user":
        raise HTTPException(status_code=403, detail="Only user can access this endpoint")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT private_files_id FROM private_files_url 
            WHERE id = %s AND private_files_id IN (
                SELECT private_files_id FROM private_files WHERE uploaded_by = %s AND category = %s
            )
            """,
            (file_id, current_user["id"], category)
        )
        result = cur.fetchone()

        if not result:
            raise HTTPException(status_code=200, detail="File not found or not accessible")

        private_files_id = result[0]

        cur.execute(
            """
            DELETE FROM private_files_url WHERE id = %s
            """,
            (file_id,)
        )
        conn.commit()

        return {
            "message": "File deleted successfully",
            "deleted_file_id": file_id,
            "private_files_id": private_files_id,
            "category": category
        }

    except Exception as e:
        logging.error(f"[DELETE Error] {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete file")
    finally:
        cur.close()
        conn.close()

@router.delete("/admin/private/delete_all", response_model=Dict[str, Any])
async def delete_files_by_private_id(
    private_files_id: int = Query(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "user":
        raise HTTPException(status_code=403, detail="Only user can access this endpoint")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT 1 FROM private_files 
            WHERE private_files_id = %s AND uploaded_by = %s
            """,
            (private_files_id, current_user["id"])
        )
        if not cur.fetchone():
            raise HTTPException(status_code=200, detail="private_files_id not found or not owned by user")

        cur.execute(
            """
            DELETE FROM private_files_url 
            WHERE private_files_id = %s
            """,
            (private_files_id,)
        )
        conn.commit()

        return {
            "message": "All files under the given private_files_id deleted successfully",
            "private_files_id": private_files_id
        }

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error deleting files by private_id {private_files_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to delete files")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/user/private/select-files", response_model=Dict[str, Any])
async def user_select_files(request: FileSelectionsRequest):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        updated_records = []
        for file_data in request.private_files:
            private_files_id = file_data.private_files_id
            selected_urls = file_data.selected_urls

            cur.execute("SELECT category FROM private_files WHERE private_files_id = %s", (private_files_id,))
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail=f"Invalid private_files_id: {private_files_id}")
            category = result[0]

            cur.execute("SELECT id, file_url, user_selected_files FROM private_files_url WHERE private_files_id = %s", (private_files_id,))
            all_files = cur.fetchall()

            for fid, url, existing_selection in all_files:
                is_selected = url in selected_urls
                updated_selection = json.dumps({"selected": is_selected})

                cur.execute("""
                    UPDATE private_files_url
                    SET user_selected_files = %s
                    WHERE id = %s
                    RETURNING id, file_url, user_selected_files
                """, (updated_selection, fid))
                record = cur.fetchone()
                updated_records.append((record[0], record[1], record[2], category))

        conn.commit()

        updated_result = [
            {
                "file_id": row[0],
                "file_url": row[1],
                "selected_status": row[2] if isinstance(row[2], dict) else json.loads(row[2]),
                "category": row[3]
            }
            for row in updated_records
        ]

        return {
            "message": "File selections updated successfully.",
            "private_files_id": private_files_id,
            "uploaded_by": request.user_id,
            "updated_files": updated_result
        }

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error updating user selections: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.get("/user/private/get_select_files", response_model=Dict[str, Any])
async def user_get_all_selected_files(user_id: int = Query(...)):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT private_files_id, category FROM private_files WHERE uploaded_by = %s", (user_id,))
        private_files = cur.fetchall()

        all_files_result = []
        for private_files_id, category in private_files:
            cur.execute("SELECT id, file_url, user_selected_files FROM private_files_url WHERE private_files_id = %s", (private_files_id,))
            all_files = cur.fetchall()

            updated_result = [
                {
                    "file_id": row[0],
                    "file_url": row[1],
                    "selected_status": row[2] if isinstance(row[2], dict) else (json.loads(row[2]) if row[2] else {})
                }
                for row in all_files
            ]

            all_files_result.append({
                "category": category,
                "private_files_id": private_files_id,
                "uploaded_by": user_id,
                "selected_files": updated_result
            })

        return {
            "message": "File selection data fetched successfully.",
            "files_data": all_files_result
        }

    except Exception as e:
        logger.error(f"Error fetching file selections for user {user_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.get("/admin/private/unselected-files", response_model=Dict[str, Any])
async def admin_get_unselected_files(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT 
                pf.private_files_id,
                pf.category,
                pf.uploaded_by,
                pfu.id AS file_id,
                pfu.file_url,
                pfu.user_selected_files
            FROM private_files pf
            JOIN private_files_url pfu ON pf.private_files_id = pfu.private_files_id
            WHERE pf.uploaded_by = %s
              AND (
                  pfu.user_selected_files IS NULL
                  OR (pfu.user_selected_files::json ->> 'selected')::boolean = false
              )
        """, (user_id,))

        rows = cur.fetchall()
        result = [
            {
                "private_files_id": row["private_files_id"],
                "category": row["category"],
                "uploaded_by": row["uploaded_by"],
                "file_id": row["file_id"],
                "file_url": row["file_url"],
                "selected_status": (
                    row["user_selected_files"]
                    if isinstance(row["user_selected_files"], dict)
                    else json.loads(row["user_selected_files"]) if row["user_selected_files"]
                    else {"selected": False}
                )
            }
            for row in rows
        ]

        return {
            "uploaded_by": user_id,
            "unselected_files": result,
            "total_unselected": len(result)
        }

    except Exception as e:
        logger.error(f"Error fetching unselected files for user {user_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to fetch unselected files")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.get("/admin/product_frames", response_model=List[Dict[str, Any]])
async def get_product_frames(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM product_frames ORDER BY id DESC")
        frames = cur.fetchall()
        return [dict(f) for f in frames]
    except Exception as e:
        logger.error(f"Error fetching product frames: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/admin/product_frames", response_model=Dict[str, Any])
async def create_admin_product_frame(
    frame_name: str = Form(...),
    phone_number: str = Form(...),
    user_photos: List[UploadFile] = File(...),
    frame_size: str = Form(...),
    frame_colors: List[UploadFile] = File(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        user_photo_urls = [file_handler.upload_file(photo, "admin_user_photos") for photo in user_photos]
        frame_color_urls = [file_handler.upload_file(color, "admin_frame_colors") for color in frame_colors]

        cur.execute(
            """
            INSERT INTO product_frames (
                frame_name, phone_number, frame_size,
                user_photo_urls, frame_color_urls, uploaded_by, uploaded_by_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (frame_name, phone_number, frame_size, user_photo_urls, frame_color_urls, current_user["id"], "admin")
        )
        frame_id = cur.fetchone()[0]
        conn.commit()

        return {
            "frame_id": frame_id,
            "frame_name": frame_name,
            "phone_number": phone_number,
            "frame_size": frame_size,
            "user_photo_urls": user_photo_urls,
            "frame_color_urls": frame_color_urls,
            "uploaded_by": current_user["id"]
        }

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error creating admin product frame: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@router.post("/user/product_frame")
async def create_product_frame(
    frame_name: str = Form(...),
    phone_number: str = Form(...),
    user_photos: List[UploadFile] = File(...),
    frame_size: str = Form(...),
    frame_colors: List[UploadFile] = File(...),
):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        user_photo_urls = [file_handler.upload_file(photo, "user_photos") for photo in user_photos]
        frame_color_urls = [file_handler.upload_file(color, "frame_colors") for color in frame_colors]

        cur.execute(
            """
            INSERT INTO product_frames (frame_name, phone_number, frame_size, user_photo_urls, frame_color_urls)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (frame_name, phone_number, frame_size, user_photo_urls, frame_color_urls)
        )
        frame_id = cur.fetchone()[0]
        conn.commit()

        return {
            "message": f"{frame_name}, {frame_id}, {frame_size}, Frame submitted successfully for {phone_number}",
            "frame_id": frame_id,
            "frame_name": frame_name,
            "phone_number": phone_number,
            "frame_size": frame_size,
            "user_photo_urls": user_photo_urls,
            "frame_color_urls": frame_color_urls
        }

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logger.error(f"Error creating product frame: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
