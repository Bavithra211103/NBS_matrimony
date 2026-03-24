from fastapi import APIRouter, Depends, HTTPException, Form, File, UploadFile, Query, Body, Request
from typing import List, Dict, Any, Optional, Literal, Annotated
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
import logging
import traceback
from datetime import datetime, date, time, timedelta
import re
from core.config import settings
from db.session import get_db_connection
from utils.matchers import NakshatraMatcher
from core.security import (
    get_current_user, get_current_user_matrimony, is_user_blocked, 
    get_password_hash, verify_password, create_access_token, create_refresh_token,
    pwd_context
)
from models.schemas import (
    MatrimonyRegister, MatrimonyRegisterResponse, MatrimonyLoginRequest,
    MatrimonyToken, MatrimonyProfileResponse, MatrimonyProfilesWithMessage,
    RefreshTokenRequest, TokenResponse, OTPRequest, OTPVerify, IncrementMatrimonyIdRequest,
    DeactivationReportRequest, ChatUserRequest, ChatRequest, FavoriteProfilesRequest,
    MarkViewedRequest, ContactUsCreate, ContactUsResponse, ReportSchema,
    BlockUserSchema, UnblockUserSchema, AdminProfileVerificationSummary,
    ProfileVerificationUpdate, SpendAction, SpendRequest, EmailVerificationRequest,
    ForgotPasswordRequest, AdminChatMessage, ViewedProfilesResponse
)
from utils.file_handler import file_handler
from utils.helpers import generate_matrimony_id, send_push_notification
from astrology_terms import ASTROLOGY_TERMS
from googletrans import Translator
from pydantic import ValidationError

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/matrimony", tags=["Matrimony"])

def clean_value(value):
    if value is None:
        return None
    str_val = str(value).strip().lower()
    if str_val in ("", "nan", "none", "null", "n/a"):
        return None
    return value

def clean_int(value):
    if value is None:
        return None
    try:
        str_val = str(value).strip()
        if str_val.lower() in ("", "nan", "none", "null", "n/a"):
            return None
        return int(float(str_val))
    except:
        return None

@router.post("/register")
async def register_matrimony(
    full_name: str = Form(...),
    age: str = Form(...),
    gender: str = Form(...),
    date_of_birth: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    phone_number: str = Form(...),
    height: Optional[str] = Form(None),
    weight: Optional[str] = Form(None),
    occupation: Optional[str] = Form(None),
    annual_income: Optional[str] = Form(None),
    education: Optional[str] = Form(None),
    mother_tongue: Optional[str] = Form(None),
    profile_created_by: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    work_type: Optional[str] = Form(None),
    company: Optional[str] = Form(None),
    work_location: Optional[str] = Form(None),
    work_country: Optional[str] = Form(None),
    mother_name: Optional[str] = Form(None),
    father_name: Optional[str] = Form(None),
    sibling_count: Optional[str] = Form(None),
    elder_brother: Optional[str] = Form(None),
    elder_sister: Optional[str] = Form(None),
    younger_sister: Optional[str] = Form(None),
    younger_brother: Optional[str] = Form(None),
    native: Optional[str] = Form(None),
    mother_occupation: Optional[str] = Form(None),
    father_occupation: Optional[str] = Form(None),
    religion: Optional[str] = Form(None),
    caste: Optional[str] = Form(None),
    sub_caste: Optional[str] = Form(None),
    nakshatra: Optional[str] = Form(None),
    rashi: Optional[str] = Form(None),
    other_dhosham: Optional[str] = Form(None),
    quarter: Optional[str] = Form(None),
    birth_time: Optional[str] = Form(None),
    birth_place: Optional[str] = Form(None),
    ascendent: Optional[str] = Form(None),
    dhosham: Optional[str] = Form(None),
    user_type: Optional[str] = Form(None),
    marital_status: Optional[str] = Form(None),
    preferred_age_min: Optional[str] = Form(None),
    preferred_age_max: Optional[str] = Form(None),
    preferred_height_min: Optional[str] = Form(None),
    preferred_height_max: Optional[str] = Form(None),
    preferred_religion: Optional[str] = Form(None),
    preferred_caste: Optional[str] = Form(None),
    preferred_sub_caste: Optional[str] = Form(None),
    preferred_nakshatra: Optional[str] = Form(None),
    preferred_rashi: Optional[str] = Form(None),
    preferred_location: Optional[str] = Form(None),
    preferred_work_status: Optional[str] = Form(None),
    photo: Annotated[Optional[UploadFile], File()] = None,
    photos: Annotated[Optional[List[UploadFile]], File()] = None,
    horoscope_documents: Annotated[Optional[List[UploadFile]], File()] = None,
    matrimony_id: Optional[str] = Form(None),
    blood_group: Optional[str] = Form(None)
):
    try:
        hashed_password = get_password_hash(password)

        # Handle single profile photo
        photo_url = None
        if photo:
            try:
                photo_url = file_handler.upload_file(photo, "profile_photos")
            except Exception as e:
                logger.error(f"Profile photo upload failed: {str(e)}")
                raise HTTPException(status_code=400, detail="Profile photo upload failed")

        # Handle multiple photos
        photos_urls = []
        if photos:
            for p in photos:
                try:
                    url = file_handler.upload_file(p, "photos")
                    photos_urls.append(url)
                except Exception as e:
                    logger.error(f"Photo upload failed: {str(e)}")
                    continue

        # Handle horoscope documents
        horoscope_urls = []
        if horoscope_documents:
            for h in horoscope_documents:
                try:
                    url = file_handler.upload_file(h, "horoscopes")
                    horoscope_urls.append(url)
                except Exception as e:
                    logger.error(f"Horoscope upload failed: {str(e)}")
                    continue

        def format_array(urls):
            return "{" + ",".join(urls) + "}" if urls else None

        photos_array = format_array(photos_urls)
        horoscope_array = format_array(horoscope_urls)

        actual_matrimony_id = matrimony_id or generate_matrimony_id()

        values = (
            clean_value(actual_matrimony_id),
            clean_value(full_name),
            clean_int(age),
            clean_value(gender),
            clean_value(date_of_birth),
            clean_value(email),
            hashed_password,
            clean_value(phone_number),
            clean_int(height),
            clean_int(weight),
            clean_value(occupation),
            clean_int(annual_income),
            clean_value(education),
            clean_value(mother_tongue),
            clean_value(profile_created_by),
            clean_value(address),
            clean_value(work_type),
            clean_value(company),
            clean_value(work_location),
            clean_value(work_country),
            clean_value(mother_name),
            clean_value(father_name),
            clean_int(sibling_count),
            clean_int(elder_brother),
            clean_int(elder_sister),
            clean_int(younger_sister),
            clean_int(younger_brother),
            clean_value(native),
            clean_value(mother_occupation),
            clean_value(father_occupation),
            clean_value(religion),
            clean_value(caste),
            clean_value(sub_caste),
            clean_value(nakshatra),
            clean_value(rashi),
            clean_value(birth_time),
            clean_value(birth_place),
            clean_value(ascendent),
            clean_value(user_type),
            clean_int(preferred_age_min),
            clean_int(preferred_age_max),
            clean_int(preferred_height_min),
            clean_int(preferred_height_max),
            clean_value(preferred_religion),
            clean_value(preferred_caste),
            clean_value(preferred_sub_caste),
            clean_value(preferred_nakshatra),
            clean_value(preferred_rashi),
            clean_value(preferred_location),
            clean_value(preferred_work_status),
            clean_value(photo_url),
            clean_value(photos_array),
            clean_value(horoscope_array),
            clean_value(dhosham),
            clean_value(other_dhosham),
            clean_value(quarter),
            clean_value(marital_status),
            clean_value(blood_group),
        )

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)

        query = f"""
        INSERT INTO matrimony_profiles (
            matrimony_id, full_name, age, gender, date_of_birth,
            email, password, phone_number, height, weight, occupation,
            annual_income, education, mother_tongue, profile_created_by,
            address, work_type, company, work_location, work_country,
            mother_name, father_name, sibling_count, elder_brother, elder_sister, younger_sister, younger_brother,
            native, mother_occupation, father_occupation,
            religion, caste, sub_caste, nakshatra, rashi, birth_time,
            birth_place, ascendent, user_type, preferred_age_min,
            preferred_age_max, preferred_height_min, preferred_height_max,
            preferred_religion, preferred_caste, preferred_sub_caste,
            preferred_nakshatra, preferred_rashi, preferred_location,
            preferred_work_status, photo_path, photos, 
            horoscope_documents, dhosham, other_dhosham, quarter, marital_status, blood_group
        ) VALUES (
            {','.join(['%s'] * len(values))}
        ) ON CONFLICT (email) DO NOTHING
        RETURNING matrimony_id;
        """

        cur.execute(query, values)
        result = cur.fetchone()
        conn.commit()

        return {
            "status": "success",
            "message": "Profile registered successfully",
            "matrimony_id": result["matrimony_id"] if result else actual_matrimony_id,
            "email": email,
            "password": password
        }

    except psycopg2.Error as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Database error in matrimony registration: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal database error")
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Unexpected error in matrimony registration: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

@router.post("/login", response_model=MatrimonyToken)
async def login_matrimony(request: MatrimonyLoginRequest):
    try:
        if not request.password and not request.phone_number:
            raise HTTPException(status_code=400, detail="Either password or phone_number must be provided")

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)

        cur.execute("SELECT * FROM matrimony_profiles WHERE matrimony_id = %s", (request.matrimony_id,))
        user = cur.fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        cur.execute("SELECT * FROM blocked_users WHERE blocked_matrimony_id = %s AND is_blocked = true", (request.matrimony_id,))
        blocked_record = cur.fetchone()

        if blocked_record:
            reason = blocked_record.get("reason") or "No reason specified"
            raise HTTPException(status_code=403, detail=f"Admin has blocked this profile. Reason: {reason}")

        stored_password = user.get("password")
        stored_phone = user.get("phone_number")

        if request.via_link:
            if not request.password:
                raise HTTPException(status_code=400, detail="Password is required for link login")
            if not stored_password or not verify_password(request.password, stored_password):
                raise HTTPException(status_code=401, detail="Invalid password for link login")
        else:
            if request.password:
                if not stored_password or not verify_password(request.password, stored_password):
                    raise HTTPException(status_code=401, detail="Invalid password")
            elif request.phone_number:
                if request.phone_number != stored_phone:
                    raise HTTPException(status_code=401, detail="Invalid phone number")
            else:
                raise HTTPException(status_code=400, detail="Password or phone number is required")

        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(data={"sub": user["matrimony_id"], "user_type": "user"}, expires_delta=access_token_expires)

        refresh_token_expires = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
        refresh_token = create_refresh_token(data={"sub": user["matrimony_id"], "user_type": "user"}, expires_delta=refresh_token_expires)

        expires_at = datetime.utcnow() + refresh_token_expires
        cur.execute("""
            INSERT INTO matrimony_refresh_tokens (matrimony_id, token, expires_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (matrimony_id) DO UPDATE
            SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
        """, (user["matrimony_id"], refresh_token, expires_at))
        conn.commit()

        return MatrimonyToken(access_token=access_token, refresh_token=refresh_token, token_type="bearer", matrimony_id=request.matrimony_id)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in matrimony login for ID {request.matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

@router.get("/lastMatrimonyId")
def get_last_matrimony_id():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT matrimony_id FROM matrimony_profiles ORDER BY matrimony_id DESC LIMIT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"last_matrimony_id": result[0] if result else 11111}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.put("/incrementMatrimonyId")
def increment_matrimony_id(request: IncrementMatrimonyIdRequest):
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        numeric_part = int(re.search(r'\d+', request.last_matrimony_id).group())
        new_numeric_part = numeric_part + 1
        new_matrimony_id = f"NBS{new_numeric_part:05d}"
        cur.execute("INSERT INTO matrimony_id_tracker (last_matrimony_id, updated_at) VALUES (%s, CURRENT_TIMESTAMP)", (new_matrimony_id,))
        conn.commit()
        return {"success": True, "last_matrimony_id": new_matrimony_id}
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.post("/send-otp")
async def send_otp(request: OTPRequest):
    otp = "1234"  # ✅ Hardcoded OTP for testing
    expires_at = datetime.utcnow() + timedelta(minutes=settings.OTP_EXPIRE_MINUTES)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM otp_storage WHERE mobile_number = %s", (request.mobile_number,))
        cur.execute("""
            INSERT INTO otp_storage (mobile_number, full_name, otp, expires_at)
            VALUES (%s, %s, %s, %s)
        """, (request.mobile_number, request.full_name, otp, expires_at))
        conn.commit()

        logger.info(f"[DEV LOG] OTP for {request.mobile_number}: {otp}")

        return {"message": "OTP generated and saved", "mobile_number": request.mobile_number}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        cur.close()
        conn.close()

@router.post("/verify-otp")
async def verify_otp(request: OTPVerify):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT otp, expires_at FROM otp_storage WHERE mobile_number = %s
        """, (request.mobile_number,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="OTP not found")

        stored_otp, expires_at = row
        if stored_otp != request.otp:
            raise HTTPException(status_code=400, detail="Invalid OTP")
        if datetime.utcnow() > expires_at:
            raise HTTPException(status_code=400, detail="OTP expired")

        return {"message": "OTP verified successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

@router.post("/refresh-token", response_model=TokenResponse)
async def matrimony_refresh_token(token: RefreshTokenRequest):
    conn = None
    cur = None
    try:
        from jose import jwt
        payload = jwt.decode(token.refresh_token, settings.REFRESH_SECRET_KEY, algorithms=[settings.ALGORITHM])
        matrimony_id = payload.get("sub")
        if not matrimony_id:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT is_valid FROM matrimony_refresh_tokens WHERE token = %s", (token.refresh_token,))
        db_token = cur.fetchone()
        if not db_token or not db_token[0]:
            raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

        access_token = create_access_token({"sub": matrimony_id, "user_type": "user"})
        new_refresh_token = create_refresh_token({"sub": matrimony_id, "user_type": "user"})

        cur.execute("UPDATE matrimony_refresh_tokens SET is_valid = false WHERE token = %s", (token.refresh_token,))
        cur.execute("""
            INSERT INTO matrimony_refresh_tokens (matrimony_id, token, expires_at, is_valid)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (matrimony_id) DO UPDATE SET
                token = EXCLUDED.token, expires_at = EXCLUDED.expires_at, is_valid = TRUE
        """, (matrimony_id, new_refresh_token, datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)))
        conn.commit()

        return {"access_token": access_token, "refresh_token": new_refresh_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Refresh token error: {e}")
        raise HTTPException(status_code=500, detail="Unexpected server error")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.get("/profiles", response_model=Dict[str, List[MatrimonyProfileResponse]])
async def get_matrimony_profiles(
    current_user: Dict[str, Any] = Depends(get_current_user_matrimony),
    language: Optional[str] = Query("en", description="Language for response (e.g., 'en', 'ta')"),
):

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        logger.info(f"Current user: {current_user}")
        logger.info(f"Requested language: {language}")

        user_type = (current_user.get("user_type") or "user").lower()

        # Conditional base query depending on user type
        if user_type == "admin":
            query = "SELECT * FROM matrimony_profiles WHERE is_active = true"
        else:
            query = "SELECT * FROM matrimony_profiles WHERE is_active = true AND verification_status = 'approve'"

        params = []
        

        if user_type != "admin":
            # Users: show opposite gender and exclude globally blocked profiles
            user_gender = current_user.get("gender")
            if not user_gender:
                raise HTTPException(status_code=400, detail="User gender not found")

            opposite_gender = "Female" if user_gender.lower() == "male" else "Male"

            query += """
                AND gender ILIKE %s
                AND matrimony_id NOT IN (
                    SELECT blocked_matrimony_id
                    FROM blocked_users
                    WHERE is_blocked = true
                    AND blocked_matrimony_id IS NOT NULL
                )
            """
            params.append(opposite_gender)
            logger.info(f"User view - Filtering opposite gender: {opposite_gender}")

        cur.execute(query, params)
        profiles = cur.fetchall()
        logger.info(f"Fetched profiles count: {len(profiles)}")

        if not profiles:
            return {"Profiles": []} if user_type == "admin" else {"New Profiles": [], "Default Profiles": []}

        # Optional translation
        translator = None
        if language and language.lower() != "en":
            try:
                translator = Translator()
                translator.translate("test", src="en", dest=language)
                logger.info(f"Translator initialized for language: {language}")
            except Exception as e:
                logger.error(f"Translator failed to initialize: {e}")
                translator = None

        # Local storage helpers
        def process_static_url(url, folder_name):
            if url and isinstance(url, str):
                if url.startswith("http"):
                    return url
                return f"/static/{folder_name}/{url}"
            return None

        def process_static_urls(value, folder_name):
            if not value:
                return None
            if isinstance(value, str):
                items = [item.strip().strip('"') for item in value.strip('{}').split(',') if item.strip()]
            elif isinstance(value, list):
                items = value
            else:
                return None
            if not items:
                return None
            return [
                item if item.startswith("http") or item.startswith("/static/") else f"/static/{folder_name}/{item}"
                for item in items
            ]

        def translate_static_term(term: str, lang: str) -> str:
            key = term.strip().lower().replace(" ", "_")
            return ASTROLOGY_TERMS.get(key, {}).get(lang, term)

        new_profiles = []
        default_profiles = []
        all_profiles = []
        cutoff_date = datetime.now() - timedelta(days=30)


        for profile in profiles:
            profile_dict = dict(profile)

            for key, value in profile_dict.items():
                if isinstance(value, str) and not value.strip():
                    profile_dict[key] = None

            profile_dict["photo"] = process_static_url(profile_dict.get("photo_path"), "profile_photos")
            profile_dict["photos"] = process_static_urls(profile_dict.get("photos"), "photos")
            profile_dict["horoscope_documents"] = process_static_urls(profile_dict.get("horoscope_documents"), "horoscopes")

            if isinstance(profile_dict.get("birth_time"), time):
                profile_dict["birth_time"] = profile_dict["birth_time"].strftime('%H:%M:%S')

            if isinstance(profile_dict.get("date_of_birth"), (datetime, date)):
                profile_dict["date_of_birth"] = profile_dict["date_of_birth"].strftime('%Y-%m-%d')

            if profile_dict.get("date_of_birth"):
                try:
                    dob = datetime.strptime(profile_dict["date_of_birth"], '%Y-%m-%d')
                    today = datetime.today()
                    profile_dict["age"] = str(today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day)))
                except:
                    pass

            profile_dict["is_translated"] = False

            if translator and language.lower() != "en":
                translatable_fields = [
                    "full_name", "occupation", "gender", "education", "mother_tongue", "dhosham",
                    "work_type", "company", "work_location", "religion", "caste", "sub_caste"
                ]
                for field in translatable_fields:
                    if field in profile_dict and isinstance(profile_dict[field], str):
                        try:
                            translated = translator.translate(profile_dict[field], src="en", dest=language)
                            profile_dict[field] = translated.text
                            profile_dict["is_translated"] = True
                        except Exception as e:
                            logger.warn(f"Translation failed for {field}: {e}")

                for astro_field in ["nakshatra", "rashi", "dhosham"]:
                    if profile_dict.get(astro_field):
                        profile_dict[astro_field] = translate_static_term(profile_dict[astro_field], language)
                        profile_dict["is_translated"] = True

            try:
                profile_obj = MatrimonyProfileResponse(**profile_dict)
                updated_at = profile.get("updated_at")

                if user_type == "admin":
                    all_profiles.append(profile_obj)
                else:
                    if updated_at and isinstance(updated_at, datetime) and updated_at >= cutoff_date:
                        new_profiles.append(profile_obj)
                    else:
                        default_profiles.append(profile_obj)
            except ValidationError as e:
                logger.error(f"Validation error for {profile_dict.get('matrimony_id')}: {e}")
                logger.error(f"Profile data that failed: {profile_dict}")
                continue

        return {"Profiles": all_profiles} if user_type == "admin" else {
            "New Profiles": new_profiles,
            "Default Profiles": default_profiles
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching matrimony profiles: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error while fetching profiles")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

@router.get("/preference")
async def get_matrimony_preferences(current_user: Dict[str, Any] = Depends(get_current_user_matrimony)):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        cur.execute("SELECT * FROM matrimony_profiles WHERE matrimony_id = %s", [current_user.get("matrimony_id")])
        user_profile = cur.fetchone()
        if not user_profile:
            return {"message": "User not found", "profiles": [], "matching_profiles": []}
        
        user_gender = user_profile['gender'].strip().lower()
        opposite_gender = "female" if user_gender == "male" else "male"
        user_star = user_profile['nakshatra']
        
        query = """
            SELECT * FROM matrimony_profiles
            WHERE LOWER(gender) = %s AND matrimony_id != %s
            AND is_active = TRUE AND is_verified = true AND verification_status = 'approve'
        """
        cur.execute(query, [opposite_gender, current_user['matrimony_id']])
        candidates = cur.fetchall()
        
        matcher = NakshatraMatcher()
        matches = []
        for cand in candidates:
            c = dict(cand)
            other_star = c.get("nakshatra")
            if not other_star: continue
            
            res = matcher.check_compatibility(user_star, other_star) if user_gender == "male" else matcher.check_compatibility(other_star, user_star)
            if res["is_utthamam"] or res["is_madhyamam"]:
                c["nakshatra_match_type"] = "Utthamam" if res["is_utthamam"] else "Madhyamam"
                matches.append(c)
        
        return {"message": f"Found {len(matches)} matches", "profiles": matches}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching matrimony preferences for user {current_user.get('matrimony_id')}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

# overall admin view preference, location, caste preference
@router.get("/admin/preference-overview", response_model=Dict[str, Any])
async def get_matrimony_preference_overview(
    current_user: Dict[str, Any] = Depends(get_current_user_matrimony)
):
    if is_user_blocked(current_user.get("matrimony_id")):
        raise HTTPException(status_code=403, detail="Access denied. You have been blocked by admin.")

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)

    def process_static_url(url, folder_name):
        if url and isinstance(url, str):
            if url.startswith("http"):
                return url
            return f"/static/{folder_name}/{url}"
        return None

    def process_static_urls(value, folder_name):
        if not value:
            return None
        if isinstance(value, str):
            items = [item.strip().strip('"') for item in value.strip('{}').split(',') if item.strip()]
        elif isinstance(value, list):
            items = value
        else:
            return None
        return [
            item if item.startswith("http") or item.startswith("/static/") else
            f"/static/{folder_name}/{item}"
            for item in items
        ]

    def format_profile(profile_dict):
        profile_dict = dict(profile_dict)
        profile_dict.pop("password", None)
        profile_dict["photo"] = process_static_url(profile_dict.get("photo_path"), "profile_photos")
        profile_dict["photos"] = process_static_urls(profile_dict.get("photos"), "photos")
        profile_dict["horoscope_documents"] = process_static_urls(profile_dict.get("horoscope_documents"), "horoscopes")

        if isinstance(profile_dict.get("date_of_birth"), (datetime, date)):
            profile_dict["date_of_birth"] = profile_dict["date_of_birth"].strftime('%Y-%m-%d')

        if isinstance(profile_dict.get("birth_time"), time):
            profile_dict["birth_time"] = profile_dict["birth_time"].strftime('%H:%M:%S')

        if profile_dict.get("date_of_birth"):
            try:
                dob = datetime.strptime(profile_dict["date_of_birth"], '%Y-%m-%d')
                today = datetime.today()
                profile_dict["age"] = str(today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day)))
            except:
                pass

        return profile_dict

    def fetch_matches(user_profile, preference_type):
        gender = user_profile['gender'].strip()
        opposite_gender = "Male" if gender.lower() == "female" else "Female"

        field_name = "preferred_nakshatra" if preference_type == "nakshatra" else "preferred_location"
        match_field = "nakshatra" if preference_type == "nakshatra" else "work_location"

        if not user_profile.get(field_name) or not str(user_profile[field_name]).strip():
            return []

        preference_list = [n.strip() for n in user_profile[field_name].split(",") if n.strip()]
        if not preference_list:
            return []

        query = f"""
            SELECT * FROM matrimony_profiles
            WHERE gender ILIKE %s
            AND matrimony_id != %s
            AND is_active = TRUE
            AND matrimony_id NOT IN (
                SELECT blocked_matrimony_id 
                FROM blocked_users 
                WHERE is_blocked = TRUE
            )
            AND ({match_field} IS NOT NULL AND LOWER({match_field}) = ANY(%s))
        """
        params = [opposite_gender, user_profile['matrimony_id'], [n.lower() for n in preference_list]]

        cur.execute(query, params)
        results = cur.fetchall()

        compatible_profiles = []
        for profile in results:
            profile_dict = format_profile(profile)
            if not profile_dict.get(match_field) or profile_dict[match_field].strip().lower() in ["null", "nan", "n/a", ""]:
                continue
            compatible_profiles.append(profile_dict)

        return compatible_profiles

    try:
        cur.execute("SELECT * FROM matrimony_profiles WHERE is_active = TRUE")
        all_users = cur.fetchall()

        user_profiles = []
        preference_matches = []
        location_matches = []

        for user in all_users:
            formatted_user = format_profile(user)
            user_profiles.append(formatted_user)

            # Nakshatra preference
            nakshatra_profiles = fetch_matches(user, "nakshatra")
            preference_matches.append({
                "message": f"Compatible nakshatra matches for {user['matrimony_id']}",
                "profiles": nakshatra_profiles
            })

            # Location preference
            location_profiles = fetch_matches(user, "location")
            location_matches.append({
                "message": f"Compatible location matches for {user['matrimony_id']}",
                "profiles": location_profiles
            })

        return {
            "user_profiles": user_profiles,
            "preference": preference_matches,
            "location_preference": location_matches
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()

# Endpoint to send push notifications
@router.post("/send-notification", response_model=Dict[str, Any])
async def send_notification(
    token: str = Query(..., description="Device token to send the notification to"),
    title: str = Query(..., description="Title of the notification"),
    body: str = Query(..., description="Body of the notification"),
):
    """
    Send a push notification to a specific device token.
    """
    return send_push_notification(token, title, body)

# Testing for the myprofiles endpoint
@router.get("/my_profiles")
async def get_my_profiles(current_user: dict = Depends(get_current_user_matrimony)):
    if is_user_blocked(current_user.get("matrimony_id")):
        raise HTTPException(status_code=403, detail="Access denied. You have been blocked by admin.")

    email = current_user.get("email")
    matrimony_id = current_user.get("matrimony_id")

    if not email and not matrimony_id:
        raise HTTPException(status_code=400, detail="No valid identifier found in token")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
        SELECT 
            mp.*, 
            COALESCE(sa.points_spent, 0) AS points_spent
        FROM 
            matrimony_profiles mp
        LEFT JOIN (
            SELECT matrimony_id, SUM(points) AS points_spent
            FROM spend_actions
            GROUP BY matrimony_id
        ) sa ON mp.matrimony_id = sa.matrimony_id
        WHERE 
            (%(email)s IS NULL OR mp.email = %(email)s)
            AND (%(matrimony_id)s IS NULL OR mp.matrimony_id = %(matrimony_id)s)
            AND mp.is_active = TRUE
            AND mp.is_verified = TRUE
            AND mp.verification_status = 'approve'
        LIMIT 1;
        """
        cur.execute(query, {"email": email, "matrimony_id": matrimony_id})
        profile = cur.fetchone()

        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")

        return {
            "status": "success",
            "profile": profile
        }

    except psycopg2.Error as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {type(e).__name__}: {str(e)}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@router.put("/my_profiles")
async def update_matrimony_profile(
    matrimony_id: Optional[str] = Form(None),
    full_name: Optional[str] = Form(None),
    age: Optional[str] = Form(None),
    gender: Optional[str] = Form(None),
    date_of_birth: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    phone_number: Optional[str] = Form(None),
    height: Optional[str] = Form(None),
    weight: Optional[str] = Form(None),
    occupation: Optional[str] = Form(None),
    annual_income: Optional[str] = Form(None),
    education: Optional[str] = Form(None),
    mother_tongue: Optional[str] = Form(None),
    profile_created_by: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    work_type: Optional[str] = Form(None),
    company: Optional[str] = Form(None),
    work_location: Optional[str] = Form(None),
    work_country: Optional[str] = Form(None),
    mother_name: Optional[str] = Form(None),
    father_name: Optional[str] = Form(None),
    sibling_count: Optional[str] = Form(None),
    elder_brother: Optional[str] = Form(None),
    elder_sister: Optional[str] = Form(None),
    younger_sister: Optional[str] = Form(None),
    younger_brother: Optional[str] = Form(None),
    native: Optional[str] = Form(None),
    mother_occupation: Optional[str] = Form(None),
    father_occupation: Optional[str] = Form(None),
    religion: Optional[str] = Form(None),
    caste: Optional[str] = Form(None),
    sub_caste: Optional[str] = Form(None),
    nakshatra: Optional[str] = Form(None),
    rashi: Optional[str] = Form(None),
    birth_time: Optional[str] = Form(None),
    birth_place: Optional[str] = Form(None),
    ascendent: Optional[str] = Form(None),
    marital_status: Optional[str] = Form(None),
    preferred_age_min: Optional[str] = Form(None),
    preferred_age_max: Optional[str] = Form(None),
    preferred_height_min: Optional[str] = Form(None),
    preferred_height_max: Optional[str] = Form(None),
    preferred_religion: Optional[str] = Form(None),
    preferred_caste: Optional[str] = Form(None),
    preferred_sub_caste: Optional[str] = Form(None),
    preferred_nakshatra: Optional[str] = Form(None),
    preferred_rashi: Optional[str] = Form(None),
    preferred_location: Optional[str] = Form(None),
    preferred_work_status: Optional[str] = Form(None),
    photo: Optional[UploadFile] = File(None),
    photos: Optional[List[UploadFile]] = File(None),
    horoscope_documents: Optional[List[UploadFile]] = File(None),
    blood_group: Optional[str] = Form(None),
    dhosham: Optional[str] = Form(None),
    other_dhosham: Optional[str] = Form(None),
    quarter: Optional[str] = Form(None),
    is_active: Optional[bool] = Form(None),
    is_verified: Optional[bool] = Form(None),
    verification_status: Optional[str] = Form(None),
    verification_comment: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user_matrimony)
):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing token")
        user_type = (current_user.get("user_type") or "").lower()

        if user_type == "admin":
            if not matrimony_id:
                raise HTTPException(
                    status_code=400,
                    detail="Admin must provide matrimony_id in form-data to update a profile"
                )
        elif user_type == "user":
            matrimony_id = current_user.get("matrimony_id")
            if not matrimony_id:
                raise HTTPException(status_code=403, detail="Unauthorized: No matrimony_id for user")
        else:
            raise HTTPException(status_code=403, detail="Unauthorized user type")


        update_fields = {
            k: v for k, v in {
                "matrimony_id": matrimony_id,
                "full_name": full_name,
                "age": age,
                "gender": gender,
                "date_of_birth": date_of_birth,
                "email": email,
                "password": get_password_hash(password) if password else None,
                "phone_number": phone_number,
                "height": height,
                "weight": weight,
                "occupation": occupation,
                "annual_income": annual_income,
                "education": education,
                "mother_tongue": mother_tongue,
                "profile_created_by": profile_created_by,
                "address": address,
                "work_type": work_type,
                "company": company,
                "work_location": work_location,
                "work_country": work_country,
                "mother_name": mother_name,
                "father_name": father_name,
                "sibling_count": sibling_count,
                "elder_brother": elder_brother,
                "elder_sister": elder_sister,
                "younger_sister": younger_sister,
                "younger_brother": younger_brother,
                "native": native,
                "mother_occupation": mother_occupation,
                "father_occupation": father_occupation,
                "religion": religion,
                "caste": caste,
                "sub_caste": sub_caste,
                "nakshatra": nakshatra,
                "rashi": rashi,
                "birth_time": birth_time,
                "birth_place": birth_place,
                "ascendent": ascendent,
                "marital_status": marital_status,
                "preferred_age_min": preferred_age_min,
                "preferred_age_max": preferred_age_max,
                "preferred_height_min": preferred_height_min,
                "preferred_height_max": preferred_height_max,
                "preferred_religion": preferred_religion,
                "preferred_caste": preferred_caste,
                "preferred_sub_caste": preferred_sub_caste,
                "preferred_nakshatra": preferred_nakshatra,
                "preferred_rashi": preferred_rashi,
                "preferred_location": preferred_location,
                "preferred_work_status": preferred_work_status,
                "blood_group": blood_group,
                "dhosham": dhosham,
                "other_dhosham": other_dhosham,
                "quarter": quarter,
                "is_active": is_active,
                "is_verified": is_verified,
                "verification_status": verification_status,
                "verification_comment": verification_comment
            }.items() if v is not None
        }

        if photo:
            photo_url = file_handler.upload_file(photo, "profile_photos")
            update_fields["photo_path"] = photo_url

        if photos:
            cur.execute("SELECT photos FROM matrimony_profiles WHERE matrimony_id = %s", (matrimony_id,))
            existing_record = cur.fetchone()
            existing_photos_list = (existing_record.get("photos") or []) if existing_record else []
            if isinstance(existing_photos_list, str):
                existing_photos_list = [x.strip() for x in existing_photos_list.strip('{}').split(',') if x.strip()]
            new_photo_urls = [file_handler.upload_file(file, "photos") for file in photos]
            all_photos = list(dict.fromkeys(existing_photos_list + new_photo_urls))
            update_fields["photos"] = "{" + ",".join(all_photos) + "}"


        if horoscope_documents:
            horoscope_urls = [file_handler.upload_file(file, "horoscopes") for file in horoscope_documents]
            update_fields["horoscope_documents"] = "{" + ",".join(horoscope_urls) + "}"

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields provided for update")

        set_clause = ", ".join([f"{key} = %({key})s" for key in update_fields if key != "matrimony_id"])

        update_query = f"""
        UPDATE matrimony_profiles
        SET {set_clause}
        WHERE matrimony_id = %(matrimony_id)s
        RETURNING *;
        """

        cur.execute(update_query, update_fields)
        updated_profile = cur.fetchone()
        conn.commit()

        if not updated_profile:
            raise HTTPException(status_code=404, detail="Profile not found or not updated")

        return {
            "status": "success",
            "message": "Profile updated successfully",
            "profile": updated_profile
        }

    except psycopg2.Error as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {type(e).__name__}: {str(e)}")

    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()


@router.delete("/delete-my_profiles")
async def delete_profile_by_id(
    matrimony_id: str = Query(..., description="Matrimony ID of the profile to delete"),
    current_user: dict = Depends(get_current_user_matrimony)
):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if matrimony_id != current_user.get("matrimony_id") and current_user.get("user_type") != "admin":
            raise HTTPException(status_code=403, detail="You are not authorized to delete this profile")

        # Delete dependent refresh tokens first
        cur.execute(
            "DELETE FROM matrimony_refresh_tokens WHERE matrimony_id = %s;",
            (matrimony_id,)
        )

        # Now delete the profile
        cur.execute(
            "DELETE FROM matrimony_profiles WHERE matrimony_id = %s RETURNING *;",
            (matrimony_id,)
        )
        deleted_profile = cur.fetchone()

        if not deleted_profile:
            raise HTTPException(status_code=404, detail="Profile not found")

        conn.commit()
        logger.info(f"Profile {matrimony_id} deleted by {current_user.get('matrimony_id') or current_user.get('email')}")
        return {"status": "success", "message": f"Profile with ID {matrimony_id} deleted"}

    except HTTPException:
        raise
    except psycopg2.Error as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Database error deleting profile {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal database error")
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Unexpected error deleting profile {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/my_profiles/activate")
async def set_profile_active_status(
    is_active: bool = Body(..., embed=True),
    current_user: dict = Depends(get_current_user_matrimony)
):
    matrimony_id = current_user.get("matrimony_id")
    if not matrimony_id:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE matrimony_profiles SET is_active = %s WHERE matrimony_id = %s RETURNING is_active;",
            (is_active, matrimony_id)
        )
        updated_status = cur.fetchone()
        conn.commit()

        if not updated_status:
            raise HTTPException(status_code=404, detail="Profile not found")

        logger.info(f"Profile {matrimony_id} active status set to {is_active}")
        return {"status": "success", "is_active": updated_status[0]}
    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error setting profile active status for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/my_profiles/activate")
async def get_profile_active_status(current_user: dict = Depends(get_current_user_matrimony)):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT is_active FROM matrimony_profiles WHERE matrimony_id = %s", (matrimony_id,))
        status_row = cur.fetchone()
        if not status_row:
            raise HTTPException(status_code=404, detail="Profile not found")
        return {"is_active": status_row[0]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching profile active status for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/admin/delete-profiles")
async def delete_profiles_by_admin(
    matrimony_ids: List[str] = Body(..., embed=True),
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can delete profiles")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        deleted_count = 0
        deleted_log_ids = []
        for m_id in matrimony_ids:
            try:
                cur.execute("""
                    INSERT INTO deleted_profiles 
                    SELECT * FROM matrimony_profiles 
                    WHERE matrimony_id = %s
                """, (m_id,))

                if cur.rowcount == 0:
                    logger.warning(f"Profile {m_id} not found, skipping")
                    continue

                cur.execute("DELETE FROM matrimony_refresh_tokens WHERE matrimony_id = %s", (m_id,))
                cur.execute("DELETE FROM matrimony_profiles WHERE matrimony_id = %s", (m_id,))

                deleted_count += 1
                deleted_log_ids.append(m_id)

            except Exception as e:
                logger.error(f"Error deleting profile {m_id}: {str(e)}")
                conn.rollback()
                continue

        conn.commit()
        logger.info(f"Admin deleted {deleted_count} profiles: {deleted_log_ids}")
        return {"status": "success", "message": f"{deleted_count} profiles deleted by admin"}

    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in admin profiles deletion: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/admin/deleted-profiles-list")
async def get_deleted_profiles_by_admin(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM deleted_profiles ORDER BY id DESC")
        profiles = cur.fetchall()
        return {"status": "success", "deleted_profiles": profiles}
    except Exception as e:
        logger.error(f"Error fetching deleted profiles list: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/user/deactivate-report")
async def report_deactivation(
    report: DeactivationReportRequest,
    current_user: dict = Depends(get_current_user_matrimony)
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO deactivation_reports (matrimony_id, reason, reported_at) VALUES (%s, %s, NOW())",
            (report.matrimony_id, report.reason)
        )
        conn.commit()
        return {"status": "success", "message": "Deactivation report submitted"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in deactivation report for {report.matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/admin/deactivate-report")
async def get_deactivation_reports(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        cur.execute("SELECT * FROM deactivation_reports ORDER BY id DESC")
        reports = cur.fetchall()
        return {"status": "success", "reports": reports}
    except Exception as e:
        logger.error(f"Error fetching deactivation reports: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# Wallet endpoints
@router.post("/wallet/recharge")
async def recharge_wallet(
    points: int = Body(..., embed=True),
    current_user: dict = Depends(get_current_user_matrimony)
):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_wallets (matrimony_id, balance) VALUES (%s, %s) ON CONFLICT (matrimony_id) DO UPDATE SET balance = user_wallets.balance + %s RETURNING balance;",
            (matrimony_id, points, points)
        )
        new_balance = cur.fetchone()[0]
        conn.commit()
        logger.info(f"Wallet recharge for {matrimony_id}: +{points} points, new balance: {new_balance}")
        return {"status": "success", "new_balance": new_balance}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in wallet recharge for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/wallet/spend")
async def spend_points_from_user_wallet(
    spend_request: SpendRequest,
    current_user: dict = Depends(get_current_user_matrimony)
):
    matrimony_id = current_user.get("matrimony_id")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        total_points = sum(action.points for action in spend_request.spend_requests)
        
        # Check balance
        cur.execute("SELECT balance FROM user_wallets WHERE matrimony_id = %s", (matrimony_id,))
        balance_row = cur.fetchone()
        if not balance_row or balance_row[0] < total_points:
            raise HTTPException(status_code=400, detail="Insufficient balance")

        # Record spend actions and update balance
        for action in spend_request.spend_requests:
            cur.execute(
                "INSERT INTO spend_actions (matrimony_id, profile_matrimony_id, points, created_at) VALUES (%s, %s, %s, NOW())",
                (matrimony_id, action.profile_matrimony_id, action.points)
            )
        
        cur.execute(
            "UPDATE user_wallets SET balance = balance - %s WHERE matrimony_id = %s RETURNING balance",
            (total_points, matrimony_id)
        )
        new_balance = cur.fetchone()[0]
        conn.commit()
        return {"status": "success", "new_balance": new_balance}
    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in wallet spend for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/wallet/spend/latest")
async def get_latest_spends(current_user: dict = Depends(get_current_user_matrimony)):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM spend_actions WHERE matrimony_id = %s ORDER BY created_at DESC LIMIT 10",
            (matrimony_id,)
        )
        spends = cur.fetchall()
        return {"status": "success", "latest_spends": spends}
    except Exception as e:
        logger.error(f"Error fetching latest spends for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/wallet/spend-history")
async def get_spend_history(current_user: dict = Depends(get_current_user_matrimony)):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM spend_actions WHERE matrimony_id = %s ORDER BY created_at DESC",
            (matrimony_id,)
        )
        history = cur.fetchall()
        return {"status": "success", "spend_history": history}
    except Exception as e:
        logger.error(f"Error fetching spend history for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/wallet/balance")
async def get_wallet_balance(current_user: dict = Depends(get_current_user_matrimony)):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT balance FROM user_wallets WHERE matrimony_id = %s", (matrimony_id,))
        row = cur.fetchone()
        balance = row[0] if row else 0
        return {"status": "success", "balance": balance}
    except Exception as e:
        logger.error(f"Error fetching wallet balance for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/favorite-profiles")
async def favorite_profiles(
    request: FavoriteProfilesRequest,
    current_user: dict = Depends(get_current_user_matrimony)
):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Add to favorites
        for fav_id in request.favorite_profile_ids:
            cur.execute(
                "INSERT INTO favorite_profiles (matrimony_id, favorite_matrimony_id, created_at) VALUES (%s, %s, NOW()) ON CONFLICT DO NOTHING",
                (matrimony_id, fav_id)
            )
            
        # Remove from favorites
        for unfav_id in request.unfavorite_profile_ids:
            cur.execute(
                "DELETE FROM favorite_profiles WHERE matrimony_id = %s AND favorite_matrimony_id = %s",
                (matrimony_id, unfav_id)
            )
            
        conn.commit()
        return {"status": "success", "message": "Favorite profiles updated"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error updating favorite profiles for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/get-favorite-profiles")
async def get_favorite_profiles(current_user: dict = Depends(get_current_user_matrimony)):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT mp.* 
            FROM matrimony_profiles mp
            JOIN favorite_profiles fp ON mp.matrimony_id = fp.favorite_matrimony_id
            WHERE fp.matrimony_id = %s
        """, (matrimony_id,))
        favorites = cur.fetchall()
        return {"status": "success", "favorite_profiles": favorites}
    except Exception as e:
        logger.error(f"Error fetching favorite profiles for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/verify-email")
async def verify_email(
    request: EmailVerificationRequest
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE matrimony_profiles SET is_email_verified = TRUE WHERE email = %s RETURNING id", (request.email,))
        updated = cur.fetchone()
        conn.commit()
        if not updated:
            raise HTTPException(status_code=404, detail="Email not found")
        logger.info(f"Email verified for {request.email}")
        return {"status": "success", "message": "Email verified successfully"}
    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in email verification for {request.email}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/forgot-password")
async def forgot_password(
    request: ForgotPasswordRequest
):
    if request.new_password != request.confirm_password:
        raise HTTPException(status_code=400, detail="Passwords do not match")
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        hashed_pw = get_password_hash(request.new_password)
        cur.execute("UPDATE matrimony_profiles SET password = %s WHERE email = %s RETURNING id", (hashed_pw, request.email))
        updated = cur.fetchone()
        conn.commit()
        if not updated:
            raise HTTPException(status_code=404, detail="Email not found")
        logger.info(f"Password reset for {request.email}")
        return {"status": "success", "message": "Password reset successfully"}
    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in forgot password for {request.email}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# Chat endpoints
@router.post("/chat/user-to-admin")
async def user_to_admin_chat(
    request: ChatUserRequest,
    current_user: dict = Depends(get_current_user_matrimony)
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO chat_messages (sender_id, receiver_id, message, timestamp) VALUES (%s, %s, %s, NOW())",
            (request.sender_id, "admin", request.message)
        )
        conn.commit()
        return {"status": "success", "message": "Message sent to admin"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in user-to-admin chat for user {request.sender_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/chat/admin-to-user")
async def admin_to_user_chat(
    request: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO chat_messages (sender_id, receiver_id, message, timestamp) VALUES (%s, %s, %s, NOW())",
            ("admin", request.receiver_id, request.message)
        )
        conn.commit()
        logger.info(f"Admin message sent to user {request.receiver_id}")
        return {"status": "success", "message": f"Message sent to user {request.receiver_id}"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in admin-to-user chat for user {request.receiver_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/chat/admin/all-messages", response_model=List[AdminChatMessage])
async def get_all_admin_messages(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM chat_messages ORDER BY timestamp DESC")
        messages = cur.fetchall()
        return messages
    except Exception as e:
        logger.error(f"Error fetching all admin messages: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/chat/admin/messages")
async def get_chat_messages(
    user_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if user_id:
            cur.execute(
                "SELECT * FROM chat_messages WHERE sender_id = %s OR receiver_id = %s ORDER BY timestamp ASC",
                (user_id, user_id)
            )
        else:
            cur.execute("SELECT * FROM chat_messages ORDER BY timestamp ASC")
        messages = cur.fetchall()
        return {"status": "success", "messages": messages}
    except Exception as e:
        logger.error(f"Error fetching chat messages for user {user_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# Reporting and Blocking
@router.post("/user/report")
async def report_user(
    report: ReportSchema,
    current_user: dict = Depends(get_current_user_matrimony)
):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO reported_profiles (reporter_matrimony_id, reported_matrimony_id, reason, reported_at) VALUES (%s, %s, %s, NOW())",
            (matrimony_id, report.reported_matrimony_id, report.reason)
        )
        conn.commit()
        return {"status": "success", "message": "User reported successfully"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error reporting user {report.reported_matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/admin/reported-profiles")
async def get_reported_profiles(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM reported_profiles ORDER BY reported_at DESC")
        reports = cur.fetchall()
        return {"status": "success", "reported_profiles": reports}
    except Exception as e:
        logger.error(f"Error fetching reported profiles: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/admin/block-user")
async def block_user(
    block: BlockUserSchema,
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO blocked_users (blocked_matrimony_id, reason, blocked_at, is_blocked) VALUES (%s, %s, NOW(), TRUE) ON CONFLICT (blocked_matrimony_id) DO UPDATE SET is_blocked = TRUE, reason = %s, blocked_at = NOW()",
            (block.matrimony_id, block.reason, block.reason)
        )
        conn.commit()
        logger.info(f"Admin {current_user['id']} blocked user {block.matrimony_id}")
        return {"status": "success", "message": f"User {block.matrimony_id} blocked"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error blocking user {block.matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.post("/admin/unblock-user")
async def unblock_user(
    unblock: UnblockUserSchema,
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for m_id in unblock.matrimony_id:
            cur.execute("UPDATE blocked_users SET is_blocked = FALSE WHERE blocked_matrimony_id = %s", (m_id,))
        conn.commit()
        logger.info(f"Admin {current_user.get('id')} unblocked users: {unblock.matrimony_id}")
        return {"status": "success", "message": "Users unblocked successfully"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error unblocking users: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/admin/blocked-users")
async def get_blocked_users(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM blocked_users WHERE is_blocked = TRUE ORDER BY blocked_at DESC")
        blocked = cur.fetchall()
        return {"status": "success", "blocked_users": blocked}
    except Exception as e:
        logger.error(f"Error fetching blocked users: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# Contact Us
@router.post("/contact-us", response_model=ContactUsResponse)
async def create_contact_us(contact: ContactUsCreate):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Generate a temporary ID or use a sequence
        cur.execute(
            "INSERT INTO contact_us (full_name, email, message, created_at) VALUES (%s, %s, %s, NOW()) RETURNING *",
            (contact.full_name, contact.email, contact.message)
        )
        new_contact = cur.fetchone()
        conn.commit()
        return new_contact
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in create_contact_us: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/admin/contact-us", response_model=List[ContactUsResponse])
async def get_contact_us_messages(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM contact_us ORDER BY created_at DESC")
        messages = cur.fetchall()
        return messages
    except Exception as e:
        logger.error(f"Error fetching contact us messages: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# Dashboard
@router.get("/admin/dashboards/overview")
async def get_matrimony_admin_dashboard_overview(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM matrimony_profiles")
        total_profiles = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM matrimony_profiles WHERE is_active = TRUE")
        active_profiles = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM matrimony_profiles WHERE is_verified = FALSE")
        pending_verifications = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM reported_profiles")
        total_reports = cur.fetchone()[0]
        
        return {
            "total_profiles": total_profiles,
            "active_profiles": active_profiles,
            "pending_verifications": pending_verifications,
            "total_reports": total_reports
        }
    except Exception as e:
        logger.error(f"Error fetching admin dashboard overview: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# Profile Verification
@router.post("/admin/profile/verify")
async def verify_profile_admin(
    update: ProfileVerificationUpdate,
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        is_verified = True if update.verification_status == "approve" else False
        cur.execute(
            "UPDATE matrimony_profiles SET is_verified = %s, verification_status = %s, verification_comment = %s WHERE matrimony_id = %s RETURNING id",
            (is_verified, update.verification_status, update.verification_verification_comment, update.matrimony_id)
        )
        updated = cur.fetchone()
        conn.commit()
        if not updated:
            raise HTTPException(status_code=404, detail="Profile not found")
        logger.info(f"Admin {current_user['id']} updated verification for {update.matrimony_id} to {update.verification_status}")
        return {"status": "success", "message": f"Profile {update.matrimony_id} verification updated to {update.verification_status}"}
    except HTTPException:
        raise
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error in admin profile verification for {update.matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/admin/profiles/unverified", response_model=AdminProfileVerificationSummary)
async def get_unverified_profiles(current_user: dict = Depends(get_current_user)):
    if current_user.get("user_type") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM matrimony_profiles WHERE is_verified = FALSE OR verification_status = 'pending'")
        profiles = cur.fetchall()
        
        cur.execute("SELECT COUNT(*) FROM matrimony_profiles WHERE verification_status = 'pending'")
        pending_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM matrimony_profiles WHERE verification_status = 'approve'")
        approved_count = cur.fetchone()[0]
        
        return {
            "message": "Unverified profiles retrieved",
            "pending_count": pending_count,
            "approved_count": approved_count,
            "profiles": [dict(p) for p in profiles]
        }
    except Exception as e:
        logger.error(f"Error fetching unverified profiles: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.put("/admin/profiles/verify")
async def update_profile_status_admin(
    update: ProfileVerificationUpdate,
    current_user: dict = Depends(get_current_user)
):
    return await verify_profile_admin(update, current_user)

# Viewed Profiles
@router.post("/mark-viewed")
async def mark_viewed(
    request: MarkViewedRequest,
    current_user: dict = Depends(get_current_user_matrimony)
):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for viewed_id in request.profile_matrimony_ids:
            cur.execute(
                "INSERT INTO viewed_profiles (viewer_id, viewed_id, viewed_at) VALUES (%s, %s, NOW()) ON CONFLICT DO NOTHING",
                (matrimony_id, viewed_id)
            )
        conn.commit()
        return {"status": "success", "message": "Profiles marked as viewed"}
    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        logger.error(f"Error marking profiles viewed for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

@router.get("/viewed-profiles", response_model=ViewedProfilesResponse)
async def viewed_profiles_list(current_user: dict = Depends(get_current_user_matrimony)):
    matrimony_id = current_user.get("matrimony_id")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT viewed_id FROM viewed_profiles WHERE viewer_id = %s ORDER BY viewed_at DESC", (matrimony_id,))
        rows = cur.fetchall()
        viewed_ids = [row[0] for row in rows]
        return {
            "success": True,
            "viewer_id": matrimony_id,
            "viewed_profiles": viewed_ids
        }
    except Exception as e:
        logger.error(f"Error fetching viewed profiles list for {matrimony_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()
