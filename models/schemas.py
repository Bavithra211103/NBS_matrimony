from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from fastapi import UploadFile

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    user_type: str

class UserLogin(BaseModel):
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str
    email: str
    user_type: str
    message: str

class RefreshToken(BaseModel):
    refresh_token: str

class EventForm(BaseModel):
    name: str
    contact: str
    event_date: str
    event_time: str
    event_type: str
    
class FileResponse(BaseModel):
    id: int
    filename: str
    file_url: str
    file_base64: str
    uploaded_by: int
    uploaded_at: datetime

class FileUploadRequest(BaseModel):
    category: str

class FileData(BaseModel):
    private_files_id: Optional[int]
    selected_urls: List[str]

class FileSelectionsRequest(BaseModel):
    user_id: int
    private_files: List[FileData]

class GetFileUpdate(BaseModel):
    file_id: int
    file_type: str
    file_url: str
    category: str

class MatrimonyProfile(BaseModel):
    full_name: str
    age: int
    gender: str
    date_of_birth: str
    height: float
    weight: float
    email: EmailStr
    phone_number: str
    occupation: str
    annual_income: str
    education: str
    password: str

class MatrimonyRegister(BaseModel):
    full_name: str
    age: str
    gender: str
    date_of_birth: str
    email: str
    password: str
    phone_number: str
    height: Optional[str] = None
    weight: Optional[str] = None
    occupation: Optional[str] = None
    annual_income: Optional[str] = None
    education: Optional[str] = None
    mother_tongue: Optional[str] = None
    profile_created_by: Optional[str] = None
    address: Optional[str] = None
    work_type: Optional[str] = None
    company: Optional[str] = None
    work_location: Optional[str] = None
    work_country: Optional[str] = None
    mother_name: Optional[str] = None
    father_name: Optional[str] = None
    sibling_count: Optional[int] = None
    elder_brother: Optional[str] = None
    elder_sister: Optional[str] = None
    younger_sister: Optional[str] = None
    younger_brother: Optional[str] = None
    native: Optional[str] = None
    mother_occupation: Optional[str] = None
    father_occupation: Optional[str] = None
    religion: Optional[str] = None
    caste: Optional[str] = None
    sub_caste: Optional[str] = None
    nakshatra: Optional[str] = None
    rashi: Optional[str] = None
    birth_time: Optional[str] = None
    birth_place: Optional[str] = None
    ascendent: Optional[str] = None
    dhosham: Optional[str] = None
    other_dhosham: Optional[str] = None
    quarter: Optional[str] = None
    user_type: Optional[str] = None
    preferred_age_min: Optional[str] = None
    preferred_age_max: Optional[str] = None
    preferred_height_min: Optional[str] = None
    preferred_height_max: Optional[str] = None
    preferred_religion: Optional[str] = None
    preferred_caste: Optional[str] = None
    preferred_sub_caste: Optional[str] = None
    preferred_nakshatra: Optional[str] = None
    preferred_rashi: Optional[str] = None
    preferred_location: Optional[str] = None
    preferred_work_status: Optional[str] = None

class MatrimonyRegisterResponse(BaseModel):
    message: str
    user_id: int

class MatrimonyLoginRequest(BaseModel):
    matrimony_id: str
    password: Optional[str] = None  
    phone_number: Optional[str] = None  
    via_link: Optional[bool] = False

class MatrimonyToken(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str
    matrimony_id: str

class MatrimonyProfileResponse(BaseModel):
    matrimony_id: str
    full_name: str
    age: str
    gender: str
    date_of_birth: str
    email: str
    phone_number: str
    height: Optional[str]
    weight: Optional[str]
    occupation: Optional[str]
    annual_income: Optional[str]
    education: Optional[str]
    mother_tongue: Optional[str]
    profile_created_by: Optional[str]
    address: Optional[str]
    work_type: Optional[str]
    company: Optional[str]
    work_location: Optional[str]
    work_country: Optional[str]
    mother_name: Optional[str]
    father_name: Optional[str]
    sibling_count: Optional[str]
    elder_brother: Optional[str] 
    elder_sister: Optional[str] 
    younger_sister: Optional[str] 
    younger_brother: Optional[str]
    native: Optional[str]
    mother_occupation: Optional[str]
    father_occupation: Optional[str]
    religion: Optional[str]
    caste: Optional[str]
    sub_caste: Optional[str]
    nakshatra: Optional[str]
    rashi: Optional[str]
    birth_time: Optional[str]
    birth_place: Optional[str]
    ascendent: Optional[str]
    user_type: Optional[str]
    preferred_age_min: Optional[str]
    preferred_age_max: Optional[str]
    preferred_height_min: Optional[str]
    preferred_height_max: Optional[str]
    preferred_religion: Optional[str]
    preferred_caste: Optional[str]
    preferred_sub_caste: Optional[str]
    preferred_nakshatra: Optional[str]
    preferred_rashi: Optional[str]
    preferred_location: Optional[str]
    preferred_work_status: Optional[str]
    photo: Optional[str] = None
    dhosham: Optional[str]
    other_dhosham: Optional[str]    
    quarter: Optional[str]
    photos: Optional[List[str]] = None
    horoscope_documents: Optional[List[str]] = None
    is_active: Optional[str]
    blood_group: Optional[str]
    is_verified:  Optional[str]
    verification_status: Optional[str] 
    verification_verification_comment: Optional[str] 

class MatrimonyProfilesWithMessage(BaseModel):
    message: str
    profile_details: List[MatrimonyProfileResponse]

class AdminProfileVerificationSummary(BaseModel):
    message: str
    pending_count: int
    approved_count: int
    profiles: List[MatrimonyProfileResponse]

class ProfileVerificationUpdate(BaseModel):
    matrimony_id: str
    verification_status: Literal["approve", "pending"]
    verification_verification_comment: Optional[str] = None

class OTPRequest(BaseModel):
    mobile_number: str
    full_name: str

class OTPVerify(BaseModel):
    mobile_number: str
    otp: str

class FrameDetails(BaseModel):
    frame_name: str
    phone_number: str
    user_photo: List[UploadFile]
    frame_size: str
    frame_color: List[UploadFile]

class RefreshTokenRequest(BaseModel):
    refresh_token: str

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class IncrementMatrimonyIdRequest(BaseModel):
    last_matrimony_id: str 
    
class SpendAction(BaseModel):
    profile_matrimony_id: str = Field(..., alias="profile_matrimony_id")
    points: int

class SpendRequest(BaseModel):
    spend_requests: List[SpendAction]

class FavoriteProfilesRequest(BaseModel):
    favorite_profile_ids: List[str]
    unfavorite_profile_ids: Optional[List[str]] = []

class EmailVerificationRequest(BaseModel):
    email: EmailStr

class ForgotPasswordRequest(BaseModel):
    email: EmailStr
    new_password: str = Field(..., min_length=6)
    confirm_password: str = Field(..., min_length=6)

class DeactivationReportRequest(BaseModel):
    matrimony_id: str
    reason: str

class ChatRequest(BaseModel):
    message: str
    sender_id: str
    receiver_id: str

class ChatUserRequest(BaseModel):
    message: str
    sender_id: str
    receiver_email: str

class AdminChatMessage(BaseModel):
    sender_id: str
    receiver_id: str
    message: str
    timestamp: datetime  

class ReportSchema(BaseModel):
    reported_matrimony_id: str
    reason: str

class BlockUserSchema(BaseModel):
    matrimony_id: str
    reason: str

class UnblockUserSchema(BaseModel):
    matrimony_id: List[str]

class ContactUsCreate(BaseModel):
    full_name: str
    email: EmailStr
    message: str

class ContactUsResponse(ContactUsCreate):
    id: int
    created_at: datetime

class MarkViewedRequest(BaseModel):
    profile_matrimony_ids: List[str]

class ViewedProfilesResponse(BaseModel):
    success: bool
    viewer_id: str
    viewed_profiles: List[str]
