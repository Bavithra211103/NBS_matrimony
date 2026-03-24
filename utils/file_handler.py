# import boto3
# import logging
# import traceback
# from botocore.exceptions import NoCredentialsError, ClientError
# from fastapi import UploadFile, HTTPException
# from core.config import settings

# logger = logging.getLogger(__name__)

# class FileHandler:
#     def __init__(self):
#         self.s3_client = boto3.client(
#             "s3",
#             aws_access_key_id=settings.AWS_CONFIG["access_key"],
#             aws_secret_access_key=settings.AWS_CONFIG["secret_key"],
#             region_name=settings.AWS_CONFIG["region"]
#         )
#         self.bucket_name = settings.AWS_CONFIG["bucket_name"]
        
#         # Allowed extensions
#         self.ALLOWED_EXTENSIONS = {
#             "profile_photos": ["jpg", "jpeg", "png", "webp"],
#             "photos": ["jpg", "jpeg", "png", "webp"],
#             "horoscopes": ["pdf"]
#         }

#     def _validate_file(self, filename: str, folder: str):
#         ext = filename.split(".")[-1].lower() if "." in filename else ""
#         allowed = self.ALLOWED_EXTENSIONS.get(folder, [])
#         if allowed and ext not in allowed:
#             raise HTTPException(
#                 status_code=400, 
#                 detail=f"Invalid file type for {folder}. Allowed: {', '.join(allowed)}"
#             )

#     def upload_file(self, file: UploadFile, folder: str) -> str:
#         """Uploads a file to AWS S3 and returns the public file URL."""
#         try:
#             filename = file.filename.strip().replace(" ", "_")
#             self._validate_file(filename, folder)
            
#             s3_path = f"{folder}/{filename}"
            
#             file.file.seek(0)
#             self.s3_client.upload_fileobj(
#                 file.file,
#                 self.bucket_name,
#                 s3_path,
#                 ExtraArgs={"ContentType": file.content_type}
#             )

#             # Construct the S3 Public URL
#             file_url = f"https://{self.bucket_name}.s3.{settings.AWS_CONFIG['region']}.amazonaws.com/{s3_path}"
#             logging.info(f"File uploaded to S3: {file_url}")
#             return file_url

#         except NoCredentialsError:
#             logger.error("AWS credentials not found.")
#             logger.error(traceback.format_exc())
#             raise HTTPException(status_code=500, detail="Cloud storage configuration error")
#         except ClientError as e:
#             logger.error(f"S3 Upload Error: {str(e)}")
#             logger.error(traceback.format_exc())
#             raise HTTPException(status_code=500, detail=f"S3 Upload failed: {str(e)}")
#         except Exception as e:
#             if isinstance(e, HTTPException): raise e
#             logger.error(f"Failed to upload {file.filename}: {str(e)}")
#             logger.error(traceback.format_exc())
#             raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

#     def list_files(self, folder: str):
#         """Lists files in an S3 folder."""
#         try:
#             response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{folder}/")
#             if "Contents" in response:
#                 return [obj["Key"] for obj in response["Contents"]]
#             return []
#         except Exception as e:
#             logger.error(f"Error listing files in S3: {str(e)}")
#             logger.error(traceback.format_exc())
#             return []

#     def delete_file(self, file_url: str):
#         """Deletes a file from S3 given its public URL."""
#         try:
#             # Extract key from URL
#             # Expected format: https://bucket.s3.region.amazonaws.com/folder/filename
#             key = file_url.split(".com/")[-1]
#             self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
#             logging.info(f"Deleted S3 object: {key}")
#         except Exception as e:
#             logger.error(f"Failed to delete S3 file {file_url}: {str(e)}")
#             logger.error(traceback.format_exc())
#             raise HTTPException(status_code=500, detail=f"Failed to delete file from S3: {str(e)}")

#     def process_url(self, value, folder_name):
#         """No changes needed as DB will store full S3 URLs now, 
#         but kept for backward compatibility if needed."""
#         if value and isinstance(value, str) and value.strip():
#             items = value.replace("{", "").replace("}", "").split(',')
#             return [item.strip() for item in items if item.strip()]
#         return None

# file_handler = FileHandler()

#----------------------------------------------------

import os
import uuid
import logging
import traceback
from pathlib import Path
from fastapi import UploadFile, HTTPException
from core.config import settings

logger = logging.getLogger(__name__)

class FileHandler:
    def __init__(self):
        self.ALLOWED_EXTENSIONS = {
            "profile_photos": ["jpg", "jpeg", "png", "webp"],
            "photos": ["jpg", "jpeg", "png", "webp"],
            "horoscopes": ["pdf", "jpg", "jpeg", "png"],
        }

        self.STORAGE_DIRS = {
            "profile_photos": settings.UPLOAD_DIR / "profile_photos",
            "photos": settings.UPLOAD_DIR / "photos",
            "horoscopes": settings.UPLOAD_DIR / "horoscopes",
        }

        # Ensure directories exist
        for path in self.STORAGE_DIRS.values():
            path.mkdir(parents=True, exist_ok=True)

    def _validate_file(self, filename: str, folder: str):
        if not filename:
            return
        ext = filename.split(".")[-1].lower() if "." in filename else ""
        allowed = self.ALLOWED_EXTENSIONS.get(folder, [])
        if allowed and ext not in allowed:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type '.{ext}' for {folder}. Allowed: {', '.join(allowed)}"
            )

    def upload_file(self, file: UploadFile, folder: str) -> str:
        """Saves file locally and returns the public URL path."""
        try:
            original_name = (file.filename or "upload").strip().replace(" ", "_")
            self._validate_file(original_name, folder)

            ext = original_name.split(".")[-1].lower() if "." in original_name else "jpg"
            unique_name = f"{uuid.uuid4().hex}.{ext}"

            save_dir = self.STORAGE_DIRS.get(folder, settings.UPLOAD_DIR / folder)
            save_dir.mkdir(parents=True, exist_ok=True)
            save_path = save_dir / unique_name

            file.file.seek(0)
            with open(save_path, "wb") as f:
                f.write(file.file.read())

            url = f"{settings.BASE_URL}/static/{folder}/{unique_name}"
            logger.info(f"File saved locally: {url}")
            return url

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to upload {file.filename}: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

    def list_files(self, folder: str):
        """Lists files in a local folder."""
        try:
            save_dir = self.STORAGE_DIRS.get(folder, settings.UPLOAD_DIR / folder)
            if save_dir.exists():
                return [str(f) for f in save_dir.iterdir() if f.is_file()]
            return []
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []

    def delete_file(self, file_url: str):
        """Deletes a local file given its URL."""
        try:
            filename = file_url.split("/")[-1]
            folder = file_url.split("/")[-2]
            save_dir = self.STORAGE_DIRS.get(folder, settings.UPLOAD_DIR / folder)
            file_path = save_dir / filename
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Deleted local file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete file {file_url}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")

    def process_url(self, value, folder_name):
        if value and isinstance(value, str) and value.strip():
            items = value.replace("{", "").replace("}", "").split(',')
            return [item.strip() for item in items if item.strip()]
        return None

file_handler = FileHandler()