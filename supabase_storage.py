import os
import threading
from supabase import create_client, Client
from logger import get_logger

logger = get_logger("supabase_storage")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "cyberdefensex")

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase Storage client initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")

_upload_lock = threading.Lock()

def download_file(remote_path: str, local_path: str) -> bool:
    """Download a file from Supabase Storage."""
    if not supabase:
        return False
    try:
        response = supabase.storage.from_(SUPABASE_BUCKET).download(remote_path)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
        
        with open(local_path, "wb") as f:
            f.write(response)
        logger.info(f"Downloaded {remote_path} from Supabase Storage to {local_path}")
        return True
    except Exception as e:
        logger.warning(f"Could not download {remote_path} from Supabase Storage: {e}")
        return False

def upload_file(local_path: str, remote_path: str) -> bool:
    """Upload a file to Supabase Storage."""
    if not supabase:
        return False
    
    with _upload_lock:
        if not os.path.exists(local_path):
            return False
            
        try:
            with open(local_path, "rb") as f:
                res = supabase.storage.from_(SUPABASE_BUCKET).upload(
                    file=local_path,
                    path=remote_path,
                    file_options={"cache-control": "3600", "upsert": "true"}
                )
            logger.info(f"Uploaded {local_path} to Supabase Storage at {remote_path}")
            return True
        except Exception as e:
            # Sometime supabase-py raises error on upsert if we didn't specify it correctly, 
            # fallback to manual remove and upload.
            logger.warning(f"Upsert failed, trying remove and upload: {e}")
            try:
                supabase.storage.from_(SUPABASE_BUCKET).remove([remote_path])
                with open(local_path, "rb") as f:
                    supabase.storage.from_(SUPABASE_BUCKET).upload(
                        file=local_path,
                        path=remote_path,
                        file_options={"cache-control": "3600", "upsert": "true"}
                    )
                logger.info(f"Uploaded {local_path} to Supabase Storage at {remote_path}")
                return True
            except Exception as e2:
                logger.error(f"Failed to upload {local_path} to Supabase Storage: {e2}")
                return False
