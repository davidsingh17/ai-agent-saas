import os
import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)
BUCKET = os.getenv("MINIO_BUCKET")

def upload_to_minio(file_path: str, object_name: str) -> str:
    s3.upload_file(file_path, BUCKET, object_name)
    return f"{BUCKET}/{object_name}"

def _to_key(storage_path: str) -> str:
    if not storage_path:
        return storage_path
    prefix_s3 = f"s3://{BUCKET}/"
    prefix_plain = f"{BUCKET}/"
    if storage_path.startswith(prefix_s3):
        return storage_path[len(prefix_s3):]
    if storage_path.startswith(prefix_plain):
        return storage_path[len(prefix_plain):]
    return storage_path  # già del tipo "tenant-1/raw/…"

def object_exists(storage_path: str) -> bool:
    """Ritorna True se l'oggetto esiste nel bucket."""
    key = _to_key(storage_path)
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False

def download_to_temp(storage_path: str, tmp_dir: str = "tmp") -> str:
    """Scarica l'oggetto su disco; solleva FileNotFoundError se non esiste."""
    os.makedirs(tmp_dir, exist_ok=True)
    key = _to_key(storage_path)
    if not object_exists(storage_path):
        raise FileNotFoundError(f"S3 object not found: {BUCKET}/{key}")
    local = os.path.join(tmp_dir, os.path.basename(key))
    s3.download_file(BUCKET, key, local)
    return local

def read_text(storage_path: str) -> str:
    key = _to_key(storage_path)
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8", errors="ignore")