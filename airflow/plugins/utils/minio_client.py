import os
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

MINIO_HOST = os.getenv("MINIO_HOST", "minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "minidatalake")

minio_bucket = Minio(
    f"{MINIO_HOST}:{MINIO_PORT}",
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False
)

RAW_FOLDER = "raw-data"
today = __import__('datetime').datetime.utcnow().strftime("%Y-%m-%d")

def ensure_bucket_exists(bucket_name):
    if not minio_bucket.bucket_exists(bucket_name):
        minio_bucket.make_bucket(bucket_name)
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def upload_to_minio(source_name, data):
    import json
    file_path = f"/tmp/{source_name}_{today}.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    ensure_bucket_exists(MINIO_BUCKET_NAME)
    minio_bucket.fput_object(
        MINIO_BUCKET_NAME,
        f"{RAW_FOLDER}/{source_name}/{today}.json",
        file_path
    )
    import os; os.remove(file_path)
    print(f"✅ Uploaded {source_name} to MinIO")

def verify_minio_load():
    print(f"🔍 Verifying uploads for {today}...")
    objects = minio_bucket.list_objects(MINIO_BUCKET_NAME, prefix=f"{RAW_FOLDER}/", recursive=True)
    uploaded = [obj.object_name for obj in objects if today in obj.object_name]
    if not uploaded:
        raise ValueError(f"❌ No files found in MinIO for {today}!")
    for f in uploaded:
        print(f"  ✅ {f}")
    print(f"\n✅ {len(uploaded)} file(s) loaded successfully.")