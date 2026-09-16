import io
import json
import os
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer
import pandas as pd
import base64

warnings.filterwarnings("ignore", category=DeprecationWarning)

# -----------------------------
# 1. Nạp file .env cùng thư mục
# -----------------------------
CURRENT_DIR = Path(__file__).resolve().parent
ENV_PATH = CURRENT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT") 
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") 
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") 
MINIO_BUCKET = os.getenv("MINIO_BUCKET") 

# Khớp chính xác tên topic Debezium sinh ra
TOPICS = [
    "banking_cdc.public.dim_customers",
    "banking_cdc.public.dim_accounts",
    "banking_cdc.public.fact_transactions",
]

BATCH_SIZE = 50
FLUSH_INTERVAL_SEC = 5  # Tự động xả lên MinIO sau 5 giây nếu không đủ 50 dòng

# -----------------------------
# 2. MinIO Client
# -----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if MINIO_BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=MINIO_BUCKET)


# -----------------------------
# 3. Ghi Parquet vào RAM buffer (fastparquet)
# -----------------------------


def clean_debezium_value(val):
    """Giải mã chuỗi Base64 byte của Debezium (như 'RkYj') thành số float."""
    if isinstance(val, str):
        try:
            # Giải mã chuỗi base64 thành bytes
            raw_bytes = base64.b64decode(val)
            num = int.from_bytes(raw_bytes, byteorder="big", signed=True)
            # Chuẩn hóa về 2 chữ số thập phân
            return float(num) / 100.0
        except Exception:
            try:
                return float(val)
            except ValueError:
                return 0.0
    elif isinstance(val, (int, float)):
        return float(val)
    return 0.0


def write_to_minio(table_name: str, records: list):
    if not records:
        return

    df = pd.DataFrame(records)

    # Chuẩn hóa kiểu dữ liệu cho các cột tiền tệ/số học
    numeric_cols = ["balance", "amount"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_debezium_value).astype(float)

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    s3_key = f"{table_name}/date={date_str}/{table_name}_{now.strftime('%H%M%S%f')}.parquet"

    # Ghi vào RAM buffer, không ghi đè lên ổ đĩa
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"✅ Uploaded {len(records)} records -> s3://{MINIO_BUCKET}/{s3_key}")


# -----------------------------
# 4. Consumer Engine
# -----------------------------
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

buffer = {t: [] for t in TOPICS}
last_flush = {t: time.time() for t in TOPICS}

print(f"✅ Connected to Kafka ({KAFKA_BOOTSTRAP}). Listening for CDC messages...")

try:
    while True:
        # Dùng poll thay vì lặp blocking để timeout xả buffer hoạt động
        records_batch = consumer.poll(timeout_ms=1000)

        for tp, messages in records_batch.items():
            for message in messages:
                topic = message.topic
                event = message.value
                payload = event.get("payload", event) if isinstance(event, dict) else {}
                op = payload.get("op")
                record = payload.get("before") if op == "d" else payload.get("after")

                if record:
                    buffer[topic].append(record)
                    print(f"[{topic}] -> {record}")

        now = time.time()
        for t in TOPICS:
            has_records = len(buffer[t]) > 0
            size_reached = len(buffer[t]) >= BATCH_SIZE
            time_reached = has_records and (now - last_flush[t] >= FLUSH_INTERVAL_SEC)

            if size_reached or time_reached:
                write_to_minio(t.split(".")[-1], buffer[t])
                buffer[t].clear()
                last_flush[t] = now

except KeyboardInterrupt:
    print("\n[*] Flushing remaining data before exit...")
    for t in TOPICS:
        if buffer[t]:
            write_to_minio(t.split(".")[-1], buffer[t])
    consumer.close()
    sys.exit(0)