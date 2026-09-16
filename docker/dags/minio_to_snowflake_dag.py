from datetime import datetime, timedelta
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
import snowflake.connector

# 1. Load biến môi trường
load_dotenv(Path("/opt/airflow/dags/.env"), override=True)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW_STAGING")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "BANKING_STAGE")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

# Thư mục lưu file chia sẻ giữa 2 task (nằm trong volume dags đã mount)
DOWNLOAD_DIR = "/opt/airflow/dags/downloads"

TABLE_CONFIG = {
    "CUSTOMERS": "dim_customers",
    "ACCOUNTS": "dim_accounts",
    "TRANSACTIONS": "fact_transactions",
}


def get_snowflake_conn():
    """Tạo kết nối Snowflake Key-Pair"""
    with open(Path(SNOWFLAKE_PRIVATE_KEY_PATH), "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(), password=None, backend=default_backend()
        )
    pk_bytes = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=pk_bytes,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def extract_from_minio(**context):
    """Task 1: Tải file mới nhất của 3 bảng từ MinIO về ổ đĩa chia sẻ"""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    downloaded_files = (
        {}
    )  # Lưu dạng: {'CUSTOMERS': '/opt/.../downloads/file.parquet'}

    for table, folder in TABLE_CONFIG.items():
        response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=folder)
        valid_files = [
            f
            for f in response.get("Contents", [])
            if not f["Key"].endswith("/") and f["Size"] > 0
        ]

        if not valid_files:
            print(f"⚠️ Thư mục {folder} trống, bỏ qua bảng {table}.")
            continue

        latest_file = max(valid_files, key=lambda x: x["LastModified"])
        file_key = latest_file["Key"]
        local_filename = os.path.basename(file_key)
        local_path = os.path.join(DOWNLOAD_DIR, local_filename)

        print(f"[*] [{table}] Tải {file_key} -> {local_path}...")
        s3.download_file(MINIO_BUCKET, file_key, local_path)
        downloaded_files[table] = local_path

    if not downloaded_files:
        raise ValueError("Không có file nào được tải về từ MinIO!")

    print(f"✅ Đã tải xong {len(downloaded_files)} file.")
    return downloaded_files


def load_to_snowflake(**context):
    """Task 2: Lấy danh sách file từ Task 1, PUT lên Stage và COPY INTO Snowflake"""
    ti = context["ti"]
    downloaded_files = ti.xcom_pull(task_ids="task_1_extract_minio")

    if not downloaded_files:
        raise ValueError("Task 2 không nhận được file nào từ Task 1 qua XCom!")

    conn = get_snowflake_conn()
    cur = conn.cursor()

    db = SNOWFLAKE_DATABASE 
    schema = SNOWFLAKE_SCHEMA
    stage = SNOWFLAKE_STAGE
    stage_name = f"{db}.{schema}.{stage}"

    try:
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};")
        cur.execute(f"USE DATABASE {db};")
        cur.execute(f"USE SCHEMA {schema};")

        for table, local_path in downloaded_files.items():
            if not os.path.exists(local_path):
                print(f"⚠️ Không tìm thấy file cục bộ: {local_path}, bỏ qua.")
                continue

            file_name = os.path.basename(local_path)
            full_table = f"{db}.{schema}.{table}"

            try:
                # 1. Đưa file lên Named Stage
                put_sql = f"PUT file://{local_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
                print(f"[*] Đang tải {file_name} lên Stage...")
                cur.execute(put_sql)

                # 2. Nạp dữ liệu vào bảng đích
                copy_sql = f"""
                    COPY INTO {full_table} (V, LOADED_AT)
                    FROM (
                        SELECT $1, CURRENT_TIMESTAMP()
                        FROM @{stage_name}/{file_name}
                    )
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    FORCE = TRUE;
                """
                print(f"[*] COPY INTO {full_table}...")
                cur.execute(copy_sql)
                print(f"✅ Đã nạp thành công vào bảng {table}!")

            finally:
                # Dọn dẹp file trên đĩa ngay sau khi nạp xong từng bảng
                if os.path.exists(local_path):
                    os.remove(local_path)
                    print(f"🧹 Đã xóa file trung gian: {local_path}")

    finally:
        cur.close()
        conn.close()


default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "snowflake"],
) as dag:

    t1 = PythonOperator(
        task_id="task_1_extract_minio",
        python_callable=extract_from_minio,
    )

    t2 = PythonOperator(
        task_id="task_2_load_snowflake",
        python_callable=load_to_snowflake,
    )

    t1 >> t2