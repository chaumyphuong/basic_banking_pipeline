import os
import random
import sys
import time
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import psycopg2
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

# ----------------------------------------------------------
# Config from .env
# ----------------------------------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

LOOP_DELAY_SECONDS = float(os.getenv("LOOP_DELAY_SECONDS", 2))
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 10))
NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS", 10))
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", 50))

ACCOUNT_TYPES = ["SAVINGS", "CHECKING", "CREDIT"]
CURRENCIES = ["USD", "VND", "EUR"]
TXN_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]
STATUSES = ["COMPLETED", "PENDING", "FAILED"]
STATUS_WEIGHTS = [0.85, 0.10, 0.05]


def random_money(min_val: float, max_val: float) -> Decimal:
    return Decimal(str(random.uniform(min_val, max_val))).quantize(
        Decimal("0.01"), rounding=ROUND_DOWN
    )


def log_status(batch_name: str, status: str, detail: str = ""):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    detail_str = f" | {detail}" if detail else ""
    print(f"[{timestamp}] [{status:<7}] {batch_name}{detail_str}", flush=True)


def main():
    conn = None
    cur = None
    try:
        log_status("DATABASE", "START", f"Connecting to {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        conn.autocommit = True
        cur = conn.cursor()
        log_status("DATABASE", "SUCCESS", "Connected")

        # --------------------------------------------------
        # Batch 1: Customers
        # --------------------------------------------------
        log_status("BATCH 1/3", "RUNNING", f"Generating {NUM_CUSTOMERS} customers...")
        c_ids = []
        sql_cust = "INSERT INTO dim_customers (first_name, lastname, email) VALUES (%s, %s, %s) RETURNING id;"
        for _ in range(NUM_CUSTOMERS):
            cur.execute(sql_cust, (fake.first_name(), fake.last_name(), fake.unique.email()))
            c_ids.append(cur.fetchone()[0])
        log_status("BATCH 1/3", "SUCCESS", f"Added {len(c_ids)} dim_customers")

        time.sleep(LOOP_DELAY_SECONDS)

        # --------------------------------------------------
        # Batch 2: Accounts
        # --------------------------------------------------
        log_status("BATCH 2/3", "RUNNING", f"Generating {NUM_ACCOUNTS} accounts...")
        cur.execute("SELECT id FROM dim_customers;")
        all_c_ids = [r[0] for r in cur.fetchall()]

        a_ids = []
        sql_acc = "INSERT INTO dim_accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id;"
        for _ in range(NUM_ACCOUNTS):
            cid = random.choice(all_c_ids)
            cur.execute(sql_acc, (cid, random.choice(ACCOUNT_TYPES), random_money(500, 50000), random.choice(CURRENCIES)))
            a_ids.append(cur.fetchone()[0])
        log_status("BATCH 2/3", "SUCCESS", f"Added {len(a_ids)} dim_accounts")

        time.sleep(LOOP_DELAY_SECONDS)

        # --------------------------------------------------
        # Batch 3: Transactions
        # --------------------------------------------------
        log_status("BATCH 3/3", "RUNNING", f"Generating {NUM_TRANSACTIONS} transactions...")
        cur.execute("SELECT id FROM dim_accounts;")
        all_a_ids = [r[0] for r in cur.fetchall()]

        sql_txn = "INSERT INTO fact_transactions (account_id, txn_type, amount, related_account, status) VALUES (%s, %s, %s, %s, %s);"
        for _ in range(NUM_TRANSACTIONS):
            aid = random.choice(all_a_ids)
            txn_type = random.choice(TXN_TYPES)
            amount = random_money(10.0, 2500.0)
            status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
            related_acc = random.choice([a for a in all_a_ids if a != aid]) if txn_type == "TRANSFER" else None

            cur.execute(sql_txn, (aid, txn_type, amount, related_acc, status))
        log_status("BATCH 3/3", "SUCCESS", f"Added {NUM_TRANSACTIONS} fact_transactions")

        log_status("JOB", "FINISHED", "All batches executed successfully. Shutting down.")

    except KeyboardInterrupt:
        log_status("JOB", "ABORTED", "Stopped by user")
    except Exception as e:
        log_status("JOB", "FAILED", str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        # Đảm bảo ngắt hoàn toàn tiến trình về lại PowerShell
        sys.exit(0)


if __name__ == "__main__":
    main()