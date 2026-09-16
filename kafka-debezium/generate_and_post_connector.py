import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_name = os.getenv("CONNECTOR_NAME", "banking-postgres-connector")
debezium_url = os.getenv("DEBEZIUM_URL", "http://localhost:8083")

connector_config = {
    "name": connector_name,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": os.getenv("TOPIC_PREFIX"),
        "table.include.list": os.getenv("TABLE_INCLUDE_LIST"),
        "plugin.name": "pgoutput",
        "slot.name": os.getenv("SLOT_NAME"),
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = f"{debezium_url}/connectors"
headers = {"Content-Type": "application/json"}

try:
    response = requests.post(url, headers=headers, data=json.dumps(connector_config), timeout=10)

    # -----------------------------
    # Debug/Output
    # -----------------------------
    if response.status_code == 201:
        print("Connector created successfully!")
    elif response.status_code == 409:
        print("Connector already exists.")
    else:
        print(f"Failed to create connector ({response.status_code}): {response.text}")

except requests.exceptions.RequestException as e:
    print(f"Cannot connect to Debezium at {debezium_url}: {e}")