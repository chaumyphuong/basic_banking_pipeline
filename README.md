  # 🏦 End-to-End Banking Data Pipeline

An end-to-end **Data Engineering pipeline** that simulates real-time banking transactions, captures database changes using **Change Data Capture (CDC)**, streams events through **Apache Kafka**, stores raw data in an **S3-compatible Data Lake**, orchestrates ingestion with **Apache Airflow**, and transforms data using **dbt** into analytics-ready Fact and Dimension models on **Snowflake**.

The project demonstrates a modern data platform architecture covering:

* OLTP transaction generation
* PostgreSQL WAL-based CDC
* Debezium
* Apache Kafka
* Micro-batch data ingestion
* MinIO / S3-compatible Data Lake
* Apache Airflow orchestration
* Snowflake Data Warehouse
* dbt transformation and data quality testing
* GitHub Actions CI/CD

---

# 📌 Project Overview

This project simulates a banking data platform where transactions are continuously generated in an operational PostgreSQL database.

Instead of periodically extracting the entire database, the pipeline uses **Change Data Capture (CDC)** to capture database changes from PostgreSQL's **Write-Ahead Log (WAL)**.

The captured events are published by **Debezium** to **Apache Kafka**, consumed by a custom ingestion service, and persisted into **MinIO** as raw event data.

From there, **Apache Airflow** orchestrates the ingestion of data into **Snowflake**, where **dbt** performs transformations, testing, and dimensional modeling.

---

# 🏗️ Architecture

## Architecture Diagram
<img width="1949" height="1040" alt="Blank diagram - Page 1" src="https://github.com/user-attachments/assets/7c74e22e-65eb-4341-8791-32298d47ca25" />

---

# 🔄 Data Flow
### 1. Transaction Generation & OLTP Storage

- A custom Python service (`data-generator`) simulates banking transactions, including deposits, withdrawals, fund transfers, and account status updates.
- Transactions are persisted in **PostgreSQL (OLTP)** with transactional integrity and **Write-Ahead Logging (WAL)** enabled using `wal_level = logical`.

### 2. Real-Time Change Data Capture (CDC)

- **Debezium** monitors PostgreSQL's logical replication stream and captures row-level `INSERT`, `UPDATE`, and `DELETE` events.
- CDC events are serialized as JSON messages and published to dedicated **Apache Kafka** topics, such as `banking.accounts` and `banking.transactions`.

### 3. Data Lake Ingestion

- A custom Kafka consumer service reads CDC events from Kafka topics in micro-batches.
- Raw CDC payloads are partitioned by timestamp and written as immutable objects to **MinIO**, an S3-compatible object storage layer.
- This raw layer preserves source events for auditing, downstream processing, and potential data replay.

### 4. Data Warehouse Ingestion & Transformation

- **Apache Airflow** orchestrates scheduled ingestion workflows that load data from MinIO into **Snowflake** staging tables using `COPY INTO`.
- **dbt** transforms the staged data through multiple modeling layers:

  - **Staging (`stg_`)** — Cleans and standardizes source data, casts data types, renames fields, and parses CDC payloads.
  - **Snapshots** — Tracks historical changes to selected entities, supporting **Slowly Changing Dimension (SCD) Type 2** patterns where applicable.
  - **Intermediate (`int_`)** — Applies business logic, enriches transaction data, and standardizes monetary and operational attributes.
  - **Marts (`fct_`, `dim_`)** — Builds analytical **Fact and Dimension** models using a Star Schema, including `dim_customers`, `dim_accounts`, and `fct_transactions`.

- **Data Quality & Testing** — dbt runs automated tests covering uniqueness, referential integrity, non-null constraints, accepted values, and custom financial validation rules.

### 5. Analytics & BI Serving Layer

- Analytical mart models in the Snowflake `ANALYTICS` schema are exposed to **Power BI**.
- Dashboards can be used to analyze transaction activity, customer and account balance trends, liquidity indicators, and potential risk anomalies.
- Power BI can consume the analytical models through **Import** or **DirectQuery**, depending on the reporting requirements.

---

# 🧰 Technology Stack

| Data Layer | Layer | Technology | Purpose |
|---|---|---|---|
| Source | Source / OLTP | PostgreSQL | Operational banking database |
| Bronze | CDC | Debezium | Capture database changes |
| Bronze | Streaming | Apache Kafka | Stream CDC events |
| Bronze | Ingestion | Python Consumer | Consume Kafka events and write to MinIO |
| Bronze | Data Lake | MinIO | Store raw CDC events |
| Orchestration | Orchestration | Apache Airflow | Schedule and orchestrate data workflows |
| Silver | Data Warehouse | Snowflake | Store cleaned and standardized data |
| Silver → Gold | Transformation | dbt | Transform and model data |
| Infrastructure | Containerization | Docker | Containerized local infrastructure |
| Infrastructure | CI/CD | GitHub Actions | Automated validation and deployment |

---

# 📂 Repository Structure
```text
.
├── .github/workflows/
│   ├── ci.yml              # CI pipeline: syntax validation and compile checks
│   └── cd.yml              # CD pipeline: automated prod deployments to Snowflake
├── dbt/                    # dbt project root
│   ├── models/             # Staging, intermediate, and mart SQL models
│   ├── snapshots/          # SCD Type 2 snapshot definitions
│   ├── dbt_project.yml     # dbt project configurations
│   └── profiles.yml        # Database connection profiles
├── docker/                 # Airflow and infrastructure Dockerfiles
├── docs/images/            # Architecture diagrams & documentation assets
├── kafka-debezium/         # Kafka Connect & Debezium CDC configuration
├── data-generator/         # Mock banking transaction script
├── consumer/               # Kafka consumer to sink events into MinIO
├── postgres/               # Init scripts for source database
├── docker-compose.yml      # Multi-container orchestration
└── README.md               # Project documentation
```

# 🚀 Quickstart

# Install dbt dependencies
```bash
dbt deps
```

# Test Snowflake connectivity
```bash
dbt debug --target dev
```

# Execute snapshots, models, and tests
```bash
dbt snapshot --target dev
dbt run --target dev
dbt test --target dev
```

## Step 1 — Configure Environment

```bash
cp .env.example .env
```

Update `.env` with your local and Snowflake credentials.

---

## Step 2 — Start Infrastructure

Build and start all services:

```bash
docker compose up -d --build
```

Check container status:

```bash
docker compose ps
```

Expected services include:

```text
postgres
kafka
kafka-connect
minio
airflow
consumer
data-generator
```

Check logs:

```bash
docker compose logs -f
```

To inspect one service:

```bash
docker compose logs -f postgres
```

or:

```bash
docker compose logs -f kafka
```

---

## 🔌 Step 3 — Configure Debezium CDC

Wait until Kafka Connect is available.

Verify the REST API:

```bash
curl http://localhost:8083/
```

Register the PostgreSQL connector:

```bash
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ \
  -d @kafka-debezium/postgres-connector.json
```

Check registered connectors:

```bash
curl http://localhost:8083/connectors
```

Check connector status:

```bash
curl http://localhost:8083/connectors/<connector-name>/status
```

The connector should eventually report:

```text
RUNNING
```

---

## 📡 Step 4 — Verify Kafka Events

List Kafka topics:

```bash
docker exec -it <kafka-container> \
  kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

You should see the Debezium-generated topic corresponding to the PostgreSQL table.

For example:

```text
banking.public.transactions
```

Consume events:

```bash
docker exec -it <kafka-container> \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic banking.public.transactions \
  --from-beginning
```

If the data generator is running, new banking transaction events should appear continuously.

---

## 🪣 Step 5 — Run Data Ingestion

The consumer reads Kafka events and writes micro-batches to MinIO.

Check consumer logs:

```bash
docker compose logs -f consumer
```

Verify that objects are being created in MinIO.

Open:

```text
http://localhost:9001
```

Log in using the credentials configured in `.env`.

Expected structure:

```text
bucket/
└── banking/
    └── transactions/
        └── year=2026/
            └── month=09/
                └── day=16/
                    └── ...
```

---

## 🌨️ Step 6 — Run dbt

Move into the dbt project:

```bash
cd banking_dbt
```

Install dependencies:

```bash
dbt deps
```

Validate the dbt connection:

```bash
dbt debug
```

Run models:

```bash
dbt run
```

Run data quality tests:

```bash
dbt test
```

Or execute everything:

```bash
dbt build
```

`dbt build` is recommended because it runs models and associated tests according to their dependencies.

---

# 🔄 CI/CD

GitHub Actions automates code validation and deployment.

The repository contains:

```text
.github/
└── workflows/
    ├── ci.yml
    └── cd.yml
```

---

## Continuous Integration — CI

CI runs when code is pushed or a Pull Request is created.

---

# 🚢 Continuous Delivery / Deployment — CD

CD runs after code is merged into the main branch.

For a production implementation, CD can deploy:

* Docker images
* Airflow DAGs
* dbt project
* Data ingestion services
* Infrastructure configuration

Secrets should be stored using **GitHub Actions Secrets**, not directly in workflow files.

---

# 🧪 Data Quality

Data quality is validated at multiple stages.

## Source-level validation

PostgreSQL constraints:

```text
Primary Key
Foreign Key
NOT NULL
Data Type
```

## CDC validation

Verify:

```text
Debezium connector status
Kafka topic existence
Event structure
CDC operation type
Event timestamp
```

# 🌐 Web UIs and Service Ports

| Service       | URL / Port              | Purpose                |
| ------------- | ----------------------- | ---------------------- |
| Airflow       | `http://localhost:8080` | Workflow orchestration |
| MinIO Console | `http://localhost:9001` | Data Lake management   |
| Kafka Connect | `http://localhost:8083` | Connector REST API     |
| PostgreSQL    | `localhost:5432`        | OLTP database          |
| Kafka         | `localhost:9092`        | Event streaming        |

---

# 🛠️ Troubleshooting

## 1. Container fails to start

Check:

```bash
docker compose ps
```

Then:

```bash
docker compose logs <service-name>
```

For example:

```bash
docker compose logs kafka
```

---

## 2. Kafka Connect is unavailable

Check:

```bash
docker compose logs kafka-connect
```

Make sure Kafka is healthy before starting Kafka Connect.

Then test:

```bash
curl http://localhost:8083/
```

---

## 3. Debezium connector fails

Check connector status:

```bash
curl http://localhost:8083/connectors/<connector-name>/status
```

Common causes:

* PostgreSQL is not reachable
* Incorrect database credentials
* WAL configuration is incorrect
* Kafka is unavailable
* Connector configuration is invalid
* Required replication settings are missing

---

## 4. No Kafka events are appearing

Verify:

```text
PostgreSQL
    ↓
WAL
    ↓
Debezium
    ↓
Kafka
```

Check:

```bash
docker compose logs postgres
docker compose logs kafka-connect
```

Then verify the topic exists.

---

## 5. Consumer is not writing to MinIO

Check:

```bash
docker compose logs -f consumer
```

Verify:

* Kafka connection
* Topic name
* MinIO endpoint
* Bucket name
* MinIO credentials
* Consumer group configuration

---

## 6. dbt connection fails

Run:

```bash
dbt debug
```

Check:

* Snowflake account
* Username
* Password
* Database
* Schema
* Warehouse
* Role

---

## 7. dbt tests fail

Run:

```bash
dbt test
```

Inspect the failed model:

```bash
dbt run --select <model_name>
```

Then investigate:

```text
Null values
Duplicate keys
Invalid relationships
Incorrect data types
Unexpected business values
```

---

# 🛑 Stopping the System

Stop containers:

```bash
docker compose down
```

This stops the services while preserving Docker volumes.

To stop and remove volumes:

```bash
docker compose down -v
```

> ⚠️ `docker compose down -v` removes persistent container volumes and may delete locally stored data.

To rebuild everything:

```bash
docker compose down -v
docker compose up -d --build
```

---

# 🔮 Future Improvements

Potential extensions for this project include:

### Streaming

* Kafka Schema Registry
* Avro / Protobuf
* Kafka partitioning strategy
* Dead Letter Queue
* Consumer retry mechanism
* Exactly-once processing

### Data Lake

* Apache Parquet
* Partition optimization
* Data compaction
* Lakehouse architecture
* Apache Iceberg / Delta Lake

### Data Warehouse

* Incremental loading
* Snowflake Streams
* Snowflake Tasks
* Slowly Changing Dimensions
* Dynamic tables

### dbt

* Incremental models
* Snapshots
* Source freshness
* Custom data quality tests
* Documentation generation
* Lineage visualization

### Orchestration

* Airflow sensors
* Task retries
* SLA monitoring
* Alerting
* Backfill strategy
* Data-driven scheduling

### CI/CD

* Automated Docker image builds
* Container vulnerability scanning
* Automated dbt tests
* Deployment to cloud environments
* Environment separation:

### Observability

* Prometheus
* Grafana
* OpenTelemetry
* Data quality monitoring
* Pipeline SLA monitoring

---

# 🎯 Learning Objectives

This project is designed to demonstrate practical understanding of modern Data Engineering concepts.

## Data Engineering

* OLTP vs OLAP
* Batch vs streaming
* ETL / ELT
* CDC
* Data Lake
* Data Warehouse
* Dimensional modeling

## Distributed Systems

* Kafka
* Event streaming
* Consumer groups
* Message processing
* Micro-batching

## Database Internals

* PostgreSQL WAL
* Logical replication
* Transaction logs
* Incremental change processing

## Cloud Data Platform

* S3-compatible object storage
* Snowflake
* Data warehouse architecture

## Data Transformation

* dbt
* Staging
