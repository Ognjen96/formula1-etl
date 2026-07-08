# Formula 1 Data Engineering Pipeline

An end-to-end batch ETL pipeline that ingests Formula 1 data, models it into a Kimball star schema in SQL Server, orchestrates the flow with Apache Airflow, and surfaces analytics through Power BI.

> **Scope:** Historical F1 data covering seasons **2012–2023**.

---

## Table of Contents
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model)
- [Pipeline Overview](#pipeline-overview)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Dashboard](#dashboard)
- [Design Decisions](#design-decisions)
- [Possible Extensions](#possible-extensions)

---

## Architecture

<img width="963" height="361" alt="ETL_diagram" src="https://github.com/user-attachments/assets/7de4a543-9b19-4bce-b0f3-8b39409aaf59" />

The pipeline runs as a three-task Airflow DAG: **extract → transform → load**. Airflow runs in Docker; SQL Server runs on the host and is reached from the container via `host.docker.internal`.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 3.x (Docker) |
| Processing | Python, pandas |
| Storage / Warehouse | Microsoft SQL Server (Express) |
| Source | Google Drive (Service Account API) |
| Visualization | Power BI Desktop |
| Containerization | Docker Compose |

---

## Data Model

<img width="2340" height="1382" alt="Model databases" src="https://github.com/user-attachments/assets/0e60494b-ae59-4a85-a3a9-1e6f5d54ecff" />

A **star schema** with one fact table and five dimensions.

**Fact:** `FactResults` — one row per driver per race (grain: One driver per race).

**Dimensions:**
- `DimDriver`
- `DimConstructor`
- `DimCircuit`
- `DimRace`
- `DimTime` — full calendar; surrogate `DateKey` in `YYYYMMDD` form.

<!-- TODO: briefly note key design choices (natural keys for most dims, DimTime as surrogate, etc.) -->

---

## Pipeline Overview

### 1. Extract
- Pulls the source CSV from Google Drive using a **Service Account** (no interactive OAuth).
- Chunked download to handle large files.
- Idempotent: skips the download if the file already exists locally.

### 2. Transform
- Resolves a Cartesian explosion in the source (lap × pitstop fan-out) by de-duplicating to the correct grain before aggregating.
- Builds the five dimensions and the fact table.
- Handles missing values (`\N` → `NULL`) and casts columns to correct types.

### 3. Load
- Creates tables if they don't exist (`schema.sql`, idempotent DDL).
- Truncate-and-load for a clean, repeatable full refresh.
- Loads dimensions first, fact last (respecting foreign-key order).

---

## Setup & Installation

### Prerequisites
- Docker & Docker Compose
- Microsoft SQL Server (Express) running on the host
- A Google Cloud Service Account with Drive API access
- Power BI Desktop (for the dashboard)

### Steps

#### 1. Clone the repository
```bash
git clone https://github.com//.git
cd 
```

#### 2. Prepare SQL Server (on the host)
- Enable **TCP/IP** (SQL Server Configuration Manager → Protocols → TCP/IP → Enable), set port to **1433**, and restart the SQL Server service.
- Enable **Mixed Mode authentication** (SSMS → Server Properties → Security), then restart the service.
- Create a SQL login (e.g. `airflow_user`) with `db_owner` on the target database.
- Open TCP port **1433** in Windows Firewall.

#### 3. Add the Google service-account key
- Place your service-account JSON key in `config/` (this folder is git-ignored).
- Make sure the source CSV on Google Drive is **shared** with the service account's email.

#### 4. Configure environment
Create a `.env` file in the project root:
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=pandas google-api-python-client google-auth google-auth-httplib2 sqlalchemy pymssql

#### 5. Start Airflow
```bash
docker compose up -d
```
Wait for the containers to be healthy, then open the Airflow UI at `http://localhost:8080`.

#### 6. Create the Airflow Connection and Variable
In the Airflow UI (**Admin → Connections / Variables**):

**Connection** — `sql_server_f1` (type: Generic)
| Field | Value |
|-------|-------|
| Host | `host.docker.internal` |
| Port | `1433` |
| Schema | your database name |
| Login | `airflow_user` |
| Password | your password |

**Variable** — `gdrive_file_id`: the Google Drive file ID of the source CSV.

#### 7. Run the pipeline
Trigger the `ETL_Formula1` DAG from the UI

---

## Configuration

This project keeps **no secrets in source control**. The following are required but git-ignored:

| Item | Where it lives | Notes |
|------|----------------|-------|
| Google service-account key | `config/<key>.json` | Git-ignored |
| SQL Server credentials | Airflow **Connection** (`sql_server_f1`) | Set via Airflow UI |
| Google Drive file id | Airflow **Variable** (`gdrive_file_id`) | Set via Airflow UI |

SQL Server must have **TCP/IP enabled**, **Mixed Mode authentication**, and a SQL login the container can use.

Add new instance of variable: Airflow -> Admin -> **Variables**
Key: <Name-of-Variable>
Value: <Google-file-ID-of-the-CSV>

Add new instance of connections to be able to connect to SQL server: Airflow -> Admin -> **Connections**

Connection ID: <Give-ID>
Connection Type: generic
Host: host.docker.internal
Port: 1433
Login: <Your-SQL-login>
Password: <Your-SQL-password>
Scema: <SQL-DB-Name>

---

## Running the Pipeline

```bash
# Trigger the DAG from the Airflow UI, or:
docker compose exec airflow-scheduler airflow dags trigger ETL_Formula1
```

<img width="971" height="372" alt="image" src="https://github.com/user-attachments/assets/0b59f5d7-2c8b-4381-a976-705117060548" />

---

## Dashboard


<img width="1131" height="642" alt="image" src="https://github.com/user-attachments/assets/9483132c-f1e8-47d9-afb7-d902bffdcd7d" />

<img width="1165" height="738" alt="image" src="https://github.com/user-attachments/assets/de75cb83-1859-4472-a9c4-358e73d6101c" />

<img width="393" height="704" alt="image" src="https://github.com/user-attachments/assets/692e4458-10dd-4a91-b6f0-9c3f7d8da20a" />


The Power BI report includes:
- Points by driver
- Points by constructor
- Circuits map (bubble size = number of races)
- Fastest tracks

---

## Design Decisions

<!-- TODO: this is the section that makes a portfolio project stand out.
     Briefly explain the "why" behind a few choices, e.g.: -->

- **Batch over streaming (no Kafka):** the ingestion pattern here is periodic full refresh, which is representative of most real-world pipelines; a streaming layer would add complexity without a real-time requirement.
- **Star schema:** chosen for clean, fast analytical queries and natural fit with Power BI.
- **Natural vs. surrogate keys:** Used natural keys (Source provided IDs like driverId, raceId, etc) over surrogate keys. Acceptable here because the historical data is static and never versioned. DimTime is the one exception, using a computed YYYYMMDD surrogate.
- **Idempotent load (truncate-and-load):** safe to re-run, fits Airflow's retry model.

---

## Possible Extensions

- Extend the dataset to earlier seasons (Jolpica/Ergast API supports 1950–present).
- Incremental loads (upsert / `MERGE`) instead of full refresh.
- A streaming ingestion layer (Kafka) for near-real-time updates.
- CI/CD for the DAG and schema.

---

## License

<!-- TODO: choose a license (MIT is common for portfolio projects) -->
