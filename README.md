# Crypto Price Data Pipeline

An automated ETL (Extract, Transform, Load) pipeline that fetches historical daily price data for a given cryptocurrency from the CoinGecko API, transforms it into a clean tabular format, and loads it into both a local SQLite database and AWS S3 for long-term storage. The entire workflow is orchestrated with Apache Airflow, running automatically on a daily schedule.

## Pipeline Overview

The pipeline follows a standard ETL workflow with four sequential tasks:

1. **Extract** — Fetches historical daily price data for a given coin (e.g. Bitcoin) against a target currency (e.g. USD) from the CoinGecko API.
2. **Transform** — Converts the raw price data (timestamp/price pairs) into a structured pandas DataFrame, converts UNIX timestamps into readable dates, and tags each row with the coin identifier.
3. **Load to SQLite** — Stores the cleaned data in a local SQLite database, with automatic deduplication based on date and coin.
4. **Load to S3** — Uploads a daily CSV snapshot to AWS S3 for long-term, cloud-based backup.

All four steps are orchestrated as an Airflow DAG, scheduled to run automatically once per day.

## Tech Stack

- **Python** — Core language for all ETL logic
- **pandas** — Data cleaning and transformation
- **SQLite** — Local relational database for structured storage
- **AWS S3 (boto3)** — Cloud storage for daily data backups
- **Apache Airflow** — Workflow orchestration and scheduling
- **Docker & Docker Compose** — Containerized environment for running Airflow
- **CoinGecko API** — Source of historical cryptocurrency price data

## Security

- AWS credentials are never hardcoded in the source code.
- All sensitive values are stored in a local `.env` file, which is excluded from version control via `.gitignore`.
- Credentials are injected into the Docker containers as environment variables at runtime.

## Setup & Run

1. Clone the repository:
git clone https://github.com/mrkeshavarz9/crypto-pipeline.git
cd crypto-pipeline


2. Create a `.env` file in the project root with the following variables:
AIRFLOW_UID=50000
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key


3. Start Airflow:
docker compose up airflow-init
docker compose up -d


4. Open the Airflow UI at `http://localhost:8080` (default login: `airflow` / `airflow`) and trigger the `coin_prices_pipeline` DAG.