# Network Logs Data Pipeline

A Python and PostgreSQL ETL pipeline that ingests, validates, cleans, and aggregates network log records into an analytics-ready daily mart.

## Architecture

```text
network_logs_dirty.csv
        |
        v
   Raw Layer
        |
        v
 Validation & Deduplication
        |
   +----+----+
   |         |
   v         v
Staging   Rejected
   |
   v
Daily Aggregation
   |
   v
Analytics Mart
```

### Data layers

- **Raw:** Stores original CSV values without modifying them.
- **Rejected:** Stores invalid and duplicate records with rejection reasons.
- **Staging:** Stores validated, typed, and cleaned records.
- **Analytics Mart:** Stores daily traffic metrics grouped by event date and source IP.
- **Pipeline Batches:** Tracks ingestion batches and their execution status.

## Technology Stack

- Python
- PostgreSQL
- SQLAlchemy
- Psycopg
- SQL
- pytest
- python-dotenv

## Project Structure

```text
network-logs-pipeline/
├── data/
├── logs/
├── sql/
│   ├── schema.sql
│   ├── cleaning.sql
│   ├── aggregation.sql
│   └── analytics.sql
├── src/
│   └── pipeline/
│       ├── config.py
│       ├── database.py
│       ├── ingest.py
│       ├── validate.py
│       ├── clean.py
│       ├── aggregate.py
│       ├── logging_config.py
│       └── cli.py
├── tests/
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
└── DESIGN.md
```

## Prerequisites

- Python 3.10+
- PostgreSQL
- Git

## Installation

Clone the repository:

```bash
git clone https://github.com/yazdan2727/network-logs-pipeline.git
cd network-logs-pipeline
```

Create and activate a virtual environment.

Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Database Configuration

Create a PostgreSQL database named:

```sql
CREATE DATABASE network_logs;
```

Copy `.env.example` to `.env` and configure your database credentials:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=network_logs
DB_USER=postgres
DB_PASSWORD=your_password
```

Execute the schema:

```bash
psql -U postgres -d network_logs -f sql/schema.sql
```

Place the input file at:

```text
data/network_logs_dirty.csv
```

The input CSV is intentionally excluded from Git.

## Running the Pipeline

Run individual stages:

```bash
python -m src.pipeline.cli ingest
python -m src.pipeline.cli validate
python -m src.pipeline.cli clean
python -m src.pipeline.cli aggregate
```

`load` is also available as an alias for the aggregation and mart-loading operation:

```bash
python -m src.pipeline.cli load
```

Run the complete pipeline:

```bash
python -m src.pipeline.cli all
```

Run the automated tests:

```bash
python -m pytest -v
```

## Data Quality and Reliability

The pipeline implements:

- SHA-256 batch identification.
- Idempotent raw ingestion.
- Timestamp validation and a configurable failure threshold defined by the assignment.
- IP address and port validation.
- Duplicate detection using a composite business key.
- Quarantine of rejected records with rejection reasons.
- Median imputation for missing byte values.
- Defaulting empty statuses to `UNKNOWN`.
- Idempotent daily mart aggregation.
- Logging to the terminal and `logs/pipeline.log`.

## Analytics

The analytics SQL includes:

- Top five source IPs by error events.
- Daily traffic ranking by source IP.
- Seven-day moving average of daily traffic.

## Testing

Run:

```bash
python -m pytest -v
```

The tests cover data quality, mart uniqueness, and ingestion and aggregation idempotency.

## Security

Database credentials belong in `.env`, which must not be committed. The source CSV and generated log files are also excluded from Git.

