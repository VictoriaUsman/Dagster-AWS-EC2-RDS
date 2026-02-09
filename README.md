# Car Sales Data Pipeline — dbt + Dagster on AWS

End-to-end data pipeline that extracts car sales data from a source PostgreSQL database, stages it in S3, loads it into RDS PostgreSQL, and transforms it through medallion architecture layers using dbt — all orchestrated by Dagster.

## Architecture

```
Source PostgreSQL (filess.io)
        │
        ▼
   [ Dagster: postgres_to_s3 ]
        │
        ▼
   Amazon S3 (car-sales12)
        │
        ▼
   [ Dagster: s3_to_rds ]
        │
        ▼
   RDS PostgreSQL
        │
        ▼
   [ dbt models ]
        │
        ├── staging.stg_car_sales     (raw copy)
        ├── bronze.bronze_car_sales   (cleaned prices & names)
        ├── silver.silver_car_sales   (customer tagging)
        └── gold.gold_car_sales       (production-ready)
```

## dbt Models

| Layer   | Model              | Schema  | Description |
|---------|--------------------|---------|-------------|
| Staging | `stg_car_sales`    | staging | Exact copy of raw `car_sales` table |
| Bronze  | `bronze_car_sales` | bronze  | Cleans `sale_price` (removes currency symbols, casts to numeric) and `customer_name` (title case, fixes last/first comma format) |
| Silver  | `silver_car_sales` | silver  | Adds `total_sales` per customer and `customer_tag`: Valued Customer (>=600k), Regular (<600k), Not Frequent Buyer |
| Gold    | `gold_car_sales`   | gold    | Production-ready table, copy of silver |

## Dagster Assets

1. **`postgres_to_s3`** — Extracts 100k rows from source PostgreSQL, uploads as CSV to S3
2. **`s3_to_rds`** — Downloads CSV from S3, loads into RDS PostgreSQL `car_sales` table
3. **`car_sales_dbt_assets`** — Runs dbt build (staging → bronze → silver → gold)

**Schedule**: Runs full pipeline at 6:00 AM and 6:00 PM UTC daily.

## Tech Stack

- **Orchestration**: Dagster
- **Transformation**: dbt-core + dbt-postgres
- **Source DB**: PostgreSQL (filess.io)
- **Storage**: Amazon S3
- **Target DB**: Amazon RDS PostgreSQL
- **Compute**: Amazon EC2 (t3.micro)

## Project Structure

```
├── car_sales_dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/
│       │   ├── stg_car_sales.sql
│       │   └── schema.yml
│       ├── bronze/
│       │   ├── bronze_car_sales.sql
│       │   └── schema.yml
│       ├── silver/
│       │   ├── silver_car_sales.sql
│       │   └── schema.yml
│       └── gold/
│           ├── gold_car_sales.sql
│           └── schema.yml
├── car_sales_dagster/
│   ├── car_sales_dagster/
│   │   ├── __init__.py
│   │   ├── assets.py
│   │   ├── definitions.py
│   │   └── project.py
│   ├── setup.py
│   └── workspace.yaml
├── csv_to_postgres.py
├── requirements.txt
└── requirements_ec2.txt
```

## Setup

### Prerequisites
- Python 3.11+
- AWS account with S3, RDS, EC2
- Source PostgreSQL database with `car_sales` table

### Local Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements_ec2.txt

# Create .env with your credentials (see .env.example)

# Run dbt
cd car_sales_dbt
set -a && source "../.env" && set +a
dbt run --profiles-dir .

# Run Dagster
cd ../car_sales_dagster
pip install -e .
dagster dev -w workspace.yaml -p 3000
```

### EC2 Deployment

```bash
./deploy_to_ec2.sh
```

Dagster UI will be available at `http://<EC2_IP>:3000`.
