import csv
import io
import os

import boto3
import psycopg2
from dagster import asset, OpExecutionContext


@asset
def postgres_to_s3(context: OpExecutionContext) -> None:
    """Extract car_sales from source PostgreSQL and upload as CSV to S3."""
    conn = psycopg2.connect(
        host=os.environ["SOURCE_PG_HOST"],
        port=os.environ["SOURCE_PG_PORT"],
        dbname=os.environ["SOURCE_PG_DATABASE"],
        user=os.environ["SOURCE_PG_USER"],
        password=os.environ["SOURCE_PG_PASSWORD"],
    )
    try:
        cur = conn.cursor()
        schema = os.environ["SOURCE_PG_DATABASE"]
        cur.execute(f'SELECT * FROM "{schema}".car_sales')
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        context.log.info(f"Fetched {len(rows)} rows from source PostgreSQL")
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)

    s3 = boto3.client("s3", region_name=os.environ["S3_REGION"])
    s3.put_object(
        Bucket=os.environ["S3_BUCKET"],
        Key=os.environ["S3_KEY"],
        Body=buf.getvalue().encode("utf-8"),
    )
    context.log.info(
        f"Uploaded {len(rows)} rows to s3://{os.environ['S3_BUCKET']}/{os.environ['S3_KEY']}"
    )


@asset(deps=[postgres_to_s3])
def s3_to_rds(context: OpExecutionContext) -> None:
    """Download CSV from S3 and load into RDS PostgreSQL car_sales table."""
    s3 = boto3.client("s3", region_name=os.environ["S3_REGION"])
    obj = s3.get_object(Bucket=os.environ["S3_BUCKET"], Key=os.environ["S3_KEY"])
    csv_text = obj["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    context.log.info(f"Downloaded {len(rows)} rows from S3")

    conn = psycopg2.connect(
        host=os.environ["RDS_PG_HOST"],
        port=os.environ["RDS_PG_PORT"],
        dbname=os.environ["RDS_PG_DATABASE"],
        user=os.environ["RDS_PG_USER"],
        password=os.environ["RDS_PG_PASSWORD"],
    )
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS car_sales (
                id          SERIAL PRIMARY KEY,
                sale_date   DATE NOT NULL,
                car_model   VARCHAR(100) NOT NULL,
                region      VARCHAR(50) NOT NULL,
                sale_price  VARCHAR(50) NOT NULL,
                customer_name VARCHAR(150) NOT NULL
            )
        """)
        cur.execute("TRUNCATE TABLE car_sales")

        batch = []
        batch_size = 5000
        total = 0
        for row in rows:
            batch.append((
                row["sale_date"],
                row["car_model"],
                row["region"],
                row["sale_price"],
                row["customer_name"],
            ))
            if len(batch) >= batch_size:
                _insert_batch(cur, batch)
                total += len(batch)
                batch = []
        if batch:
            _insert_batch(cur, batch)
            total += len(batch)

        conn.commit()
        context.log.info(f"Loaded {total} rows into RDS car_sales table")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _insert_batch(cur, batch):
    args_str = ",".join(
        cur.mogrify("(%s, %s, %s, %s, %s)", row).decode() for row in batch
    )
    cur.execute(
        f"INSERT INTO car_sales (sale_date, car_model, region, sale_price, customer_name) "
        f"VALUES {args_str}"
    )
