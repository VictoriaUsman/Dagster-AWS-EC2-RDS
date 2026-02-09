import os
import csv
import psycopg2

from dotenv import load_dotenv

load_dotenv()

# --- Configuration via environment variables ---
DB_HOST = os.getenv("SOURCE_PG_HOST", "localhost")
DB_PORT = os.getenv("SOURCE_PG_PORT", "5432")
DB_NAME = os.getenv("SOURCE_PG_DATABASE", "car_sales")
DB_USER = os.getenv("SOURCE_PG_USER", "postgres")
DB_PASSWORD = os.getenv("SOURCE_PG_PASSWORD", "")

CSV_FILE = os.path.join(os.path.dirname(__file__), "car_sales_2020_2026_100k.csv")
SCHEMA_NAME = DB_NAME  # schema matches the database/user name
TABLE_NAME = f'"{SCHEMA_NAME}".car_sales'
BATCH_SIZE = 5000



def create_table(cur):
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}";')
    cur.execute(f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} (
            id          SERIAL PRIMARY KEY,
            sale_date   DATE NOT NULL,
            car_model   VARCHAR(100) NOT NULL,
            region      VARCHAR(50) NOT NULL,
            sale_price  VARCHAR(50) NOT NULL,
            customer_name VARCHAR(150) NOT NULL
        );
    """)


def load_csv(cur):
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        total = 0

        for row in reader:
            batch.append((
                row["sale_date"].strip(),
                row["car_model"].strip(),
                row["region"].strip(),
                row["sale_price"].strip(),
                row["customer_name"].strip(),
            ))

            if len(batch) >= BATCH_SIZE:
                insert_batch(cur, batch)
                total += len(batch)
                print(f"  Inserted {total} rows...")
                batch = []

        if batch:
            insert_batch(cur, batch)
            total += len(batch)

    print(f"  Total rows inserted: {total}")


def insert_batch(cur, batch):
    args_str = ",".join(
        cur.mogrify("(%s, %s, %s, %s, %s)", row).decode() for row in batch
    )
    cur.execute(
        f"INSERT INTO {TABLE_NAME} (sale_date, car_model, region, sale_price, customer_name) "
        f"VALUES {args_str}"
    )


def main():
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = False

    try:
        cur = conn.cursor()
        cur.execute("SET search_path TO public;")

        print("Creating table...")
        create_table(cur)

        print("Loading CSV data...")
        load_csv(cur)

        conn.commit()
        print("Done! Data committed successfully.")

        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        count = cur.fetchone()[0]
        print(f"Verification: {count} rows in '{TABLE_NAME}' table.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
