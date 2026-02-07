import csv
import psycopg2
from pathlib import Path

SQL_FILE = Path(__file__).parent / "create_tables.sql"
DATA_DIR = Path(__file__).parent / "data"


def create_tables(cur, sql_path):
    ddl = sql_path.read_text()
    cur.execute(ddl)


def load_csv(cur, csv_path, table_name):
    with open(csv_path) as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        if not reader.fieldnames:
            return
        columns = [col.strip() for col in reader.fieldnames]
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        for row in reader:
            values = [v.strip() if v.strip() != "" else None for v in row.values()]
            cur.execute(insert_sql, values)

    print(f"Loaded {csv_path.name} into {table_name}")


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    create_tables(cur, SQL_FILE)
    conn.commit()

    load_csv(cur, DATA_DIR / "accounts.csv", "accounts")
    load_csv(cur, DATA_DIR / "products.csv", "products")
    load_csv(cur, DATA_DIR / "transactions.csv", "transactions")
    conn.commit()

    cur.close()
    conn.close()
    print("Done")


if __name__ == "__main__":
    main()
