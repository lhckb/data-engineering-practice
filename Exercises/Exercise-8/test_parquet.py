import duckdb

def main():
    con = duckdb.connect()

    res = con.execute("""
        SELECT * FROM read_parquet('output/**/*.parquet', hive_partitioning = true);
    """).fetchall()

    print(res)

if __name__ == "__main__":
    main()