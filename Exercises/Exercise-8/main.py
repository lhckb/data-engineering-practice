import duckdb


def create_table_with_data(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE electric_vehicles (
            vin VARCHAR(10),
            county VARCHAR,
            city VARCHAR,
            state VARCHAR(2),
            postal_code VARCHAR(10),
            model_year INTEGER,
            make VARCHAR,
            model VARCHAR,
            electric_vehicle_type VARCHAR,
            cafv_eligibility VARCHAR,
            electric_range INTEGER,
            base_msrp DECIMAL(10, 2),
            legislative_district INTEGER,
            dol_vehicle_id BIGINT,
            vehicle_location VARCHAR,
            electric_utility VARCHAR,
            census_tract_2020 BIGINT
        );
    """)

    con.execute("""
        INSERT INTO electric_vehicles 
        SELECT * FROM read_csv('data/Electric_Vehicle_Population_Data.csv');
    """)

def num_eletric_per_city(con: duckdb.DuckDBPyConnection):
    result = con.execute("""
        SELECT city, COUNT(*) as num_vehicles FROM electric_vehicles
        GROUP BY city
        ORDER BY num_vehicles;
    """).fetchall()

    print(f"Number of electrics per city:\n{result}")

def top_3_vehicles(con: duckdb.DuckDBPyConnection):
    result = con.execute("""
        SELECT model_year, make, model, COUNT(*) as num_vehicles FROM electric_vehicles
        GROUP BY model_year, make, model
        ORDER BY num_vehicles DESC
        LIMIT 3;
    """).fetchall()

    print(f"Top 3 electrics:\n{result}")

def most_popular_per_postal_code(con: duckdb.DuckDBPyConnection):
    result = con.execute("""
        WITH vehicle_counts AS (
            SELECT
                postal_code,
                make || ' ' || model || ' ' || model_year AS vehicle,
                COUNT(*) as num_vehicles,
                ROW_NUMBER() OVER (PARTITION BY postal_code ORDER BY COUNT(*) DESC) as rn
            FROM electric_vehicles
            GROUP BY postal_code, make, model, model_year
        )
        SELECT postal_code, vehicle, num_vehicles
        FROM vehicle_counts
        WHERE rn = 1
        ORDER BY num_vehicles DESC;
    """).fetchall()

    print(f"Most popular vehicle per postal code:\n{result}")

def number_of_cars_per_model_year(con: duckdb.DuckDBPyConnection):
    result = con.execute("""
        COPY(
            SELECT model_year, model, COUNT(*) as num_vehicles FROM electric_vehicles
            GROUP BY model_year, model
            ORDER BY num_vehicles ASC
        ) TO 'output' (FORMAT PARQUET, PARTITION_BY (model_year), OVERWRITE_OR_IGNORE)
    """).fetchall()

    # print(f"Num of cars per model year:\n{result}")

def main():
    con = duckdb.connect()

    create_table_with_data(con)
    num_eletric_per_city(con)
    top_3_vehicles(con)
    most_popular_per_postal_code(con)
    number_of_cars_per_model_year(con)

    # print(con.execute("SELECT * FROM electric_vehicles LIMIT 1;").fetchall())


if __name__ == "__main__":
    main()
