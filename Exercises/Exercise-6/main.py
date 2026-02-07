import zipfile
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
REPORTS_DIR = Path(__file__).parent / "reports"


def extract_zips(data_dir, tmp_dir):
    for zip_path in data_dir.glob("*.zip"):
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_dir)
    return tmp_dir


def load_data(spark, csv_dir):
    df = spark.read.csv(str(csv_dir), header=True, inferSchema=True)
    df = df.withColumn("date", F.to_date("started_at"))
    df = df.withColumn("month", F.date_format("started_at", "yyyy-MM"))
    df = df.withColumn(
        "tripduration",
        F.unix_timestamp("ended_at") - F.unix_timestamp("started_at"),
    )
    return df


def avg_trip_duration_per_day(df):
    """1. Average trip duration per day."""
    return df.groupBy("date").agg(
        F.avg("tripduration").alias("avg_trip_duration")
    ).orderBy("date")


def trips_per_day(df):
    """2. How many trips were taken each day."""
    return df.groupBy("date").agg(
        F.count("ride_id").alias("trip_count")
    ).orderBy("date")


def most_popular_start_station_per_month(df):
    """3. Most popular starting station for each month."""
    station_counts = df.groupBy("month", "start_station_name").agg(
        F.count("ride_id").alias("trip_count")
    )
    w = Window.partitionBy("month").orderBy(F.desc("trip_count"))
    return station_counts.withColumn("rank", F.row_number().over(w)) \
        .filter(F.col("rank") == 1) \
        .drop("rank") \
        .orderBy("month")


def top3_stations_last_two_weeks(df):
    """4. Top 3 trip stations each day for the last two weeks."""
    max_date = df.agg(F.max("date")).collect()[0][0]
    two_weeks_ago = max_date - datetime.timedelta(days=14)
    recent = df.filter(F.col("date") >= F.lit(two_weeks_ago))

    station_counts = recent.groupBy("date", "start_station_name").agg(
        F.count("ride_id").alias("trip_count")
    )
    w = Window.partitionBy("date").orderBy(F.desc("trip_count"))
    return station_counts.withColumn("rank", F.row_number().over(w)) \
        .filter(F.col("rank") <= 3) \
        .drop("rank") \
        .orderBy("date", F.desc("trip_count"))


def avg_duration_by_member_type(df):
    """5. Do members or casual riders take longer trips on average."""
    return df.groupBy("member_casual") \
        .agg(F.avg("tripduration").alias("avg_trip_duration"))


def top10_trip_durations(df):
    """6. Top 10 longest and shortest average trip durations by station."""
    avg_by_station = df.groupBy("start_station_name").agg(
        F.avg("tripduration").alias("avg_trip_duration")
    ).filter(F.col("start_station_name").isNotNull())
    longest = avg_by_station.orderBy(F.desc("avg_trip_duration")).limit(10) \
        .withColumn("category", F.lit("longest"))
    shortest = avg_by_station.orderBy(F.asc("avg_trip_duration")).limit(10) \
        .withColumn("category", F.lit("shortest"))
    return longest.union(shortest)


def save_report(df, name):
    path = str(REPORTS_DIR / name)
    df.coalesce(1).write.csv(path, header=True, mode="overwrite")
    print(f"Saved report: {name}")


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    REPORTS_DIR.mkdir(exist_ok=True)

    tmp_dir = Path("/tmp/extracted_csvs")
    tmp_dir.mkdir(exist_ok=True)
    extract_zips(DATA_DIR, str(tmp_dir))

    df = load_data(spark, str(tmp_dir))

    save_report(avg_trip_duration_per_day(df), "avg_duration_per_day")
    save_report(trips_per_day(df), "trips_per_day")
    save_report(most_popular_start_station_per_month(df), "popular_station_per_month")
    save_report(top3_stations_last_two_weeks(df), "top3_stations_last_2_weeks")
    save_report(avg_duration_by_member_type(df), "avg_duration_by_member_type")
    save_report(top10_trip_durations(df), "top10_trip_durations")

    spark.stop()
    print("All reports generated")


if __name__ == "__main__":
    main()