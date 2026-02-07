import os
import zipfile
import shutil
import datetime
import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from main import (
    extract_zips,
    load_data,
    avg_trip_duration_per_day,
    trips_per_day,
    most_popular_start_station_per_month,
    top3_stations_last_two_weeks,
    avg_duration_by_member_type,
    top10_trip_durations,
    save_report,
)


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("TestExercise6") \
        .getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_df(spark):
    data = [
        ("r1", "bike", "2020-01-01 08:00:00", "2020-01-01 08:10:00", "Station A", "1", "Station B", "2", 41.0, -87.0, 41.1, -87.1, "member"),
        ("r2", "bike", "2020-01-01 09:00:00", "2020-01-01 09:20:00", "Station A", "1", "Station C", "3", 41.0, -87.0, 41.2, -87.2, "casual"),
        ("r3", "bike", "2020-01-01 10:00:00", "2020-01-01 10:05:00", "Station B", "2", "Station A", "1", 41.1, -87.1, 41.0, -87.0, "member"),
        ("r4", "bike", "2020-01-02 08:00:00", "2020-01-02 08:30:00", "Station A", "1", "Station B", "2", 41.0, -87.0, 41.1, -87.1, "casual"),
        ("r5", "bike", "2020-01-02 09:00:00", "2020-01-02 09:15:00", "Station C", "3", "Station A", "1", 41.2, -87.2, 41.0, -87.0, "member"),
        ("r6", "bike", "2020-02-01 08:00:00", "2020-02-01 08:25:00", "Station B", "2", "Station C", "3", 41.1, -87.1, 41.2, -87.2, "casual"),
    ]
    columns = [
        "ride_id", "rideable_type", "started_at", "ended_at",
        "start_station_name", "start_station_id",
        "end_station_name", "end_station_id",
        "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
    ]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn("date", F.to_date("started_at"))
    df = df.withColumn("month", F.date_format("started_at", "yyyy-MM"))
    df = df.withColumn(
        "tripduration",
        F.unix_timestamp("ended_at") - F.unix_timestamp("started_at"),
    )
    return df


# --- extract_zips ---

class TestExtractZips:
    def test_extracts_csv_from_zip(self, tmp_path):
        zip_dir = tmp_path / "zips"
        zip_dir.mkdir()
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        csv_content = "col1,col2\n1,2\n"
        zip_path = zip_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("test.csv", csv_content)

        extract_zips(zip_dir, str(out_dir))
        assert (out_dir / "test.csv").exists()
        assert (out_dir / "test.csv").read_text() == csv_content

    def test_no_zip_files(self, tmp_path):
        zip_dir = tmp_path / "empty"
        zip_dir.mkdir()
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        extract_zips(zip_dir, str(out_dir))
        assert list(out_dir.iterdir()) == []


# --- load_data ---

class TestLoadData:
    def test_adds_computed_columns(self, spark, tmp_path):
        csv_content = (
            "ride_id,rideable_type,started_at,ended_at,start_station_name,"
            "start_station_id,end_station_name,end_station_id,"
            "start_lat,start_lng,end_lat,end_lng,member_casual\n"
            "r1,bike,2020-01-01 08:00:00,2020-01-01 08:10:00,A,1,B,2,41,-87,41.1,-87.1,member\n"
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        df = load_data(spark, str(tmp_path))
        assert "date" in df.columns
        assert "month" in df.columns
        assert "tripduration" in df.columns
        row = df.collect()[0]
        assert row["tripduration"] == 600

    def test_empty_csv_returns_no_rows(self, spark, tmp_path):
        csv_content = (
            "ride_id,rideable_type,started_at,ended_at,start_station_name,"
            "start_station_id,end_station_name,end_station_id,"
            "start_lat,start_lng,end_lat,end_lng,member_casual\n"
        )
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text(csv_content)

        df = load_data(spark, str(tmp_path))
        assert df.count() == 0


# --- avg_trip_duration_per_day ---

class TestAvgTripDurationPerDay:
    def test_returns_avg_per_day(self, sample_df):
        result = avg_trip_duration_per_day(sample_df)
        rows = result.collect()
        assert len(rows) == 3
        assert "avg_trip_duration" in result.columns

    def test_single_trip_day_avg_equals_duration(self, sample_df):
        single_day = sample_df.filter(F.col("date") == "2020-02-01")
        result = avg_trip_duration_per_day(single_day)
        row = result.collect()[0]
        assert row["avg_trip_duration"] == 1500.0


# --- trips_per_day ---

class TestTripsPerDay:
    def test_correct_count(self, sample_df):
        result = trips_per_day(sample_df)
        rows = {row["date"].isoformat(): row["trip_count"] for row in result.collect()}
        assert rows["2020-01-01"] == 3
        assert rows["2020-01-02"] == 2

    def test_single_trip_day(self, sample_df):
        single = sample_df.filter(F.col("date") == "2020-02-01")
        result = trips_per_day(single)
        assert result.collect()[0]["trip_count"] == 1


# --- most_popular_start_station_per_month ---

class TestMostPopularStartStation:
    def test_returns_one_station_per_month(self, sample_df):
        result = most_popular_start_station_per_month(sample_df)
        rows = result.collect()
        months = [row["month"] for row in rows]
        assert len(months) == len(set(months))

    def test_correct_station_for_jan(self, sample_df):
        result = most_popular_start_station_per_month(sample_df)
        jan = [r for r in result.collect() if r["month"] == "2020-01"][0]
        assert jan["start_station_name"] == "Station A"


# --- top3_stations_last_two_weeks ---

class TestTop3StationsLastTwoWeeks:
    def test_max_3_per_day(self, sample_df):
        result = top3_stations_last_two_weeks(sample_df)
        rows = result.collect()
        from collections import Counter
        counts = Counter(row["date"] for row in rows)
        for count in counts.values():
            assert count <= 3

    def test_filters_to_last_two_weeks(self, sample_df):
        result = top3_stations_last_two_weeks(sample_df)
        rows = result.collect()
        max_date = datetime.date(2020, 2, 1)
        cutoff = max_date - datetime.timedelta(days=14)
        for row in rows:
            assert row["date"] >= cutoff


# --- avg_duration_by_member_type ---

class TestAvgDurationByMemberType:
    def test_returns_both_types(self, sample_df):
        result = avg_duration_by_member_type(sample_df)
        types = {row["member_casual"] for row in result.collect()}
        assert types == {"member", "casual"}

    def test_casual_longer_than_member(self, sample_df):
        result = avg_duration_by_member_type(sample_df)
        rows = {row["member_casual"]: row["avg_trip_duration"] for row in result.collect()}
        assert rows["casual"] > rows["member"]


# --- top10_trip_durations ---

class TestTop10TripDurations:
    def test_has_longest_and_shortest(self, sample_df):
        result = top10_trip_durations(sample_df)
        categories = {row["category"] for row in result.collect()}
        assert categories == {"longest", "shortest"}

    def test_max_10_per_category(self, sample_df):
        result = top10_trip_durations(sample_df)
        rows = result.collect()
        longest = [r for r in rows if r["category"] == "longest"]
        shortest = [r for r in rows if r["category"] == "shortest"]
        assert len(longest) <= 10
        assert len(shortest) <= 10


# --- save_report ---

class TestSaveReport:
    def test_creates_csv_output(self, sample_df, tmp_path):
        import main
        original = main.REPORTS_DIR
        main.REPORTS_DIR = tmp_path

        result_df = trips_per_day(sample_df)
        save_report(result_df, "test_report")

        report_dir = tmp_path / "test_report"
        csv_files = list(report_dir.glob("part-*.csv"))
        assert len(csv_files) == 1

        main.REPORTS_DIR = original

    def test_overwrites_existing(self, sample_df, tmp_path):
        import main
        original = main.REPORTS_DIR
        main.REPORTS_DIR = tmp_path

        result_df = trips_per_day(sample_df)
        save_report(result_df, "test_overwrite")
        save_report(result_df, "test_overwrite")

        report_dir = tmp_path / "test_overwrite"
        csv_files = list(report_dir.glob("part-*.csv"))
        assert len(csv_files) == 1

        main.REPORTS_DIR = original