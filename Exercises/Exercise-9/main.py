import polars as pl


def main():
    lf = pl.scan_csv("data/202306-divvy-tripdata.csv")

    # Convert data types
    lf = lf.with_columns(
        pl.col("started_at").str.to_datetime("%Y-%m-%d %H:%M:%S"),
        pl.col("ended_at").str.to_datetime("%Y-%m-%d %H:%M:%S"),
        pl.col("start_lat").cast(pl.Float64),
        pl.col("start_lng").cast(pl.Float64),
        pl.col("end_lat").cast(pl.Float64),
        pl.col("end_lng").cast(pl.Float64),
    )

    # Count rides per day
    rides_per_day = (
        lf.with_columns(pl.col("started_at").dt.date().alias("day"))
        .group_by("day")
        # casting the result of pl.len() to Int64 later allows for subtraction without overflowing
        # when calculating Difference from same day last week. Thanks Claude! Would have spent ages
        # trying to fix this myself
        .agg(pl.len().cast(pl.Int64).alias("ride_count"))
        .sort("day")
        .collect()
    )
    print("Rides per day:")
    print(rides_per_day)

    # Average, max, and min rides per week
    rides_per_week = (
        lf.with_columns(
            pl.col("started_at").dt.year().alias("year"),
            pl.col("started_at").dt.week().alias("week"),
        )
        .group_by("year", "week")
        .agg(pl.len().cast(pl.Int64).alias("ride_count"))
        .select(
            pl.col("ride_count").mean().alias("avg_rides_per_week"),
            pl.col("ride_count").max().alias("max_rides_per_week"),
            pl.col("ride_count").min().alias("min_rides_per_week"),
        )
        .collect()
    )
    print("\nWeekly ride stats:")
    print(rides_per_week)

    # Difference from same day last week
    diff_from_last_week = rides_per_day.with_columns(
        (
            pl.col("ride_count") - pl.col("ride_count").shift(7)
        ).alias("diff_from_last_week")
    )
    print("\nDifference from last week:")
    print(diff_from_last_week)


if __name__ == "__main__":
    main()
