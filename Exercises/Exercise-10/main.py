from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format,
)
import great_expectations as gx
from great_expectations.expectations import ExpectColumnValuesToBeBetween
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

# Create a SparkSession
spark = SparkSession.builder.appName("BikeRideDuration").getOrCreate()

# Define the schema based on the provided CSV structure
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

input_csv_path = "data/202306-divvy-tripdata.csv"

df = spark.read.csv(
    input_csv_path,
    header=True,
    schema=schema,
    mode="DROPMALFORMED"
)

df = df.withColumn(
    "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
)

df = df.withColumn(
    "duration_seconds",
    unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
)

# Validate that no trip duration exceeds 24 hours (86400 seconds)
# ---------------------------------------------------------------
context = gx.get_context()
data_source = context.data_sources.add_spark("spark_source", spark_config={})
data_asset = data_source.add_dataframe_asset("trips")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
suite = context.suites.add(gx.ExpectationSuite(name="trip_validations"))
suite.add_expectation(
    ExpectColumnValuesToBeBetween(
        column="duration_seconds", min_value=0, max_value=86400
    )
)
validation_def = context.validation_definitions.add(
    gx.ValidationDefinition(name="validate_trips", data=batch_definition, suite=suite)
)
checkpoint = context.checkpoints.add(
    gx.Checkpoint(name="trip_checkpoint", validation_definitions=[validation_def])
)
results = checkpoint.run(batch_parameters={"dataframe": df})
print(f"Validation passed: {results.success}")
# ---------------------------------------------------------------

df = df.withColumn(
    "date", date_format(col("started_at"), "yyyy-MM-dd")
)

daily_durations = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)

output_parquet_path = "results/output_file.parquet"
daily_durations.write.mode("overwrite").parquet(output_parquet_path)


