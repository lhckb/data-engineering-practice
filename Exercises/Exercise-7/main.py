import zipfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DATA_DIR = Path(__file__).parent / "data"


def extract_zips(data_dir, tmp_dir):
    for zip_path in data_dir.glob("*.zip"):
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_dir)
    return tmp_dir


def load_data(spark, csv_dir):
    return spark.read.csv(str(csv_dir), header=True, inferSchema=True)


def add_source_file(df):
    """1. Add the file name as a column."""
    return df.withColumn("source_file", F.input_file_name())


def add_file_date(df):
    """2. Extract date from source_file name."""
    date_str = F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)
    return df.withColumn("file_date", F.to_date(date_str, "yyyy-MM-dd"))


def add_brand(df):
    """3. Extract brand from model column (text before first space, or 'unknown')."""
    return df.withColumn(
        "brand",
        F.when(
            F.col("model").contains(" "),
            F.element_at(F.split("model", " "), 1),
        ).otherwise(F.lit("unknown")),
    )


def add_storage_ranking(df):
    """4. Rank models by capacity_bytes (most to least)."""
    model_capacity = df.groupBy("model").agg(
        F.max("capacity_bytes").alias("max_capacity")
    )
    w = Window.orderBy(F.desc("max_capacity"))
    model_ranked = model_capacity.withColumn("storage_ranking", F.dense_rank().over(w)) \
        .select("model", "storage_ranking")
    return df.join(model_ranked, on="model", how="left")


def add_primary_key(df):
    """5. Hash of date + serial_number as primary key."""
    return df.withColumn("primary_key", F.md5(F.concat_ws("-", "date", "serial_number")))


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    tmp_dir = Path("/tmp/extracted_csvs")
    tmp_dir.mkdir(exist_ok=True)
    extract_zips(DATA_DIR, str(tmp_dir))

    df = load_data(spark, str(tmp_dir))
    df = add_source_file(df)
    df = add_file_date(df)
    df = add_brand(df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)

    df.show(truncate=False)

    output_path = str(DATA_DIR / "result")
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    print(f"Saved result to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
