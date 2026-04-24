from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract, first, concat

spark = SparkSession.builder.appName("BroadwayComparisonTables").getOrCreate()

user = "lb4140_nyu_edu"

full_raw = spark.read.text(f"hdfs:///user/{user}/season_output_full/part-r-00000") \
    .withColumn("dataset_name", lit("full"))

recent_raw = spark.read.text(f"hdfs:///user/{user}/season_output_recent/part-r-00000") \
    .withColumn("dataset_name", lit("recent"))

df = full_raw.unionByName(recent_raw)

parsed = df.select(
    col("dataset_name"),
    regexp_extract("value", r'^(ALL|NO_BIG)_', 1).alias("group_type"),
    regexp_extract("value", r'^(?:ALL|NO_BIG)_(\d{4})_', 1).alias("year"),
    regexp_extract("value", r'^(?:ALL|NO_BIG)_\d{4}_(summer|winter)_', 1).alias("season"),
    regexp_extract("value", r'_(ATT|CAP|GROSS)\s+', 1).alias("metric"),
    regexp_extract("value", r'Average=([0-9.E+-]+)', 1).cast("double").alias("average")
)

pivot_ready = parsed.withColumn(
    "season_metric",
    concat(col("season"), lit("_"), col("metric"))
)

# Table 1: All shows
all_df = (
    pivot_ready.filter(col("group_type") == "ALL")
    .groupBy("dataset_name", "year")
    .pivot("season_metric")
    .agg(first("average"))
    .orderBy("dataset_name", "year")
)

print("\n=== ALL SHOWS TABLE ===")
all_df.show(200, truncate=False)

all_df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"hdfs:///user/{user}/comparison_table_all")

# Table 2:  Shows excluding wicked and lion king
no_big_df = (
    pivot_ready.filter(col("group_type") == "NO_BIG")
    .groupBy("dataset_name", "year")
    .pivot("season_metric")
    .agg(first("average"))
    .orderBy("dataset_name", "year")
)

print("\n=== SHOWS TABLE (EXCLUDING WICKED AND LION KING) ===")
no_big_df.show(200, truncate=False)

no_big_df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"hdfs:///user/{user}/comparison_table_no_big")

spark.stop()