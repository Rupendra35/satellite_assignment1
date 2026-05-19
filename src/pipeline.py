from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =========================================================
# CREATE SPARK SESSION
# =========================================================

spark = SparkSession.builder \
    .appName("Satellite Intelligence Assignment") \
    .getOrCreate()

# =========================================================
# READ INPUT FILES
# =========================================================

readings_df = spark.read.csv(
    "data/parcel_readings.csv",
    header=True,
    inferSchema=True
)

metadata_df = spark.read.csv(
    "data/parcel_metadata.csv",
    header=True,
    inferSchema=True
)


# INITIAL DATA INSPECTION


print("Readings Count :", readings_df.count())
print("Metadata Count :", metadata_df.count())

print("Readings Schema")
readings_df.printSchema()

print("Metadata Schema")
metadata_df.printSchema()

# DATA QUALITY


print("NULL VALUE CHECK")


readings_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in readings_df.columns
]).show()

metadata_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in metadata_df.columns
]).show()




readings_duplicates = readings_df.count() - readings_df.dropDuplicates().count()

metadata_duplicates = metadata_df.count() - metadata_df.dropDuplicates().count()

print("Readings Duplicate Rows :", readings_duplicates)
print("Metadata Duplicate Rows :", metadata_duplicates)


# INVALID NDVI CHECK


print("INVALID NDVI CHECK")


invalid_ndvi_df = readings_df.filter(
    (col("ndvi_value") < -1) |
    (col("ndvi_value") > 1)
)

print("Invalid NDVI Count :", invalid_ndvi_df.count())

# SENSOR STATUS CHECK


print("SENSOR STATUS VALUES")


readings_df.select("sensor_status").distinct().show(truncate=False)

# CLEANING STARTS



# CONVERT DATE COLUMNS



readings_df = readings_df.withColumn(
    "date",
    to_date(col("date"))
)




# REMOVE INVALID NDVI VALUES


readings_df = readings_df.filter(
    (col("ndvi_value") >= -1) &
    (col("ndvi_value") <= 1)
)


# STANDARDIZE SENSOR STATUS


readings_df = readings_df.withColumn(
    "sensor_status",
    lower(trim(col("sensor_status")))
)

readings_df = readings_df.withColumn(
    "sensor_status",
    when(col("sensor_status") == "ok", "good")
    .when(col("sensor_status") == "error", "bad")
    .when(col("sensor_status").isin("na", "nan", "null"), "unknown")
    .otherwise("unknown")
)



readings_df = readings_df.withColumn(
    "date",
    when(
        lower(trim(col("date"))).isin("null"),
        None
    ).otherwise(col("date"))
)



final_df = readings_df.join(
    metadata_df,
    on="parcel_id",
    how="left"
)





# WRITE CLEANED DATASET


final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cleaned_parcel_timeseries")

print("Cleaned dataset written successfully")



# IGNORE BAD SENSOR STATUS


good_df = final_df.filter(
    col("sensor_status") == "good"
)


# CALCULATE DAYS DIFFERENCE


analysis_df = good_df.withColumn(
    "days_diff",
    datediff(col("date"), col("sowing_date"))
)

# BEFORE SOWING
# -30 to -1

before_df = analysis_df.filter(
    (col("days_diff") >= -30) &
    (col("days_diff") < 0)
)

before_agg = before_df.groupBy("crop_type").agg(
    round(avg("ndvi_value"), 3).alias("mean_ndvi_before")
)

# AFTER SOWING
# 1 to 30


after_df = analysis_df.filter(
    (col("days_diff") > 0) &
    (col("days_diff") <= 30)
)

after_agg = after_df.groupBy("crop_type").agg(
    round(avg("ndvi_value"), 3).alias("mean_ndvi_after")
)





# FINAL ANALYSIS OUTPUT


result_df = before_agg.join(
    after_agg,
    on="crop_type",
    how="inner"
)


result_df.show(truncate=False)


result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/crop_analysis_output")


spark.stop()


