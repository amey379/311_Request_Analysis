import sys
import os

# ✅ Add `scripts/` directory to Python's module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import get_spark_session, DB_CONFIG, DATA_PATH
from pyspark.sql.functions import lit,year, current_timestamp, col, trim, split, when, coalesce,row_number, date_format
from pyspark.sql.types import DateType, IntegerType, StringType,DecimalType
from pyspark.sql.window import Window

# ✅ Get Spark Session
spark = get_spark_session("fact_311_request_load")

process_id = os.getpid()

# ✅ Use Centralized Database Config
driver = DB_CONFIG["driver"]
url = DB_CONFIG["url"]
user = DB_CONFIG["user"]
password = DB_CONFIG["password"]

#write_table = "boston_311_stage"

table = "boston_311_stage"
readtable = "boston_311_stage"
fact_311_request = "fact_311_request"

DIM_TABLES = {
    "dim_date": "dim_date",
    "dim_time": "dim_time",
    "dim_location": "dim_location",
    "dim_source": "dim_source",
    "dim_request_dtl" : "dim_request_dtl"
    }

# Read required columns from `boston_311_stage`
df_stage = spark.read \
    .format("jdbc") \
    .option("driver", DB_CONFIG["driver"]) \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", readtable) \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .load()


df_fact = df_stage.withColumn("open_date_key", date_format(col("open_dt"), "yyyyMMdd").cast(IntegerType())) \
                  .withColumn("open_time_key", date_format(col("open_dt"), "HHmmss").cast(IntegerType())) \
                  .withColumn("sla_date_key", date_format(col("sla_target_dt"), "yyyyMMdd").cast(IntegerType())) \
                  .withColumn("sla_time_key", date_format(col("sla_target_dt"), "HHmmss").cast(IntegerType())) \
                  .withColumn("close_date_key", date_format(col("closed_dt"), "yyyyMMdd").cast(IntegerType())) \
                  .withColumn("close_time_key", date_format(col("closed_dt"), "HHmmss").cast(IntegerType()))

df_location = spark.read \
    .format("jdbc") \
    .option("driver", DB_CONFIG["driver"]) \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", DIM_TABLES["dim_location"]) \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .load()


df_fact = df_fact.join(df_location,
                       (df_fact["location_street_name"] == df_location["location_street_name"]) &
                       (df_fact["neighborhood"] == df_location["neighborhood"]) &
                       (df_fact["location_zipcode"] == df_location["zipcode"]),
                       "left") \
                 .select(df_fact["*"], df_location["location_key"])

# ✅ Step 4: Lookup `request_dtl_key` from `dim_request_dtl`
df_request_dtl = spark.read \
    .format("jdbc") \
    .option("driver", DB_CONFIG["driver"]) \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", DIM_TABLES["dim_request_dtl"]) \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .load()

df_fact = df_fact.join(df_request_dtl,
    on=[
        df_fact["case_title"] == df_request_dtl["case_title"],
        df_fact["subject"] == df_request_dtl["subject"],
        df_fact["reason"] == df_request_dtl["reason"],
        df_fact["queue"] == df_request_dtl["queue"],
        df_fact["type"] == df_request_dtl["type"]
    ],
    how="left"
).select(df_fact["*"], df_request_dtl["request_dtl_key"])

# ✅ Step 5: Lookup `source_key` from `dim_source`
df_source = spark.read \
    .format("jdbc") \
    .option("driver", DB_CONFIG["driver"]) \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", DIM_TABLES["dim_source"]) \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .load()

df_fact = df_fact.join(df_source,
                       df_fact["source"] == df_source["source"],
                       "left") \
                 .select(df_fact["*"], df_source["source_key"])

# ✅ Step 6: Rename & Handle `NULL` Foreign Keys with `-1`
df_fact = df_fact.withColumnRenamed("bos_case_id", "case_stg_id") \
                 .withColumnRenamed("open_dt", "open_date") \
                 .withColumnRenamed("sla_target_dt", "sla_target_date") \
                 .withColumnRenamed("closed_dt", "closed_date") 

df_fact = df_fact.fillna({
    "open_date_key": -1,
    "open_time_key": -1,
    "sla_date_key": -1,
    "sla_time_key": -1,
    "close_date_key": -1,
    "close_time_key": -1,
    "location_key": -1,
    "request_dtl_key": -1,
    "source_key": -1
})

# ✅ Step 7: Add Metadata Columns
df_fact = df_fact.withColumn("db_created_datetime", current_timestamp()) \
                 .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
                 .withColumn("created_by", lit("system")) \
                 .withColumn("modified_by", lit(None).cast("string")) \
                 .withColumn("process_id", lit(1))

# ✅ Step 8: Select Only Required Columns Before Insert
df_fact = df_fact.select(
    col("case_stg_id"),
    col("case_enquiry_id"),
    col("open_date"),
    col("sla_target_date"),
    col("closed_date"),
    col("open_date_key"),
    col("open_time_key"),
    col("sla_date_key"),
    col("sla_time_key"),
    col("close_date_key"),
    col("close_time_key"),
    col("request_dtl_key"),
    col("source_key"),
    col("location_key"),
    col("closure_reason"),
    col("case_status"), 
    col("on_time"),
    col("db_created_datetime"),
    col("db_modified_datetime"),
    col("created_by"),
    col("modified_by"),
    col("process_id")
)

for y in range(2015, 2025):
    print(f"📌 Processing Year: {y} ({df_fact.filter(year('open_date') == y).count()} records)...")

    # ✅ Filter Data for the Current Year
    df_year = df_fact.filter(year("open_date") == y)

    try:
        df_year.write \
            .format("jdbc") \
            .option("driver", DB_CONFIG["driver"]) \
            .option("url", DB_CONFIG["url"]) \
            .option("dbtable", fact_311_request) \
            .option("user", DB_CONFIG["user"]) \
            .option("password", DB_CONFIG["password"]) \
            .option("batchsize", 5000) \
            .mode("append") \
            .save()

        print(f"✅ Year {y} inserted successfully!")

    except Exception as e:
        print(f"❌ Error inserting Year {y}: {e}")
        break  # Stop if an error occurs

print("🎯 All yearly inserts completed successfully!")


# Stop the Spark session
spark.stop()
print("✅ Spark session closed.")
    