import sys
import os

# ✅ Add `scripts/` directory to Python's module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import get_spark_session, DB_CONFIG, DATA_PATH
from pyspark.sql.functions import lit, current_timestamp, col, trim, split, when, coalesce,row_number
from pyspark.sql.types import DateType, IntegerType, StringType,DecimalType
from pyspark.sql.window import Window

# ✅ Get Spark Session
spark = get_spark_session("dim_source_load")

process_id = os.getpid()

# ✅ Use Centralized Database Config
driver = DB_CONFIG["driver"]
url = DB_CONFIG["url"]
user = DB_CONFIG["user"]
password = DB_CONFIG["password"]

#write_table = "boston_311_stage"

table = "boston_311_stage"
readtable = "boston_311_stage"


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


df_source = df_stage.select("source").distinct().filter(col("source").isNotNull())

# Assign Unique `source_key` using `row_number()`
window_spec = Window.orderBy("source")
df_source = df_source.withColumn("source_key", row_number().over(window_spec))

# Add Metadata Columns
df_source = df_source.withColumn("db_created_datetime", current_timestamp()) \
                     .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
                     .withColumn("created_by", lit("system")) \
                     .withColumn("modified_by", lit(None).cast("string")) \
                     .withColumn("process_id", lit(process_id))

# Write to MySQL (Optimized with batch inserts)
try:
    df_source.write \
        .format("jdbc") \
           .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", DIM_TABLES["dim_source"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("batchsize", 1000) \
        .mode("append") \
        .save()

    print("✅ Dim Location Table Loaded!")
except Exception as e:
    print(f"❌ Error loading Dim Location Table: {e}")


# Stop the Spark session
spark.stop()
print("✅ Spark session closed.")
    