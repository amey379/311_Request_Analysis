import sys
import os

# ✅ Add `scripts/` directory to Python's module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import get_spark_session, DB_CONFIG, DATA_PATH
from pyspark.sql.functions import lit, current_timestamp, col, trim, upper, substring, split, explode, when, regexp_extract
from pyspark.sql.functions import date_format, year, month, dayofmonth, dayofweek, hour, minute, second,sequence
from pyspark.sql.types import DateType, IntegerType, StringType

# ✅ Get Spark Session
spark = get_spark_session("dim_date_time_load")

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
}



start_date = "2015-01-01"  # Start date
end_date = "2040-12-31"    # End date

# Generate a sequence of dates using SQL expression
df_dates = spark.sql(f"""
    SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_sequence
""").selectExpr("explode(date_sequence) as date")


df_date_table = df_dates.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("date_key_str", date_format(col("date"), "yyyyMMdd")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("day_of_month", dayofmonth(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("db_created_datetime", current_timestamp()) \
    .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
    .withColumn("created_by", lit("system")) \
    .withColumn("modified_by", lit("system")) \
    .withColumn("process_id", lit(process_id)) 


try:
    df_date_table.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", DIM_TABLES["dim_date"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("append") \
        .save()
    print("✅ Dim Date Table Loaded!")
except Exception as e:
    print(f"❌ Error loading Dim Date Table: {e}")


start_time = "00:00:00"  # Start time
end_time = "23:59:59"    # End time

# Generate a sequence of times using SQL
df_times = spark.sql(f"""
    SELECT sequence(to_timestamp('{start_time}'), to_timestamp('{end_time}'), interval 1 second) as time_sequence
""").selectExpr("explode(time_sequence) as time")

# Add required columns
df_time_table = df_times.withColumn("time_key", date_format(col("time"), "HHmmss").cast(IntegerType())) \
    .withColumn("time_key_str", date_format(col("time"), "HHmmss").cast(StringType())) \
    .withColumn("hour", hour(col("time"))) \
    .withColumn("minute", minute(col("time"))) \
    .withColumn("second", second(col("time"))) \
    .withColumn("db_created_datetime", current_timestamp()) \
    .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
    .withColumn("created_by", lit("system")) \
    .withColumn("modified_by", lit("system")) \
    .withColumn("process_id", lit(process_id))

try:
    df_time_table.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", DIM_TABLES["dim_time"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("append") \
        .save()
    print("✅ Dim Time Table Loaded!")
except Exception as e:
    print(f"❌ Error loading Dim Time Table: {e}")


# Stop the Spark session
spark.stop()
print("✅ Spark session closed.")
    